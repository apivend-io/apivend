// Package kubewatch follows one key of one ConfigMap or Secret through the
// Kubernetes API.
//
// A ConfigMap mounted as a volume is refreshed by the kubelet on its own
// schedule (up to about a minute, at a different moment in every pod), so
// replicas of the filter disagree about prices after a change. Watching the
// object directly brings every replica up to date within the API server's
// watch latency.
package kubewatch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// ConfigMap watches Key of the ConfigMap Namespace/Name.
type ConfigMap struct {
	Client    kubernetes.Interface
	Namespace string
	Name      string
	Key       string
	// OnChange receives the key's value whenever it differs from the last
	// call; ok is false when the ConfigMap or the key does not exist. Calls
	// are sequential and always carry the latest state, so a burst of
	// updates may be delivered as one call.
	OnChange func(value string, ok bool)
}

// Start begins watching. It returns once the first list from the API server
// has been delivered to OnChange (exactly one call), or with an error when
// ctx ends first. Watching continues, reconnecting as needed, until ctx ends.
func (w *ConfigMap) Start(ctx context.Context) error {
	f := oneObject(w.Client, w.Namespace, w.Name)
	cms := f.Core().V1().ConfigMaps()
	lister := cms.Lister().ConfigMaps(w.Namespace)
	return follow(ctx, f, cms.Informer(), func() (string, bool, error) {
		cm, err := lister.Get(w.Name)
		if err != nil {
			return "", false, err
		}
		v, ok := cm.Data[w.Key]
		return v, ok, nil
	}, w.OnChange)
}

// Secret watches Key of the Secret Namespace/Name, like ConfigMap. The
// settler reads its providers (which hold facilitator credentials) this way.
type Secret struct {
	Client    kubernetes.Interface
	Namespace string
	Name      string
	Key       string
	OnChange  func(value string, ok bool)
}

// Start behaves as ConfigMap.Start.
func (w *Secret) Start(ctx context.Context) error {
	f := oneObject(w.Client, w.Namespace, w.Name)
	secrets := f.Core().V1().Secrets()
	lister := secrets.Lister().Secrets(w.Namespace)
	return follow(ctx, f, secrets.Informer(), func() (string, bool, error) {
		sec, err := lister.Get(w.Name)
		if err != nil {
			return "", false, err
		}
		v, ok := sec.Data[w.Key]
		return string(v), ok, nil
	}, w.OnChange)
}

// oneObject is an informer factory for one named object, which also lets
// RBAC restrict the reader to that name (resourceNames applies to list and
// watch with this field selector).
func oneObject(c kubernetes.Interface, namespace, name string) informers.SharedInformerFactory {
	return informers.NewSharedInformerFactoryWithOptions(c, 0,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("metadata.name", name).String()
		}))
}

func follow(ctx context.Context, f informers.SharedInformerFactory, inf cache.SharedIndexInformer,
	read func() (string, bool, error), onChange func(string, bool)) error {
	// Events only signal; the current value is always read from the cache,
	// so a late event can never roll the value back.
	kick := make(chan struct{}, 1)
	signal := func() {
		select {
		case kick <- struct{}{}:
		default:
		}
	}
	if _, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { signal() },
		UpdateFunc: func(any, any) { signal() },
		DeleteFunc: func(any) { signal() },
	}); err != nil {
		return err
	}

	var (
		mu      sync.Mutex
		started bool
		last    string
		lastOK  bool
	)
	deliver := func() {
		mu.Lock()
		defer mu.Unlock()
		value, ok, err := read()
		if err != nil && !apierrors.IsNotFound(err) {
			return // the lister only returns NotFound; be safe anyway
		}
		if started && ok == lastOK && value == last {
			return
		}
		started, last, lastOK = true, value, ok
		onChange(value, ok)
	}

	f.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return errors.New("watch did not sync")
	}
	deliver()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-kick:
				deliver()
			}
		}
	}()
	return nil
}

// Client returns a Kubernetes client from the usual places: $KUBECONFIG or
// ~/.kube/config when present, otherwise the pod's service account.
func Client() (kubernetes.Interface, error) {
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("kubernetes client config: %w", err)
	}
	return kubernetes.NewForConfig(cfg)
}

// PodNamespace is $POD_NAMESPACE, or the namespace of the pod's service
// account; empty outside a pod.
func PodNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
