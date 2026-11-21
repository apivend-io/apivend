package kubewatch

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type change struct {
	value string
	ok    bool
}

func watch(t *testing.T, objs ...*corev1.ConfigMap) (*fake.Clientset, <-chan change) {
	t.Helper()
	cs := fake.NewClientset()
	for _, o := range objs {
		if _, err := cs.CoreV1().ConfigMaps(o.Namespace).Create(context.Background(), o, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	ch := make(chan change, 16)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w := &ConfigMap{Client: cs, Namespace: "ns", Name: "routes", Key: "routes.yaml",
		OnChange: func(v string, ok bool) { ch <- change{v, ok} }}
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return cs, ch
}

func cm(ns, name, value string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Data:       map[string]string{"routes.yaml": value},
	}
}

func next(t *testing.T, ch <-chan change) change {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("no change delivered")
		return change{}
	}
}

func quiet(t *testing.T, ch <-chan change) {
	t.Helper()
	select {
	case c := <-ch:
		t.Fatalf("unexpected change %+v", c)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestStartDeliversCurrentValueBeforeReturning(t *testing.T) {
	_, ch := watch(t, cm("ns", "routes", "v1"))
	select {
	case c := <-ch:
		if c != (change{"v1", true}) {
			t.Fatalf("got %+v", c)
		}
	default:
		t.Fatal("Start returned before delivering the current value")
	}
	quiet(t, ch)
}

func TestMissingThenCreatedUpdatedDeleted(t *testing.T) {
	cs, ch := watch(t)
	if c := next(t, ch); c != (change{"", false}) {
		t.Fatalf("missing ConfigMap: got %+v", c)
	}
	ctx := context.Background()
	api := cs.CoreV1().ConfigMaps("ns")

	if _, err := api.Create(ctx, cm("ns", "routes", "v1"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if c := next(t, ch); c != (change{"v1", true}) {
		t.Fatalf("created: got %+v", c)
	}

	if _, err := api.Update(ctx, cm("ns", "routes", "v2"), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if c := next(t, ch); c != (change{"v2", true}) {
		t.Fatalf("updated: got %+v", c)
	}

	// A label change leaves the value alone: no call.
	u := cm("ns", "routes", "v2")
	u.Labels = map[string]string{"x": "y"}
	if _, err := api.Update(ctx, u, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	quiet(t, ch)

	if err := api.Delete(ctx, "routes", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if c := next(t, ch); c != (change{"", false}) {
		t.Fatalf("deleted: got %+v", c)
	}
}

func TestOtherObjectsAreIgnored(t *testing.T) {
	// The fake clientset ignores field selectors, so other ConfigMaps do
	// reach the informer here; they must not change the value.
	cs, ch := watch(t, cm("ns", "routes", "v1"))
	next(t, ch)
	ctx := context.Background()
	if _, err := cs.CoreV1().ConfigMaps("ns").Create(ctx, cm("ns", "other", "x"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.CoreV1().ConfigMaps("elsewhere").Create(ctx, cm("elsewhere", "routes", "x"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	quiet(t, ch)
}

func TestMissingKey(t *testing.T) {
	_, ch := watch(t, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "routes"}})
	if c := next(t, ch); c != (change{"", false}) {
		t.Fatalf("got %+v", c)
	}
}

func TestStartFailsWhenContextEndsBeforeSync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &ConfigMap{Client: fake.NewClientset(), Namespace: "ns", Name: "routes", Key: "k",
		OnChange: func(string, bool) { t.Error("OnChange called") }}
	if err := w.Start(ctx); err == nil {
		t.Fatal("Start succeeded with a cancelled context")
	}
}

func TestSecretFollowsChanges(t *testing.T) {
	cs := fake.NewClientset()
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "providers"}, Data: map[string][]byte{"providers.yaml": []byte("v1")}}
	other := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "other"}, Data: map[string][]byte{"providers.yaml": []byte("nope")}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for _, s := range []*corev1.Secret{sec, other} {
		if _, err := cs.CoreV1().Secrets("ns").Create(ctx, s, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	ch := make(chan change, 16)
	w := &Secret{Client: cs, Namespace: "ns", Name: "providers", Key: "providers.yaml", OnChange: func(v string, ok bool) { ch <- change{v, ok} }}
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if c := next(t, ch); c != (change{"v1", true}) {
		t.Fatalf("first: %+v", c)
	}
	sec.Data["providers.yaml"] = []byte("v2")
	if _, err := cs.CoreV1().Secrets("ns").Update(ctx, sec, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if c := next(t, ch); c != (change{"v2", true}) {
		t.Fatalf("update: %+v", c)
	}
	other.Data["providers.yaml"] = []byte("changed")
	cs.CoreV1().Secrets("ns").Update(ctx, other, metav1.UpdateOptions{})
	quiet(t, ch)
	cs.CoreV1().Secrets("ns").Delete(ctx, "providers", metav1.DeleteOptions{})
	if c := next(t, ch); c.ok {
		t.Fatalf("delete: %+v", c)
	}
}
