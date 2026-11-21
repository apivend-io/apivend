// x402-controller reconciles PaymentPolicy and SettlementProvider objects into
// the price table, the settler's providers and the gateway configuration.
package main

import (
	"flag"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	x402v1 "apivend.io/apivend/api/v1alpha1"
	"apivend.io/apivend/internal/controller"
	"apivend.io/apivend/internal/pricing"
)

func main() {
	var (
		namespace     = flag.String("namespace", os.Getenv("POD_NAMESPACE"), "namespace of the data plane; generated config is written here (env POD_NAMESPACE)")
		routesCM      = flag.String("routes-configmap", "x402-routes", "ConfigMap to write the price table to")
		providersSec  = flag.String("providers-secret", "x402-providers", "Secret to write settlement providers to")
		filterService = flag.String("filter-service", "x402-filter", "Service of the ext_proc filter")
		filterPort    = flag.Int64("filter-port", 9002, "gRPC port of the filter Service")
		msgTimeout    = flag.Duration("message-timeout", controller.DefaultMessageTimeout, "ext_proc message timeout on every priced route; must exceed the filter's -chain-read-timeout")
		metricsAddr   = flag.String("metrics", ":9104", "listen address for controller metrics")
		probeAddr     = flag.String("health", ":9105", "listen address for health probes")
		leaderElect   = flag.Bool("leader-elect", true, "run only one active controller")
		batchAuth     = flag.String("batch-receiver-authorizer", "", "address of our batch-settlement claim key (the settler holds the key); empty: no route may offer batch-settlement")
		batchDelay    = flag.Uint64("batch-withdraw-delay", 86400, "batch-settlement withdrawal delay in seconds (900 to 2592000): how long a payer waits to withdraw, and our window to claim")
	)
	flag.Parse()
	ctrl.SetLogger(zap.New())
	log := ctrl.Log.WithName("setup")
	if *namespace == "" {
		log.Info("-namespace (or POD_NAMESPACE) is required")
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = x402v1.AddToScheme(scheme)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: *metricsAddr},
		HealthProbeBindAddress:  *probeAddr,
		LeaderElection:          *leaderElect,
		LeaderElectionID:        "x402-controller",
		LeaderElectionNamespace: *namespace,
		// Secrets and ConfigMaps are only ever read in our own namespace;
		// do not cache (or need RBAC for) the rest of the cluster's.
		Cache: cache.Options{ByObject: map[client.Object]cache.ByObject{
			&corev1.Secret{}:    {Namespaces: map[string]cache.Config{*namespace: {}}},
			&corev1.ConfigMap{}: {Namespaces: map[string]cache.Config{*namespace: {}}},
		}},
	})
	if err != nil {
		log.Error(err, "create manager")
		os.Exit(1)
	}
	r := &controller.Reconciler{
		Client:          mgr.GetClient(),
		Namespace:       *namespace,
		RoutesConfigMap: *routesCM,
		ProvidersSecret: *providersSec,
		FilterService:   *filterService,
		FilterPort:      *filterPort,
		MessageTimeout:  *msgTimeout,
	}
	if *batchAuth != "" {
		r.Batch = &pricing.BatchConfig{ReceiverAuthorizer: *batchAuth, WithdrawDelay: *batchDelay}
	}
	if err := r.SetupWithManager(mgr); err != nil {
		log.Error(err, "set up controller")
		os.Exit(1)
	}
	_ = mgr.AddHealthzCheck("healthz", healthz.Ping)
	_ = mgr.AddReadyzCheck("readyz", healthz.Ping)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "run manager")
		os.Exit(1)
	}
}
