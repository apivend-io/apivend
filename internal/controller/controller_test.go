package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"sigs.k8s.io/yaml"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	x402v1 "apivend.io/apivend/api/v1alpha1"
	"apivend.io/apivend/internal/pricing"
	"apivend.io/apivend/internal/settle"
)

const (
	systemNS = "x402-system"
	payTo    = "0x209693Bc6afc0C5328bA36FaF03C514EF312287C"
)

func httpRoute(ns, name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(httpRouteGVK)
	u.SetNamespace(ns)
	u.SetName(name)
	return u
}

func policy(ns, name, target string, created time.Time) *x402v1.PaymentPolicy {
	return &x402v1.PaymentPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(ns + "-" + name), CreationTimestamp: metav1.NewTime(created)},
		Spec: x402v1.PaymentPolicySpec{
			TargetRef: x402v1.TargetRef{Group: "gateway.networking.k8s.io", Kind: "HTTPRoute", Name: target},
			Price:     x402v1.Price{Amount: "0.01", Currency: "USD"},
			Accepts:   []x402v1.Accept{{Network: "base", Asset: "USDC", PayTo: payTo}},
		},
	}
}

func provider(name string) *x402v1.SettlementProvider {
	return &x402v1.SettlementProvider{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: x402v1.SettlementProviderSpec{Backends: []x402v1.SettlementBackend{{
			Type: "facilitator", URL: "https://facilitator.example", Priority: 1,
			CredentialsRef: &x402v1.SecretKeyRef{Name: "fac-key"},
		}}},
	}
}

var t0 = time.Unix(1_800_000_000, 0)

type world struct {
	t *testing.T
	c client.Client
	r *Reconciler
}

func newWorld(t *testing.T, objs ...client.Object) *world {
	scheme := runtime.NewScheme()
	clientgoscheme.AddToScheme(scheme)
	x402v1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&x402v1.PaymentPolicy{}, &x402v1.SettlementProvider{}).Build()
	return &world{t: t, c: c, r: &Reconciler{Client: c, Namespace: systemNS, RoutesConfigMap: "x402-routes",
		ProvidersSecret: "x402-providers", FilterService: "x402-filter", FilterPort: 9002}}
}

func (w *world) reconcile() {
	w.t.Helper()
	if _, err := w.r.Reconcile(context.Background(), syncRequest); err != nil {
		w.t.Fatal(err)
	}
}

func (w *world) condition(ns, name, typ string) *metav1.Condition {
	w.t.Helper()
	var p x402v1.PaymentPolicy
	if err := w.c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &p); err != nil {
		w.t.Fatal(err)
	}
	return meta.FindStatusCondition(p.Status.Conditions, typ)
}

func (w *world) accepted(ns, name string, want bool, reason string) {
	w.t.Helper()
	c := w.condition(ns, name, x402v1.ConditionAccepted)
	if c == nil || (c.Status == metav1.ConditionTrue) != want || c.Reason != reason {
		w.t.Fatalf("%s/%s Accepted = %+v; want %v/%s", ns, name, c, want, reason)
	}
}

// table loads the generated ConfigMap with the filter's own parser.
func (w *world) table() *pricing.Table {
	w.t.Helper()
	var cm corev1.ConfigMap
	if err := w.c.Get(context.Background(), types.NamespacedName{Namespace: systemNS, Name: "x402-routes"}, &cm); err != nil {
		w.t.Fatal(err)
	}
	path := w.t.TempDir() + "/routes.yaml"
	writeFile(w.t, path, cm.Data[RoutesKey])
	table, err := pricing.Load(path)
	if err != nil {
		w.t.Fatalf("filter cannot load the generated price table: %v\n%s", err, cm.Data[RoutesKey])
	}
	return table
}

func (w *world) extensionPolicy(ns, name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(extensionGVK)
	if err := w.c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, u); err != nil {
		return nil
	}
	return u
}

func base() []client.Object {
	return []client.Object{
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: systemNS, Name: "fac-key"}, Data: map[string][]byte{"token": []byte("s3cret\n")}},
		provider("default"),
		httpRoute("prod", "search"),
	}
}

// The controller's default per-route cap reaches async routes that set
// none, never below their price; sync routes and explicit caps keep theirs.
func TestDefaultMaxUnsettledPerRoute(t *testing.T) {
	cheap := policy("prod", "search-price", "search", t0)
	dear := policy("prod", "dear-price", "dear", t0)
	dear.Spec.Price.Amount = "20"
	dear.Spec.Settlement = &x402v1.Settlement{MaxUnsettledPerPayer: "50"}
	sync := policy("prod", "sync-price", "sync", t0)
	sync.Spec.Settlement = &x402v1.Settlement{Mode: "sync"}
	own := policy("prod", "own-price", "own", t0)
	own.Spec.Settlement = &x402v1.Settlement{MaxUnsettledPerRoute: "2"}
	w := newWorld(t, append(base(), httpRoute("prod", "dear"), httpRoute("prod", "sync"), httpRoute("prod", "own"), cheap, dear, sync, own)...)
	w.r.DefaultMaxUnsettledPerRoute = "5.00"
	w.reconcile()
	table := w.table()
	for name, want := range map[string]string{"prod/search-price": "5000000", "prod/dear-price": "20000000", "prod/own-price": "2000000", "prod/sync-price": ""} {
		r := table.Route(name)
		if r == nil {
			t.Fatalf("%s not in the table", name)
		}
		got := ""
		if c := r.Offers[0].MaxUnsettledPerRoute; c != nil {
			got = c.String()
		}
		if got != want {
			t.Errorf("%s: cap %q, want %q", name, got, want)
		}
	}
}

func TestPolicyIsProgrammed(t *testing.T) {
	p := policy("prod", "search-price", "search", t0)
	p.Spec.Description = "Search API"
	p.Spec.Settlement = &x402v1.Settlement{MaxUnsettledPerPayer: "0.50"}
	p.Spec.Risk = &x402v1.Risk{Freshness: &metav1.Duration{Duration: 2 * time.Second}, OnStateUnavailable: "allow"}
	w := newWorld(t, append(base(), p)...)
	w.reconcile()
	w.reconcile() // idempotent

	w.accepted("prod", "search-price", true, "Accepted")
	if c := w.condition("prod", "search-price", x402v1.ConditionProgrammed); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("Programmed = %+v", c)
	}

	// The price table prices exactly that HTTPRoute, by Envoy route name.
	table := w.table()
	route := table.Match("any.host", "/whatever", "httproute/prod/search/rule/0/match/0")
	if route == nil || route.Name != "prod/search-price" || route.Description != "Search API" {
		t.Fatalf("route = %+v", route)
	}
	if o := route.Offers[0]; o.Wire.Amount != "10000" || o.Wire.Network != "eip155:8453" || o.MaxUnsettled.String() != "500000" {
		t.Fatalf("offer = %+v", o)
	}
	if route.Freshness != 2*time.Second || !route.AllowOnUnavailable || route.Provider != "default" {
		t.Fatalf("risk not carried over: %+v", route)
	}
	if table.Match("any.host", "/whatever", "httproute/prod/other/rule/0/match/0") != nil {
		t.Fatal("another HTTPRoute is priced")
	}

	// The gateway extension policy targets the route and fails closed.
	eep := w.extensionPolicy("prod", "x402-search-price")
	if eep == nil {
		t.Fatal("no EnvoyExtensionPolicy")
	}
	target, _, _ := unstructured.NestedSlice(eep.Object, "spec", "targetRefs")
	extProc, _, _ := unstructured.NestedSlice(eep.Object, "spec", "extProc")
	ep := extProc[0].(map[string]any)
	backend := ep["backendRefs"].([]any)[0].(map[string]any)
	attrs, _, _ := unstructured.NestedSlice(ep, "processingMode", "request", "attributes")
	_, hasResponse, _ := unstructured.NestedMap(ep, "processingMode", "response")
	if target[0].(map[string]any)["name"] != "search" || backend["name"] != "x402-filter" || backend["namespace"] != systemNS ||
		backend["port"] != int64(9002) || ep["failOpen"] != false || len(attrs) != 1 || attrs[0] != "xds.route_name" || !hasResponse {
		t.Fatalf("EnvoyExtensionPolicy spec = %v", eep.Object["spec"])
	}
	// Envoy's 200ms default is shorter than a cold chain read over a real
	// network (measured 105-155ms to sepolia.base.org alone): a timeout is a 504.
	if got := ep["messageTimeout"]; got != DefaultMessageTimeout.String() {
		t.Fatalf("async messageTimeout = %v, want %v", got, DefaultMessageTimeout)
	}
	if owners := eep.GetOwnerReferences(); len(owners) != 1 || owners[0].Kind != "PaymentPolicy" || owners[0].Name != "search-price" {
		t.Fatalf("owner refs = %+v (deleting the policy must remove the extension policy)", owners)
	}

	// And it may reference the filter Service across namespaces.
	grant := &unstructured.Unstructured{}
	grant.SetGroupVersionKind(referenceGrantGVK)
	if err := w.c.Get(context.Background(), types.NamespacedName{Namespace: systemNS, Name: "x402-filter-from-prod"}, grant); err != nil {
		t.Fatalf("no ReferenceGrant: %v", err)
	}
}

func TestProvidersSecret(t *testing.T) {
	w := newWorld(t, append(base(), provider("broken"))...)
	// "broken" references a secret that does not exist.
	var broken x402v1.SettlementProvider
	w.c.Get(context.Background(), types.NamespacedName{Name: "broken"}, &broken)
	broken.Spec.Backends[0].CredentialsRef.Name = "missing"
	w.c.Update(context.Background(), &broken)
	w.reconcile()

	var sec corev1.Secret
	if err := w.c.Get(context.Background(), types.NamespacedName{Namespace: systemNS, Name: "x402-providers"}, &sec); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/providers.yaml"
	writeFile(t, path, string(sec.Data[ProvidersKey]))
	backends, err := settle.LoadProviders(path, time.Second)
	if err != nil {
		t.Fatalf("settler cannot load the generated providers: %v", err)
	}
	if len(backends) != 1 || len(backends["default"]) != 1 {
		t.Fatalf("providers = %v", backends)
	}
	if !strings.Contains(string(sec.Data[ProvidersKey]), "s3cret") || strings.Contains(string(sec.Data[ProvidersKey]), "s3cret\\n") {
		t.Fatalf("token not resolved cleanly: %s", sec.Data[ProvidersKey])
	}

	w.c.Get(context.Background(), types.NamespacedName{Name: "broken"}, &broken)
	c := meta.FindStatusCondition(broken.Status.Conditions, x402v1.ConditionAccepted)
	if c == nil || c.Status != metav1.ConditionFalse || !strings.Contains(c.Message, "missing") {
		t.Fatalf("broken provider condition = %+v", c)
	}
}

// A CDP backend: the key file in the Secret reaches the settler, which can
// load it; a malformed key is reported on the provider, not at settle time.
func TestProvidersCDPAuth(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	keyFile, _ := json.Marshal(map[string]string{"id": "3f1c2a9e-0d4b-4c1e-9a7f-1b2c3d4e5f60", "privateKey": base64.StdEncoding.EncodeToString(priv)})
	cdp := &x402v1.SettlementProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "cdp"},
		Spec: x402v1.SettlementProviderSpec{Backends: []x402v1.SettlementBackend{{
			Type: "facilitator", URL: "https://api.cdp.coinbase.com/platform/v2/x402", Auth: "cdp",
			CredentialsRef: &x402v1.SecretKeyRef{Name: "cdp-key", Key: "key.json"},
		}}},
	}
	bad := cdp.DeepCopy()
	bad.Name = "cdp-bad"
	bad.Spec.Backends[0].CredentialsRef = &x402v1.SecretKeyRef{Name: "cdp-bad-key", Key: "key.json"}
	secrets := []client.Object{
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: systemNS, Name: "cdp-key"}, Data: map[string][]byte{"key.json": keyFile}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: systemNS, Name: "cdp-bad-key"}, Data: map[string][]byte{"key.json": []byte(`{"id":"x","privateKey":"bm9wZQ=="}`)}},
	}
	w := newWorld(t, append(append(base(), cdp, bad), secrets...)...)
	w.reconcile()

	var sec corev1.Secret
	if err := w.c.Get(context.Background(), types.NamespacedName{Namespace: systemNS, Name: "x402-providers"}, &sec); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/providers.yaml"
	writeFile(t, path, string(sec.Data[ProvidersKey]))
	backends, err := settle.LoadProviders(path, time.Second)
	if err != nil {
		t.Fatalf("settler cannot load the generated providers: %v", err)
	}
	if len(backends["cdp"]) != 1 {
		t.Fatalf("cdp provider missing: %v", backends)
	}
	if _, ok := backends["cdp-bad"]; ok {
		t.Fatal("a provider with a malformed CDP key was rendered")
	}
	var got x402v1.SettlementProvider
	w.c.Get(context.Background(), types.NamespacedName{Name: "cdp-bad"}, &got)
	c := meta.FindStatusCondition(got.Status.Conditions, x402v1.ConditionAccepted)
	if c == nil || c.Status != metav1.ConditionFalse || !strings.Contains(c.Message, "cdp key") {
		t.Fatalf("malformed key condition = %+v", c)
	}
	if strings.Contains(c.Message, "bm9wZQ") {
		t.Fatal("the condition message leaks the key")
	}
}

func TestRejections(t *testing.T) {
	invalid := policy("prod", "invalid", "search", t0)
	invalid.Spec.Accepts[0].Network = "dogechain"
	orphan := policy("prod", "orphan", "does-not-exist", t0)
	noProvider := policy("prod", "no-provider", "search", t0)
	noProvider.Spec.Settlement = &x402v1.Settlement{ProviderRef: &x402v1.ProviderRef{Name: "nope"}}
	tooTight := policy("prod", "too-tight", "search", t0)
	tooTight.Spec.Settlement = &x402v1.Settlement{MaxUnsettledPerPayer: "0.001"} // below the price

	w := newWorld(t, append(base(), invalid, orphan, noProvider, tooTight)...)
	w.reconcile()

	w.accepted("prod", "invalid", false, "Invalid")
	w.accepted("prod", "orphan", false, "TargetNotFound")
	w.accepted("prod", "no-provider", false, "ProviderNotFound")
	w.accepted("prod", "too-tight", false, "Invalid")
	if n := w.table().Len(); n != 0 {
		t.Fatalf("%d rejected policies reached the price table", n)
	}
	// A rejected policy's route is refused, not served free: the filter
	// stays attached and the table lists the route.
	if !w.table().Refused("httproute/prod/search/rule/0/match/0") {
		t.Fatal("route of a rejected policy is not refused")
	}
	if w.extensionPolicy("prod", "x402-invalid") == nil {
		t.Fatal("rejected policy's route lost the filter")
	}
	if w.table().Refused("httproute/prod/searching/rule/0/match/0") {
		t.Fatal("refusal reaches a route that only shares a name prefix")
	}
	// Fixing it prices the route and lifts the refusal.
	fixed := policy("prod", "invalid", "search", t0)
	cur := &x402v1.PaymentPolicy{}
	if err := w.c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "invalid"}, cur); err != nil {
		t.Fatal(err)
	}
	cur.Spec = fixed.Spec
	for _, n := range []string{"no-provider", "too-tight", "orphan"} {
		o := &x402v1.PaymentPolicy{}
		w.c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: n}, o)
		w.c.Delete(context.Background(), o)
	}
	if err := w.c.Update(context.Background(), cur); err != nil {
		t.Fatal(err)
	}
	w.reconcile()
	if w.table().Refused("httproute/prod/search/rule/0/match/0") || w.table().Match("h", "/", "httproute/prod/search/rule/0/match/0") == nil {
		t.Fatal("fixed policy: route still refused or not priced")
	}
}

// A policy whose target is not an HTTPRoute never refuses an HTTPRoute that
// happens to share its name.
func TestRejectedWrongKindRefusesNothing(t *testing.T) {
	p := policy("prod", "wrong-kind", "search", t0)
	p.Spec.TargetRef.Kind = "Service"
	w := newWorld(t, append(base(), p)...)
	w.reconcile()
	w.accepted("prod", "wrong-kind", false, "Invalid")
	if w.table().Refused("httproute/prod/search/rule/0/match/0") || w.extensionPolicy("prod", "x402-wrong-kind") != nil {
		t.Fatal("a non-HTTPRoute policy refused an HTTPRoute")
	}
}

func TestOldestPolicyWinsARoute(t *testing.T) {
	first := policy("prod", "zz-first", "search", t0)
	second := policy("prod", "aa-second", "search", t0.Add(time.Minute))
	second.Spec.Price.Amount = "5.00"
	second.Spec.Settlement = &x402v1.Settlement{MaxUnsettledPerPayer: "10"}
	w := newWorld(t, append(base(), first, second)...)
	w.reconcile()

	w.accepted("prod", "zz-first", true, "Accepted")
	w.accepted("prod", "aa-second", false, "Conflicted")
	if w.table().Refused("httproute/prod/search/rule/0/match/0") {
		t.Fatal("a conflicting policy refused a route another policy prices")
	}
	if o := w.table().Match("h", "/", "httproute/prod/search/rule/0/match/0").Offers[0]; o.Wire.Amount != "10000" {
		t.Fatalf("the newer policy repriced the route: %s", o.Wire.Amount)
	}
}

func TestDeletingPolicyFreesTheRoute(t *testing.T) {
	p := policy("prod", "search-price", "search", t0)
	w := newWorld(t, append(base(), p)...)
	w.reconcile()
	if w.table().Len() != 1 || w.extensionPolicy("prod", "x402-search-price") == nil {
		t.Fatal("setup: policy not programmed")
	}

	if err := w.c.Delete(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	w.reconcile()
	if n := w.table().Len(); n != 0 {
		t.Fatalf("price table still has %d routes", n)
	}
	// The fake client has no garbage collector; the controller prunes too.
	if w.extensionPolicy("prod", "x402-search-price") != nil {
		t.Fatal("EnvoyExtensionPolicy outlived its PaymentPolicy")
	}
	grant := &unstructured.Unstructured{}
	grant.SetGroupVersionKind(referenceGrantGVK)
	if err := w.c.Get(context.Background(), types.NamespacedName{Namespace: systemNS, Name: "x402-filter-from-prod"}, grant); err == nil {
		t.Fatal("ReferenceGrant outlived the last policy in its namespace")
	}
}

// The documented example must be accepted by the same validation users hit.
func TestExampleManifestIsValid(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/example/k8s/payment-policy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	docs := strings.Split(string(raw), "\n---\n")
	var sp x402v1.SettlementProvider
	var p x402v1.PaymentPolicy
	if err := yaml.UnmarshalStrict([]byte(docs[0]), &sp); err != nil {
		t.Fatal(err)
	}
	if err := yaml.UnmarshalStrict([]byte(docs[1]), &p); err != nil {
		t.Fatal(err)
	}
	w := newWorld(t, &sp, &p, httpRoute(p.Namespace, p.Spec.TargetRef.Name))
	w.reconcile()
	w.accepted(p.Namespace, p.Name, true, "Accepted")
	if w.table().Match("h", "/", "httproute/prod/search/rule/0/match/0") == nil {
		t.Fatal("example policy did not price its route")
	}
}

func TestSyncPolicyRaisesMessageTimeout(t *testing.T) {
	p := policy("prod", "reports", "search", t0)
	p.Spec.Settlement = &x402v1.Settlement{Mode: "sync"}
	w := newWorld(t, append(base(), p)...)
	w.reconcile()
	w.accepted("prod", "reports", true, "Accepted")

	if r := w.table().Match("h", "/", "httproute/prod/search/rule/0/match/0"); !r.Sync || r.SyncTimeout != pricing.DefaultSyncTimeout {
		t.Fatalf("route is not sync: %+v", r)
	}
	extProc, _, _ := unstructured.NestedSlice(w.extensionPolicy("prod", "x402-reports").Object, "spec", "extProc")
	if got := extProc[0].(map[string]any)["messageTimeout"]; got != "12s" {
		t.Fatalf("messageTimeout = %v; Envoy would cut off the settlement wait", got)
	}
}
