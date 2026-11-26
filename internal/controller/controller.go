// Package controller turns PaymentPolicy and SettlementProvider objects into
// what the data plane consumes:
//
//   - a ConfigMap with the filter's price table,
//   - a Secret with the settler's providers,
//   - one Envoy Gateway EnvoyExtensionPolicy per priced HTTPRoute, which
//     switches the ext_proc filter on for that route only,
//   - ReferenceGrants so those policies may point at the filter Service.
//
// Every reconcile renders the whole state from scratch. Policies are few and
// the render is cheap; that keeps conflict handling and deletion trivial.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	x402v1 "apivend.io/apivend/api/v1alpha1"
	"apivend.io/apivend/internal/pricing"
	"apivend.io/apivend/internal/settle"
	"apivend.io/apivend/internal/x402"
)

var (
	httpRouteGVK      = schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}
	referenceGrantGVK = schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1beta1", Kind: "ReferenceGrant"}
	extensionGVK      = schema.GroupVersionKind{Group: "gateway.envoyproxy.io", Version: "v1alpha1", Kind: "EnvoyExtensionPolicy"}
)

const (
	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "x402-controller"

	// RoutesKey and ProvidersKey are the file names inside the generated
	// ConfigMap and Secret.
	RoutesKey    = "routes.yaml"
	ProvidersKey = "providers.yaml"
)

// Reconciler renders all x402 configuration.
type Reconciler struct {
	client.Client

	// Namespace is where the data plane runs and generated config is written.
	Namespace string
	// RoutesConfigMap and ProvidersSecret are the generated objects' names.
	RoutesConfigMap string
	ProvidersSecret string
	// FilterService and FilterPort locate the ext_proc gRPC Service.
	FilterService string
	FilterPort    int64
	// MessageTimeout is the ext_proc message timeout set on every priced
	// route. It must cover the filter's work on one message, including the
	// synchronous chain read for an unknown payer (the filter's
	// -chain-read-timeout); Envoy's 200ms default does not, and a timeout is a
	// 504. Sync routes get at least the settlement wait on top. Default 1s.
	MessageTimeout time.Duration
	// Batch holds the batch-settlement channel terms (our receiver
	// authorizer). Nil: policies offering batch-settlement are rejected.
	Batch *pricing.BatchConfig
	// DefaultMaxUnsettledPerRoute applies to policies that set no
	// settlement.maxUnsettledPerRoute ("" : no cap).
	DefaultMaxUnsettledPerRoute string
}

// DefaultMessageTimeout is Reconciler.MessageTimeout when unset.
const DefaultMessageTimeout = time.Second

// syncRequest is the single reconcile key: any change re-renders everything.
var syncRequest = reconcile.Request{NamespacedName: types.NamespacedName{Name: "sync"}}

// SetupWithManager wires the watches.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	all := handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
		return []reconcile.Request{syncRequest}
	})
	ownNamespace := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
		if o.GetNamespace() != r.Namespace {
			return nil
		}
		return []reconcile.Request{syncRequest}
	})
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(httpRouteGVK)
	return ctrl.NewControllerManagedBy(mgr).
		Named("x402").
		Watches(&x402v1.PaymentPolicy{}, all).
		Watches(&x402v1.SettlementProvider{}, all).
		Watches(&corev1.Secret{}, ownNamespace).
		Watches(&corev1.ConfigMap{}, ownNamespace).
		Watches(route, all).
		Complete(r)
}

// Reconcile renders the whole configuration.
func (r *Reconciler) Reconcile(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
	providerNames, err := r.syncProviders(ctx)
	if err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, r.syncPolicies(ctx, providerNames)
}

// ---- SettlementProviders -> Secret

func (r *Reconciler) syncProviders(ctx context.Context) (map[string]bool, error) {
	var list x402v1.SettlementProviderList
	if err := r.List(ctx, &list); err != nil {
		return nil, err
	}
	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Name < list.Items[j].Name })

	var cfg settle.ProvidersConfig
	accepted := map[string]bool{}
	for i := range list.Items {
		sp := &list.Items[i]
		pc, err := r.providerConfig(ctx, sp)
		if err == nil {
			// The settler's own parser has the last word.
			_, err = settle.BuildProviders(settle.ProvidersConfig{Providers: []settle.ProviderConfig{pc}}, 0)
		}
		cond := metav1.Condition{Type: x402v1.ConditionAccepted, Status: metav1.ConditionTrue, Reason: "Accepted", Message: "provider is valid"}
		if err != nil {
			cond.Status, cond.Reason, cond.Message = metav1.ConditionFalse, "Invalid", err.Error()
		} else {
			cfg.Providers = append(cfg.Providers, pc)
			accepted[sp.Name] = true
		}
		if err := r.setProviderCondition(ctx, sp, cond); err != nil {
			return nil, err
		}
	}

	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: r.ProvidersSecret, Namespace: r.Namespace}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		setManaged(secret)
		secret.Data = map[string][]byte{ProvidersKey: raw}
		return nil
	})
	return accepted, err
}

func (r *Reconciler) providerConfig(ctx context.Context, sp *x402v1.SettlementProvider) (settle.ProviderConfig, error) {
	pc := settle.ProviderConfig{Name: sp.Name}
	for i, b := range sp.Spec.Backends {
		bc := settle.BackendConfig{Type: b.Type, URL: b.URL, Priority: int(b.Priority), Auth: b.Auth, MaxFee: b.MaxFee}
		if ref := b.CredentialsRef; ref != nil {
			key := ref.Key
			if key == "" {
				key = "token"
			}
			var s corev1.Secret
			if err := r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: ref.Name}, &s); err != nil {
				if apierrors.IsNotFound(err) {
					return pc, fmt.Errorf("backends[%d]: secret %s/%s not found", i, r.Namespace, ref.Name)
				}
				return pc, err
			}
			token := strings.TrimSpace(string(s.Data[key]))
			if token == "" {
				return pc, fmt.Errorf("backends[%d]: secret %s/%s has no key %q", i, r.Namespace, ref.Name, key)
			}
			bc.Token = token
		}
		pc.Backends = append(pc.Backends, bc)
	}
	return pc, nil
}

// ---- PaymentPolicies -> ConfigMap + EnvoyExtensionPolicies + ReferenceGrants

type renderedPolicy struct {
	policy *x402v1.PaymentPolicy
	route  pricing.RouteConfig
}

func (r *Reconciler) syncPolicies(ctx context.Context, providers map[string]bool) error {
	var list x402v1.PaymentPolicyList
	if err := r.List(ctx, &list); err != nil {
		return err
	}
	// Oldest first, so that when two policies target one route the first
	// keeps it and the result does not flap.
	sort.Slice(list.Items, func(i, j int) bool {
		a, b := &list.Items[i], &list.Items[j]
		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			return a.CreationTimestamp.Before(&b.CreationTimestamp)
		}
		return a.Namespace+"/"+a.Name < b.Namespace+"/"+b.Name
	})

	var accepted []renderedPolicy
	var refused []*x402v1.PaymentPolicy // rejected: their routes answer 503
	claimed := map[string]string{}      // target route -> policy that owns it
	for i := range list.Items {
		p := &list.Items[i]
		rc, reason, err := r.acceptPolicy(ctx, p, providers, claimed)
		if err != nil {
			return err
		}
		if reason != nil {
			if err := r.setPolicyConditions(ctx, p, *reason, metav1.Condition{
				Type: x402v1.ConditionProgrammed, Status: metav1.ConditionFalse, Reason: "NotAccepted",
				Message: "policy is not accepted: its HTTPRoute answers 503 until it is fixed or deleted",
			}); err != nil {
				return err
			}
			// A route its owner meant to price is refused, never served
			// free, unless another policy prices it.
			if ref := p.Spec.TargetRef; reason.Reason != "Conflicted" && ref.Group == httpRouteGVK.Group && ref.Kind == httpRouteGVK.Kind {
				refused = append(refused, p)
			}
			continue
		}
		accepted = append(accepted, renderedPolicy{policy: p, route: rc})
	}

	// 1. The price table.
	cfg := pricing.Config{Batch: r.Batch}
	for _, a := range accepted {
		cfg.Routes = append(cfg.Routes, a.route)
	}
	for _, p := range refused {
		if _, priced := claimed[p.Namespace+"/"+p.Spec.TargetRef.Name]; !priced && validTargetName(p.Spec.TargetRef.Name) {
			cfg.Refuse = append(cfg.Refuse, refusePrefix(p))
		}
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: r.RoutesConfigMap, Namespace: r.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		setManaged(cm)
		cm.Data = map[string]string{RoutesKey: string(raw)}
		return nil
	}); err != nil {
		return err
	}

	// 2. Gateway wiring, then status.
	namespaces := map[string]bool{}
	for _, a := range accepted {
		namespaces[a.policy.Namespace] = true
	}
	for _, p := range refused {
		namespaces[p.Namespace] = true
	}
	if err := r.syncReferenceGrants(ctx, namespaces); err != nil {
		return err
	}
	for _, a := range accepted {
		programmed := metav1.Condition{Type: x402v1.ConditionProgrammed, Status: metav1.ConditionTrue, Reason: "Programmed",
			Message: "price table and gateway extension policy written"}
		if err := r.applyExtensionPolicy(ctx, a.policy); err != nil {
			if !isGatewayMissing(err) {
				return err
			}
			programmed.Status, programmed.Reason = metav1.ConditionFalse, "GatewayNotSupported"
			programmed.Message = "could not create EnvoyExtensionPolicy: is Envoy Gateway installed? (" + err.Error() + ")"
		}
		acceptedCond := metav1.Condition{Type: x402v1.ConditionAccepted, Status: metav1.ConditionTrue, Reason: "Accepted", Message: "policy is valid"}
		if err := r.setPolicyConditions(ctx, a.policy, acceptedCond, programmed); err != nil {
			return err
		}
	}
	// A refused route keeps the filter attached, so the refusal is applied.
	for _, p := range refused {
		if err := r.applyExtensionPolicy(ctx, p); err != nil && !isGatewayMissing(err) {
			return err
		}
	}
	return r.pruneExtensionPolicies(ctx, accepted, refused)
}

func refusePrefix(p *x402v1.PaymentPolicy) string {
	return fmt.Sprintf("httproute/%s/%s/", p.Namespace, p.Spec.TargetRef.Name)
}

// validTargetName: a DNS label (Kubernetes object name), so the prefix
// cannot reach another route's name.
func validTargetName(n string) bool {
	return n != "" && len(n) <= 253 && !strings.ContainsAny(n, "/ ")
}

// acceptPolicy validates one policy. It returns either the route to render or
// the Accepted=False condition explaining why not.
func (r *Reconciler) acceptPolicy(ctx context.Context, p *x402v1.PaymentPolicy, providers map[string]bool, claimed map[string]string) (pricing.RouteConfig, *metav1.Condition, error) {
	reject := func(reason, msg string) (pricing.RouteConfig, *metav1.Condition, error) {
		return pricing.RouteConfig{}, &metav1.Condition{Type: x402v1.ConditionAccepted, Status: metav1.ConditionFalse, Reason: reason, Message: msg}, nil
	}
	if d := p.Spec.Discovery; d != nil {
		if _, err := BazaarFor(d); err != nil {
			return reject("Invalid", "discovery: "+err.Error())
		}
	}
	rc := RouteConfigFor(p)
	// The gateway's default per-route cap, for async routes: a sync route
	// settles before it serves, so its unsettled amount is only what is
	// settling at that moment.
	// A route priced above the default gets its own price as the cap.
	if d := r.DefaultMaxUnsettledPerRoute; d != "" && rc.Settlement.MaxUnsettledPerRoute == "" && rc.Settlement.Mode != "sync" {
		rc.Settlement.MaxUnsettledPerRoute = d
		if price, ok := new(big.Rat).SetString(rc.Price.Amount); ok {
			if cap, ok := new(big.Rat).SetString(d); ok && price.Cmp(cap) > 0 {
				rc.Settlement.MaxUnsettledPerRoute = rc.Price.Amount
			}
		}
	}

	// One parser: exactly the validation the filter will apply.
	if _, err := pricing.Build(pricing.Config{Routes: []pricing.RouteConfig{rc}, Batch: r.Batch}); err != nil {
		return reject("Invalid", err.Error())
	}
	ref := p.Spec.TargetRef
	if ref.Group != httpRouteGVK.Group || ref.Kind != httpRouteGVK.Kind {
		return reject("Invalid", "targetRef must be an HTTPRoute in gateway.networking.k8s.io")
	}
	route := &unstructured.Unstructured{}
	route.SetGroupVersionKind(httpRouteGVK)
	if err := r.Get(ctx, types.NamespacedName{Namespace: p.Namespace, Name: ref.Name}, route); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return reject("TargetNotFound", fmt.Sprintf("HTTPRoute %s/%s not found", p.Namespace, ref.Name))
		}
		return pricing.RouteConfig{}, nil, err
	}
	provider := rc.Settlement.ProviderRef.Name
	if !providers[provider] {
		return reject("ProviderNotFound", fmt.Sprintf("SettlementProvider %q does not exist or is not accepted; served requests could never be settled", provider))
	}
	target := p.Namespace + "/" + ref.Name
	if owner, taken := claimed[target]; taken {
		return reject("Conflicted", fmt.Sprintf("HTTPRoute %s is already priced by PaymentPolicy %s", target, owner))
	}
	claimed[target] = p.Namespace + "/" + p.Name
	return rc, nil, nil
}

// RouteConfigFor translates a PaymentPolicy into the filter's route config.
func RouteConfigFor(p *x402v1.PaymentPolicy) pricing.RouteConfig {
	rc := pricing.RouteConfig{
		// Also the "route" label on metrics and the route in receipts.
		Name:        p.Namespace + "/" + p.Name,
		Description: p.Spec.Description,
		Match:       pricing.MatchConfig{EnvoyRoutePrefix: fmt.Sprintf("httproute/%s/%s/", p.Namespace, p.Spec.TargetRef.Name)},
		Price:       pricing.PriceConfig{Amount: p.Spec.Price.Amount, Currency: p.Spec.Price.Currency},
		Settlement:  pricing.SettlementConfig{ProviderRef: &pricing.ProviderRef{Name: "default"}},
	}
	for _, a := range p.Spec.Accepts {
		ac := pricing.AcceptConfig{Network: a.Network, Asset: a.Asset, PayTo: a.PayTo}
		for _, s := range a.Schemes {
			ac.Schemes = append(ac.Schemes, string(s))
		}
		rc.Accepts = append(rc.Accepts, ac)
	}
	if p.Spec.MaxTimeoutSeconds != nil {
		rc.MaxTimeoutSeconds = int(*p.Spec.MaxTimeoutSeconds)
	}
	if s := p.Spec.Settlement; s != nil {
		rc.Settlement.Mode = s.Mode
		rc.Settlement.MaxUnsettledPerPayer = s.MaxUnsettledPerPayer
		rc.Settlement.MaxUnsettledPerRoute = s.MaxUnsettledPerRoute
		if s.ProviderRef != nil {
			rc.Settlement.ProviderRef = &pricing.ProviderRef{Name: s.ProviderRef.Name}
		}
	}
	if k := p.Spec.Risk; k != nil {
		rc.Risk.Simulate = k.Simulate
		rc.Risk.OnStateUnavailable = k.OnStateUnavailable
		if k.Freshness != nil {
			rc.Risk.Freshness = k.Freshness.Duration.String()
		}
	}
	if d := p.Spec.Discovery; d != nil {
		bazaar, _ := BazaarFor(d) // reconcile has rejected the policy if this fails
		rc.Discovery = &pricing.DiscoveryConfig{Publish: d.Enabled, ServiceName: d.ServiceName, Tags: d.Tags, IconURL: d.IconURL, Bazaar: bazaar}
		for _, q := range d.QueryParams {
			// Names the gateway cannot check are left to the upstream, so
			// a policy that was valid before stays valid.
			if q.Required && pricing.RequirableParam(q.Name) && len(rc.RequiredQuery) < 16 {
				rc.RequiredQuery = append(rc.RequiredQuery, q.Name)
			}
		}
	}
	return rc
}

// BazaarFor builds the bazaar discovery extension ({info, schema}, x402
// specs/extensions/bazaar.md) from a policy's discovery block: info carries
// the example call and response, schema (JSON Schema 2020-12) describes the
// input, so catalogues and agents know which parameters exist and which are
// required. The examples validate against it, as CDP requires.
func BazaarFor(d *x402v1.Discovery) (json.RawMessage, error) {
	method := d.Method
	if method == "" {
		method = "GET"
	}
	input := map[string]any{"type": "http", "method": method}
	inputProps := map[string]any{"type": map[string]any{"const": "http"}, "method": map[string]any{"const": method}}
	inputRequired := []string{"type", "method"}
	if len(d.QueryParams) > 0 {
		values, props, required := map[string]any{}, map[string]any{}, []string{}
		for _, q := range d.QueryParams {
			if _, dup := props[q.Name]; dup {
				return nil, fmt.Errorf("query parameter %q is listed twice", q.Name)
			}
			prop := map[string]any{"type": "string"}
			if q.Description != "" {
				prop["description"] = q.Description
			}
			if q.Example != "" {
				prop["examples"] = []string{q.Example}
				values[q.Name] = q.Example
			} else if q.Required {
				return nil, fmt.Errorf("required query parameter %q needs an example", q.Name)
			}
			props[q.Name] = prop
			if q.Required {
				required = append(required, q.Name)
			}
		}
		input["queryParams"] = values
		qp := map[string]any{"type": "object", "properties": props}
		if len(required) > 0 {
			qp["required"] = required
			inputRequired = append(inputRequired, "queryParams")
		}
		inputProps["queryParams"] = qp
	}
	switch method {
	case "POST", "PUT", "PATCH":
		bodyType := d.BodyType
		if bodyType == "" {
			bodyType = "json"
		}
		body := json.RawMessage(`{}`)
		if d.Body != nil && len(d.Body.Raw) > 0 {
			if !json.Valid(d.Body.Raw) {
				return nil, errors.New("body is not valid JSON")
			}
			body = d.Body.Raw
		}
		input["bodyType"], input["body"] = bodyType, body
		inputProps["bodyType"] = map[string]any{"const": bodyType}
		inputProps["body"] = map[string]any{}
		inputRequired = append(inputRequired, "bodyType", "body")
	default:
		if d.BodyType != "" || d.Body != nil {
			return nil, fmt.Errorf("a %s request has no body", method)
		}
	}
	info := map[string]any{"input": input}
	schemaProps := map[string]any{"input": map[string]any{"type": "object", "properties": inputProps, "required": inputRequired}}
	if o := d.Output; o != nil {
		typ := o.Type
		if typ == "" {
			typ = "json"
		}
		output := map[string]any{"type": typ}
		exampleSchema := map[string]any{}
		if o.Example != nil && len(o.Example.Raw) > 0 {
			if len(o.Example.Raw) > pricing.MaxBazaarBytes {
				return nil, fmt.Errorf("output.example is larger than %d bytes", pricing.MaxBazaarBytes)
			}
			var v any
			if err := json.Unmarshal(o.Example.Raw, &v); err != nil {
				return nil, errors.New("output.example is not valid JSON")
			}
			output["example"] = json.RawMessage(o.Example.Raw)
			// Directories rank a described output higher (CDP counts the
			// output schema towards a listing's quality): the schema is
			// inferred from the example, so every listed route has one.
			if typ == "json" {
				exampleSchema = pricing.SchemaOf(v)
			}
		}
		info["output"] = output
		schemaProps["output"] = map[string]any{"type": "object", "properties": map[string]any{"type": map[string]any{"type": "string"}, "example": exampleSchema}, "required": []string{"type"}}
	}
	ext := map[string]any{
		"info": info,
		"schema": map[string]any{
			"$schema":    "https://json-schema.org/draft/2020-12/schema",
			"type":       "object",
			"properties": schemaProps,
			"required":   []string{"input"},
		},
	}
	b, err := json.Marshal(ext)
	if err != nil || len(b) <= pricing.MaxBazaarBytes || d.Output == nil {
		return b, err
	}
	// The inferred schema is several times the example's size: rather than
	// push the extension over the limit (and the route out of service, as a
	// v0.3 policy with a large example would be), publish it without one.
	schemaProps["output"].(map[string]any)["properties"].(map[string]any)["example"] = map[string]any{}
	return json.Marshal(ext)
}

func extensionPolicyName(p *x402v1.PaymentPolicy) string { return "x402-" + p.Name }

// applyExtensionPolicy switches the ext_proc filter on for the policy's
// HTTPRoute. The object is owned by the PaymentPolicy, so deleting the policy
// removes it and the route is free again.
func (r *Reconciler) applyExtensionPolicy(ctx context.Context, p *x402v1.PaymentPolicy) error {
	eep := &unstructured.Unstructured{}
	eep.SetGroupVersionKind(extensionGVK)
	eep.SetName(extensionPolicyName(p))
	eep.SetNamespace(p.Namespace)
	extProc := map[string]any{
		"backendRefs": []any{map[string]any{
			"name": r.FilterService, "namespace": r.Namespace, "port": r.FilterPort,
		}},
		// A priced route must never be served because the filter was down.
		"failOpen": false,
		"processingMode": map[string]any{
			// Request headers plus the route name the price table matches on.
			"request": map[string]any{"attributes": []any{"xds.route_name"}},
			// Response headers: charge only on 2xx.
			"response": map[string]any{},
			// The filter asks for a paid answer's body when it checks the
			// answer before charging (-answer-check).
			"allowModeOverride": true,
		},
	}
	// Always set: Envoy's 200ms default is shorter than a cold chain read
	// over a real network, and a timed-out message is a 504.
	timeout := r.MessageTimeout
	if timeout <= 0 {
		timeout = DefaultMessageTimeout
	}
	if p.Spec.Settlement != nil && p.Spec.Settlement.Mode == "sync" {
		// The filter holds the response while the payment settles.
		timeout = max(timeout, pricing.DefaultSyncTimeout+2*time.Second)
	}
	for _, a := range p.Spec.Accepts {
		if slices.Contains(a.Schemes, x402v1.Scheme(x402.SchemeBatch)) {
			// A batch-settlement deposit is executed before the request is
			// served, as a sync payment is.
			timeout = max(timeout, pricing.DefaultSyncTimeout+2*time.Second)
		}
	}
	extProc["messageTimeout"] = timeout.String()
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, eep, func() error {
		setManaged(eep)
		if err := controllerutil.SetControllerReference(p, eep, r.Scheme()); err != nil {
			return err
		}
		eep.Object["spec"] = map[string]any{
			"targetRefs": []any{map[string]any{
				"group": httpRouteGVK.Group, "kind": httpRouteGVK.Kind, "name": p.Spec.TargetRef.Name,
			}},
			"extProc": []any{extProc},
		}
		return nil
	})
	return err
}

func (r *Reconciler) pruneExtensionPolicies(ctx context.Context, accepted []renderedPolicy, refused []*x402v1.PaymentPolicy) error {
	keep := map[string]bool{}
	for _, a := range accepted {
		keep[a.policy.Namespace+"/"+extensionPolicyName(a.policy)] = true
	}
	for _, p := range refused {
		keep[p.Namespace+"/"+extensionPolicyName(p)] = true
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(extensionGVK)
	if err := r.List(ctx, list, client.MatchingLabels{managedByLabel: managedByValue}); err != nil {
		if isGatewayMissing(err) {
			return nil
		}
		return err
	}
	for i := range list.Items {
		o := &list.Items[i]
		if !keep[o.GetNamespace()+"/"+o.GetName()] {
			if err := r.Delete(ctx, o); client.IgnoreNotFound(err) != nil {
				return err
			}
		}
	}
	return nil
}

// syncReferenceGrants lets EnvoyExtensionPolicies in the given namespaces
// reference the filter Service in the controller's namespace.
func (r *Reconciler) syncReferenceGrants(ctx context.Context, namespaces map[string]bool) error {
	for ns := range namespaces {
		if ns == r.Namespace {
			continue
		}
		g := &unstructured.Unstructured{}
		g.SetGroupVersionKind(referenceGrantGVK)
		g.SetName("x402-filter-from-" + ns)
		g.SetNamespace(r.Namespace)
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, g, func() error {
			setManaged(g)
			g.Object["spec"] = map[string]any{
				"from": []any{map[string]any{"group": extensionGVK.Group, "kind": extensionGVK.Kind, "namespace": ns}},
				"to":   []any{map[string]any{"group": "", "kind": "Service", "name": r.FilterService}},
			}
			return nil
		}); err != nil && !isGatewayMissing(err) {
			return err
		}
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(referenceGrantGVK)
	if err := r.List(ctx, list, client.InNamespace(r.Namespace), client.MatchingLabels{managedByLabel: managedByValue}); err != nil {
		if isGatewayMissing(err) {
			return nil
		}
		return err
	}
	for i := range list.Items {
		o := &list.Items[i]
		if ns, ok := strings.CutPrefix(o.GetName(), "x402-filter-from-"); ok && !namespaces[ns] {
			if err := r.Delete(ctx, o); client.IgnoreNotFound(err) != nil {
				return err
			}
		}
	}
	return nil
}

// isGatewayMissing reports whether err means the Gateway API or Envoy Gateway
// CRDs are not installed.
func isGatewayMissing(err error) bool {
	return meta.IsNoMatchError(err) || apierrors.IsNotFound(err) && strings.Contains(err.Error(), "the server could not find the requested resource")
}

func setManaged(o client.Object) {
	l := o.GetLabels()
	if l == nil {
		l = map[string]string{}
	}
	l[managedByLabel] = managedByValue
	o.SetLabels(l)
}

func (r *Reconciler) setPolicyConditions(ctx context.Context, p *x402v1.PaymentPolicy, conds ...metav1.Condition) error {
	changed := false
	for _, c := range conds {
		c.ObservedGeneration = p.Generation
		if meta.SetStatusCondition(&p.Status.Conditions, c) {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return client.IgnoreNotFound(r.Status().Update(ctx, p))
}

func (r *Reconciler) setProviderCondition(ctx context.Context, sp *x402v1.SettlementProvider, c metav1.Condition) error {
	c.ObservedGeneration = sp.Generation
	if !meta.SetStatusCondition(&sp.Status.Conditions, c) {
		return nil
	}
	return client.IgnoreNotFound(r.Status().Update(ctx, sp))
}
