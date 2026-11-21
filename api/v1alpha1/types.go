package v1alpha1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Condition types, following Gateway API policy conventions.
const (
	// ConditionAccepted: the object is valid and its references resolve.
	ConditionAccepted = "Accepted"
	// ConditionProgrammed: the gateway configuration for it has been written.
	ConditionProgrammed = "Programmed"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=paypol
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetRef.name`
// +kubebuilder:printcolumn:name="Price",type=string,JSONPath=`.spec.price.amount`
// +kubebuilder:printcolumn:name="Accepted",type=string,JSONPath=`.status.conditions[?(@.type=="Accepted")].status`
// +kubebuilder:printcolumn:name="Programmed",type=string,JSONPath=`.status.conditions[?(@.type=="Programmed")].status`

// PaymentPolicy puts an x402 price on an HTTPRoute in the same namespace.
// Deleting the policy makes the route free again.
type PaymentPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PaymentPolicySpec   `json:"spec"`
	Status PaymentPolicyStatus `json:"status,omitempty"`
}

type PaymentPolicySpec struct {
	// TargetRef names the HTTPRoute to price. It must be in the policy's
	// namespace.
	TargetRef TargetRef `json:"targetRef"`

	// Description is shown to clients in the 402 challenge.
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Description string `json:"description,omitempty"`

	// Price of one request.
	Price Price `json:"price"`

	// Accepts lists the networks and assets the price can be paid in.
	// +kubebuilder:validation:MinItems=1
	Accepts []Accept `json:"accepts"`

	// MaxTimeoutSeconds is how long a client's payment authorization should
	// stay valid. Default 60.
	// +optional
	// +kubebuilder:validation:Minimum=15
	// +kubebuilder:validation:Maximum=3600
	MaxTimeoutSeconds *int32 `json:"maxTimeoutSeconds,omitempty"`

	// +optional
	Settlement *Settlement `json:"settlement,omitempty"`

	// +optional
	Risk *Risk `json:"risk,omitempty"`

	// Discovery describes the route for x402 catalogues (the bazaar
	// extension) and for the gateway's own catalogue. Nothing is published
	// until Enabled is true.
	// +optional
	Discovery *Discovery `json:"discovery,omitempty"`
}

// Discovery is what an agent needs to find and call the route: its name,
// tags, how to call it and what comes back.
type Discovery struct {
	// Enabled puts the bazaar extension in the route's 402 and on the
	// payments it settles. Facilitators that index resources list the route
	// from it: CDP's Bazaar lists it after the first settled payment, with no
	// review. Default false: the metadata is kept but not published.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// ServiceName is a short display name.
	// +optional
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^[ -~]*$`
	ServiceName string `json:"serviceName,omitempty"`

	// Tags help catalogues group the route.
	// +optional
	// +kubebuilder:validation:MaxItems=5
	// +kubebuilder:validation:items:MaxLength=32
	Tags []string `json:"tags,omitempty"`

	// IconURL is a PNG, JPEG, WebP or GIF image over HTTPS (catalogues do
	// not accept SVG).
	// +optional
	// +kubebuilder:validation:Pattern=`^https://`
	IconURL string `json:"iconUrl,omitempty"`

	// Method of the request. Default GET.
	// +optional
	// +kubebuilder:validation:Enum=GET;HEAD;DELETE;POST;PUT;PATCH
	Method string `json:"method,omitempty"`

	// QueryParams the route takes, each with an example value.
	// +optional
	// +kubebuilder:validation:MaxItems=20
	QueryParams []Param `json:"queryParams,omitempty"`

	// BodyType of a POST, PUT or PATCH request. Default json.
	// +optional
	// +kubebuilder:validation:Enum=json;form-data;text
	BodyType string `json:"bodyType,omitempty"`

	// Body is an example request body (POST, PUT, PATCH).
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Body *apiextensionsv1.JSON `json:"body,omitempty"`

	// Output describes the response.
	// +optional
	Output *Output `json:"output,omitempty"`
}

type Param struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	Name string `json:"name"`
	// +optional
	// +kubebuilder:validation:MaxLength=200
	Description string `json:"description,omitempty"`
	// +optional
	Required bool `json:"required,omitempty"`
	// Example is shown to clients as the value to send.
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Example string `json:"example,omitempty"`
}

type Output struct {
	// Type of the response, e.g. json. Default json.
	// +optional
	// +kubebuilder:validation:MaxLength=32
	Type string `json:"type,omitempty"`
	// Example response.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:Schemaless
	Example *apiextensionsv1.JSON `json:"example,omitempty"`
}

type TargetRef struct {
	// +kubebuilder:validation:Enum=gateway.networking.k8s.io
	Group string `json:"group"`
	// +kubebuilder:validation:Enum=HTTPRoute
	Kind string `json:"kind"`
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

type Price struct {
	// Amount as a decimal string, e.g. "0.01".
	// +kubebuilder:validation:Pattern=`^[0-9]*\.?[0-9]+$`
	Amount string `json:"amount"`
	// +kubebuilder:validation:Enum=USD
	Currency string `json:"currency"`
}

type Accept struct {
	// Network, e.g. "base" or "base-sepolia".
	Network string `json:"network"`
	// Asset symbol, e.g. "USDC".
	Asset string `json:"asset"`
	// PayTo is the address that receives payments.
	// +kubebuilder:validation:Pattern=`^0x[0-9a-fA-F]{40}$`
	PayTo string `json:"payTo"`
	// Schemes offered: exact (default: one transfer per request) and/or
	// batch-settlement (payment channels: a deposit, then signed vouchers,
	// claimed in batches). batch-settlement needs the controller's
	// -batch-receiver-authorizer.
	// +optional
	// +kubebuilder:validation:MaxItems=2
	Schemes []Scheme `json:"schemes,omitempty"`
}

// +kubebuilder:validation:Enum=exact;batch-settlement
type Scheme string

type Settlement struct {
	// Mode is async (respond, then settle; the default) or sync (hold the
	// response until the payment has settled, up to 10s).
	// +optional
	// +kubebuilder:validation:Enum=async;sync
	Mode string `json:"mode,omitempty"`

	// ProviderRef names the SettlementProvider. Default "default".
	// +optional
	ProviderRef *ProviderRef `json:"providerRef,omitempty"`

	// MaxUnsettledPerPayer caps, in the price currency, what one payer may
	// have in flight plus served-but-unsettled. It bounds the seller's loss
	// per payer in async mode. Default "1.00".
	// +optional
	// +kubebuilder:validation:Pattern=`^[0-9]*\.?[0-9]+$`
	MaxUnsettledPerPayer string `json:"maxUnsettledPerPayer,omitempty"`
}

type ProviderRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

type Risk struct {
	// Freshness is the maximum age of a cached payer balance. Default 5s.
	// +optional
	Freshness *metav1.Duration `json:"freshness,omitempty"`

	// Simulate is reserved; true is rejected for now.
	// +optional
	Simulate bool `json:"simulate,omitempty"`

	// OnStateUnavailable decides what happens when payer state cannot be
	// reached: deny (default) refuses the request, allow serves it unpaid.
	// +optional
	// +kubebuilder:validation:Enum=deny;allow
	OnStateUnavailable string `json:"onStateUnavailable,omitempty"`
}

type PaymentPolicyStatus struct {
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true

type PaymentPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PaymentPolicy `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=settleprov
// +kubebuilder:printcolumn:name="Accepted",type=string,JSONPath=`.status.conditions[?(@.type=="Accepted")].status`

// SettlementProvider says how committed payments are settled. It is
// cluster-scoped; the Secrets it references live in the controller's
// namespace.
type SettlementProvider struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SettlementProviderSpec   `json:"spec"`
	Status SettlementProviderStatus `json:"status,omitempty"`
}

type SettlementProviderSpec struct {
	// Backends are tried in priority order (lowest first).
	// +kubebuilder:validation:MinItems=1
	Backends []SettlementBackend `json:"backends"`
}

type SettlementBackend struct {
	// Type is facilitator (an x402 facilitator settles) or direct (the
	// settler submits USDC transferWithAuthorization itself through its own
	// -rpc endpoints, gas paid by the key in credentialsRef; no compliance
	// screening).
	// +kubebuilder:validation:Enum=facilitator;direct
	Type string `json:"type"`

	// URL of the facilitator, without the /settle suffix.
	// +optional
	URL string `json:"url,omitempty"`

	// CredentialsRef names a Secret in the controller's namespace holding the
	// facilitator credentials: a bearer token, or with auth cdp the Coinbase
	// Developer Platform Secret API key file ({"id": ..., "privateKey": ...}).
	// For direct: the gas key, 32 bytes of hex. It pays gas only; fund it
	// with a little ETH on each network.
	// +optional
	CredentialsRef *SecretKeyRef `json:"credentialsRef,omitempty"`

	// Auth is how the credentials are sent: bearer (default) as a static
	// token, or cdp as a fresh JWT per request signed with the CDP API key
	// (Coinbase's facilitator, https://api.cdp.coinbase.com/platform/v2/x402).
	// +kubebuilder:validation:Enum=bearer;cdp
	// +optional
	Auth string `json:"auth,omitempty"`

	// MaxFee (direct) is the most one settlement may spend on gas, in ETH.
	// Default "0.00002", about 20 times a Base settlement at 0.01 gwei.
	// +optional
	// +kubebuilder:validation:Pattern=`^[0-9]*\.?[0-9]+$`
	MaxFee string `json:"maxFee,omitempty"`

	// RPCRef and SignerRef are reserved.
	// +optional
	RPCRef *ProviderRef `json:"rpcRef,omitempty"`
	// +optional
	SignerRef *ProviderRef `json:"signerRef,omitempty"`

	// +optional
	Priority int32 `json:"priority,omitempty"`
}

type SecretKeyRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Key within the Secret. Default "token".
	// +optional
	Key string `json:"key,omitempty"`
}

type SettlementProviderStatus struct {
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true

type SettlementProviderList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SettlementProvider `json:"items"`
}
