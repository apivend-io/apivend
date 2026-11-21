// Package v1alpha1 contains the PaymentPolicy and SettlementProvider APIs.
//
// +kubebuilder:object:generate=true
// +groupName=apivend.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	GroupVersion  = schema.GroupVersion{Group: "apivend.io", Version: "v1alpha1"}
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	AddToScheme   = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&PaymentPolicy{}, &PaymentPolicyList{}, &SettlementProvider{}, &SettlementProviderList{})
}
