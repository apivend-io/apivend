// Package x402 defines the x402 v2 wire types and their HTTP header encoding.
//
// Source: x402 specification v2 and its HTTP transport
// (https://github.com/x402-foundation/x402/tree/main/specs).
package x402

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	Version = 2

	HeaderPaymentRequired  = "payment-required"
	HeaderPaymentSignature = "payment-signature"
	HeaderPaymentResponse  = "payment-response"

	SchemeExact = "exact"
	// SchemeBatch is batch-settlement (payment channels, cumulative vouchers).
	SchemeBatch = "batch-settlement"

	// MaxHeaderBytes bounds the PAYMENT-SIGNATURE value before any decoding.
	// A plain exact/EIP-3009 payload is about 1 KB.
	MaxHeaderBytes = 16 << 10
)

// Error codes from section 9 of the specification.
const (
	ErrInsufficientFunds   = "insufficient_funds"
	ErrValidAfter          = "invalid_exact_evm_payload_authorization_valid_after"
	ErrValidBefore         = "invalid_exact_evm_payload_authorization_valid_before"
	ErrValueMismatch       = "invalid_exact_evm_payload_authorization_value_mismatch"
	ErrSignature           = "invalid_exact_evm_payload_signature"
	ErrRecipientMismatch   = "invalid_exact_evm_payload_recipient_mismatch"
	ErrInvalidNetwork      = "invalid_network"
	ErrInvalidPayload      = "invalid_payload"
	ErrInvalidRequirements = "invalid_payment_requirements"
	ErrInvalidScheme       = "invalid_scheme"
	ErrInvalidVersion      = "invalid_x402_version"
	ErrUnexpectedVerify    = "unexpected_verify_error"
)

// Error codes not in the specification, as returned by the x402.org
// facilitator (observed 2026-11-17 on Base Sepolia).
const (
	// The payer's balance did not cover the transfer at settlement.
	ErrEVMInsufficientBalance = "invalid_exact_evm_insufficient_balance"
	// The facilitator's transaction failed; seen transiently, settling on retry.
	ErrEVMTransactionFailed = "invalid_exact_evm_transaction_failed"
)

// Error codes of the batch-settlement EVM binding (scheme_batch_settlement_evm.md).
const (
	ErrBatchChannelIDMismatch    = "invalid_batch_settlement_evm_channel_id_mismatch"
	ErrBatchChannelNotFound      = "invalid_batch_settlement_evm_channel_not_found"
	ErrBatchChannelBusy          = "invalid_batch_settlement_evm_channel_busy"
	ErrBatchCumulativeMismatch   = "invalid_batch_settlement_evm_cumulative_amount_mismatch"
	ErrBatchCumulativeBelow      = "invalid_batch_settlement_evm_cumulative_below_claimed"
	ErrBatchExceedsBalance       = "invalid_batch_settlement_evm_cumulative_exceeds_balance"
	ErrBatchPayloadType          = "invalid_batch_settlement_evm_payload_type"
	ErrBatchReceiverMismatch     = "invalid_batch_settlement_evm_receiver_mismatch"
	ErrBatchAuthorizerMismatch   = "invalid_batch_settlement_evm_receiver_authorizer_mismatch"
	ErrBatchTokenMismatch        = "invalid_batch_settlement_evm_token_mismatch"
	ErrBatchWithdrawDelay        = "invalid_batch_settlement_evm_withdraw_delay_mismatch"
	ErrBatchVoucherPayload       = "invalid_batch_settlement_evm_voucher_payload"
	ErrBatchVoucherSignature     = "invalid_batch_settlement_evm_voucher_signature"
	ErrBatchChannelStateReadFail = "invalid_batch_settlement_evm_channel_state_read_failed"
	ErrBatchDepositPayload       = "invalid_batch_settlement_evm_deposit_payload"
	ErrBatchERC3009Required      = "invalid_batch_settlement_evm_erc3009_authorization_required"
	ErrBatchReceiveSignature     = "invalid_batch_settlement_evm_receive_authorization_signature"
	ErrBatchDepositValidAfter    = "invalid_batch_settlement_evm_payload_authorization_valid_after"
	ErrBatchDepositValidBefore   = "invalid_batch_settlement_evm_payload_authorization_valid_before"
	ErrBatchDepositFailed        = "invalid_batch_settlement_evm_deposit_transaction_failed"
	ErrBatchRefundAmount         = "invalid_batch_settlement_evm_refund_amount_invalid"
	ErrBatchRefundExceeds        = "invalid_batch_settlement_evm_refund_amount_exceeds_balance"
	ErrBatchRefundNoBalance      = "invalid_batch_settlement_evm_refund_no_balance"
	ErrBatchRefundFailed         = "invalid_batch_settlement_evm_refund_transaction_failed"
)

// BatchChannelConfig is a channel's configuration on the wire.
type BatchChannelConfig struct {
	Payer              string `json:"payer"`
	PayerAuthorizer    string `json:"payerAuthorizer"`
	Receiver           string `json:"receiver"`
	ReceiverAuthorizer string `json:"receiverAuthorizer"`
	Token              string `json:"token"`
	WithdrawDelay      uint64 `json:"withdrawDelay"`
	Salt               string `json:"salt"`
}

// BatchVoucher is a signed cumulative ceiling for a channel.
type BatchVoucher struct {
	ChannelID          string `json:"channelId"`
	MaxClaimableAmount string `json:"maxClaimableAmount"`
	Signature          string `json:"signature"`
}

// BatchEVMPayload is PaymentPayload.Payload for scheme batch-settlement.
// Type is deposit, voucher or refund; Deposit only on deposits.
type BatchEVMPayload struct {
	Type          string             `json:"type"`
	ChannelConfig BatchChannelConfig `json:"channelConfig"`
	Voucher       BatchVoucher       `json:"voucher"`
	Deposit       json.RawMessage    `json:"deposit,omitempty"`
	Amount        string             `json:"amount,omitempty"`
}

// BatchChannelState is the channel snapshot in responses and corrective 402s.
type BatchChannelState struct {
	ChannelID               string `json:"channelId"`
	Balance                 string `json:"balance"`
	TotalClaimed            string `json:"totalClaimed"`
	WithdrawRequestedAt     uint64 `json:"withdrawRequestedAt"`
	RefundNonce             string `json:"refundNonce"`
	ChargedCumulativeAmount string `json:"chargedCumulativeAmount"`
}

// ResourceInfo describes the protected resource. ServiceName, Tags and
// IconURL are discovery metadata (specification v2 section 5.1 and the
// bazaar extension): catalogues show them.
type ResourceInfo struct {
	URL         string   `json:"url"`
	Description string   `json:"description,omitempty"`
	MimeType    string   `json:"mimeType,omitempty"`
	ServiceName string   `json:"serviceName,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	IconURL     string   `json:"iconUrl,omitempty"`
}

// Requirements is one acceptable way to pay (a PaymentRequirements object).
type Requirements struct {
	Scheme            string         `json:"scheme"`
	Network           string         `json:"network"`
	Amount            string         `json:"amount"`
	Asset             string         `json:"asset"`
	PayTo             string         `json:"payTo"`
	MaxTimeoutSeconds int            `json:"maxTimeoutSeconds"`
	Extra             map[string]any `json:"extra,omitempty"`
}

// PaymentRequired is carried in the PAYMENT-REQUIRED header of a 402.
type PaymentRequired struct {
	X402Version int            `json:"x402Version"`
	Error       string         `json:"error,omitempty"`
	Resource    ResourceInfo   `json:"resource"`
	Accepts     []Requirements `json:"accepts"`
	// Extensions by key, each {info, schema} (specification v2 section
	// 5.1). Clients echo them into the payment payload.
	Extensions map[string]json.RawMessage `json:"extensions,omitempty"`
}

// ExtensionBazaar is the discovery extension's key: facilitators that index
// resources (CDP's Bazaar) list a route from it once a payment settles.
const ExtensionBazaar = "bazaar"

// PaymentPayload is carried in the PAYMENT-SIGNATURE header of the retry.
type PaymentPayload struct {
	X402Version int             `json:"x402Version"`
	Resource    *ResourceInfo   `json:"resource,omitempty"`
	Accepted    Requirements    `json:"accepted"`
	Payload     json.RawMessage `json:"payload"`
	Extensions  json.RawMessage `json:"extensions,omitempty"`
}

// ExactEVMPayload is PaymentPayload.Payload for scheme "exact" with EIP-3009.
type ExactEVMPayload struct {
	Signature     string `json:"signature"`
	Authorization struct {
		From        string `json:"from"`
		To          string `json:"to"`
		Value       string `json:"value"`
		ValidAfter  string `json:"validAfter"`
		ValidBefore string `json:"validBefore"`
		Nonce       string `json:"nonce"`
	} `json:"authorization"`
}

// SettlementResponse is carried in the PAYMENT-RESPONSE header.
type SettlementResponse struct {
	Success     bool   `json:"success"`
	ErrorReason string `json:"errorReason,omitempty"`
	Payer       string `json:"payer,omitempty"`
	Transaction string `json:"transaction"`
	Network     string `json:"network"`
	// Batch settlement: Amount is "" for a voucher (nothing moved on
	// chain); Extra carries chargedAmount and channelState.
	Amount *string        `json:"amount,omitempty"`
	Extra  map[string]any `json:"extra,omitempty"`
}

// EncodeHeader serialises v as base64-encoded JSON.
func EncodeHeader(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// DecodeHeader parses a base64-encoded JSON header value into v. Padded and
// unpadded standard base64 are both accepted; trailing data is rejected.
func DecodeHeader(value string, v any) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("empty header")
	}
	if len(value) > MaxHeaderBytes {
		return fmt.Errorf("header exceeds %d bytes", MaxHeaderBytes)
	}
	raw, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		raw, err = base64.RawStdEncoding.DecodeString(value)
		if err != nil {
			return errors.New("header is not valid base64")
		}
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(v); err != nil {
		return errors.New("header is not valid JSON for this type")
	}
	if dec.More() {
		return errors.New("trailing data after JSON value")
	}
	return nil
}
