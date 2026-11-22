package ws

import (
	"encoding/json"

	carbium "github.com/carbium-sh/carbium-go"
)

// The eight served subscriptions, each with its params and the notification it delivers.

// AccountConfig tunes AccountSubscribe. Encoding is one of carbium's encodings or "binary".
type AccountConfig struct {
	Commitment carbium.Commitment `json:"commitment,omitempty"`
	Encoding   string             `json:"encoding,omitempty"`
	DataSlice  *carbium.DataSlice `json:"dataSlice,omitempty"`
}

// ProgramConfig tunes ProgramSubscribe: at most four filters.
type ProgramConfig struct {
	Commitment carbium.Commitment               `json:"commitment,omitempty"`
	Encoding   carbium.Encoding                 `json:"encoding,omitempty"`
	DataSlice  *carbium.DataSlice               `json:"dataSlice,omitempty"`
	Filters    []carbium.ProgramSubscribeFilter `json:"filters,omitempty"`
}

// CommitmentConfig tunes LogsSubscribe and SignatureSubscribe.
type CommitmentConfig struct {
	Commitment carbium.Commitment `json:"commitment,omitempty"`
}

// BlockConfig tunes BlockSubscribe. MaxSupportedTransactionVersion defaults to 1: without it a block holding a
// version-1 transaction arrives as block null.
type BlockConfig struct {
	Commitment                     carbium.Commitment `json:"commitment,omitempty"`         // confirmed or finalized
	Encoding                       string             `json:"encoding,omitempty"`           // json, base64 or base58
	TransactionDetails             string             `json:"transactionDetails,omitempty"` // full, signatures or none
	ShowRewards                    *bool              `json:"showRewards,omitempty"`
	MaxSupportedTransactionVersion *uint64            `json:"maxSupportedTransactionVersion,omitempty"`
}

// LogsFilter picks the logs: LogsAll, LogsAllWithVotes or LogsMentioning(address).
type LogsFilter struct {
	name    string
	mention string
}

var (
	LogsAll          = LogsFilter{name: "all"}
	LogsAllWithVotes = LogsFilter{name: "allWithVotes"}
)

// LogsMentioning picks the logs of transactions that mention one address.
func LogsMentioning(address string) LogsFilter { return LogsFilter{mention: address} }

func (f LogsFilter) MarshalJSON() ([]byte, error) {
	if f.name != "" {
		return json.Marshal(f.name)
	}
	return json.Marshal(map[string][]string{"mentions": {f.mention}})
}

// BlockFilter picks the blocks: BlockAll or BlockMentioning(address).
type BlockFilter struct{ mention string }

// BlockAll picks every block.
var BlockAll = BlockFilter{}

// BlockMentioning picks the blocks that mention one account or program.
func BlockMentioning(address string) BlockFilter { return BlockFilter{mention: address} }

func (f BlockFilter) MarshalJSON() ([]byte, error) {
	if f.mention == "" {
		return json.Marshal("all")
	}
	return json.Marshal(map[string]string{"mentionsAccountOrProgram": f.mention})
}

// SlotInfo is a slotSubscribe notification.
type SlotInfo struct {
	Slot   uint64 `json:"slot"`
	Parent uint64 `json:"parent"`
	Root   uint64 `json:"root"`
}

// SlotsUpdate is a slotsUpdatesSubscribe notification.
type SlotsUpdate struct {
	Type      string             `json:"type"`
	Slot      uint64             `json:"slot"`
	Timestamp uint64             `json:"timestamp"`
	Parent    *uint64            `json:"parent,omitempty"`
	Stats     map[string]float64 `json:"stats,omitempty"`
	Err       *string            `json:"err,omitempty"`
}

// typed decodes each notification into T; one that does not fit goes to OnError.
func typed[T any](s *Socket, method string, fn func(T)) func(json.RawMessage) {
	return func(raw json.RawMessage) {
		var v T
		if err := json.Unmarshal(raw, &v); err != nil {
			if s.opts.OnError != nil {
				s.opts.OnError(carbium.NewError(carbium.ErrorInit{Kind: carbium.KindSocket, Method: method, Message: "a notification does not fit its type: " + err.Error(), Cause: err}))
			}
			return
		}
		fn(v)
	}
}

func withConfig(first any, config any, set bool) []any {
	if !set {
		return []any{first}
	}
	return []any{first, config}
}

// AccountSubscribe follows one account. config may be nil.
func (s *Socket) AccountSubscribe(pubkey string, config *AccountConfig, fn func(carbium.WithContext[carbium.AccountInfo])) (*Subscription, error) {
	return s.Subscribe("accountSubscribe", withConfig(pubkey, config, config != nil), typed(s, "accountSubscribe", fn))
}

// ProgramSubscribe follows the accounts a program owns. config may be nil.
func (s *Socket) ProgramSubscribe(programID string, config *ProgramConfig, fn func(carbium.WithContext[carbium.KeyedAccount])) (*Subscription, error) {
	return s.Subscribe("programSubscribe", withConfig(programID, config, config != nil), typed(s, "programSubscribe", fn))
}

// LogsSubscribe follows transaction logs. config may be nil.
func (s *Socket) LogsSubscribe(filter LogsFilter, config *CommitmentConfig, fn func(carbium.WithContext[carbium.LogsNotification])) (*Subscription, error) {
	return s.Subscribe("logsSubscribe", withConfig(filter, config, config != nil), typed(s, "logsSubscribe", fn))
}

// SignatureSubscribe fires once, then the server ends the subscription. After a reconnect, check
// getSignatureStatuses. config may be nil.
func (s *Socket) SignatureSubscribe(signature string, config *CommitmentConfig, fn func(carbium.WithContext[carbium.SignatureNotification])) (*Subscription, error) {
	return s.Subscribe("signatureSubscribe", withConfig(signature, config, config != nil), typed(s, "signatureSubscribe", fn))
}

// SlotSubscribe follows each slot.
func (s *Socket) SlotSubscribe(fn func(SlotInfo)) (*Subscription, error) {
	return s.Subscribe("slotSubscribe", nil, typed(s, "slotSubscribe", fn))
}

// RootSubscribe follows each new root.
func (s *Socket) RootSubscribe(fn func(uint64)) (*Subscription, error) {
	return s.Subscribe("rootSubscribe", nil, typed(s, "rootSubscribe", fn))
}

// SlotsUpdatesSubscribe follows each slot's progress.
func (s *Socket) SlotsUpdatesSubscribe(fn func(SlotsUpdate)) (*Subscription, error) {
	return s.Subscribe("slotsUpdatesSubscribe", nil, typed(s, "slotsUpdatesSubscribe", fn))
}

// BlockSubscribe follows blocks. config may be nil; MaxSupportedTransactionVersion is sent as 1 unless set.
func (s *Socket) BlockSubscribe(filter BlockFilter, config *BlockConfig, fn func(carbium.WithContext[carbium.BlockNotification])) (*Subscription, error) {
	c := BlockConfig{}
	if config != nil {
		c = *config
	}
	if c.MaxSupportedTransactionVersion == nil {
		one := uint64(1)
		c.MaxSupportedTransactionVersion = &one
	}
	return s.Subscribe("blockSubscribe", []any{filter, c}, typed(s, "blockSubscribe", fn))
}
