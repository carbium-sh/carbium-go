package carbium

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// The spec types a few values as a union of unlike JSON shapes. Each has its own type here, read from either shape
// and written back in the shape the service takes.

func isNull(b []byte) bool { return string(bytes.TrimSpace(b)) == "null" }

func firstByte(b []byte) byte {
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return 0
	}
	return b[0]
}

// TransactionError is why a transaction failed, kept as the service sent it: a string such as "AccountInUse" or an
// object such as {"InstructionError":[0,{"Custom":1}]}. nil means the transaction did not fail.
type TransactionError json.RawMessage

func (e TransactionError) MarshalJSON() ([]byte, error) {
	if e == nil {
		return []byte("null"), nil
	}
	return e, nil
}

func (e *TransactionError) UnmarshalJSON(b []byte) error {
	if isNull(b) {
		*e = nil
		return nil
	}
	*e = append((*e)[:0], b...)
	return nil
}

func (e TransactionError) String() string { return string(e) }

// EncodedData is an account's data: Data and Encoding for base58, base64 and base64+zstd, or Parsed for jsonParsed.
// The bare string the service sends when no encoding was asked for reads as base58 Data.
type EncodedData struct {
	Data     string
	Encoding AccountDataEncoding
	Parsed   *ParsedData
}

func (d EncodedData) MarshalJSON() ([]byte, error) {
	if d.Parsed != nil {
		return json.Marshal(d.Parsed)
	}
	return json.Marshal([2]string{d.Data, string(d.Encoding)})
}

func (d *EncodedData) UnmarshalJSON(b []byte) error {
	switch firstByte(b) {
	case 'n':
		return nil
	case '"':
		d.Encoding, d.Parsed = AccountDataEncodingBase58, nil
		return json.Unmarshal(b, &d.Data)
	case '[':
		var pair []string
		if err := json.Unmarshal(b, &pair); err != nil {
			return err
		}
		if len(pair) < 2 {
			return fmt.Errorf("carbium: EncodedData: %d items, want 2", len(pair))
		}
		d.Data, d.Encoding, d.Parsed = pair[0], AccountDataEncoding(pair[1]), nil
		return nil
	}
	d.Parsed = new(ParsedData)
	return json.Unmarshal(b, d.Parsed)
}

// EncodedTransaction is a transaction as sent back: Data and Encoding for base58 and base64; Signatures and Message
// for json and jsonParsed; Signatures and AccountKeys for a block read with transactionDetails "accounts".
type EncodedTransaction struct {
	Data        string
	Encoding    BinaryEncoding
	Signatures  []string
	Message     map[string]json.RawMessage
	AccountKeys []TransactionAccountKey
}

func (t EncodedTransaction) MarshalJSON() ([]byte, error) {
	if t.AccountKeys != nil {
		return json.Marshal(struct {
			Signatures  []string                `json:"signatures"`
			AccountKeys []TransactionAccountKey `json:"accountKeys"`
		}{t.Signatures, t.AccountKeys})
	}
	if t.Message != nil || t.Signatures != nil {
		return json.Marshal(struct {
			Signatures []string                   `json:"signatures"`
			Message    map[string]json.RawMessage `json:"message"`
		}{t.Signatures, t.Message})
	}
	return json.Marshal([2]string{t.Data, string(t.Encoding)})
}

func (t *EncodedTransaction) UnmarshalJSON(b []byte) error {
	switch firstByte(b) {
	case 'n':
		return nil
	case '[':
		var pair []string
		if err := json.Unmarshal(b, &pair); err != nil {
			return err
		}
		if len(pair) < 2 {
			return fmt.Errorf("carbium: EncodedTransaction: %d items, want 2", len(pair))
		}
		t.Data, t.Encoding = pair[0], BinaryEncoding(pair[1])
		return nil
	}
	var obj struct {
		Signatures  []string                   `json:"signatures"`
		Message     map[string]json.RawMessage `json:"message"`
		AccountKeys []TransactionAccountKey    `json:"accountKeys"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return err
	}
	t.Signatures, t.Message, t.AccountKeys = obj.Signatures, obj.Message, obj.AccountKeys
	return nil
}

// DASGrouping limits searchAssets to one group, for example Key "collection" and Value the collection address. It
// is sent as [key, value]; the {collection} and {group_key, group_value} forms are read too.
type DASGrouping struct {
	Key   string
	Value string
}

func (g DASGrouping) MarshalJSON() ([]byte, error) { return json.Marshal([2]string{g.Key, g.Value}) }

func (g *DASGrouping) UnmarshalJSON(b []byte) error {
	switch firstByte(b) {
	case 'n':
		return nil
	case '[':
		var pair []string
		if err := json.Unmarshal(b, &pair); err != nil {
			return err
		}
		if len(pair) < 2 {
			return fmt.Errorf("carbium: DASGrouping: %d items, want 2", len(pair))
		}
		g.Key, g.Value = pair[0], pair[1]
		return nil
	}
	var obj struct {
		Collection *string `json:"collection"`
		GroupKey   string  `json:"group_key"`
		GroupValue string  `json:"group_value"`
	}
	if err := json.Unmarshal(b, &obj); err != nil {
		return err
	}
	if obj.Collection != nil {
		g.Key, g.Value = "collection", *obj.Collection
		return nil
	}
	g.Key, g.Value = obj.GroupKey, obj.GroupValue
	return nil
}

// ProgramAccounts is the getProgramAccounts answer. Value holds the accounts; Context is set only when the config
// asked for withContext.
type ProgramAccounts struct {
	Context *Context
	Value   []KeyedAccount
}

func (p ProgramAccounts) MarshalJSON() ([]byte, error) {
	if p.Context == nil {
		return json.Marshal(p.Value)
	}
	return json.Marshal(WithContext[[]KeyedAccount]{Context: *p.Context, Value: p.Value})
}

func (p *ProgramAccounts) UnmarshalJSON(b []byte) error {
	switch firstByte(b) {
	case 'n':
		return nil
	case '[':
		p.Context = nil
		return json.Unmarshal(b, &p.Value)
	}
	var w WithContext[[]KeyedAccount]
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	p.Context, p.Value = &w.Context, w.Value
	return nil
}

// TxStatus is the older status form, {"Ok": null} or {"Err": ...}. Err is nil when the transaction succeeded.
type TxStatus struct {
	Err TransactionError
}

// OK reports whether the transaction succeeded.
func (s TxStatus) OK() bool { return s.Err == nil }

func (s TxStatus) MarshalJSON() ([]byte, error) {
	if s.Err == nil {
		return []byte(`{"Ok":null}`), nil
	}
	return json.Marshal(struct {
		Err TransactionError `json:"Err"`
	}{s.Err})
}

func (s *TxStatus) UnmarshalJSON(b []byte) error {
	if isNull(b) {
		return nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(b, &obj); err != nil {
		return err
	}
	if raw, ok := obj["Err"]; ok && !isNull(raw) {
		s.Err = append(TransactionError(nil), raw...)
		return nil
	}
	if _, ok := obj["Ok"]; !ok {
		return fmt.Errorf("carbium: TxStatus: neither Ok nor Err")
	}
	s.Err = nil
	return nil
}

// TxVersion is a transaction's version: "legacy", "0" or "1". The service sends the numbers as JSON numbers.
type TxVersion string

const (
	TxVersionLegacy TxVersion = "legacy"
	TxVersion0      TxVersion = "0"
	TxVersion1      TxVersion = "1"
)

func (v TxVersion) MarshalJSON() ([]byte, error) {
	if v == "" {
		return []byte("null"), nil
	}
	if n, err := strconv.ParseUint(string(v), 10, 8); err == nil {
		return []byte(strconv.FormatUint(n, 10)), nil
	}
	return json.Marshal(string(v))
}

func (v *TxVersion) UnmarshalJSON(b []byte) error {
	switch c := firstByte(b); {
	case c == 'n':
		*v = ""
		return nil
	case c == '"':
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*v = TxVersion(s)
		return nil
	default:
		var n uint8
		if err := json.Unmarshal(b, &n); err != nil {
			return fmt.Errorf("carbium: TxVersion: %w", err)
		}
		*v = TxVersion(strconv.FormatUint(uint64(n), 10))
		return nil
	}
}

// Decimal is a u64 amount as decimal digits. It is sent as a JSON string and read from a string or a number, so no
// amount above 2^53 is rounded.
type Decimal string

func (d Decimal) MarshalJSON() ([]byte, error) { return json.Marshal(string(d)) }

func (d *Decimal) UnmarshalJSON(b []byte) error {
	switch c := firstByte(b); {
	case c == 'n':
		*d = ""
		return nil
	case c == '"':
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*d = Decimal(s)
		return nil
	default:
		var n json.Number
		if err := json.Unmarshal(b, &n); err != nil {
			return fmt.Errorf("carbium: Decimal: %w", err)
		}
		*d = Decimal(n.String())
		return nil
	}
}
