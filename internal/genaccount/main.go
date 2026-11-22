// Command genaccount writes package account's types and calls from the account API's OpenAPI description: every
// operation the TypeScript SDK offers, and none that it leaves to the dashboard. It fails on an operation it neither
// maps nor excludes, a mapping that names no operation, a credential that does not fit the client, or a schema it
// cannot type.
//
// Usage: go run ./internal/genaccount <account-openapi.json> <out.go>
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"slices"
	"sort"
	"strings"
	"unicode"
)

type mapping struct {
	op     string
	client string // Client (account key), Session (session token) or Public (no credential)
	group  string // a group of Client, or "" for a call on the client itself
	name   string
	result string // the answer's schema when the operation answers oneOf by step
	step   string // the step a wallet call sends
	slow   bool   // waits at least 25 s: the service tries a test delivery within 15 s
}

var mappings = []mapping{
	{op: "get_v1_keys", client: "Client", group: "Keys", name: "List"},
	{op: "post_v1_keys", client: "Client", group: "Keys", name: "Create"},
	{op: "post_v1_keys_id_rotate", client: "Client", group: "Keys", name: "Rotate"},
	{op: "post_v1_keys_id_revoke", client: "Client", group: "Keys", name: "Revoke"},
	{op: "delete_v1_keys_id", client: "Client", group: "Keys", name: "Delete"},
	{op: "post_v1_keys_id_name", client: "Client", group: "Keys", name: "Rename"},
	{op: "patch_v1_keys_id_scope", client: "Client", group: "Keys", name: "SetScope"},
	{op: "patch_v1_keys_id_caps", client: "Client", group: "Keys", name: "SetCaps"},
	{op: "patch_v1_keys_id_allowlist", client: "Client", group: "Keys", name: "SetAllowlist"},
	{op: "patch_v1_keys_id_limits", client: "Client", group: "Keys", name: "SetLimits"},
	{op: "get_v1_webhooks", client: "Client", group: "Webhooks", name: "List"},
	{op: "get_v1_webhooks_id", client: "Client", group: "Webhooks", name: "Get"},
	{op: "get_v1_webhooks_id_deliveries", client: "Client", group: "Webhooks", name: "Deliveries"},
	{op: "post_v1_webhooks", client: "Client", group: "Webhooks", name: "Create"},
	{op: "patch_v1_webhooks_id", client: "Client", group: "Webhooks", name: "Update"},
	{op: "delete_v1_webhooks_id", client: "Client", group: "Webhooks", name: "Delete"},
	{op: "post_v1_webhooks_id_rotate", client: "Client", group: "Webhooks", name: "RotateSecret"},
	{op: "post_v1_webhooks_id_test", client: "Client", group: "Webhooks", name: "Test", slow: true},
	{op: "get_v1_usage", client: "Client", group: "Usage", name: "Daily"},
	{op: "get_v1_keys_usage", client: "Client", group: "Usage", name: "PerKey"},
	{op: "get_v1_keys_load", client: "Client", group: "Usage", name: "Load"},
	{op: "get_v1_usage_hourly", client: "Client", group: "Usage", name: "Hourly"},
	{op: "get_v1_usage_latency", client: "Client", group: "Usage", name: "Latency"},
	{op: "get_v1_account", client: "Client", group: "Account", name: "Get"},
	{op: "get_v1_balance", client: "Client", group: "Account", name: "Balance"},
	{op: "get_v1_invoices", client: "Client", group: "Account", name: "Invoices"},
	{op: "get_v1_payments", client: "Client", group: "Account", name: "Payments"},
	{op: "get_v1_modules", client: "Client", group: "Account", name: "Modules"},
	{op: "get_v1_billing_crypto_intent_id", client: "Client", group: "Account", name: "CryptoIntent"},
	{op: "post_v1_modules_module", client: "Client", group: "Modules", name: "EnableFree"},
	{op: "get_v1_api_key", client: "Client", name: "Self"},
	{op: "get_v1_account_api_keys_id_activity", client: "Client", name: "Activity"},

	{op: "post_v1_account_api_keys", client: "Session", name: "CreateAccountAPIKey"},
	{op: "get_v1_account_api_keys", client: "Session", name: "ListAccountAPIKeys"},
	{op: "delete_v1_account_api_keys_id", client: "Session", name: "RevokeAccountAPIKey"},
	{op: "get_v1_keys", client: "Session", name: "ListKeys"},
	{op: "post_v1_keys_id_secret", client: "Session", name: "RevealKeySecret"},
	{op: "post_v1_modules_module", client: "Session", name: "EnableModule"},
	{op: "delete_v1_modules_module", client: "Session", name: "DisableModule"},
	{op: "post_v1_account_link_wallet", client: "Session", name: "LinkWalletChallenge", step: "nonce", result: "wallet.ChallengeResp"},
	{op: "post_v1_account_link_wallet", client: "Session", name: "LinkWallet", step: "link", result: "wallet.LinkedResp"},
	{op: "post_v1_logout", client: "Session", name: "Logout"},

	{op: "get_v1_terms", client: "Public", name: "Terms"},
	{op: "get_v1_plans", client: "Public", name: "Plans"},
	{op: "get_v1_signup_challenge", client: "Public", name: "SignupChallenge"},
	{op: "post_v1_login_pin", client: "Public", name: "PinStart"},
	{op: "post_v1_login_pin_verify", client: "Public", name: "PinVerify"},
	{op: "post_v1_auth_wallet", client: "Public", name: "WalletChallenge", step: "challenge", result: "wallet.ChallengeResp"},
	{op: "post_v1_auth_wallet", client: "Public", name: "WalletVerify", step: "verify", result: "signup.SessionResp"},
}

// excluded are the operations the TypeScript SDK leaves to the dashboard: payments, plan changes and closing the
// account.
var excluded = []string{"delete_v1_account", "get_v1_account_deletion", "post_v1_billing_checkout", "post_v1_billing_crypto_intent",
	"post_v1_billing_portal", "post_v1_billing_renewal", "delete_v1_billing_renewal", "get_v1_payments_id_document"}

// secretFields are answer fields the service shows once: they decode into carbium.Secret, which never prints.
var secretFields = map[string]bool{"raw_key": true, "secret": true, "session_token": true}

type Schema struct {
	Ref        string            `json:"$ref"`
	Type       json.RawMessage   `json:"type"`
	Format     string            `json:"format"`
	Enum       []json.RawMessage `json:"enum"`
	Properties *Props            `json:"properties"`
	Required   []string          `json:"required"`
	Items      *Schema           `json:"items"`
	MinItems   *int              `json:"minItems"`
	AnyOf      []*Schema         `json:"anyOf"`
	OneOf      []*Schema         `json:"oneOf"`
	AllOf      []*Schema         `json:"allOf"`
	Additional json.RawMessage   `json:"additionalProperties"`
	extra      map[string]bool
	raw        []byte
}

var known = []string{"$ref", "type", "format", "enum", "properties", "required", "items", "minItems", "anyOf", "oneOf", "allOf", "additionalProperties", "description", "title", "example"}

func (s *Schema) UnmarshalJSON(b []byte) error {
	type plain Schema
	var p plain
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}
	var keys map[string]json.RawMessage
	_ = json.Unmarshal(b, &keys)
	for k := range keys {
		if !slices.Contains(known, k) {
			return fmt.Errorf("unknown schema keyword %q", k)
		}
	}
	*s = Schema(p)
	s.raw = append([]byte(nil), b...)
	return nil
}

type Props struct {
	Names []string
	By    map[string]*Schema
}

func (p *Props) UnmarshalJSON(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return fmt.Errorf("properties: not an object")
	}
	p.By = map[string]*Schema{}
	for dec.More() {
		t, _ := dec.Token()
		var s Schema
		if err := dec.Decode(&s); err != nil {
			return err
		}
		p.Names = append(p.Names, t.(string))
		p.By[t.(string)] = &s
	}
	return nil
}

type Param struct {
	Name     string  `json:"name"`
	In       string  `json:"in"`
	Required bool    `json:"required"`
	Schema   *Schema `json:"schema"`
}

type Op struct {
	ID          string             `json:"operationId"`
	Security    []map[string][]any `json:"security"`
	Scope       string             `json:"x-scope"`
	Parameters  []Param            `json:"parameters"`
	RequestBody *struct {
		Content map[string]struct {
			Schema *Schema `json:"schema"`
		} `json:"content"`
	} `json:"requestBody"`
	Responses map[string]struct {
		Content map[string]struct {
			Schema *Schema `json:"schema"`
		} `json:"content"`
	} `json:"responses"`
	verb, path string
}

type gen struct {
	schemas map[string]*Schema
	decls   map[string]string
	order   []string
	errs    []string
	request bool            // typing a request body: an optional string becomes a pointer, so "" can be sent
	sides   map[string]bool // type name -> declared on the request side
}

func (g *gen) fail(f string, a ...any) { g.errs = append(g.errs, fmt.Sprintf(f, a...)) }

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: genaccount <account-openapi.json> <out.go>")
		os.Exit(2)
	}
	src, err := run(os.Args[1])
	if err == nil {
		err = os.WriteFile(os.Args[2], src, 0o644)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "genaccount:", err)
		os.Exit(1)
	}
}

func run(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc struct {
		Info       struct{ Title, Version string }       `json:"info"`
		Paths      map[string]map[string]json.RawMessage `json:"paths"`
		Components struct {
			Schemas map[string]*Schema `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	g := &gen{schemas: doc.Components.Schemas, decls: map[string]string{}, sides: map[string]bool{}}
	ops := map[string]*Op{}
	for p, verbs := range doc.Paths {
		for verb, raw := range verbs {
			if !slices.Contains([]string{"get", "post", "put", "patch", "delete"}, verb) {
				continue
			}
			var op Op
			if err := json.Unmarshal(raw, &op); err != nil {
				return nil, fmt.Errorf("%s %s: %w", verb, p, err)
			}
			op.verb, op.path = strings.ToUpper(verb), p
			ops[op.ID] = &op
		}
	}
	used := map[string]bool{}
	for _, m := range mappings {
		if ops[m.op] == nil {
			g.fail("mapping %s.%s names no operation %s", m.client, m.name, m.op)
		}
		used[m.op] = true
	}
	for _, id := range excluded {
		if ops[id] == nil {
			g.fail("exclusion %s names no operation", id)
		}
		if used[id] {
			g.fail("%s is both mapped and excluded", id)
		}
		used[id] = true
	}
	for id := range ops {
		if !used[id] {
			g.fail("operation %s is neither mapped nor excluded", id)
		}
	}

	var methods, routes strings.Builder
	groups := map[string]bool{}
	for _, m := range mappings {
		op := ops[m.op]
		if op == nil {
			continue
		}
		g.checkSecurity(m, op)
		if m.group != "" {
			groups[m.group] = true
		}
		g.method(&methods, m, op)
		fmt.Fprintf(&routes, "\t{Client: %q, Group: %q, Name: %q, Verb: %q, Path: %q, Scope: %q},\n", m.client, m.group, m.name, op.verb, op.path, op.Scope)
	}
	if len(g.errs) > 0 {
		sort.Strings(g.errs)
		return nil, fmt.Errorf("%d problems:\n  %s", len(g.errs), strings.Join(slices.Compact(g.errs), "\n  "))
	}
	var out strings.Builder
	fmt.Fprintf(&out, "// Code generated by internal/genaccount from the account API description (%s, version %s); DO NOT EDIT.\n\npackage account\n\n", doc.Info.Title, doc.Info.Version)
	out.WriteString("import (\n\t\"context\"\n\t\"encoding/json\"\n\t\"net/http\"\n\t\"net/url\"\n\t\"strconv\"\n\n\tcarbium \"github.com/carbium-sh/carbium-go\"\n)\n\n")
	out.WriteString("var (\n\t_ = json.Marshal\n\t_ = strconv.Itoa\n\t_ url.Values\n\t_ carbium.Secret\n)\n\n")
	out.WriteString("// routes holds every call this package makes.\nvar routes = []Route{\n" + routes.String() + "}\n\n")
	var names []string
	for gname := range groups {
		names = append(names, gname)
	}
	sort.Strings(names)
	for _, gname := range names {
		fmt.Fprintf(&out, "// %s groups the %s calls of an account API key.\ntype %s struct{ c *Client }\n\n", groupType(gname), strings.ToLower(gname), groupType(gname))
	}
	out.WriteString("func (c *Client) wire() {\n")
	for _, gname := range names {
		fmt.Fprintf(&out, "\tc.%s = &%s{c}\n", gname, groupType(gname))
	}
	out.WriteString("}\n\n")
	for _, name := range g.order {
		out.WriteString(g.decls[name] + "\n")
	}
	out.WriteString(methods.String())
	return format.Source([]byte(out.String()))
}

func groupType(name string) string {
	if name == "Account" {
		return "Records"
	}
	return name
}

func (g *gen) checkSecurity(m mapping, op *Op) {
	has := func(scheme string) bool {
		for _, s := range op.Security {
			if _, ok := s[scheme]; ok {
				return true
			}
		}
		return false
	}
	switch m.client {
	case "Client":
		if !has("accountApiKey") {
			g.fail("%s: %s does not take an account API key", m.name, m.op)
		}
	case "Session":
		if !has("session") {
			g.fail("%s: %s does not take a session", m.name, m.op)
		}
	case "Public":
		anonymous := len(op.Security) == 0
		for _, s := range op.Security {
			if len(s) == 0 {
				anonymous = true
			}
		}
		if !anonymous {
			g.fail("%s: %s needs a credential", m.name, m.op)
		}
	default:
		g.fail("%s: unknown client %s", m.name, m.client)
	}
}

func refName(ref string) string { return strings.TrimPrefix(ref, "#/components/schemas/") }

func (s *Schema) types() []string {
	var one string
	if json.Unmarshal(s.Type, &one) == nil {
		return []string{one}
	}
	var many []string
	_ = json.Unmarshal(s.Type, &many)
	return many
}

type goType struct {
	expr               string
	nilable, stringish bool
	isStruct           bool
}

func (g *gen) named(name string) goType {
	s := g.schemas[name]
	if s == nil {
		g.fail("$ref to unknown schema %s", name)
		return goType{expr: "json.RawMessage", nilable: true}
	}
	return g.typ(s, name, pascal(name), "")
}

func (g *gen) typ(s *Schema, path, hint, field string) goType {
	if s.Ref != "" {
		return g.named(refName(s.Ref))
	}
	if string(bytes.TrimSpace(s.raw)) == "{}" {
		return goType{expr: "json.RawMessage", nilable: true}
	}
	if len(s.AllOf) > 0 {
		return g.allOf(s, path, hint)
	}
	if len(s.OneOf) > 0 {
		g.fail("%s: oneOf needs a result override in the mapping table", path)
		return goType{expr: "json.RawMessage", nilable: true}
	}
	if len(s.AnyOf) > 0 {
		var rest []*Schema
		nullable := false
		for _, b := range s.AnyOf {
			if t := b.types(); len(t) == 1 && t[0] == "null" {
				nullable = true
				continue
			}
			rest = append(rest, b)
		}
		if len(rest) != 1 {
			g.fail("%s: anyOf of unlike types has no recipe", path)
			return goType{expr: "json.RawMessage", nilable: true}
		}
		t := g.typ(rest[0], path, hint, field)
		if nullable && !t.nilable {
			t = goType{expr: "*" + t.expr, nilable: true}
		}
		return t
	}
	types := s.types()
	nullable := slices.Contains(types, "null")
	types = slices.DeleteFunc(slices.Clone(types), func(t string) bool { return t == "null" })
	if len(types) != 1 {
		g.fail("%s: type %v has no recipe", path, s.types())
		return goType{expr: "json.RawMessage", nilable: true}
	}
	var t goType
	switch types[0] {
	case "string":
		t = goType{expr: "string", stringish: true}
		if secretFields[field] {
			t.expr = "carbium.Secret"
		}
	case "integer":
		t = goType{expr: "int64"}
	case "number":
		t = goType{expr: "float64"}
	case "boolean":
		t = goType{expr: "bool"}
	case "array":
		if s.Items == nil {
			g.fail("%s: array without items", path)
			return goType{}
		}
		it := g.typ(s.Items, path+"[]", singular(hint), "")
		t = goType{expr: "[]" + it.expr, nilable: true}
	case "object":
		if s.Properties != nil && len(s.Properties.Names) > 0 {
			t = g.structDecl(s, path, hint)
			break
		}
		if len(s.Additional) > 0 {
			var ap Schema
			if err := json.Unmarshal(s.Additional, &ap); err != nil {
				g.fail("%s: %v", path, err)
			}
			v := g.typ(&ap, path+"{}", hint+"Value", "")
			t = goType{expr: "map[string]" + v.expr, nilable: true}
			break
		}
		g.fail("%s: an object with neither properties nor additionalProperties", path)
		return goType{expr: "json.RawMessage", nilable: true}
	default:
		g.fail("%s: type %s has no recipe", path, types[0])
		return goType{}
	}
	if nullable && !t.nilable {
		t = goType{expr: "*" + t.expr, nilable: true}
	}
	return t
}

// allOf merges its parts into one object; a later part's property wins, and required keys add up.
func (g *gen) allOf(s *Schema, path, hint string) goType {
	merged := &Schema{Type: json.RawMessage(`"object"`), Properties: &Props{By: map[string]*Schema{}}}
	for _, part := range s.AllOf {
		p := part
		if part.Ref != "" {
			p = g.schemas[refName(part.Ref)]
		}
		if p == nil || p.Properties == nil {
			g.fail("%s: an allOf part without properties", path)
			continue
		}
		for _, n := range p.Properties.Names {
			if _, seen := merged.Properties.By[n]; !seen {
				merged.Properties.Names = append(merged.Properties.Names, n)
			}
			merged.Properties.By[n] = p.Properties.By[n]
		}
		for _, r := range p.Required {
			if !slices.Contains(merged.Required, r) {
				merged.Required = append(merged.Required, r)
			}
		}
	}
	return g.structDecl(merged, path, hint)
}

func (g *gen) structDecl(s *Schema, path, hint string) goType {
	if _, done := g.decls[hint]; done {
		if g.sides[hint] != g.request {
			g.fail("%s: type %s is both a request and an answer", path, hint)
		}
		return goType{expr: hint, isStruct: true}
	}
	g.sides[hint] = g.request
	g.decls[hint] = "" // reserved: a type may refer to itself
	g.order = append(g.order, hint)
	var b strings.Builder
	fmt.Fprintf(&b, "// %s is the description's %s.\ntype %s struct {\n", hint, path, hint)
	var nilSlices []string // required slices of a request: sent as [] when nil, since the service refuses null
	for _, name := range s.Properties.Names {
		sub := s.Properties.By[name]
		t := g.typ(sub, path+"."+name, hint+pascal(name), name)
		expr, tag := t.expr, name
		if !slices.Contains(s.Required, name) {
			tag += ",omitempty"
			if !t.nilable && (!t.stringish || g.request) {
				expr = "*" + expr
			}
		} else if g.request && strings.HasPrefix(expr, "[]") {
			nilSlices = append(nilSlices, pascal(name)+"\x00"+expr)
		}
		comment := ""
		if len(sub.Enum) > 0 {
			var vals []string
			for _, e := range sub.Enum {
				vals = append(vals, strings.Trim(string(e), `"`))
			}
			comment = " // one of: " + strings.Join(vals, ", ")
		}
		if sub.Items != nil && len(sub.Items.Enum) > 0 {
			var vals []string
			for _, e := range sub.Items.Enum {
				vals = append(vals, strings.Trim(string(e), `"`))
			}
			comment = " // each one of: " + strings.Join(vals, ", ")
		}
		fmt.Fprintf(&b, "\t%s %s `json:%q`%s\n", pascal(name), expr, tag, comment)
	}
	b.WriteString("}\n")
	if len(nilSlices) > 0 {
		fmt.Fprintf(&b, "\n// MarshalJSON sends a nil list as [].\nfunc (r %s) MarshalJSON() ([]byte, error) {\n\ttype plain %s\n", hint, hint)
		for _, f := range nilSlices {
			parts := strings.SplitN(f, "\x00", 2)
			fmt.Fprintf(&b, "\tif r.%s == nil {\n\t\tr.%s = %s{}\n\t}\n", parts[0], parts[0], parts[1])
		}
		fmt.Fprintf(&b, "\treturn json.Marshal(plain(r))\n}\n")
	}
	g.decls[hint] = b.String()
	return goType{expr: hint, isStruct: true}
}

func (g *gen) method(w *strings.Builder, m mapping, op *Op) {
	var args []string
	pathExpr := `"` + op.path + `"`
	var queryLines []string
	queryType := ""
	for _, p := range op.Parameters {
		switch p.In {
		case "path":
			arg := lower(pascal(p.Name))
			t := g.typ(p.Schema, m.op+"("+p.Name+")", "", "")
			args = append(args, arg+" "+t.expr)
			conv := "url.PathEscape(" + arg + ")"
			if t.expr == "int64" {
				conv = "strconv.FormatInt(" + arg + ", 10)"
			}
			pathExpr = strings.Replace(pathExpr, "{"+p.Name+"}", `" + `+conv+` + "`, 1)
		case "query":
			if queryType == "" {
				queryType = m.client + m.group + m.name + "Query"
				if m.client == "Client" {
					queryType = m.group + m.name + "Query"
				}
			}
			t := g.typ(p.Schema, m.op+"("+p.Name+")", "", "")
			field := pascal(p.Name)
			switch t.expr {
			case "string":
				queryLines = append(queryLines, fmt.Sprintf("\t\tif q.%s != \"\" {\n\t\t\tv.Set(%q, q.%s)\n\t\t}", field, p.Name, field))
			case "int64":
				queryLines = append(queryLines, fmt.Sprintf("\t\tif q.%s != nil {\n\t\t\tv.Set(%q, strconv.FormatInt(*q.%s, 10))\n\t\t}", field, p.Name, field))
			default:
				g.fail("%s: query param %s of type %s has no recipe", m.op, p.Name, t.expr)
			}
			if _, done := g.decls[queryType]; !done {
				g.decls[queryType] = ""
				g.order = append(g.order, queryType)
			}
			expr := t.expr
			if expr == "int64" {
				expr = "*int64"
			}
			g.decls[queryType] = strings.TrimSuffix(g.decls[queryType], "}\n")
			if g.decls[queryType] == "" {
				g.decls[queryType] = fmt.Sprintf("// %s holds the optional query of %s %s.\ntype %s struct {\n", queryType, op.verb, op.path, queryType)
			}
			g.decls[queryType] += fmt.Sprintf("\t%s %s\n}\n", field, expr)
		default:
			g.fail("%s: param %s in %s has no recipe", m.op, p.Name, p.In)
		}
	}
	pathExpr = strings.TrimSuffix(pathExpr, ` + ""`)
	if queryType != "" {
		args = append(args, "q "+queryType)
	}
	body := "nil"
	if op.RequestBody != nil {
		js, ok := op.RequestBody.Content["application/json"]
		if !ok || js.Schema == nil {
			g.fail("%s: a request body that is not JSON", m.op)
		} else {
			g.request = true
			t := g.typ(js.Schema, m.op+"(body)", pascal(refName(js.Schema.Ref)), "")
			g.request = false
			args = append(args, "req "+t.expr)
			body = "req"
		}
	}
	result := ""
	var resultType goType
	if r, ok := op.Responses["200"]; ok {
		js := r.Content["application/json"]
		s := js.Schema
		if m.result != "" {
			s = &Schema{Ref: "#/components/schemas/" + m.result, raw: []byte(`{"$ref":""}`)}
			if js.Schema == nil || len(js.Schema.OneOf) == 0 || !oneOfHas(js.Schema, m.result) {
				g.fail("%s: result override %s is not one of the answers", m.name, m.result)
			}
		}
		if s == nil {
			g.fail("%s: a 200 answer without a JSON schema", m.op)
		} else {
			resultType = g.typ(s, m.op+"->", pascal(m.name)+"Result", "")
			result = resultType.expr
		}
	} else if _, ok := op.Responses["204"]; !ok {
		g.fail("%s: neither a 200 nor a 204 answer", m.op)
	}
	recv, doer := "", ""
	switch {
	case m.client == "Public":
		args = append(args, "opts *Options")
		doer = "d, err := public(opts)\n\tif err != nil {\n\t\treturn %ZERO%err\n\t}\n"
	case m.client == "Session":
		recv = "(s *Session) "
		doer = "d := s.d\n"
	case m.group == "":
		recv = "(c *Client) "
		doer = "d := c.d\n"
	default:
		recv = "(g *" + groupType(m.group) + ") "
		doer = "d := g.c.d\n"
	}
	sig := strings.Join(append([]string{"ctx context.Context"}, args...), ", ")
	ret, zero := "error", ""
	switch {
	case result == "":
	case resultType.isStruct:
		ret, zero = "(*"+result+", error)", "nil, "
	default:
		ret, zero = "("+result+", error)", "nil, "
	}
	doer = strings.ReplaceAll(doer, "%ZERO%", zero)
	fmt.Fprintf(w, "// %s calls %s %s", m.name, op.verb, op.path)
	if op.Scope != "" {
		fmt.Fprintf(w, " (%s)", op.Scope)
	}
	w.WriteString(".")
	if m.slow {
		w.WriteString(" It waits at least 25 s for the answer.")
	}
	w.WriteString("\nfunc " + recv + m.name + "(" + sig + ") " + ret + " {\n\t" + doer)
	if m.step != "" {
		fmt.Fprintf(w, "\treq.Step = %q\n", m.step)
	}
	timeout := "0"
	if m.slow {
		timeout = "slowCall"
	}
	query := "nil"
	if queryType != "" {
		w.WriteString("\tv := url.Values{}\n\t{\n" + strings.Join(queryLines, "\n") + "\n\t}\n")
		query = "v"
	}
	label := m.name
	if m.group != "" {
		label = strings.ToLower(m.group[:1]) + m.group[1:] + "." + m.name
	}
	switch {
	case result == "":
		fmt.Fprintf(w, "\t_, err := send[json.RawMessage](ctx, d, %q, http.Method%s, %s, %s, %s, %s)\n\treturn err\n}\n\n", label, verbName(op.verb), pathExpr, query, body, timeout)
	case resultType.isStruct:
		fmt.Fprintf(w, "\tout, err := send[%s](ctx, d, %q, http.Method%s, %s, %s, %s, %s)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\treturn &out, nil\n}\n\n", result, label, verbName(op.verb), pathExpr, query, body, timeout)
	default:
		fmt.Fprintf(w, "\treturn send[%s](ctx, d, %q, http.Method%s, %s, %s, %s, %s)\n}\n\n", result, label, verbName(op.verb), pathExpr, query, body, timeout)
	}
}

func oneOfHas(s *Schema, name string) bool {
	for _, b := range s.OneOf {
		if refName(b.Ref) == name {
			return true
		}
	}
	return false
}

func verbName(v string) string {
	return map[string]string{"GET": "Get", "POST": "Post", "PATCH": "Patch", "PUT": "Put", "DELETE": "Delete"}[v]
}

var initialisms = map[string]string{"id": "ID", "url": "URL", "api": "API", "ip": "IP", "ms": "Ms", "rps": "RPS", "pdf": "PDF"}

func pascal(s string) string {
	var b strings.Builder
	var word []rune
	flush := func() {
		if len(word) == 0 {
			return
		}
		w := string(word)
		if v, ok := initialisms[strings.ToLower(w)]; ok {
			b.WriteString(v)
		} else {
			b.WriteString(strings.ToUpper(w[:1]) + w[1:])
		}
		word = nil
	}
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush()
			continue
		}
		if unicode.IsUpper(r) && len(word) > 0 {
			flush()
		}
		word = append(word, r)
	}
	flush()
	return b.String()
}

func lower(s string) string {
	if s == "" {
		return s
	}
	if strings.ToUpper(s) == s {
		return strings.ToLower(s)
	}
	return strings.ToLower(s[:1]) + s[1:]
}

func singular(name string) string {
	switch {
	case name == "":
		return ""
	case strings.HasSuffix(name, "ies"):
		return strings.TrimSuffix(name, "ies") + "y"
	case strings.HasSuffix(name, "s") && !strings.HasSuffix(name, "ss"):
		return strings.TrimSuffix(name, "s")
	}
	return name + "Item"
}
