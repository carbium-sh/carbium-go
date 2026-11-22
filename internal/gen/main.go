// Command gen writes the typed JSON-RPC surface of package carbium from the spec's openrpc.json: the result and
// config types, the method table and one typed method per served method. It refuses anything it cannot type: a
// number with no entry in the number table, an unlike anyOf with no recipe, an unknown schema keyword, a table entry
// that matches nothing, a name collision, or a method set that differs from served.json.
//
// Usage: go run ./internal/gen <openrpc.json> <served.json> <out.go>
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/format"
	"go/token"
	"os"
	"slices"
	"sort"
	"strings"
	"unicode"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: gen <openrpc.json> <served.json> <out.go>")
		os.Exit(2)
	}
	src, err := generate(os.Args[1], os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(os.Args[3], src, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
}

// Schema is the part of JSON Schema draft 07 the spec uses. Any other keyword is refused.
type Schema struct {
	Ref         string            `json:"$ref"`
	Type        json.RawMessage   `json:"type"`
	Enum        []json.RawMessage `json:"enum"`
	Const       json.RawMessage   `json:"const"`
	Properties  *Props            `json:"properties"`
	Required    []string          `json:"required"`
	Items       json.RawMessage   `json:"items"`
	MinItems    *int              `json:"minItems"`
	MaxItems    *int              `json:"maxItems"`
	AnyOf       []*Schema         `json:"anyOf"`
	Additional  json.RawMessage   `json:"additionalProperties"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	raw         json.RawMessage
}

var keywords = []string{"$ref", "type", "enum", "const", "properties", "required", "items", "minItems", "maxItems", "anyOf", "additionalProperties", "title", "description"}

func (s *Schema) UnmarshalJSON(b []byte) error {
	type plain Schema
	var p plain
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(b, &keys); err != nil {
		return err
	}
	for k := range keys {
		if !slices.Contains(keywords, k) {
			return fmt.Errorf("unknown schema keyword %q", k)
		}
	}
	*s = Schema(p)
	s.raw = append(json.RawMessage(nil), b...)
	return nil
}

// Props keeps the properties in the spec's order, so struct fields follow it.
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
		t, err := dec.Token()
		if err != nil {
			return err
		}
		name := t.(string)
		var s Schema
		if err := dec.Decode(&s); err != nil {
			return fmt.Errorf("property %s: %w", name, err)
		}
		p.Names = append(p.Names, name)
		p.By[name] = &s
	}
	return nil
}

type Param struct {
	Name     string  `json:"name"`
	Required bool    `json:"required"`
	Schema   *Schema `json:"schema"`
}

type Method struct {
	Name           string                  `json:"name"`
	Tags           []struct{ Name string } `json:"tags"`
	Servers        []json.RawMessage       `json:"servers"`
	ParamStructure string                  `json:"paramStructure"`
	Params         []Param                 `json:"params"`
	Result         struct {
		Schema *Schema `json:"schema"`
	} `json:"result"`
	Batch  string `json:"x-batch"`
	Permit *bool  `json:"x-permit"`
}

type Doc struct {
	Methods    []Method `json:"methods"`
	Components struct {
		Schemas json.RawMessage `json:"schemas"`
	} `json:"components"`
}

// goType is what a schema becomes in Go.
type goType struct {
	expr      string // the Go type expression
	nilable   bool   // nil stands for null or absent (pointer, slice, map, raw JSON)
	stringish bool   // a string or string enum: optional means empty, no pointer needed
	isStruct  bool   // a named struct: returned and optional as a pointer
}

type gen struct {
	schemas   map[string]*Schema
	order     []string
	decls     map[string]string // Go type name -> declaration source
	declOrder []string
	declShape map[string]string // Go type name -> canonical shape, to catch two shapes on one name
	usedNum   map[string]bool
	usedOver  map[string]bool
	usedName  map[string]bool
	errs      []string
}

func (g *gen) fail(format string, args ...any) { g.errs = append(g.errs, fmt.Sprintf(format, args...)) }

func generate(specPath, servedPath string) ([]byte, error) {
	b, err := os.ReadFile(specPath)
	if err != nil {
		return nil, err
	}
	var doc Doc
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("%s: %w", specPath, err)
	}
	var schemas Props
	if err := json.Unmarshal(doc.Components.Schemas, &schemas); err != nil {
		return nil, fmt.Errorf("components.schemas: %w", err)
	}
	sb, err := os.ReadFile(servedPath)
	if err != nil {
		return nil, err
	}
	var served struct {
		RPC []string `json:"rpc"`
		WS  []string `json:"ws"`
	}
	if err := json.Unmarshal(sb, &served); err != nil {
		return nil, fmt.Errorf("%s: %w", servedPath, err)
	}

	g := &gen{schemas: schemas.By, order: schemas.Names, decls: map[string]string{}, declShape: map[string]string{},
		usedNum: map[string]bool{}, usedOver: map[string]bool{}, usedName: map[string]bool{}}

	// Every component first, in spec order, so the WebSocket package can use the notification types too.
	for _, name := range g.order {
		if _, hand := handTypes[name]; hand {
			g.named(name)
			continue
		}
		if shp, ok := socketOnly[name]; ok {
			g.checkShape("socket-only type "+name, g.schemas[name], shp)
			continue
		}
		g.named(name)
	}
	g.checkSocketOnly(doc)

	var rpc, subs []Method
	for _, m := range doc.Methods {
		if len(m.Servers) > 0 {
			if strings.HasSuffix(m.Name, "Subscribe") {
				subs = append(subs, m)
			}
			continue
		}
		rpc = append(rpc, m)
	}
	sort.Slice(rpc, func(i, j int) bool { return rpc[i].Name < rpc[j].Name })

	var names []string
	for _, m := range rpc {
		names = append(names, m.Name)
	}
	g.sameSet("HTTP methods", names, served.RPC)
	var subNames []string
	for _, m := range subs {
		subNames = append(subNames, m.Name)
	}
	sort.Strings(subNames)
	g.sameSet("subscriptions", subNames, served.WS)

	var methodsSrc, tableSrc strings.Builder
	goNames := map[string]string{}
	for _, m := range rpc {
		goName := pascal(m.Name)
		if other, dup := goNames[goName]; dup {
			g.fail("%s and %s both become %s", other, m.Name, goName)
		}
		goNames[goName] = m.Name
		g.method(&methodsSrc, m, goName)
		plane := ""
		if len(m.Tags) == 1 {
			plane = m.Tags[0].Name
		}
		if !slices.Contains([]string{"rpc", "das", "quote"}, plane) {
			g.fail("%s: plane tag %q is not rpc, das or quote", m.Name, plane)
		}
		if !slices.Contains([]string{"any", "das", "never"}, m.Batch) {
			g.fail("%s: x-batch %q is not any, das or never", m.Name, m.Batch)
		}
		if m.Permit == nil {
			g.fail("%s: no x-permit", m.Name)
		}
		byName := map[string]string{"by-position": "false", "by-name": "true"}[m.ParamStructure]
		if byName == "" {
			g.fail("%s: paramStructure %q is not by-position or by-name", m.Name, m.ParamStructure)
		}
		fmt.Fprintf(&tableSrc, "\t%q: {Plane: %q, ByName: %s, Batch: %q, Permit: %t, GoName: %q},\n",
			m.Name, plane, byName, m.Batch, m.Permit != nil && *m.Permit, goName)
	}

	for path := range numbers {
		if !g.usedNum[path] {
			g.fail("number table entry %q matches no number in the spec", path)
		}
	}
	for path := range overrides {
		if !g.usedOver[path] {
			g.fail("override %q matches no schema in the spec", path)
		}
	}
	for path := range typeNames {
		if !g.usedName[path] {
			g.fail("type name entry %q matches no inline schema in the spec", path)
		}
	}
	for name := range handTypes {
		if _, ok := g.schemas[name]; !ok {
			g.fail("hand-written type %s names no component schema", name)
		}
	}
	g.sameShapes()
	if len(g.errs) > 0 {
		sort.Strings(g.errs)
		g.errs = slices.Compact(g.errs)
		return nil, fmt.Errorf("%d problems:\n  %s", len(g.errs), strings.Join(g.errs, "\n  "))
	}

	var out strings.Builder
	out.WriteString("// methodTable holds every served JSON-RPC method and nothing else.\nvar methodTable = map[string]MethodSpec{\n")
	out.WriteString(tableSrc.String())
	out.WriteString("}\n\n// subscriptionNames holds every served WebSocket subscription.\nvar subscriptionNames = []string{")
	for i, n := range subNames {
		if i > 0 {
			out.WriteString(", ")
		}
		fmt.Fprintf(&out, "%q", n)
	}
	out.WriteString("}\n\n")
	for _, name := range g.declOrder {
		out.WriteString(g.decls[name])
		out.WriteString("\n")
	}
	out.WriteString(methodsSrc.String())
	var head strings.Builder
	head.WriteString("// Code generated by internal/gen from spec/openrpc.json; DO NOT EDIT.\n\npackage carbium\n\nimport (\n")
	for _, imp := range []string{"context", "encoding/json", "fmt"} {
		if strings.Contains(out.String(), imp[strings.LastIndex(imp, "/")+1:]+".") {
			fmt.Fprintf(&head, "\t%q\n", imp)
		}
	}
	head.WriteString(")\n\n")
	src, err := format.Source([]byte(head.String() + out.String()))
	if err != nil {
		return nil, fmt.Errorf("gofmt of the output: %w", err)
	}
	return src, nil
}

func (g *gen) sameSet(what string, got, want []string) {
	w := map[string]bool{}
	for _, n := range want {
		w[n] = true
	}
	gotSet := map[string]bool{}
	for _, n := range got {
		gotSet[n] = true
		if !w[n] {
			g.fail("%s: %s is in the spec but not in served.json", what, n)
		}
	}
	for _, n := range want {
		if !gotSet[n] {
			g.fail("%s: %s is in served.json but not in the spec", what, n)
		}
	}
}

func refName(ref string) string {
	const prefix = "#/components/schemas/"
	if !strings.HasPrefix(ref, prefix) {
		return ""
	}
	return strings.TrimPrefix(ref, prefix)
}

func (s *Schema) types() []string {
	if len(s.Type) == 0 {
		return nil
	}
	var one string
	if json.Unmarshal(s.Type, &one) == nil {
		return []string{one}
	}
	var many []string
	_ = json.Unmarshal(s.Type, &many)
	return many
}

func (s *Schema) isNull() bool {
	t := s.types()
	return len(t) == 1 && t[0] == "null" && s.Properties == nil
}

func (s *Schema) empty() bool { return string(bytes.TrimSpace(s.raw)) == "{}" }

// shape is a canonical form of a schema, to compare two schemas.
func shape(s *Schema) string {
	var v any
	_ = json.Unmarshal(s.raw, &v)
	b, _ := json.Marshal(canon(v))
	return string(b)
}

func canon(v any) any {
	switch t := v.(type) {
	case map[string]any:
		delete(t, "description")
		delete(t, "title")
		for k, x := range t {
			t[k] = canon(x)
		}
		if r, ok := t["required"].([]any); ok {
			sort.Slice(r, func(i, j int) bool { return fmt.Sprint(r[i]) < fmt.Sprint(r[j]) })
		}
		return t
	case []any:
		for i, x := range t {
			t[i] = canon(x)
		}
	}
	return v
}

// named returns the Go type of a component schema, declaring it on first use.
func (g *gen) named(name string) goType {
	if h, ok := handTypes[name]; ok {
		g.checkShape("hand-written type "+name, g.schemas[name], h.shape)
		return h.goType
	}
	s, ok := g.schemas[name]
	if !ok {
		g.fail("$ref to unknown schema %s", name)
		return goType{expr: "json.RawMessage", nilable: true}
	}
	return g.typ(s, name, pascal(name))
}

// typ maps a schema at a spec path to its Go type. hint names a type the schema may need to declare.
func (g *gen) typ(s *Schema, path, hint string) goType {
	if o, ok := overrides[path]; ok {
		g.usedOver[path] = true
		g.checkShape("override "+path, s, o.shape)
		return o.goType
	}
	if n, ok := typeNames[path]; ok {
		g.usedName[path] = true
		hint = n
	}
	if s.Ref != "" {
		name := refName(s.Ref)
		if name == "" {
			g.fail("%s: $ref %q is not a component", path, s.Ref)
			return goType{}
		}
		return g.named(name)
	}
	if s.empty() {
		return goType{expr: "json.RawMessage", nilable: true}
	}
	if len(s.AnyOf) > 0 {
		return g.anyOf(s, path, hint)
	}
	types := s.types()
	nullable := slices.Contains(types, "null")
	types = slices.DeleteFunc(slices.Clone(types), func(t string) bool { return t == "null" })
	var t goType
	switch {
	case len(types) == 2 && slices.Contains(types, "number") && slices.Contains(types, "string") && len(s.Enum) == 0:
		// A number or a decimal string: sent as a decimal string and read from either, so no u64 is rounded.
		t = goType{expr: "Decimal", stringish: true}
	case len(types) != 1:
		g.fail("%s: type %v has no recipe", path, s.types())
		return goType{expr: "json.RawMessage", nilable: true}
	default:
		t = g.single(s, types[0], path, hint)
	}
	if nullable && !t.nilable {
		t = goType{expr: "*" + t.expr, nilable: true}
	}
	return t
}

func (g *gen) single(s *Schema, kind, path, hint string) goType {
	switch kind {
	case "string":
		if len(s.Enum) > 0 {
			return g.enum(s, path, hint)
		}
		return goType{expr: "string", stringish: true}
	case "number", "integer":
		g.usedNum[path] = true
		n, ok := numbers[path]
		if !ok {
			g.fail("%s: number with no entry in the number table", path)
			return goType{expr: "float64"}
		}
		return goType{expr: n}
	case "boolean":
		return goType{expr: "bool"}
	case "array":
		return g.array(s, path, hint)
	case "object":
		if s.Properties != nil && len(s.Properties.Names) > 0 {
			return g.object(s, path, hint)
		}
		if len(s.Additional) > 0 {
			var ap Schema
			if err := json.Unmarshal(s.Additional, &ap); err != nil {
				g.fail("%s: additionalProperties: %v", path, err)
				return goType{}
			}
			v := g.typ(&ap, path+"{}", hint+"Value")
			return goType{expr: "map[string]" + v.expr, nilable: true}
		}
		g.fail("%s: an object with neither properties nor additionalProperties", path)
		return goType{expr: "json.RawMessage", nilable: true}
	}
	g.fail("%s: type %q has no recipe", path, kind)
	return goType{expr: "json.RawMessage", nilable: true}
}

func (g *gen) enum(s *Schema, path, hint string) goType {
	var values []string
	for _, raw := range s.Enum {
		var v string
		if json.Unmarshal(raw, &v) != nil {
			g.fail("%s: enum value %s is not a string", path, raw)
			return goType{}
		}
		values = append(values, v)
	}
	return g.enumOf(values, s, path, hint)
}

// enumOf declares a string type with one constant per value. A value the type does not list still decodes.
func (g *gen) enumOf(values []string, s *Schema, path, hint string) goType {
	var b strings.Builder
	fmt.Fprintf(&b, "// %s is one of: %s. A value not listed here is kept as sent.\ntype %s string\n\nconst (\n", hint, strings.Join(values, ", "), hint)
	seen := map[string]bool{}
	for _, v := range values {
		c := hint + pascal(v)
		if seen[c] {
			g.fail("%s: enum constants repeat as %s", path, c)
		}
		seen[c] = true
		fmt.Fprintf(&b, "\t%s %s = %q\n", c, hint, v)
	}
	b.WriteString(")\n")
	g.declare(hint, shape(s), b.String(), path)
	return goType{expr: hint, stringish: true}
}

func (g *gen) array(s *Schema, path, hint string) goType {
	if len(s.Items) == 0 {
		g.fail("%s: array without items", path)
		return goType{}
	}
	if bytes.TrimSpace(s.Items)[0] == '[' {
		var items []*Schema
		if err := json.Unmarshal(s.Items, &items); err != nil {
			g.fail("%s: items: %v", path, err)
			return goType{}
		}
		if s.MinItems == nil || s.MaxItems == nil || *s.MinItems != len(items) || *s.MaxItems != len(items) {
			g.fail("%s: a fixed array needs minItems = maxItems = its item count", path)
			return goType{}
		}
		return g.tuple(s, items, path, hint)
	}
	var item Schema
	if err := json.Unmarshal(s.Items, &item); err != nil {
		g.fail("%s: items: %v", path, err)
		return goType{}
	}
	t := g.typ(&item, path+"[]", singular(hint))
	if s.MinItems != nil || s.MaxItems != nil {
		// Only logsSubscribe's mentions carries bounds (one address); the type stays a slice.
		if s.MinItems == nil || s.MaxItems == nil || *s.MinItems != 1 || *s.MaxItems != 1 {
			g.fail("%s: array bounds %v..%v have no recipe", path, s.MinItems, s.MaxItems)
		}
	}
	return goType{expr: "[]" + t.expr, nilable: true}
}

// tuple declares a fixed-length JSON array as a struct whose fields take the items' titles, or as a Go array when
// the items have no titles and share one type.
func (g *gen) tuple(s *Schema, items []*Schema, path, hint string) goType {
	titled := true
	for _, it := range items {
		if it.Title == "" {
			titled = false
		}
	}
	types := make([]goType, len(items))
	for i, it := range items {
		types[i] = g.typ(it, fmt.Sprintf("%s[%d]", path, i), hint+pascal(it.Title))
	}
	if !titled {
		for _, t := range types[1:] {
			if t.expr != types[0].expr {
				g.fail("%s: a fixed array without item titles mixes types", path)
				return goType{}
			}
		}
		return goType{expr: fmt.Sprintf("[%d]%s", len(items), types[0].expr)}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "// %s travels as a JSON array of %d items; items after them are ignored.\ntype %s struct {\n", hint, len(items), hint)
	fields := make([]string, len(items))
	for i, it := range items {
		fields[i] = pascal(it.Title)
		fmt.Fprintf(&b, "\t%s %s\n", fields[i], types[i].expr)
	}
	b.WriteString("}\n\n")
	fmt.Fprintf(&b, "func (t %s) MarshalJSON() ([]byte, error) {\n\treturn json.Marshal([%d]any{", hint, len(items))
	for i, f := range fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("t." + f)
	}
	b.WriteString("})\n}\n\n")
	fmt.Fprintf(&b, "func (t *%s) UnmarshalJSON(b []byte) error {\n\tif string(b) == \"null\" {\n\t\treturn nil\n\t}\n", hint)
	b.WriteString("\tvar a []json.RawMessage\n\tif err := json.Unmarshal(b, &a); err != nil {\n\t\treturn err\n\t}\n")
	fmt.Fprintf(&b, "\tif len(a) < %d {\n\t\treturn fmt.Errorf(\"carbium: %s: %%d items, want at least %d\", len(a))\n\t}\n", len(items), hint, len(items))
	for i, f := range fields {
		fmt.Fprintf(&b, "\tif err := json.Unmarshal(a[%d], &t.%s); err != nil {\n\t\treturn err\n\t}\n", i, f)
	}
	b.WriteString("\treturn nil\n}\n")
	g.declare(hint, shape(s), b.String(), path)
	return goType{expr: hint, isStruct: true}
}

// anyOf handles "X or null", a string or a list of strings, and objects told apart by their keys (merged into one
// struct). Every other union needs a hand-written type in the override table.
func (g *gen) anyOf(s *Schema, path, hint string) goType {
	var branches []*Schema
	nullable := false
	for _, b := range s.AnyOf {
		if b.isNull() {
			nullable = true
			continue
		}
		branches = append(branches, b)
	}
	var t goType
	switch {
	case len(branches) == 1:
		t = g.typ(branches[0], path, hint)
	case len(branches) == 2 && isPlainString(branches[0]) && isStringList(branches[1]):
		t = goType{expr: "[]string", nilable: true}
	case g.allStringValues(branches) != nil:
		t = g.enumOf(g.allStringValues(branches), s, path, hint)
	default:
		objects := make([]*Schema, 0, len(branches))
		for _, b := range branches {
			r := b
			if b.Ref != "" {
				r = g.schemas[refName(b.Ref)]
			}
			if r == nil || r.Properties == nil || len(r.AnyOf) > 0 {
				g.fail("%s: an anyOf of unlike types has no recipe: add a hand-written type to the override table", path)
				return goType{expr: "json.RawMessage", nilable: true}
			}
			objects = append(objects, r)
		}
		t = g.merged(s, objects, path, hint)
	}
	if nullable && !t.nilable {
		t = goType{expr: "*" + t.expr, nilable: true}
	}
	return t
}

func isPlainString(s *Schema) bool {
	t := s.types()
	return s.Ref == "" && len(t) == 1 && t[0] == "string" && len(s.Enum) == 0 && len(s.Const) == 0
}

func isStringList(s *Schema) bool {
	t := s.types()
	if s.Ref != "" || len(t) != 1 || t[0] != "array" || len(s.Items) == 0 {
		return false
	}
	var item Schema
	return json.Unmarshal(s.Items, &item) == nil && isPlainString(&item)
}

// merged declares one struct for objects told apart by their keys: a key is required only when every branch
// requires it, and two branches must give a shared key the same Go type.
func (g *gen) merged(s *Schema, objects []*Schema, path, hint string) goType {
	union := &Schema{Properties: &Props{By: map[string]*Schema{}}, Type: json.RawMessage(`"object"`)}
	requiredIn := map[string]int{}
	types := map[string]string{}
	var keySets []string
	for i, o := range objects {
		keySets = append(keySets, "{"+strings.Join(o.Required, ", ")+"}")
		for _, name := range o.Properties.Names {
			sub := o.Properties.By[name]
			t := g.typ(sub, fmt.Sprintf("%s|%d.%s", path, i, name), hint+pascal(name))
			if prev, seen := types[name]; seen {
				if prev != t.expr {
					g.fail("%s: key %s is %s in one branch and %s in another", path, name, prev, t.expr)
				}
				continue
			}
			types[name] = t.expr
			union.Properties.Names = append(union.Properties.Names, name)
			union.Properties.By[name] = sub
		}
		for _, r := range o.Required {
			requiredIn[r]++
		}
	}
	for name, n := range requiredIn {
		if n == len(objects) {
			union.Required = append(union.Required, name)
		}
	}
	sort.Strings(union.Required)
	return g.structDecl(union, path, hint, objects, "One of these shapes: "+strings.Join(keySets, " or ")+".")
}

func (g *gen) object(s *Schema, path, hint string) goType {
	if strings.HasPrefix(hint, "WithContext") && s.Properties != nil && len(s.Properties.Names) == 2 &&
		s.Properties.By["context"] != nil && refName(s.Properties.By["context"].Ref) == "Context" && s.Properties.By["value"] != nil &&
		slices.Contains(s.Required, "context") && slices.Contains(s.Required, "value") {
		inner := strings.TrimPrefix(hint, "WithContext")
		v := g.typ(s.Properties.By["value"], path+".value", inner)
		return goType{expr: "WithContext[" + v.expr + "]", isStruct: true}
	}
	return g.structDecl(s, path, hint, []*Schema{s}, "")
}

func (g *gen) structDecl(s *Schema, path, hint string, branches []*Schema, note string) goType {
	if prev, taken := g.declShape[hint]; taken {
		if prev != shape(s) {
			g.fail("%s: type name %s is already taken by another shape", path, hint)
		}
		return goType{expr: hint, isStruct: true}
	}
	required := map[string]bool{}
	for _, r := range s.Required {
		required[r] = true
	}
	var b strings.Builder
	if note == "" {
		fmt.Fprintf(&b, "// %s is sent or received as the spec's %s.\n", hint, displayPath(path))
	} else {
		fmt.Fprintf(&b, "// %s is sent or received as the spec's %s. %s\n", hint, displayPath(path), note)
	}
	if s.Additional != nil && string(bytes.TrimSpace(s.Additional)) == "{}" {
		b.WriteString("// The service may add keys; ones not listed here are dropped.\n")
	}
	fmt.Fprintf(&b, "type %s struct {\n", hint)
	fieldNames := map[string]string{}
	for _, name := range s.Properties.Names {
		sub := s.Properties.By[name]
		subPath := path + "." + name
		if len(branches) > 1 {
			for i, o := range branches {
				if o.Properties.By[name] != nil {
					subPath = fmt.Sprintf("%s|%d.%s", path, i, name)
					break
				}
			}
		}
		t := g.typ(sub, subPath, hint+pascal(name))
		field := pascal(name)
		if other, dup := fieldNames[field]; dup {
			g.fail("%s: keys %s and %s both become field %s", path, other, name, field)
		}
		fieldNames[field] = name
		expr, tag := t.expr, name
		if !required[name] {
			tag += ",omitempty"
			if !t.nilable && !t.stringish {
				expr = "*" + expr
			}
		}
		fmt.Fprintf(&b, "\t%s %s `json:%q`\n", field, expr, tag)
	}
	b.WriteString("}\n")
	g.declare(hint, shape(s), b.String(), path)
	return goType{expr: hint, isStruct: true}
}

func displayPath(p string) string { return strings.ReplaceAll(p, "|", " branch ") }

func (g *gen) declare(name, shp, src, path string) {
	if prev, ok := g.declShape[name]; ok {
		if prev != shp {
			g.fail("%s: type name %s is already taken by another shape", path, name)
		}
		return
	}
	if token.Lookup(strings.ToLower(name)).IsKeyword() {
		g.fail("%s: type name %s is a Go keyword", path, name)
	}
	g.declShape[name] = shp
	g.decls[name] = src
	g.declOrder = append(g.declOrder, name)
}

func (g *gen) method(w *strings.Builder, m Method, goName string) {
	resultHint := goName + "Result"
	r := g.typ(m.Result.Schema, m.Name+"->", resultHint)
	ret := r.expr
	if r.isStruct {
		ret = "*" + r.expr
	}
	var args, pass []string
	byName := m.ParamStructure == "by-name"
	if byName && len(m.Params) > 0 {
		params := &Schema{Type: json.RawMessage(`"object"`), Properties: &Props{By: map[string]*Schema{}}}
		for _, p := range m.Params {
			params.Properties.Names = append(params.Properties.Names, p.Name)
			params.Properties.By[p.Name] = p.Schema
			if p.Required {
				params.Required = append(params.Required, p.Name)
			}
		}
		obj := map[string]any{"type": "object", "properties": rawProps(params)}
		if len(params.Required) > 0 {
			obj["required"] = params.Required
		}
		params.raw, _ = json.Marshal(obj)
		name := goName + "Params"
		if n, ok := typeNames[m.Name+"()"]; ok {
			g.usedName[m.Name+"()"] = true
			name = n
		}
		if alias, ok := paramSchemas[m.Name]; ok {
			name = alias
		}
		var t goType
		if comp, ok := g.schemas[name]; ok {
			if shape(comp) != shape(params) {
				g.fail("%s: its params differ from the schema %s", m.Name, name)
			}
			t = g.named(name)
		} else {
			if _, ok := paramSchemas[m.Name]; ok {
				g.fail("%s: params schema %s does not exist", m.Name, name)
			}
			t = g.structDecl(params, m.Name+"()", name, []*Schema{params}, "")
		}
		args = append(args, "params "+t.expr)
		pass = append(pass, "params")
	} else if !byName {
		for _, p := range m.Params {
			arg := lowerFirst(pascal(p.Name))
			if token.Lookup(arg).IsKeyword() || slices.Contains([]string{"ctx", "c", "out", "err"}, arg) {
				g.fail("%s: param name %s collides", m.Name, p.Name)
			}
			t := g.typ(p.Schema, fmt.Sprintf("%s(%s)", m.Name, p.Name), goName+pascal(p.Name))
			expr := t.expr
			switch {
			case p.Required:
				pass = append(pass, arg)
			case t.nilable && strings.HasPrefix(expr, "[]"):
				pass = append(pass, "optSlice("+arg+")")
			case t.nilable && strings.HasPrefix(expr, "*"):
				pass = append(pass, "opt("+arg+")")
			case t.nilable:
				g.fail("%s: optional param %s of type %s has no recipe", m.Name, p.Name, expr)
			default:
				expr = "*" + expr
				pass = append(pass, "opt("+arg+")")
			}
			args = append(args, arg+" "+expr)
		}
	}
	params := "nil"
	if byName && len(pass) > 0 {
		params = pass[0]
	} else if !byName {
		params = "positional(" + strings.Join(pass, ", ") + ")"
		if len(pass) == 0 {
			params = "nil"
		}
	}
	sig := strings.Join(append([]string{"ctx context.Context"}, args...), ", ")
	fmt.Fprintf(w, "// %s calls %s.%s\n", goName, m.Name, methodNote(m))
	fmt.Fprintf(w, "func (c *Client) %s(%s) (%s, error) {\n", goName, sig, ret)
	if r.isStruct {
		fmt.Fprintf(w, "\tvar out %s\n\tif err := c.call(ctx, %q, %s, &out); err != nil {\n\t\treturn nil, err\n\t}\n\treturn &out, nil\n}\n\n", r.expr, m.Name, params)
	} else {
		fmt.Fprintf(w, "\tvar out %s\n\terr := c.call(ctx, %q, %s, &out)\n\treturn out, err\n}\n\n", r.expr, m.Name, params)
	}
}

func methodNote(m Method) string {
	var notes []string
	switch m.Batch {
	case "never":
		notes = append(notes, "It never shares a batch.")
	case "das":
		notes = append(notes, "It shares a batch only with other DAS methods.")
	}
	if m.Permit != nil && *m.Permit {
		notes = append(notes, "A batch holds at most two methods of its class.")
	}
	if m.Name == "sendTransaction" {
		notes = append(notes, "It relays a transaction the caller already signed, once: it is never retried.")
	}
	if m.Name == "getTransaction" || m.Name == "getBlock" {
		notes = append(notes, "maxSupportedTransactionVersion is sent as 1 unless config sets it.")
	}
	if len(notes) == 0 {
		return ""
	}
	return " " + strings.Join(notes, " ")
}

func rawProps(s *Schema) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for _, n := range s.Properties.Names {
		out[n] = s.Properties.By[n].raw
	}
	return out
}

var initialisms = map[string]string{"id": "ID", "ids": "IDs", "url": "URL", "api": "API", "rpc": "RPC", "json": "JSON", "ui": "UI", "tpu": "TPU", "usd": "USD", "nft": "NFT", "das": "DAS",
	"ok": "OK", "tvl": "TVL", "amm": "AMM", "quic": "QUIC"}

func words(s string) []string {
	var out []string
	var cur []rune
	rs := []rune(s)
	for i, r := range rs {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			if len(cur) > 0 {
				out = append(out, string(cur))
				cur = nil
			}
			continue
		}
		if unicode.IsUpper(r) && len(cur) > 0 && i > 0 && (unicode.IsLower(rs[i-1]) || unicode.IsDigit(rs[i-1])) {
			out = append(out, string(cur))
			cur = nil
		}
		cur = append(cur, r)
	}
	if len(cur) > 0 {
		out = append(out, string(cur))
	}
	return out
}

func pascal(s string) string {
	var b strings.Builder
	for _, w := range words(s) {
		if v, ok := initialisms[strings.ToLower(w)]; ok {
			b.WriteString(v)
			continue
		}
		r := []rune(w)
		b.WriteString(strings.ToUpper(string(r[0])) + string(r[1:]))
	}
	return b.String()
}

func lowerFirst(s string) string {
	for k, v := range initialisms {
		if strings.HasPrefix(s, v) && (len(s) == len(v) || unicode.IsUpper(rune(s[len(v)]))) {
			return k + s[len(v):]
		}
	}
	r := []rune(s)
	return strings.ToLower(string(r[0])) + string(r[1:])
}

func singular(name string) string {
	switch {
	case strings.HasSuffix(name, "ies"):
		return strings.TrimSuffix(name, "ies") + "y"
	case strings.HasSuffix(name, "ss"):
		return name + "Item"
	case strings.HasSuffix(name, "s"):
		return strings.TrimSuffix(name, "s")
	}
	return name + "Item"
}

// allStringValues returns the values of an anyOf whose every branch is a string enum or constant (directly or by
// $ref), or nil.
func (g *gen) allStringValues(branches []*Schema) []string {
	var out []string
	for _, b := range branches {
		r := b
		if b.Ref != "" {
			r = g.schemas[refName(b.Ref)]
		}
		if r == nil {
			return nil
		}
		t := r.types()
		if len(t) != 1 || t[0] != "string" {
			return nil
		}
		switch {
		case len(r.Const) > 0:
			var v string
			if json.Unmarshal(r.Const, &v) != nil {
				return nil
			}
			out = append(out, v)
		case len(r.Enum) > 0:
			for _, raw := range r.Enum {
				var v string
				if json.Unmarshal(raw, &v) != nil {
					return nil
				}
				out = append(out, v)
			}
		default:
			return nil
		}
	}
	return out
}

// shapeHash is the first 16 hex digits of the sha256 of a schema's canonical JSON.
func shapeHash(s *Schema) string {
	sum := sha256.Sum256([]byte(shape(s)))
	return hex.EncodeToString(sum[:8])
}

// checkShape fails when the spec's schema is not the one a hand-written type was written for.
func (g *gen) checkShape(what string, s *Schema, want string) {
	if s == nil {
		g.fail("%s: no schema in the spec", what)
		return
	}
	if got := shapeHash(s); got != want {
		g.fail("%s: the spec's shape is %s, the hand-written code was written for %q; update the code, then the table. Shape: %s", what, got, want, shape(s))
	}
}

// checkSocketOnly fails when a socket-only type is used by an HTTP method or a generated type.
func (g *gen) checkSocketOnly(doc Doc) {
	for name := range socketOnly {
		needle := []byte(`"#/components/schemas/` + name + `"`)
		for _, m := range doc.Methods {
			if len(m.Servers) > 0 {
				continue
			}
			for _, p := range m.Params {
				if bytes.Contains(p.Schema.raw, needle) {
					g.fail("socket-only type %s is used by %s", name, m.Name)
				}
			}
			if bytes.Contains(m.Result.Schema.raw, needle) {
				g.fail("socket-only type %s is used by %s", name, m.Name)
			}
		}
		for other, s := range g.schemas {
			if other != name && bytes.Contains(s.raw, needle) {
				if _, alsoSocket := socketOnly[other]; !alsoSocket {
					g.fail("socket-only type %s is used by %s", name, other)
				}
			}
		}
	}
}

// sameShapes fails when two declared structs have one shape: name them once in typeNames.
func (g *gen) sameShapes() {
	byShape := map[string][]string{}
	for _, name := range g.declOrder {
		if strings.Contains(g.decls[name], " struct {") {
			body := g.decls[name][strings.Index(g.decls[name], " struct {"):]
			byShape[body] = append(byShape[body], name)
		}
	}
	for _, names := range byShape {
		if len(names) > 1 {
			g.fail("types %s have one shape: give them one name in typeNames", strings.Join(names, ", "))
		}
	}
}
