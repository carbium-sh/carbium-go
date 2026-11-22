package carbium

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// handWritten are the exported methods of *Client that are not a served JSON-RPC method.
var handWritten = map[string]bool{"Call": true, "Batch": true, "Endpoint": true, "Format": true, "String": true, "GoString": true}

type servedFile struct {
	RPC  []string `json:"rpc"`
	WS   []string `json:"ws"`
	REST []string `json:"rest"`
}

func readServed(t *testing.T) servedFile {
	t.Helper()
	b, err := os.ReadFile("spec/served.json")
	if err != nil {
		t.Fatal(err)
	}
	var s servedFile
	if err := json.Unmarshal(b, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

// Every served method has a typed call and nothing else has one. Each typed call is made through the client's own
// path, and the method name it puts on the wire is read back from a planted server.
func TestEveryServedMethodHasATypedCallAndNothingElseDoes(t *testing.T) {
	served := readServed(t)
	isServed := map[string]bool{}
	for _, name := range served.RPC {
		isServed[name] = true
		if _, ok := methodTable[name]; !ok {
			t.Errorf("served but has no typed call: %s", name)
		}
	}
	for name := range methodTable {
		if !isServed[name] {
			t.Errorf("has a typed call but is not served: %s", name)
		}
	}

	p := plant(t)
	c, _ := testClient(t, p, nil)
	cv := reflect.ValueOf(c)
	byGoName := map[string]string{}
	for wire, spec := range methodTable {
		byGoName[spec.GoName] = wire
	}
	var exported []string
	for i := 0; i < cv.NumMethod(); i++ {
		exported = append(exported, cv.Type().Method(i).Name)
	}
	sort.Strings(exported)
	isREST := map[string]bool{}
	for _, goName := range restRoutes {
		isREST[goName] = true
	}
	for _, name := range exported {
		if !handWritten[name] && byGoName[name] == "" && !isREST[name] {
			t.Errorf("method %s is neither a served method nor a known helper", name)
		}
	}

	ctx := reflect.ValueOf(context.Background())
	for _, wire := range Methods() {
		spec := methodTable[wire]
		m := cv.MethodByName(spec.GoName)
		if !m.IsValid() {
			t.Errorf("%s: the table names %s, which *Client lacks", wire, spec.GoName)
			continue
		}
		args := []reflect.Value{ctx}
		for i := 1; i < m.Type().NumIn(); i++ {
			args = append(args, reflect.Zero(m.Type().In(i)))
		}
		before := len(p.seen())
		out := m.Call(args)
		if err, _ := out[len(out)-1].Interface().(error); err != nil {
			t.Errorf("%s: %v", spec.GoName, err)
			continue
		}
		got := p.seen()[before:]
		if len(got) != 1 {
			t.Errorf("%s made %d requests, want 1", spec.GoName, len(got))
			continue
		}
		if sent := got[0].method(); sent != wire {
			t.Errorf("%s: its call %s sends %s", wire, spec.GoName, sent)
		}
	}
	if len(methodTable) != len(served.RPC) {
		t.Errorf("%d typed methods, %d served", len(methodTable), len(served.RPC))
	}

	// The REST routes on the same endpoint: the table against served.json both ways, then each call's own verb and
	// path read back.
	restServed := map[string]bool{}
	for _, route := range served.REST {
		restServed[route] = true
		if restRoutes[route] == "" {
			t.Errorf("served but has no typed call: %s", route)
		}
	}
	for route, goName := range restRoutes {
		if !restServed[route] {
			t.Errorf("has a typed call but is not served: %s", route)
			continue
		}
		m := cv.MethodByName(goName)
		if !m.IsValid() {
			t.Errorf("%s: the table names %s, which *Client lacks", route, goName)
			continue
		}
		args := []reflect.Value{ctx}
		for i := 1; i < m.Type().NumIn(); i++ {
			v := reflect.New(m.Type().In(i)).Elem()
			for f := 0; f < v.NumField(); f++ {
				if v.Field(f).Kind() == reflect.String {
					v.Field(f).SetString("x") // required strings: a placeholder
				}
			}
			args = append(args, v)
		}
		before := len(p.seen())
		out := m.Call(args)
		if err, _ := out[len(out)-1].Interface().(error); err != nil {
			t.Errorf("%s: %v", goName, err)
			continue
		}
		got := p.seen()[before:]
		if len(got) != 1 {
			t.Errorf("%s made %d requests, want 1", goName, len(got))
			continue
		}
		if sent := got[0].verb + " " + strings.SplitN(got[0].path, "?", 2)[0]; sent != route {
			t.Errorf("%s: its call %s sends %s", route, goName, sent)
		}
	}
}

func TestTheSubscriptionNamesAreTheServedOnes(t *testing.T) {
	served := readServed(t)
	sort.Strings(served.WS)
	if !reflect.DeepEqual(Subscriptions(), served.WS) {
		t.Fatalf("subscriptions %v, served %v", Subscriptions(), served.WS)
	}
}

// The stream module keeps its own copy of served.json, so it tests alone; it must stay a copy.
func TestTheStreamModulesServedListIsACopy(t *testing.T) {
	a, err := os.ReadFile("spec/served.json")
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile("stream/testdata/served.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatal("stream/testdata/served.json differs from spec/served.json")
	}
}
