package carbium_test

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// prelude stands in for the names the README's fragments use without making them, with the types a caller has.
const prelude = `package p

import (
	"context"
	"net/http"

	carbium "github.com/carbium-sh/carbium-go"
)

var (
	ctx                                                                   context.Context
	c                                                                     *carbium.Client
	a, b, mint, owner, sol, usdc, wallet, pubkey, programID, signedBase64 string
	mints                                                                 []string
	rawBody                                                               []byte
	r                                                                     *http.Request
)

func reloadStateOverRPC() {}
func reloadOverRPC()      {}
`

var goBlock = regexp.MustCompile("(?ms)^```go\n(.*?)^```")

// readmeModule writes every go block of readme into dir as a module that uses this module and the stream module as
// they are here: a whole program as it is, and each fragment as the body of a function in a package of its own, with
// its imports, the quick start's and the prelude's names. It returns the number of blocks.
func readmeModule(t *testing.T, readme []byte, dir string) int {
	t.Helper()
	root, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, src string) {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var sums []byte
	for _, f := range []string{"go.sum", "stream/go.sum"} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		sums = append(sums, b...)
	}
	write("go.sum", string(sums))
	write("go.mod", fmt.Sprintf("module readme\n\ngo 1.26.0\n\nrequire (\n\tgithub.com/carbium-sh/carbium-go v0.0.0\n\tgithub.com/carbium-sh/carbium-go/stream v0.0.0\n)\n\n"+
		"replace github.com/carbium-sh/carbium-go => %q\n\nreplace github.com/carbium-sh/carbium-go/stream => %q\n", root, filepath.Join(root, "stream")))
	blocks := goBlock.FindAllSubmatchIndex(readme, -1)
	for _, m := range blocks {
		line := bytes.Count(readme[:m[2]], []byte("\n")) + 1 // the block's first line in README.md
		code := string(readme[m[2]:m[3]])
		pkg := fmt.Sprintf("line%d", line)
		if strings.HasPrefix(code, "package main\n") {
			write(pkg+"/main.go", code)
			continue
		}
		imports := map[string]bool{"context": true, "errors": true, "fmt": true, "os": true, "github.com/carbium-sh/carbium-go": true}
		lines := strings.Split(code, "\n")
		for len(lines) > 0 && (strings.HasPrefix(lines[0], "import") || strings.HasPrefix(lines[0], "\t\"") || lines[0] == ")" || lines[0] == "") {
			if q := strings.IndexByte(lines[0], '"'); q >= 0 {
				imports[strings.Trim(lines[0][q:], `"`)] = true
			}
			lines = lines[1:]
		}
		src := "package p\n\nimport (\n"
		for _, path := range slices.Sorted(maps.Keys(imports)) {
			src += fmt.Sprintf("\t%q\n", path)
		}
		src += ")\n\nvar _, _, _, _, _ = context.Background, errors.As, fmt.Sprint, os.Getenv, carbium.Ptr[int]\n\nfunc example() {\n" +
			strings.Join(lines, "\n") + "}\n"
		file, err := parser.ParseFile(token.NewFileSet(), "", src, 0)
		if err != nil {
			t.Fatalf("README.md:%d: %v", line, err)
		}
		// Each name the fragment makes is used once at its end, as the caller's next lines would.
		uses := ""
		for _, stmt := range file.Decls[len(file.Decls)-1].(*ast.FuncDecl).Body.List {
			if as, ok := stmt.(*ast.AssignStmt); ok && as.Tok == token.DEFINE {
				for _, lhs := range as.Lhs {
					if id, ok := lhs.(*ast.Ident); ok && id.Name != "_" {
						uses += "\t_ = " + id.Name + "\n"
					}
				}
			}
		}
		write(pkg+"/example.go", strings.TrimSuffix(src, "}\n")+uses+"}\n")
		write(pkg+"/prelude.go", prelude)
	}
	return len(blocks)
}

func vet(dir string) (string, bool) {
	cmd := exec.Command("go", "vet", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GOWORK=off")
	out, err := cmd.CombinedOutput()
	return string(out), err == nil
}

func TestEveryGoBlockInTheReadmeVets(t *testing.T) {
	if _, err := os.Stat("stream/go.mod"); err != nil {
		t.Skip("the stream module is not beside this one, as in a module zip")
	}
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	readme = bytes.ReplaceAll(readme, []byte("\r\n"), []byte("\n")) // a CRLF checkout reads the same
	dir := t.TempDir()
	if readmeModule(t, readme, dir) == 0 {
		t.Fatal("README.md has no go block")
	}
	if out, ok := vet(dir); !ok {
		t.Fatalf("a README block does not vet:\n%s", out)
	}
	bad := t.TempDir() // the same check on a planted bad call fails, naming it
	readmeModule(t, append(readme, "\n```go\nc.NotAMethod(ctx)\n```\n"...), bad)
	if out, ok := vet(bad); ok || !strings.Contains(out, "NotAMethod") {
		t.Fatalf("a planted bad call was not caught:\n%s", out)
	}
}
