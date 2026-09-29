package main

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Keep implementation packages independent. These are production imports;
// integration tests may combine implementations at the composition root.
func TestInternalPackageDependencies(t *testing.T) {
	allowed := map[string][]string{
		"model": nil, "config": nil, "retry": nil, "watchdog": nil,
		"aggregate": {"model"}, "collector": {"model"}, "writer": {"model"},
		"exchange": {"model", "retry"}, "warehouse": {"model", "retry"},
	}
	dirs, err := os.ReadDir("internal")
	if err != nil {
		t.Fatal(err)
	}
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		names, ok := allowed[dir.Name()]
		if !ok {
			t.Fatalf("unexpected internal package: %s", dir.Name())
		}
		imports := make(map[string]bool)
		for _, name := range names {
			imports["collector/internal/"+name] = true
		}
		files, err := filepath.Glob(filepath.Join("internal", dir.Name(), "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatal(err)
			}
			for _, spec := range parsed.Imports {
				path, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					t.Fatal(err)
				}
				if strings.HasPrefix(path, "collector/") && !imports[path] {
					t.Errorf("%s imports forbidden project package %s", dir.Name(), path)
				}
			}
		}
	}
}
