package chronos

import (
	"runtime"
	"strings"
	"testing"
)

func TestResourceAttributesLabelTheProcess(t *testing.T) {
	attrs := withResourceAttributes(map[string]string{"db.system": "mysql"}, Config{ServiceVersion: "1.4.2"})

	if attrs["app.language"] != "go" {
		t.Fatalf("app.language = %q, want go", attrs["app.language"])
	}
	if want := strings.TrimPrefix(runtime.Version(), "go"); attrs["app.language.version"] != want {
		t.Fatalf("app.language.version = %q, want %q", attrs["app.language.version"], want)
	}
	if attrs["app.version"] != "1.4.2" || attrs["service.version"] != "1.4.2" {
		t.Fatalf("version attributes = %q / %q", attrs["app.version"], attrs["service.version"])
	}
	if attrs["db.system"] != "mysql" {
		t.Fatal("the span's own attributes must survive the merge")
	}
}

// The version is configuration, so it must not be cached across clients: a
// process holding two clients would otherwise label one with the other's release.
func TestResourceAttributesDoNotCacheConfiguration(t *testing.T) {
	first := withResourceAttributes(nil, Config{ServiceVersion: "1.0.0"})
	second := withResourceAttributes(nil, Config{})

	if first["app.version"] != "1.0.0" {
		t.Fatalf("app.version = %q, want 1.0.0", first["app.version"])
	}
	if _, ok := second["app.version"]; ok {
		t.Fatalf("app.version leaked across clients: %q", second["app.version"])
	}
	if second["app.language"] != "go" {
		t.Fatal("the process family must still be present")
	}
}

// An explicitly set attribute is a statement about that span; the resource family
// is a default underneath it.
func TestSpanAttributesWinOverResource(t *testing.T) {
	attrs := withResourceAttributes(map[string]string{"app.version": "override"}, Config{ServiceVersion: "1.0.0"})
	if attrs["app.version"] != "override" {
		t.Fatalf("app.version = %q, want override", attrs["app.version"])
	}
}
