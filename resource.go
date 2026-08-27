package chronos

import (
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
)

// The `app.*` / `service.*` family: the language, release and revision of the
// observed process, stamped on every span.
//
// The PHP extension has done this since it shipped (native/src/spool.rs,
// app_identity_attributes) and the desktop leans on it: a service with no
// `app.language` cannot be labelled with its stack, and `app.commit` is the only
// thing that attributes a latency step to a deploy in an estate that ships by
// checkout rather than by a release pipeline. A Go service emitted none of it, so
// every Go service in the estate read as language-unknown.
//
// Attributes rather than envelope fields, for the same reason PHP chose them: the
// `chronos.tracing.span-batch.v1` schema is unchanged and an older engine carries
// them through as opaque attributes.
var (
	resourceOnce  sync.Once
	resourceAttrs map[string]string
)

// processAttributes is computed once per process: neither the Go version nor the
// build's VCS stamp can change while it runs. Configuration-derived keys are NOT
// cached here — a process may hold more than one client, and caching the first
// client's version would label the second's spans with it.
func processAttributes() map[string]string {
	resourceOnce.Do(func() {
		attrs := map[string]string{"app.language": "go"}
		// runtime.Version() is "go1.24.1"; the PHP collector reports a bare
		// "8.3.2" for app.language.version, so the prefix comes off to keep the
		// two comparable.
		if version := strings.TrimPrefix(runtime.Version(), "go"); version != "" {
			attrs["app.language.version"] = version
		}
		// A binary built from a checkout carries its revision in build info, so
		// the commit needs neither a build flag nor a .git directory beside the
		// running process — which is what the PHP collector has to read.
		if info, ok := debug.ReadBuildInfo(); ok {
			for _, setting := range info.Settings {
				switch setting.Key {
				case "vcs.revision":
					if setting.Value != "" {
						attrs["app.commit"] = setting.Value
					}
				case "vcs.modified":
					// A dirty build is not the commit it claims to be, and a
					// wrong commit is worse than none for attributing a change.
					if setting.Value == "true" {
						attrs["app.dirty"] = "true"
					}
				}
			}
		}
		resourceAttrs = attrs
	})
	return resourceAttrs
}

// withResourceAttributes returns the span's attributes plus the process resource
// family. The span's own attributes win: an explicitly set key is a deliberate
// statement about that span and the resource family is a default.
//
// This bypasses SetAttribute's per-span cap on purpose. The cap exists to stop an
// application writing unbounded attributes; dropping the process's own identity
// because a span happened to be attribute-heavy would make the estate's language
// labelling depend on how chatty one span was.
func withResourceAttributes(attrs map[string]string, cfg Config) map[string]string {
	resource := processAttributes()
	merged := make(map[string]string, len(attrs)+len(resource)+2)
	for key, value := range resource {
		merged[key] = value
	}
	if cfg.ServiceVersion != "" {
		merged["app.version"] = cfg.ServiceVersion
		merged["service.version"] = cfg.ServiceVersion
	}
	for key, value := range attrs {
		merged[key] = value
	}
	return merged
}
