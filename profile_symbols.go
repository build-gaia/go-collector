package chronos

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
)

// internalModule is the module name the desktop dims (same convention as PHP builtins).
const internalModule = "internal"

// sdkPackage is this SDK's own import path. Frames from it are the profiler observing
// itself; they are library frames, never userland.
const sdkPackage = "chronos.dev/collector/sdk/go"

// packageOf returns the import path of the package a Go symbol belongs to.
//
// Go symbol names are "<import path>.<Func>", "<import path>.(*Type).Method" or
// "<import path>.Outer.funcN". The import path itself contains dots
// ("github.com/x/y"), so the split point is the FIRST dot AFTER the LAST slash —
// splitting on the last dot would return "github.com/x/y.(*T)".
//
// Returns "" for a symbol with no package qualifier (assembly, stripped frames).
func packageOf(function string) string {
	if function == "" {
		return ""
	}
	slash := strings.LastIndex(function, "/")
	segment := function[slash+1:]
	dot := strings.Index(segment, ".")
	if dot <= 0 {
		return ""
	}
	return function[:slash+1] + segment[:dot]
}

var (
	mainModuleOnce sync.Once
	mainModule     string
)

// mainModulePath is the go.mod module path of the running binary, used to shorten
// application packages to a repo-relative path in the flame chart.
//
// CHRONOS_GO_MODULE_ROOT wins when set. Otherwise the build records it — except when the
// binary was built from a FILE argument (`go build ./main.go`, `go run main.go`, which is
// what air and most Dockerfiles do). That produces the synthetic "command-line-arguments"
// package and an empty Main.Path, so the shortening would silently no-op and every frame
// would carry its full import path. inferModuleRoot covers that case.
func mainModulePath() string {
	mainModuleOnce.Do(func() {
		if env := strings.TrimSpace(os.Getenv("CHRONOS_GO_MODULE_ROOT")); env != "" {
			mainModule = env
			return
		}
		if info, ok := debug.ReadBuildInfo(); ok && info.Main.Path != "" && info.Main.Path != "command-line-arguments" {
			mainModule = info.Main.Path
		}
	})
	return mainModule
}

// inferModuleRoot guesses the module path from one of its package paths, for binaries
// whose build did not record it.
//
// Go module paths are either a bare name ("service-stock-analytics") or a domain-rooted
// triple ("github.com/org/repo"); a dot in the first segment distinguishes them. Guessing
// only ever affects how a frame is labelled, never whether it is kept.
func inferModuleRoot(pkg string) string {
	parts := strings.Split(pkg, "/")
	if len(parts) == 0 {
		return ""
	}
	if strings.Contains(parts[0], ".") {
		if len(parts) < 3 {
			return pkg
		}
		return strings.Join(parts[:3], "/")
	}
	return parts[0]
}

// frameModule is the grouping key the desktop shows above a frame's function.
//
// Library / runtime frames collapse to "internal" so they can be dimmed. Application
// frames resolve to their package path, shortened against the main module: a frame in
// "service-stock-analytics/internal/service/stock" becomes "internal/service/stock".
//
// The mapping file is only a last resort: a statically linked Go binary has ONE mapping
// (the executable), so using it gives every application frame an identical module and
// leaves the flame chart with nothing to group by.
func frameModule(function, file, mappingModule string) string {
	if isLibraryFrame(function, file) {
		return internalModule
	}
	if pkg := packageOf(function); pkg != "" {
		root := mainModulePath()
		if root == "" {
			root = inferModuleRoot(pkg)
		}
		if root != "" {
			if pkg == root {
				return baseName(root)
			}
			if rel := strings.TrimPrefix(pkg, root+"/"); rel != pkg {
				return rel
			}
		}
		return pkg
	}
	return mappingModule
}

var (
	goRootOnce sync.Once
	goRoot     string
)

func cachedGoRoot() string {
	goRootOnce.Do(func() {
		goRoot = filepath.ToSlash(runtime.GOROOT())
	})
	return goRoot
}

// IsLibraryFrame reports whether a Go stack frame is toolchain / dependency code
// rather than the application's own packages. Exported for tests.
func IsLibraryFrame(function, file string) bool {
	return isLibraryFrame(function, file)
}

func isLibraryFrame(function, file string) bool {
	file = filepath.ToSlash(file)
	if function != "" {
		if strings.HasPrefix(function, "runtime.") ||
			strings.HasPrefix(function, "runtime/") ||
			strings.HasPrefix(function, "runtime_asm") {
			return true
		}
		// A local `replace` directive puts the SDK outside the module cache, so the
		// path markers below miss it; match the import path instead.
		if isSDKFrame(function) {
			return true
		}
	}
	if file == "" {
		return false
	}
	lower := strings.ToLower(file)
	for _, marker := range []string{
		"/pkg/mod/",
		"/go/pkg/mod/",
		"/vendor/",
		"/src/runtime/",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	if root := cachedGoRoot(); root != "" {
		rootSlash := strings.TrimRight(strings.ToLower(root), "/") + "/"
		if strings.HasPrefix(lower, rootSlash) {
			return true
		}
	}
	if strings.Contains(lower, "/go/src/") {
		return true
	}
	return false
}

func isSDKFrame(function string) bool {
	return strings.HasPrefix(function, sdkPackage+".") || strings.HasPrefix(function, sdkPackage+"/")
}

// hasUserlandFrame reports whether a stack contains at least one application frame.
//
// Wall / off-CPU sampling sees every goroutine in the process, including the runtime's
// own (GC workers, scavenger, the finaliser) and the profiler's. A stack with no
// application frame cannot be acted on by whoever reads the flame chart, so it is
// dropped rather than shipped as noise.
func hasUserlandFrame(frames []ProfileFrame) bool {
	for _, f := range frames {
		if f.Function == "" {
			continue
		}
		if f.Module != internalModule && !isSDKFrame(f.Function) {
			return true
		}
	}
	return false
}

func baseName(path string) string {
	if path == "" {
		return ""
	}
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[i+1:]
	}
	return path
}

// PackageOf is packageOf, exported for tests.
func PackageOf(function string) string { return packageOf(function) }

// isSelfProfilingStack reports a stack that is only the profiler observing itself:
// SDK frames over runtime frames, with no application code anywhere. Those samples
// measure the cost of measuring and belong in no series.
//
// A stack of pure runtime frames with no SDK frame is NOT self-profiling — that is GC
// or scheduler cost, which is a real finding and is kept.
func isSelfProfilingStack(frames []ProfileFrame) bool {
	sawSDK := false
	for _, f := range frames {
		if f.Function == "" {
			continue
		}
		if isSDKFrame(f.Function) {
			sawSDK = true
			continue
		}
		if f.Module != internalModule {
			return false
		}
	}
	return sawSDK
}
