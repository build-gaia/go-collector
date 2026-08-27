package chronos_test

import (
	"testing"

	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"

	chronos "chronos.dev/collector/sdk/go"
)

func TestNormalizePprofRootFirst(t *testing.T) {
	leaf := &profile.Function{ID: 1, Name: "leaf", Filename: "/app/leaf.go"}
	root := &profile.Function{ID: 2, Name: "root", Filename: "/app/root.go"}
	mapping := &profile.Mapping{ID: 1, File: "/usr/bin/stock-analytics"}
	locLeaf := &profile.Location{
		ID:      1,
		Mapping: mapping,
		Line:    []profile.Line{{Function: leaf, Line: 10}},
	}
	locRoot := &profile.Location{
		ID:      2,
		Mapping: mapping,
		Line:    []profile.Line{{Function: root, Line: 20}},
	}
	prof := &profile.Profile{
		Period: 10_000_000,
		Sample: []*profile.Sample{{
			Location: []*profile.Location{locLeaf, locRoot}, // leaf-first
			Value:    []int64{1, 10_000_000},
			Label:    map[string][]string{"route": {"/ages"}, "http.method": {"GET"}},
		}},
	}

	samples := chronos.NormalizePprof(prof, chronos.NormalizeOptions{
		Organisation: "org-local",
		Project:      "project-a",
		Application:  "stock-analytics",
		SeriesID:     "go-cpu",
		SampleType:   "PROFILE_SAMPLE_TYPE_CPU",
		Unit:         "nanoseconds",
		Labels:       map[string]string{"service": "stock-analytics"},
	})
	require.Len(t, samples, 1)
	require.Equal(t, "root", samples[0].Stack[0].Function)
	require.Equal(t, "leaf", samples[0].Stack[len(samples[0].Stack)-1].Function)
	require.Equal(t, "stock-analytics", samples[0].Labels["service"])
	require.Equal(t, "/ages", samples[0].Labels["route"])
	require.Equal(t, "GET", samples[0].Labels["http.method"])
	require.Equal(t, "stock-analytics", samples[0].Stack[0].Module)
}

func TestNormalizePprofTagsLibraryFramesInternal(t *testing.T) {
	app := &profile.Function{ID: 1, Name: "main.handle", Filename: "/Users/me/app/handler.go"}
	rt := &profile.Function{ID: 2, Name: "runtime.gopark", Filename: "/usr/local/go/src/runtime/proc.go"}
	mod := &profile.Function{ID: 3, Name: "github.com/gin-gonic/gin.(*Engine).ServeHTTP", Filename: "/Users/me/go/pkg/mod/github.com/gin-gonic/gin@v1.9.0/gin.go"}
	mapping := &profile.Mapping{ID: 1, File: "/usr/bin/app"}
	prof := &profile.Profile{
		Period: 10_000_000,
		Sample: []*profile.Sample{{
			Location: []*profile.Location{
				{ID: 1, Mapping: mapping, Line: []profile.Line{{Function: app, Line: 1}}},
				{ID: 2, Mapping: mapping, Line: []profile.Line{{Function: rt, Line: 2}}},
				{ID: 3, Mapping: mapping, Line: []profile.Line{{Function: mod, Line: 3}}},
			},
			Value: []int64{1, 1},
		}},
	}
	samples := chronos.NormalizePprof(prof, chronos.NormalizeOptions{
		Organisation: "o", Project: "p", Application: "a",
		SeriesID: "go-cpu", SampleType: "PROFILE_SAMPLE_TYPE_CPU", Unit: "nanoseconds",
	})
	require.Len(t, samples, 1)
	stack := samples[0].Stack
	require.Equal(t, "internal", stack[0].Module) // mod cache (root after reverse? leaf-first so reverse: mod, rt, app)
	// leaf-first locations: app, rt, mod → root-first: mod, rt, app
	require.Equal(t, "github.com/gin-gonic/gin.(*Engine).ServeHTTP", stack[0].Function)
	require.Equal(t, "internal", stack[0].Module)
	require.Equal(t, "runtime.gopark", stack[1].Function)
	require.Equal(t, "internal", stack[1].Module)
	require.Equal(t, "main.handle", stack[2].Function)
	// The module comes from the symbol's package, not the binary's mapping file: a
	// static Go binary has one mapping, so mapping-derived modules are all identical.
	require.Equal(t, "main", stack[2].Module)
}

func TestIsLibraryFrame(t *testing.T) {
	require.True(t, chronos.IsLibraryFrame("runtime.mallocgc", "/usr/local/go/src/runtime/malloc.go"))
	require.True(t, chronos.IsLibraryFrame("net/http.(*conn).serve", "/usr/local/go/src/net/http/server.go"))
	require.True(t, chronos.IsLibraryFrame("pkg.F", "/home/u/go/pkg/mod/github.com/x@v1/f.go"))
	require.True(t, chronos.IsLibraryFrame("v.F", "/app/vendor/github.com/x/f.go"))
	require.False(t, chronos.IsLibraryFrame("main.handle", "/Users/me/code/app/handler.go"))
	require.False(t, chronos.IsLibraryFrame("acme.co/app/internal/job.Run", "/Users/me/code/app/internal/job/run.go"))
}

func TestPackageOfSplitsAfterLastSlash(t *testing.T) {
	// The import path contains dots, so a last-dot split would yield "github.com/x/y.(*T)".
	require.Equal(t, "github.com/gin-gonic/gin", chronos.PackageOf("github.com/gin-gonic/gin.(*Engine).ServeHTTP"))
	require.Equal(t, "service-stock-analytics/internal/service/stock",
		chronos.PackageOf("service-stock-analytics/internal/service/stock.(*stockChunkJob).Run"))
	require.Equal(t, "main", chronos.PackageOf("main.main"))
	require.Equal(t, "runtime", chronos.PackageOf("runtime.gopark"))
	require.Equal(t, "", chronos.PackageOf("unqualified"))
	require.Equal(t, "", chronos.PackageOf(""))
}

func TestNormalizePprofLiftsTraceCorrelation(t *testing.T) {
	fn := &profile.Function{ID: 1, Name: "acme/app/job.Run", Filename: "/src/app/job/run.go"}
	mapping := &profile.Mapping{ID: 1, File: "/usr/bin/app"}
	prof := &profile.Profile{
		Period: 10_000_000,
		Sample: []*profile.Sample{{
			Location: []*profile.Location{{ID: 1, Mapping: mapping, Line: []profile.Line{{Function: fn, Line: 7}}}},
			Value:    []int64{1, 10_000_000},
			Label: map[string][]string{
				"job":      {"stock-42"},
				"trace_id": {"0123456789abcdef0123456789abcdef"},
				"span_id":  {"0123456789abcdef"},
			},
		}},
	}
	samples := chronos.NormalizePprof(prof, chronos.NormalizeOptions{
		Organisation: "o", Project: "p", Application: "a",
		SeriesID: "go-cpu", SampleType: "PROFILE_SAMPLE_TYPE_CPU", Unit: "nanoseconds",
	})
	require.Len(t, samples, 1)
	require.NotNil(t, samples[0].Correlation)
	require.Equal(t, "0123456789abcdef0123456789abcdef", samples[0].Correlation.TraceID)
	require.Equal(t, "0123456789abcdef", samples[0].Correlation.SpanID)
	// Lifted out of the label bag, not duplicated into it.
	require.NotContains(t, samples[0].Labels, "trace_id")
	require.NotContains(t, samples[0].Labels, "span_id")
	require.Equal(t, "stock-42", samples[0].Labels["job"])
	require.Equal(t, "acme/app/job", samples[0].Stack[0].Module)
}
