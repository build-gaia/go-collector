package chronos_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	chronos "chronos.dev/collector/sdk/go"
	"chronos.dev/collector/sdk/go/spool"
	"github.com/stretchr/testify/require"
)

// The filesystem helpers do not hand the span back — the whole point is that a
// caller writes ordinary file code — so these read what was actually spooled,
// which is also the only thing that proves an attribute survived encoding.

type fileSpan struct {
	Name       string            `json:"name"`
	Attributes map[string]string `json:"attributes"`
}

var fsSpoolDirs = map[*chronos.Client]string{}

func fsClient(t *testing.T) *chronos.Client {
	t.Helper()
	dir := t.TempDir()
	client := chronos.StartWithConfig(chronos.Config{
		Enabled:      true,
		Organisation: "org-local",
		Project:      "project-a",
		Application:  "feed-stream-consumer",
		SpoolDir:     dir,
		ServiceName:  "service-feed-stream-consumer",
		APMEnabled:   true,
	})
	fsSpoolDirs[client] = dir
	return client
}

func recordedSpans(t *testing.T, client *chronos.Client) []fileSpan {
	t.Helper()
	client.Flush()
	frames, err := spool.Read(fsSpoolDirs[client])
	require.NoError(t, err)
	var out []fileSpan
	for _, frame := range frames {
		if frame.Signal != string(spool.SignalTrace) {
			continue
		}
		decoder := json.NewDecoder(bytes.NewReader(frame.Payload))
		for {
			var batch struct {
				Spans []fileSpan `json:"spans"`
			}
			if err := decoder.Decode(&batch); err != nil {
				break
			}
			out = append(out, batch.Spans...)
		}
	}
	return out
}

func lastSpan(t *testing.T, client *chronos.Client) fileSpan {
	t.Helper()
	spans := recordedSpans(t, client)
	require.NotEmpty(t, spans, "no spans were spooled")
	return spans[len(spans)-1]
}

// A file span has to be findable the way the desktop actually looks for one:
// `url.full:prefix:string:file://`. Asserting the prefix rather than the whole
// value is the point — the predicate is what breaks if the spelling drifts.
func TestFileSpanCarriesTheFileURLTheDesktopSelectsOn(t *testing.T) {
	client := fsClient(t)
	defer client.Shutdown(context.Background())

	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"a":1}`), 0o600))

	_, err := client.ReadFile(context.Background(), path)
	require.NoError(t, err)

	span := lastSpan(t, client)
	require.True(t, strings.HasPrefix(span.Attributes[chronos.AttrURLFull], "file://"),
		"url.full %q must start file:// or the desktop's file-span predicate misses it",
		span.Attributes[chronos.AttrURLFull])
	require.Equal(t, path, span.Attributes[chronos.AttrFilePath])
	require.Equal(t, "read", span.Attributes[chronos.AttrFileOperation])
	require.Equal(t, "client", span.Attributes["span.kind"])
	require.Equal(t, "7", span.Attributes[chronos.AttrFileSize])
}

// Sized from the bytes that actually moved, not from a stat taken afterwards.
func TestWriteAndCopyRecordTheBytesTheyMoved(t *testing.T) {
	client := fsClient(t)
	defer client.Shutdown(context.Background())

	path := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, client.WriteFile(context.Background(), path, []byte("0123456789"), 0o600))
	require.Equal(t, "10", lastSpan(t, client).Attributes[chronos.AttrFileSize])

	var sink bytes.Buffer
	moved, err := client.Copy(context.Background(), path, &sink, strings.NewReader("abcd"))
	require.NoError(t, err)
	require.Equal(t, int64(4), moved)
	require.Equal(t, "4", lastSpan(t, client).Attributes[chronos.AttrFileSize])
}

// A copy that dies half way still moved bytes, and "how far did it get" is the
// first thing anyone asks. The error must also reach the caller untouched.
func TestAFailedCopyStillReportsWhatItMoved(t *testing.T) {
	client := fsClient(t)
	defer client.Shutdown(context.Background())

	moved, err := client.Copy(context.Background(), "/tmp/x", failingWriter{after: 3}, strings.NewReader("abcdef"))
	require.Error(t, err)
	require.Equal(t, int64(3), moved)

	span := lastSpan(t, client)
	require.Equal(t, "3", span.Attributes[chronos.AttrFileSize])
	require.Equal(t, "true", span.Attributes["error"])
}

// Telemetry never changes what the caller sees. A missing file fails exactly as
// os.ReadFile fails, and the span records it rather than swallowing it.
func TestAMissingFileFailsTheWayItWouldWithoutUs(t *testing.T) {
	client := fsClient(t)
	defer client.Shutdown(context.Background())

	_, err := client.ReadFile(context.Background(), filepath.Join(t.TempDir(), "absent"))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Equal(t, "true", lastSpan(t, client).Attributes["error"])
}

// The nil client is the fail-open path: an application that never started
// Chronos still reads its files.
func TestANilClientStillDoesTheWork(t *testing.T) {
	var client *chronos.Client

	path := filepath.Join(t.TempDir(), "f")
	require.NoError(t, client.WriteFile(context.Background(), path, []byte("hi"), 0o600))
	body, err := client.ReadFile(context.Background(), path)
	require.NoError(t, err)
	require.Equal(t, "hi", string(body))
}

// Relative paths resolve, because a reader cannot use one without knowing the
// working directory — and a container is what changes it.
func TestARelativePathIsRecordedAbsolute(t *testing.T) {
	client := fsClient(t)
	defer client.Shutdown(context.Background())

	dir := t.TempDir()
	t.Chdir(dir)
	require.NoError(t, os.WriteFile("local.txt", []byte("x"), 0o600))

	_, err := client.ReadFile(context.Background(), "local.txt")
	require.NoError(t, err)

	got := lastSpan(t, client).Attributes[chronos.AttrFilePath]
	require.True(t, filepath.IsAbs(got), "recorded path %q should be absolute", got)
	require.True(t, strings.HasSuffix(got, "local.txt"))
}

// The decorator form: code that already takes an fs.FS becomes observable
// without learning about Chronos.
func TestFSDecoratorSpansTheOpenAndReturnsTheFile(t *testing.T) {
	client := fsClient(t)
	defer client.Shutdown(context.Background())

	fsys := client.FS(fstest.MapFS{"tpl/page.html": &fstest.MapFile{Data: []byte("<p>hi</p>")}})
	file, err := fsys.Open("tpl/page.html")
	require.NoError(t, err)
	defer file.Close()

	body, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, "<p>hi</p>", string(body), "the caller must still get its bytes")

	span := lastSpan(t, client)
	require.Equal(t, "open", span.Attributes[chronos.AttrFileOperation])
	require.Equal(t, "9", span.Attributes[chronos.AttrFileSize])
}

// A span name is read off a crowded row, so it carries the base name; the full
// address stays one attribute away.
func TestSpanNameIsTheVerbAndTheBaseName(t *testing.T) {
	client := fsClient(t)
	defer client.Shutdown(context.Background())

	path := filepath.Join(t.TempDir(), "deeply", "nested")
	require.NoError(t, os.MkdirAll(path, 0o755))
	target := filepath.Join(path, "report.csv")
	require.NoError(t, os.WriteFile(target, []byte("a"), 0o600))

	_, err := client.Stat(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, "STAT report.csv", lastSpan(t, client).Name)
}

// File contents are never an attribute. Stated as a test because it is the one
// property that cannot be re-established after a release that broke it.
func TestFileContentsAreNeverCaptured(t *testing.T) {
	client := fsClient(t)
	defer client.Shutdown(context.Background())

	secret := "SUPER-SECRET-PAYLOAD"
	path := filepath.Join(t.TempDir(), "secret.txt")
	require.NoError(t, client.WriteFile(context.Background(), path, []byte(secret), 0o600))
	_, err := client.ReadFile(context.Background(), path)
	require.NoError(t, err)

	for _, span := range recordedSpans(t, client) {
		for key, value := range span.Attributes {
			require.NotContains(t, value, secret, "attribute %q leaked file contents", key)
		}
	}
}

type failingWriter struct{ after int }

func (w failingWriter) Write(p []byte) (int, error) {
	if len(p) <= w.after {
		return len(p), nil
	}
	return w.after, io.ErrShortWrite
}
