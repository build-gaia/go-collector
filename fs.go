package chronos

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Filesystem work, as a span.
//
// # Why a file deserves a span at all
//
// A service that reads a 200 MB manifest off a network mount, or writes a batch
// to object storage through a filesystem abstraction, spends real wall-clock
// time doing it — and until now that time landed nowhere. It is not a database
// call, not an HTTP call, and not CPU, so a reader looking at the trace sees a
// gap and concludes the process was idle. The gap IS the work; it just had no
// name.
//
// # The address is a URL, because the reader already has one
//
// Every span here carries `url.full` as a `file://` URL. That spelling is not
// decorative: the desktop already selects file work with the predicate
// `url.full:prefix:string:file://` (`state/span-sampling.ts`), and has a `file`
// kind with its own colour on the telemetry breakdown. The consumer existed
// before any emitter did. This is the emitter.
//
// # What is NOT captured
//
// File CONTENTS, ever, by any path here. A file's bytes are the application's
// data in the most literal sense, and unlike an HTTP body there is no bound
// anyone could sensibly put on them. Size, path, operation and outcome answer
// "what did this cost and did it work"; the bytes answer a question telemetry
// should not be asking.
//
// Paths are absolute where they can be resolved, because a relative path is
// meaningless to a reader who does not know the process's working directory —
// and the working directory is exactly what a container changes.
const (
	// AttrURLFull is the file:// address, and the key the desktop's file-span
	// predicate matches on. Shared with HTTP deliberately: both answer "what did
	// this operation address", and a reader should not need two keys for one idea.
	AttrURLFull = "url.full"
	// AttrFilePath is the resolved path on its own, for a reader who wants the
	// path without unpicking a URL.
	AttrFilePath = "file.path"
	// AttrFileOperation is the verb — read, write, open, stat, remove, walk.
	AttrFileOperation = "file.operation"
	// AttrFileSize is bytes read or written, when the operation knows. ABSENT
	// rather than zero when it does not: zero is a measurement meaning "an empty
	// file", and an unmeasured read must not be able to claim it.
	AttrFileSize = "file.size"
)

// maxFilePathLength bounds a pathological path rather than the ordinary one. A
// path longer than this is already not something a human reads off a trace.
const maxFilePathLength = 1024

// File runs fn as one filesystem span.
//
// The primitive the rest of this file is written in terms of, and the one an
// application reaches for when its file work does not look like any of the
// helpers below — a chunked copy, a rename-into-place, an fsync.
//
// fn's error is returned untouched and marks the span failed. Telemetry never
// changes what the caller sees: a file operation that failed must fail in
// exactly the way it would have without instrumentation.
func File(ctx context.Context, operation, path string, fn func(context.Context) error) error {
	return Default().File(ctx, operation, path, fn)
}

// File runs fn as one filesystem span on this client.
func (c *Client) File(ctx context.Context, operation, path string, fn func(context.Context) error) error {
	if c == nil {
		return fn(ctx)
	}
	ctx, span := c.StartSpan(ctx, fileSpanName(operation, path))
	decorateFile(span, operation, path)
	err := fn(ctx)
	if err != nil {
		span.SetStatus("error")
		span.SetAttribute("error", "true")
		span.SetAttribute("exception.message", err.Error())
	}
	span.End()
	return err
}

// ReadFile is os.ReadFile with a span, recording the bytes it actually read.
func ReadFile(ctx context.Context, path string) ([]byte, error) {
	return Default().ReadFile(ctx, path)
}

// ReadFile is os.ReadFile with a span for this client.
func (c *Client) ReadFile(ctx context.Context, path string) ([]byte, error) {
	if c == nil {
		return os.ReadFile(path)
	}
	var out []byte
	err := c.file(ctx, "read", path, func(_ context.Context, span *Span) error {
		var err error
		out, err = os.ReadFile(path)
		// Sized from what was read rather than from a Stat: a file that grew
		// between the two calls would otherwise report a size this read never saw.
		if err == nil {
			span.SetAttribute(AttrFileSize, strconv.Itoa(len(out)))
		}
		return err
	})
	return out, err
}

// WriteFile is os.WriteFile with a span, sized from the buffer handed over.
func WriteFile(ctx context.Context, path string, data []byte, perm os.FileMode) error {
	return Default().WriteFile(ctx, path, data, perm)
}

// WriteFile is os.WriteFile with a span for this client.
func (c *Client) WriteFile(ctx context.Context, path string, data []byte, perm os.FileMode) error {
	if c == nil {
		return os.WriteFile(path, data, perm)
	}
	return c.file(ctx, "write", path, func(_ context.Context, span *Span) error {
		span.SetAttribute(AttrFileSize, strconv.Itoa(len(data)))
		return os.WriteFile(path, data, perm)
	})
}

// Copy is io.Copy with a span, sized from the bytes that moved.
//
// The operation that most often hides the time: a stream copy between two
// things neither of which is a file the caller named, which is what an upload
// or a download through a filesystem abstraction usually is. `path` is whatever
// the caller can say about the destination; an empty one still gets a span,
// because the DURATION is the fact worth having even when the address is not.
func Copy(ctx context.Context, path string, dst io.Writer, src io.Reader) (int64, error) {
	return Default().Copy(ctx, path, dst, src)
}

// Copy is io.Copy with a span for this client.
func (c *Client) Copy(ctx context.Context, path string, dst io.Writer, src io.Reader) (int64, error) {
	if c == nil {
		return io.Copy(dst, src)
	}
	var moved int64
	err := c.file(ctx, "copy", path, func(_ context.Context, span *Span) error {
		var err error
		moved, err = io.Copy(dst, src)
		// Recorded even on failure: a copy that died half way moved real bytes,
		// and "how far did it get" is the first thing anyone asks about it.
		span.SetAttribute(AttrFileSize, strconv.FormatInt(moved, 10))
		return err
	})
	return moved, err
}

// Stat is os.Stat with a span.
func Stat(ctx context.Context, path string) (os.FileInfo, error) {
	return Default().Stat(ctx, path)
}

// Stat is os.Stat with a span for this client.
func (c *Client) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	if c == nil {
		return os.Stat(path)
	}
	var info os.FileInfo
	err := c.file(ctx, "stat", path, func(_ context.Context, span *Span) error {
		var err error
		info, err = os.Stat(path)
		if err == nil && !info.IsDir() {
			span.SetAttribute(AttrFileSize, strconv.FormatInt(info.Size(), 10))
		}
		return err
	})
	return info, err
}

// Remove is os.Remove with a span.
func Remove(ctx context.Context, path string) error {
	return Default().Remove(ctx, path)
}

// Remove is os.Remove with a span for this client.
func (c *Client) Remove(ctx context.Context, path string) error {
	if c == nil {
		return os.Remove(path)
	}
	return c.file(ctx, "remove", path, func(_ context.Context, _ *Span) error {
		return os.Remove(path)
	})
}

// FS wraps an fs.FS so every Open through it is a span.
//
// The decorator form, for code that already takes an fs.FS and should not have
// to learn about Chronos to be observable — a template loader, an embedded
// asset store, a bucket exposed as a filesystem. Reads THROUGH the opened file
// are not individually spanned: an fs.File is read in a loop by whoever holds
// it, and a span per Read would produce thousands of rows describing one
// logical operation. The Open is the operation; its duration is where the
// latency of a slow mount actually shows up.
func (c *Client) FS(fsys fs.FS) fs.FS {
	if c == nil || fsys == nil {
		return fsys
	}
	return &instrumentedFS{inner: fsys, client: c}
}

type instrumentedFS struct {
	inner  fs.FS
	client *Client
}

// Open spans the open. Context is deliberately absent from fs.FS's signature,
// so this cannot parent onto the caller's span — the span stands alone rather
// than being attached to whatever happened to be active on another goroutine,
// which would be a worse lie than an orphan.
func (f *instrumentedFS) Open(name string) (fs.File, error) {
	var file fs.File
	_ = f.client.file(context.Background(), "open", name, func(_ context.Context, span *Span) error {
		var err error
		file, err = f.inner.Open(name)
		if err == nil {
			if info, statErr := file.Stat(); statErr == nil && !info.IsDir() {
				span.SetAttribute(AttrFileSize, strconv.FormatInt(info.Size(), 10))
			}
		}
		return err
	})
	if file == nil {
		// Re-open rather than cache the error: the span already recorded the
		// failure, and the caller must get the inner FS's own error value, not a
		// copy this wrapper decided to keep.
		return f.inner.Open(name)
	}
	return file, nil
}

// file is the shared body: span, decorate, run, record, end.
func (c *Client) file(ctx context.Context, operation, path string, fn func(context.Context, *Span) error) error {
	ctx, span := c.StartSpan(ctx, fileSpanName(operation, path))
	decorateFile(span, operation, path)
	err := fn(ctx, span)
	if err != nil {
		span.SetStatus("error")
		span.SetAttribute("error", "true")
		span.SetAttribute("exception.message", err.Error())
	}
	span.End()
	return err
}

// decorateFile puts the address on the span in both spellings.
func decorateFile(span *Span, operation, path string) {
	span.SetAttribute("span.kind", "client")
	if operation != "" {
		span.SetAttribute(AttrFileOperation, operation)
	}
	resolved := resolveFilePath(path)
	if resolved == "" {
		return
	}
	span.SetAttribute(AttrFilePath, resolved)
	span.SetAttribute(AttrURLFull, "file://"+resolved)
}

// resolveFilePath makes a path absolute where it can, and bounds it.
//
// Absolute because a relative path cannot be read by anyone who does not know
// the process's working directory, and a container is precisely the thing that
// changes it. A path that will not resolve is kept as given rather than
// dropped: a name the reader recognises beats no address at all.
func resolveFilePath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}
	if !filepath.IsAbs(trimmed) {
		if abs, err := filepath.Abs(trimmed); err == nil {
			trimmed = abs
		}
	}
	if len(trimmed) > maxFilePathLength {
		return trimmed[:maxFilePathLength]
	}
	return trimmed
}

// fileSpanName reads as the operation over the file's base name — the whole
// path would push everything else off the row, and the full address is one
// attribute away on the span itself.
func fileSpanName(operation, path string) string {
	verb := strings.ToUpper(strings.TrimSpace(operation))
	if verb == "" {
		verb = "FILE"
	}
	base := filepath.Base(strings.TrimSpace(path))
	if base == "" || base == "." || base == string(filepath.Separator) {
		return verb
	}
	return verb + " " + base
}
