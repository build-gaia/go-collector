// Package spool implements the Chronos atomic spool write protocol
// (collector/proto/SPOOL_CONTRACT.md).
package spool

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

// Signal is a spool file extension / ingest route family.
type Signal string

const (
	SignalTrace   Signal = "trace"
	SignalLog     Signal = "log"
	SignalProfile Signal = "profile"
	SignalMetrics Signal = "metrics"
	SignalDST     Signal = "dst"
)

// Writer atomically writes JSON envelopes into a Chronos spool directory.
type Writer struct {
	Dir string
}

// WriteJSON marshals value and appends it to the tenant's spool log as one
// framed entry (ADR 0035), returning the segment it landed in.
//
// The name is kept from the era when this published {sha256}.{signal} through a
// tmp-{uuid} rename. Every caller's contract is unchanged — hand over a value
// and the signal it is, and it reaches the agent — but a document is now a
// frame in a shared segment rather than a file of its own, which is one write
// instead of a create, a write and a rename.
//
// The content address survives as the frame's id: it is what deduplicates a
// document re-shipped after a failed POST, a job the filename used to do.
func (w Writer) WriteJSON(signal Signal, value any) (string, error) {
	if w.Dir == "" {
		return "", fmt.Errorf("spool: empty directory")
	}
	body, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("spool: marshal: %w", err)
	}
	sum := sha256.Sum256(body)
	if err := Append(w.Dir, string(signal), "json", hex.EncodeToString(sum[:]), body); err != nil {
		return "", err
	}
	generations := Generations(w.Dir)
	if len(generations) == 0 {
		return w.Dir, nil
	}
	return filepath.Join(w.Dir, segmentName(generations[len(generations)-1])), nil
}

// WriteDocumentFile is the pre-ADR-0035 write: content-addressed, published via
// a tmp-{uuid} rename.
//
// Retained for the alias window — an agent that predates the framed log ships
// whole files — and as the fallback for a spool directory the log layout cannot
// be trusted on (a network filesystem, where a single append is not atomic).
func (w Writer) WriteDocumentFile(signal Signal, value any) (string, error) {
	if w.Dir == "" {
		return "", fmt.Errorf("spool: empty directory")
	}
	if err := os.MkdirAll(w.Dir, 0o750); err != nil {
		return "", fmt.Errorf("spool: mkdir: %w", err)
	}
	body, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("spool: marshal: %w", err)
	}
	sum := sha256.Sum256(body)
	hexName := hex.EncodeToString(sum[:])
	finalName := hexName + "." + string(signal)
	finalPath := filepath.Join(w.Dir, finalName)

	tmpName := "tmp-" + uuid.NewString()
	tmpPath := filepath.Join(w.Dir, tmpName)
	if err := os.WriteFile(tmpPath, body, 0o600); err != nil {
		return "", fmt.Errorf("spool: write tmp: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("spool: rename: %w", err)
	}
	return finalPath, nil
}
