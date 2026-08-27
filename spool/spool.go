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

// WriteJSON marshals value, hashes the body, and atomically publishes
// {sha256}.{signal} via a tmp-{uuid} rename.
func (w Writer) WriteJSON(signal Signal, value any) (string, error) {
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
