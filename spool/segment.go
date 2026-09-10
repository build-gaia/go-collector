package spool

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

// The framed, append-only spool log (ADR 0035).
//
// A tenant spool directory holds segment-000001.spool, segment-000002.spool, …
// and every document is appended to the newest one as a single self-describing
// frame:
//
//	magic       "CHRN"   4 bytes
//	headerLen   u16 LE   2 bytes
//	payloadLen  u32 LE   4 bytes
//	checksum    u32 LE   4 bytes   CRC-32 (IEEE) over header || payload
//	header      JSON     headerLen bytes
//	payload              payloadLen bytes
//
// Three rules make it safe, and they are the same three the Rust writer in the
// PHP extension follows:
//
//  1. One frame is one Write on an O_APPEND descriptor, so concurrent writers
//     on one host cannot interleave. A short write is a failed write, never
//     continued — a torn frame costs the reader every frame after it.
//  2. The writer rotates; the reader deletes. Rotation uses O_EXCL so exactly
//     one racing writer creates the next generation. Only the agent, which
//     holds the read cursor, knows what has been shipped and may delete it.
//  3. Nothing is synced. The spool must survive process death, which the page
//     cache already provides — not a power cut.
const (
	// SegmentMaxBytes is the size at which the active segment is retired.
	SegmentMaxBytes = 64 * 1024 * 1024
	// MaxSegments is how many segments one directory may hold before the
	// oldest is dropped to make room.
	MaxSegments = 4

	segmentPrefix    = "segment-"
	segmentExtension = ".spool"
	framePrefixBytes = 14
	maxFramePayload  = 8 * 1024 * 1024
)

var frameMagic = [4]byte{'C', 'H', 'R', 'N'}

var (
	droppedFrames atomic.Uint64
	droppedBytes  atomic.Uint64
)

// Dropped reports frames this process has discarded: the segment budget was
// reached, or a frame could not be written atomically.
//
// Fail-open is only acceptable when it is counted. A caller that reports
// nothing else about the spool should still report this.
func Dropped() uint64 { return droppedFrames.Load() }

// DroppedBytes reports the bytes discarded alongside Dropped.
func DroppedBytes() uint64 { return droppedBytes.Load() }

// frameHeader is what a frame says about itself. Field ORDER is part of the
// cross-language golden vector: encoding/json marshals in declaration order and
// the Rust side's serde does the same, so these three stay in this order.
type frameHeader struct {
	Signal   string `json:"signal"`
	Encoding string `json:"encoding"`
	ID       string `json:"id"`
}

// EncodeFrame builds one frame. Exported so the golden vector can be asserted
// against the reader's, which is the only thing keeping the two encoders honest.
func EncodeFrame(signal, encoding, id string, payload []byte) ([]byte, error) {
	if signal == "" || encoding == "" {
		return nil, fmt.Errorf("spool: a frame must name its signal and encoding")
	}
	if len(payload) > maxFramePayload {
		return nil, fmt.Errorf("spool: payload of %d bytes exceeds the frame budget", len(payload))
	}
	header, err := json.Marshal(frameHeader{Signal: signal, Encoding: encoding, ID: id})
	if err != nil {
		return nil, fmt.Errorf("spool: marshal frame header: %w", err)
	}
	if len(header) > 0xFFFF {
		return nil, fmt.Errorf("spool: frame header of %d bytes exceeds 65535", len(header))
	}

	body := make([]byte, 0, len(header)+len(payload))
	body = append(body, header...)
	body = append(body, payload...)

	frame := make([]byte, 0, framePrefixBytes+len(body))
	frame = append(frame, frameMagic[:]...)
	frame = binary.LittleEndian.AppendUint16(frame, uint16(len(header)))
	frame = binary.LittleEndian.AppendUint32(frame, uint32(len(payload)))
	frame = binary.LittleEndian.AppendUint32(frame, crc32.ChecksumIEEE(body))
	frame = append(frame, body...)
	return frame, nil
}

func segmentName(generation uint64) string {
	return fmt.Sprintf("%s%06d%s", segmentPrefix, generation, segmentExtension)
}

func generationOf(name string) (uint64, bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentExtension) {
		return 0, false
	}
	digits := strings.TrimSuffix(strings.TrimPrefix(name, segmentPrefix), segmentExtension)
	generation, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return generation, true
}

// Generations lists every segment generation in dir, ascending.
func Generations(dir string) []uint64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var generations []uint64
	for _, entry := range entries {
		if generation, ok := generationOf(entry.Name()); ok {
			generations = append(generations, generation)
		}
	}
	sort.Slice(generations, func(i, j int) bool { return generations[i] < generations[j] })
	return generations
}

func createSegment(dir string, generation uint64) (*os.File, error) {
	return os.OpenFile(
		filepath.Join(dir, segmentName(generation)),
		os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY,
		0o600,
	)
}

// openActive opens the newest segment for appending, creating the first one if
// the directory holds none.
func openActive(dir string) (uint64, *os.File, error) {
	for {
		generations := Generations(dir)
		if len(generations) == 0 {
			generation := uint64(1)
			file, err := createSegment(dir, generation)
			if os.IsExist(err) {
				continue
			}
			if err != nil {
				return 0, nil, err
			}
			return generation, file, nil
		}
		generation := generations[len(generations)-1]
		file, err := os.OpenFile(
			filepath.Join(dir, segmentName(generation)),
			os.O_APPEND|os.O_WRONLY,
			0o600,
		)
		// Retired by the agent between the scan and the open: rescan rather
		// than recreating a generation the reader has finished with.
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return 0, nil, err
		}
		return generation, file, nil
	}
}

// rotate retires the active segment and returns the next one. O_EXCL is the
// whole concurrency story: several writers can reach the threshold at once,
// exactly one creates generation N+1, and the rest open what the winner made.
func rotate(dir string, active uint64) (uint64, *os.File, error) {
	next := active + 1
	file, err := createSegment(dir, next)
	if os.IsExist(err) {
		return openActive(dir)
	}
	if err != nil {
		return 0, nil, err
	}
	enforceSegmentBudget(dir)
	return next, file, nil
}

// enforceSegmentBudget drops the oldest segments until at most MaxSegments
// remain.
//
// This is the fail-open edge of the design: those frames may never have been
// shipped. Un-shipped telemetry is worth less than the host's remaining disk,
// but only if the trade is counted rather than silent.
func enforceSegmentBudget(dir string) {
	generations := Generations(dir)
	if len(generations) <= MaxSegments {
		return
	}
	for _, generation := range generations[:len(generations)-MaxSegments] {
		path := filepath.Join(dir, segmentName(generation))
		var size int64
		if info, err := os.Stat(path); err == nil {
			size = info.Size()
		}
		if err := os.Remove(path); err == nil {
			droppedFrames.Add(1)
			droppedBytes.Add(uint64(size))
		}
	}
}

// Append writes one document to the tenant's spool log as a framed entry.
//
// signal is the name the document used to carry as a filename extension and is
// what ingest dispatches on; id is its identity, which used to be the
// content-addressed filename and is now what deduplication reads.
func Append(dir, signal, encoding, id string, payload []byte) error {
	frame, err := EncodeFrame(signal, encoding, id, payload)
	if err != nil {
		droppedFrames.Add(1)
		droppedBytes.Add(uint64(len(payload)))
		return err
	}
	if dir == "" {
		return fmt.Errorf("spool: empty directory")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("spool: mkdir: %w", err)
	}
	generation, file, err := openActive(dir)
	if err != nil {
		return fmt.Errorf("spool: open segment: %w", err)
	}
	if info, err := file.Stat(); err == nil && info.Size() >= SegmentMaxBytes {
		_, next, err := rotate(dir, generation)
		_ = file.Close()
		if err != nil {
			return fmt.Errorf("spool: rotate segment: %w", err)
		}
		file = next
	}
	// Deferred AFTER any rotation, so it closes the descriptor actually written
	// to rather than the one that was replaced.
	defer file.Close()

	// ONE write, and its result inspected.
	written, err := file.Write(frame)
	if err != nil {
		return fmt.Errorf("spool: append frame: %w", err)
	}
	if written != len(frame) {
		droppedFrames.Add(1)
		droppedBytes.Add(uint64(len(frame)))
		return fmt.Errorf("spool: short append (%d of %d bytes)", written, len(frame))
	}
	// No Sync. See the package documentation and ADR 0035 §1.
	return nil
}
