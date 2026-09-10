package spool

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// The cross-language contract. engine-core's spool_frame tests and the PHP
// extension's spool_log tests assert these exact bytes for this exact input; if
// any of the three drifts, the reader stops reading what a writer writes.
func TestGoldenVectorMatchesTheReaders(t *testing.T) {
	frame, err := EncodeFrame("trace", "json", "abc123", []byte("{}"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	const expected = "4348524e3200020000001980212a7b227369676e616c223a227472616365222c" +
		"22656e636f64696e67223a226a736f6e222c226964223a22616263313233227d7b7d"
	if got := hex.EncodeToString(frame); got != expected {
		t.Fatalf("frame bytes drifted from the reader's vector:\n got %s\nwant %s", got, expected)
	}
	if sum := crc32.ChecksumIEEE([]byte("chronos")); sum != 0x4AE2E10F {
		t.Fatalf("checksum drifted: got %#x", sum)
	}
}

func TestAppendsLandInOneSegmentInOrder(t *testing.T) {
	dir := t.TempDir()
	if err := Append(dir, "trace", "json", "one", []byte(`{"a":1}`)); err != nil {
		t.Fatalf("first append: %v", err)
	}
	if err := Append(dir, "log", "json", "two", []byte(`{"b":2}`)); err != nil {
		t.Fatalf("second append: %v", err)
	}
	generations := Generations(dir)
	if len(generations) != 1 || generations[0] != 1 {
		t.Fatalf("expected one segment, got %v", generations)
	}
	first, _ := EncodeFrame("trace", "json", "one", []byte(`{"a":1}`))
	second, _ := EncodeFrame("log", "json", "two", []byte(`{"b":2}`))
	written, err := os.ReadFile(filepath.Join(dir, segmentName(1)))
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if !bytes.Equal(written, append(first, second...)) {
		t.Fatal("the segment is not the two frames, in order")
	}
}

// The claim the design rests on: N goroutines appending produce N intact frames
// and no interleaving.
func TestConcurrentWritersNeverInterleaveAFrame(t *testing.T) {
	dir := t.TempDir()
	const writers, perWriter = 8, 40
	var group sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		group.Add(1)
		go func(writer int) {
			defer group.Done()
			for sequence := 0; sequence < perWriter; sequence++ {
				// Unequal payload lengths on purpose: equal-sized writes could
				// interleave and still decode by luck.
				payload := fmt.Sprintf(
					`{"writer":%d,"sequence":%d,"pad":%q}`,
					writer, sequence, string(bytes.Repeat([]byte("x"), writer*37+sequence)),
				)
				id := fmt.Sprintf("%d-%d", writer, sequence)
				if err := Append(dir, "trace", "json", id, []byte(payload)); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(writer)
	}
	group.Wait()

	seen := map[string]bool{}
	for _, generation := range Generations(dir) {
		segment, err := os.ReadFile(filepath.Join(dir, segmentName(generation)))
		if err != nil {
			t.Fatalf("read segment: %v", err)
		}
		for offset := 0; offset < len(segment); {
			rest := segment[offset:]
			if !bytes.Equal(rest[:4], frameMagic[:]) {
				t.Fatalf("frame boundary lost at byte %d", offset)
			}
			headerLen := int(binary.LittleEndian.Uint16(rest[4:6]))
			payloadLen := int(binary.LittleEndian.Uint32(rest[6:10]))
			expected := binary.LittleEndian.Uint32(rest[10:14])
			total := framePrefixBytes + headerLen + payloadLen
			body := rest[framePrefixBytes:total]
			if crc32.ChecksumIEEE(body) != expected {
				t.Fatalf("checksum mismatch at byte %d", offset)
			}
			var header frameHeader
			if err := json.Unmarshal(body[:headerLen], &header); err != nil {
				t.Fatalf("header at byte %d: %v", offset, err)
			}
			seen[header.ID] = true
			offset += total
		}
	}
	if len(seen) != writers*perWriter {
		t.Fatalf("expected %d frames, found %d", writers*perWriter, len(seen))
	}
}

func TestTheSegmentBudgetDropsTheOldest(t *testing.T) {
	dir := t.TempDir()
	for generation := uint64(1); generation <= MaxSegments+2; generation++ {
		file, err := createSegment(dir, generation)
		if err != nil {
			t.Fatalf("create segment %d: %v", generation, err)
		}
		_ = file.Close()
	}
	before := Dropped()
	enforceSegmentBudget(dir)
	generations := Generations(dir)
	if len(generations) != MaxSegments {
		t.Fatalf("expected %d segments, got %v", MaxSegments, generations)
	}
	if generations[0] != 3 {
		t.Fatalf("the oldest generations must be the dropped ones, kept %v", generations)
	}
	if Dropped() <= before {
		t.Fatal("a drop must be counted")
	}
}

func TestRotationIsWonByExactlyOneWriter(t *testing.T) {
	dir := t.TempDir()
	file, err := createSegment(dir, 1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = file.Close()

	first, one, err := rotate(dir, 1)
	if err != nil {
		t.Fatalf("first rotation: %v", err)
	}
	_ = one.Close()
	second, two, err := rotate(dir, 1)
	if err != nil {
		t.Fatalf("racing rotation: %v", err)
	}
	_ = two.Close()
	if first != 2 || second != 2 {
		t.Fatalf("a losing writer must append to the winner's segment, got %d and %d", first, second)
	}
}

func TestAnUnnamedSignalIsRefused(t *testing.T) {
	if _, err := EncodeFrame("", "json", "id", []byte("{}")); err == nil {
		t.Fatal("a frame with no signal must be refused at the source")
	}
}
