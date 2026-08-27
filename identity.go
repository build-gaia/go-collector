package chronos

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// ProcessingIdentity is the spool envelope processing block required by every signal.
type ProcessingIdentity struct {
	MessageID      string `json:"messageId"`
	BatchID        string `json:"batchId"`
	OrganisationID string `json:"organisationId,omitempty"`
	ProjectID      string `json:"projectId,omitempty"`
	ApplicationID  string `json:"applicationId,omitempty"`
}

// OrganisationRef mirrors chronos.common.v1.OrganisationRef JSON.
type OrganisationRef struct {
	OrganisationID string `json:"organisationId"`
}

// ApplicationRef mirrors chronos.common.v1.ApplicationRef JSON (project nested optional).
type ApplicationRef struct {
	ApplicationID string      `json:"applicationId"`
	Project       *ProjectRef `json:"project,omitempty"`
}

// ProjectRef nests organisation under project for richer spool envelopes.
type ProjectRef struct {
	Organisation OrganisationRef `json:"organisation"`
	ProjectID    string          `json:"projectId"`
}

func (c Config) organisationRef() OrganisationRef {
	return OrganisationRef{OrganisationID: c.Organisation}
}

func (c Config) applicationRef() ApplicationRef {
	return ApplicationRef{
		ApplicationID: c.Application,
		Project: &ProjectRef{
			Organisation: c.organisationRef(),
			ProjectID:    c.Project,
		},
	}
}

func (c Config) processing(seed string) ProcessingIdentity {
	batchID := deterministicID("batch", c.Organisation, c.Application, seed)
	messageID := deterministicID("message", c.Organisation, c.Application, seed, batchID)
	return ProcessingIdentity{
		MessageID:      messageID,
		BatchID:        batchID,
		OrganisationID: c.Organisation,
		ProjectID:      c.Project,
		ApplicationID:  c.Application,
	}
}

func deterministicID(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

func newTraceID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func newSpanID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func formatRFC3339Nano(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000Z")
}

func seedNow() string {
	return fmt.Sprintf("%d", time.Now().UTC().UnixNano())
}
