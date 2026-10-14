// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func init() {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

// fakeMsg is a test double for jetstream.Msg that records whether Ack or Nak was called.
type fakeMsg struct {
	subject  string
	data     []byte
	acked    bool
	nacked   bool
	ackCalls int
	nakCalls int
}

func (m *fakeMsg) Subject() string                           { return m.subject }
func (m *fakeMsg) Data() []byte                              { return m.data }
func (m *fakeMsg) Headers() nats.Header                      { return nil }
func (m *fakeMsg) Reply() string                             { return "" }
func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (m *fakeMsg) Ack() error {
	m.acked = true
	m.ackCalls++
	return nil
}
func (m *fakeMsg) DoubleAck(context.Context) error { return m.Ack() }
func (m *fakeMsg) Nak() error {
	m.nacked = true
	m.nakCalls++
	return nil
}
func (m *fakeMsg) NakWithDelay(time.Duration) error { return m.Nak() }
func (m *fakeMsg) InProgress() error                { return nil }
func (m *fakeMsg) Term() error                      { return nil }
func (m *fakeMsg) TermWithReason(string) error      { return nil }

// errProc is a committeeEventProcessor that always returns a transient error.
func errProc(_ context.Context, _ string, _ []byte) error {
	return errors.New("transient V1 API failure")
}

// okProc is a committeeEventProcessor that always returns nil (success or permanent skip).
func okProc(_ context.Context, _ string, _ []byte) error {
	return nil
}

// TestCommitteeEventsIngestHandler_UnknownSubject verifies that an unrecognised
// subject is ACKed (not retried) regardless of the processor outcome.
func TestCommitteeEventsIngestHandler_UnknownSubject(t *testing.T) {
	h := newCommitteeEventsIngestHandler(errProc, errProc)
	msg := &fakeMsg{subject: "lfx.project.created", data: []byte(`{}`)}
	h(msg)
	if !msg.acked {
		t.Error("expected ACK for unknown subject")
	}
	if msg.nacked {
		t.Error("expected no NAK for unknown subject")
	}
}

// TestCommitteeEventsIngestHandler_CommitteeSubject_TransientError verifies that a
// transient error from the committee processor causes a NAK so JetStream redelivers.
func TestCommitteeEventsIngestHandler_CommitteeSubject_TransientError(t *testing.T) {
	h := newCommitteeEventsIngestHandler(errProc, okProc)
	msg := &fakeMsg{subject: "lfx.committee.updated", data: []byte(`{}`)}
	h(msg)
	if msg.acked {
		t.Error("expected no ACK on transient error")
	}
	if !msg.nacked {
		t.Error("expected NAK on transient error so JetStream redelivers")
	}
}

// TestCommitteeEventsIngestHandler_CommitteeSubject_Success verifies that a
// successful (nil-error) committee processor result causes an ACK.
func TestCommitteeEventsIngestHandler_CommitteeSubject_Success(t *testing.T) {
	h := newCommitteeEventsIngestHandler(okProc, errProc)
	msg := &fakeMsg{subject: "lfx.committee.created", data: []byte(`{}`)}
	h(msg)
	if !msg.acked {
		t.Error("expected ACK on success")
	}
	if msg.nacked {
		t.Error("expected no NAK on success")
	}
}

// TestCommitteeEventsIngestHandler_MemberSubject_TransientError verifies that a
// transient error from the member processor causes a NAK.
func TestCommitteeEventsIngestHandler_MemberSubject_TransientError(t *testing.T) {
	h := newCommitteeEventsIngestHandler(okProc, errProc)
	msg := &fakeMsg{subject: "lfx.committee_member.deleted", data: []byte(`{}`)}
	h(msg)
	if msg.acked {
		t.Error("expected no ACK on transient error")
	}
	if !msg.nacked {
		t.Error("expected NAK on transient error so JetStream redelivers")
	}
}

// TestCommitteeEventsIngestHandler_MemberSubject_Success verifies that a
// successful member processor result causes an ACK.
func TestCommitteeEventsIngestHandler_MemberSubject_Success(t *testing.T) {
	h := newCommitteeEventsIngestHandler(errProc, okProc)
	msg := &fakeMsg{subject: "lfx.committee_member.created", data: []byte(`{}`)}
	h(msg)
	if !msg.acked {
		t.Error("expected ACK on success")
	}
	if msg.nacked {
		t.Error("expected no NAK on success")
	}
}

// TestCommitteeEventsIngestHandler_InvalidJSON_ACKs verifies that malformed JSON
// payloads are ACKed (permanent failure — retrying won't produce valid JSON).
func TestCommitteeEventsIngestHandler_InvalidJSON_ACKs(t *testing.T) {
	tests := []struct {
		name    string
		subject string
	}{
		{"committee subject", "lfx.committee.created"},
		{"committee_member subject", "lfx.committee_member.created"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use the real process functions: invalid JSON returns nil immediately.
			h := newCommitteeEventsIngestHandler(processCommitteeIndexingEvent, processCommitteeMemberIndexingEvent)
			msg := &fakeMsg{subject: tt.subject, data: []byte(`not valid json`)}
			h(msg)
			if !msg.acked {
				t.Errorf("expected ACK for invalid JSON on %s (permanent failure, no retry)", tt.subject)
			}
			if msg.nacked {
				t.Errorf("expected no NAK for invalid JSON on %s", tt.subject)
			}
		})
	}
}

// TestCommitteeEventsIngestHandler_Dispatch verifies that committee subjects route
// to committeeProc and committee_member subjects route to memberProc.
func TestCommitteeEventsIngestHandler_Dispatch(t *testing.T) {
	var committeeHit, memberHit bool
	trackCommittee := func(_ context.Context, _ string, _ []byte) error { committeeHit = true; return nil }
	trackMember := func(_ context.Context, _ string, _ []byte) error { memberHit = true; return nil }

	h := newCommitteeEventsIngestHandler(trackCommittee, trackMember)

	h(&fakeMsg{subject: "lfx.committee.updated", data: []byte(`{}`)})
	if !committeeHit {
		t.Error("lfx.committee.* subject did not route to committeeProc")
	}
	if memberHit {
		t.Error("lfx.committee.* subject should not route to memberProc")
	}

	committeeHit, memberHit = false, false

	h(&fakeMsg{subject: "lfx.committee_member.updated", data: []byte(`{}`)})
	if !memberHit {
		t.Error("lfx.committee_member.* subject did not route to memberProc")
	}
	if committeeHit {
		t.Error("lfx.committee_member.* subject should not route to committeeProc")
	}
}
