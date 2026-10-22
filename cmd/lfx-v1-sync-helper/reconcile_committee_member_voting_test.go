// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"testing"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

func TestClassifyVotingReconcile(t *testing.T) {
	tests := []struct {
		v2, v1       string
		wantMismatch bool
		wantTarget   string
	}{
		{"", "", false, ""},
		{"", "None", false, ""},
		{"None", "", false, ""},
		{"", "Observer", false, ""},
		{"", "observer", false, ""},
		{"", "Voting Rep", true, v1NoVotingStatus},
		{"none", "Alternate Voting Rep", true, v1NoVotingStatus},
		{"Voting Rep", "Voting Rep", false, ""},
		{"Voting Rep", "voting rep", false, ""},
		{"Observer", "Voting Rep", true, "Observer"},
		{"Voting Rep", "", true, "Voting Rep"},
		{"Voting Rep", "None", true, "Voting Rep"},
		{"Observer", "", true, "Observer"},
	}
	for _, tt := range tests {
		mismatch, target := classifyVotingReconcile(tt.v2, tt.v1)
		if mismatch != tt.wantMismatch || target != tt.wantTarget {
			t.Errorf("classifyVotingReconcile(%q, %q) = (%v, %q), want (%v, %q)",
				tt.v2, tt.v1, mismatch, target, tt.wantMismatch, tt.wantTarget)
		}
	}
}

const (
	reconcileTestProject   = "a0941000002wBz9AAE"
	reconcileTestCommittee = "c0000000-0000-0000-0000-000000000001"
	reconcileTestRecordA   = "a0000000-0000-0000-0000-00000000000a"
	reconcileTestRecordB   = "b0000000-0000-0000-0000-00000000000b"
	reconcileTestRecordC   = "c0000000-0000-0000-0000-00000000000c"
	reconcileTestContactA  = "lfContactAAAAAAAAA"
	reconcileTestContactB  = "lfContactBBBBBBBBB"
	reconcileTestContactC  = "lfContactCCCCCCCCC"
)

func reconcileTestLogger(t *testing.T) {
	t.Helper()
	orig := logger
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Cleanup(func() { logger = orig })
}

func TestReconcileCommitteeMemberVotingJoin(t *testing.T) {
	reconcileTestLogger(t)

	members := map[string][]byte{
		"lookup/committee-members-by-email/x.m1": []byte(`{}`),
		// v2 empty, v1 Voting Rep → mismatch, created in Self Serve (no companion).
		"m1": []byte(`{"uid":"m1","committee_uid":"cu","voting":{"status":""}}`),
		// v2 empty, v1 Observer → matched, companion present.
		"m2": []byte(`{"uid":"m2","committee_uid":"cu","voting":{"status":""}}`),
		// legacy reverse mapping storing the record sfid; v2 Voting Rep, v1 None → mismatch.
		"m3": []byte(`{"uid":"m3","committee_uid":"cu","voting":{"status":"Voting Rep"}}`),
		// no reverse mapping.
		"m4": []byte(`{"uid":"m4","committee_uid":"cu"}`),
		// tombstoned reverse mapping.
		"m5": []byte(`{"uid":"m5","committee_uid":"cu"}`),
		// malformed reverse mapping.
		"m6": []byte(`{"uid":"m6","committee_uid":"cu"}`),
		// reverse mapping with no v1 record.
		"m7": []byte(`{"uid":"m7","committee_uid":"cu"}`),
		// undecodable.
		"m8": []byte(`not json`),
	}
	reverse := map[string]string{
		"m1": reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestContactA,
		"m2": reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestContactB,
		"m3": reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestRecordC,
		"m5": tombstoneMarker,
		"m6": "only-one-part",
		"m7": reconcileTestProject + ":" + reconcileTestCommittee + ":lfContactZZZZZZZZZ",
	}
	companions := map[string]string{
		"m2": reconcileTestRecordB,
		"m3": reconcileTestRecordC,
	}
	objects := map[string][]byte{
		reconcileTestRecordA: []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"` + reconcileTestContactA + `","voting_status__c":"Voting Rep"}`),
		reconcileTestRecordB: []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"` + reconcileTestContactB + `","voting_status__c":"Observer"}`),
		reconcileTestRecordC: []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"` + reconcileTestContactC + `","voting_status__c":"None"}`),
		// soft-deleted record for m7's contact: must not be joined.
		"d0000000-0000-0000-0000-00000000000d": []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"lfContactZZZZZZZZZ","_sdc_deleted_at":"2026-01-01T00:00:00Z"}`),
		"e0000000-0000-0000-0000-00000000000e": {0xc1}, // invalid in both JSON and msgpack
	}

	idx, undecodable := buildV1MemberVotingIndex(objects)
	if undecodable != 1 {
		t.Errorf("undecodable = %d, want 1", undecodable)
	}
	res := reconcileCommitteeMemberVotingResult{}
	items := joinVotingReconcile(t.Context(), members, reverse, companions, idx, &res)

	want := reconcileCommitteeMemberVotingResult{
		inspected:       8,
		noMapping:       2,
		poisonedMapping: 1,
		noV1Record:      1,
		matched:         1,
		mismatched:      2,
		mappingsMissing: 1,
		errored:         1,
	}
	if res != want {
		t.Errorf("counters = %+v\nwant %+v", res, want)
	}

	sort.Slice(items, func(i, j int) bool { return items[i].memberUID < items[j].memberUID })
	if len(items) != 2 {
		t.Fatalf("items = %+v, want m1 and m3", items)
	}
	m1, m3 := items[0], items[1]
	if m1.memberUID != "m1" || !m1.mismatch || m1.target != v1NoVotingStatus || !m1.needsMappings ||
		m1.recordSFID != reconcileTestRecordA || m1.contactSFID != reconcileTestContactA || m1.projectSFID != reconcileTestProject {
		t.Errorf("m1 = %+v", m1)
	}
	if m3.memberUID != "m3" || !m3.mismatch || m3.target != "Voting Rep" || m3.needsMappings ||
		m3.recordSFID != reconcileTestRecordC || m3.contactSFID != reconcileTestContactC {
		t.Errorf("m3 = %+v", m3)
	}
}

func TestReconcileCommitteeMemberVotingJoinAmbiguous(t *testing.T) {
	reconcileTestLogger(t)
	objects := map[string][]byte{
		reconcileTestRecordA: []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"` + reconcileTestContactA + `","voting_status__c":"Voting Rep"}`),
		reconcileTestRecordB: []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"` + reconcileTestContactA + `","voting_status__c":"Observer"}`),
	}
	idx, _ := buildV1MemberVotingIndex(objects)
	res := reconcileCommitteeMemberVotingResult{}
	items := joinVotingReconcile(t.Context(),
		map[string][]byte{"m1": []byte(`{"uid":"m1","committee_uid":"cu","voting":{"status":""}}`)},
		map[string]string{"m1": reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestContactA},
		nil, idx, &res)
	if len(items) != 0 || res.ambiguousV1Record != 1 {
		t.Errorf("items = %+v, ambiguous = %d; want none and 1", items, res.ambiguousV1Record)
	}
}

func TestReconcileCommitteeMemberVotingApply(t *testing.T) {
	reconcileTestLogger(t)
	origFetch, origGet, origUpdate := reconcileFetchCommitteeMemberFn, reconcileGetV1ObjectFn, reconcileUpdateV1MemberFn
	t.Cleanup(func() {
		reconcileFetchCommitteeMemberFn, reconcileGetV1ObjectFn, reconcileUpdateV1MemberFn = origFetch, origGet, origUpdate
	})

	item := votingReconcileItem{
		memberUID: "m1", committeeUID: "cu", projectSFID: reconcileTestProject,
		committeeSFID: reconcileTestCommittee, contactSFID: reconcileTestContactA, recordSFID: reconcileTestRecordA,
	}
	member := func(status string) *committeeservice.CommitteeMemberFullWithReadonlyAttributes {
		m := &committeeservice.CommitteeMemberFullWithReadonlyAttributes{}
		if status != "" {
			m.Voting = &struct {
				Status    string
				StartDate *string
				EndDate   *string
			}{Status: status}
		}
		return m
	}

	tests := []struct {
		name        string
		liveV2      string
		liveV1      map[string]any
		v1Found     bool
		updateErr   error
		wantPatch   string // "" means no PATCH expected
		wantPatched int
		wantEqual   int
		wantNoV1    int
		wantErrored int
	}{
		{name: "still mismatched patches Observer", liveV1: map[string]any{"voting_status__c": "Voting Rep"}, v1Found: true, wantPatch: v1NoVotingStatus, wantPatched: 1},
		{name: "now equal is skipped", liveV2: "Voting Rep", liveV1: map[string]any{"voting_status__c": "Voting Rep"}, v1Found: true, wantEqual: 1},
		{name: "v1 record gone", v1Found: false, wantNoV1: 1},
		{name: "v1 PATCH failure is counted", liveV2: "Observer", liveV1: map[string]any{"voting_status__c": "Voting Rep"}, v1Found: true, updateErr: errors.New("boom"), wantPatch: "Observer", wantErrored: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reconcileFetchCommitteeMemberFn = func(context.Context, string, string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
				return member(tt.liveV2), "etag", nil
			}
			reconcileGetV1ObjectFn = func(_ context.Context, key string) (map[string]any, bool, error) {
				if key != "platform-community__c."+reconcileTestRecordA {
					t.Errorf("v1 object key = %q", key)
				}
				return tt.liveV1, tt.v1Found, nil
			}
			var patched string
			reconcileUpdateV1MemberFn = func(_ context.Context, projectSFID, committeeSFID, memberSFID string, payload projectServiceCommitteeMemberUpdate) error {
				if projectSFID != reconcileTestProject || committeeSFID != reconcileTestCommittee || memberSFID != reconcileTestContactA {
					t.Errorf("PATCH path = %s/%s/%s; want the contact SFID path", projectSFID, committeeSFID, memberSFID)
				}
				if payload != (projectServiceCommitteeMemberUpdate{VotingStatus: payload.VotingStatus}) {
					t.Errorf("PATCH payload carries more than VotingStatus: %+v", payload)
				}
				patched = payload.VotingStatus
				return tt.updateErr
			}

			res := reconcileCommitteeMemberVotingResult{}
			applyVotingReconcilePatch(t.Context(), item, "platform-community__c.", &res)
			if patched != tt.wantPatch {
				t.Errorf("PATCH VotingStatus = %q, want %q", patched, tt.wantPatch)
			}
			if res.patched != tt.wantPatched || res.skippedNowEqual != tt.wantEqual || res.noV1Record != tt.wantNoV1 || res.errored != tt.wantErrored {
				t.Errorf("counters = %+v", res)
			}
		})
	}
}

func TestReconcileCommitteeMemberVotingCreateMapping(t *testing.T) {
	reconcileTestLogger(t)
	store := newFakeMappingStore()
	orig := mappingStore
	mappingStore = store
	t.Cleanup(func() { mappingStore = orig })
	ctx := t.Context()

	res := reconcileCommitteeMemberVotingResult{}
	if !createReconcileMapping(ctx, "k.new", "v", &res) || res.mappingsBackfilled != 1 {
		t.Fatalf("absent key: res = %+v", res)
	}
	if !createReconcileMapping(ctx, "k.new", "v", &res) || res.mappingsBackfilled != 1 || res.mappingConflicts != 0 {
		t.Fatalf("same value: res = %+v", res)
	}
	if _, err := store.Put(ctx, "k.other", []byte("someone-else")); err != nil {
		t.Fatal(err)
	}
	if createReconcileMapping(ctx, "k.other", "v", &res) || res.mappingConflicts != 1 {
		t.Fatalf("different value: res = %+v", res)
	}
	if entry, _ := store.Get(ctx, "k.other"); string(entry.Value) != "someone-else" {
		t.Errorf("conflicting key was overwritten: %q", entry.Value)
	}
	store.nextGetErr = errors.New("store unavailable")
	if createReconcileMapping(ctx, "k.other", "v", &res) || res.errored != 1 || res.mappingConflicts != 1 {
		t.Fatalf("read-back failure: res = %+v", res)
	}
}

func TestReconcileCommitteeMemberVotingBackfillMappings(t *testing.T) {
	reconcileTestLogger(t)
	item := votingReconcileItem{memberUID: "m1", committeeUID: "cu", recordSFID: reconcileTestRecordA}
	forwardKey := "committee_member.sfid." + reconcileTestRecordA
	companionKey := committeeMemberRecordSFIDKey("m1")

	tests := []struct {
		name          string
		forward       string // pre-existing forward value; "" means absent
		wantCompanion bool
		wantForward   string
		wantCreated   int
		wantConflicts int
	}{
		{name: "absent forward writes both", wantCompanion: true, wantForward: "cu:m1", wantCreated: 2},
		{name: "matching forward writes companion only", forward: "cu:m1", wantCompanion: true, wantForward: "cu:m1", wantCreated: 1},
		{name: "forward owned by another member writes nothing", forward: "cu:other", wantForward: "cu:other", wantConflicts: 1},
		{name: "tombstoned forward writes nothing", forward: tombstoneMarker, wantForward: tombstoneMarker, wantConflicts: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeMappingStore()
			orig := mappingStore
			mappingStore = store
			t.Cleanup(func() { mappingStore = orig })
			ctx := t.Context()
			if tt.forward != "" {
				if _, err := store.Put(ctx, forwardKey, []byte(tt.forward)); err != nil {
					t.Fatal(err)
				}
			}

			res := reconcileCommitteeMemberVotingResult{}
			backfillReconcileMappings(ctx, item, "committee_member.sfid.", &res)

			companion, err := store.Get(ctx, companionKey)
			if gotCompanion := err == nil; gotCompanion != tt.wantCompanion {
				t.Errorf("companion written = %v, want %v", gotCompanion, tt.wantCompanion)
			} else if gotCompanion && string(companion.Value) != reconcileTestRecordA {
				t.Errorf("companion = %q, want %q", companion.Value, reconcileTestRecordA)
			}
			if forward, _ := store.Get(ctx, forwardKey); string(forward.Value) != tt.wantForward {
				t.Errorf("forward = %q, want %q", forward.Value, tt.wantForward)
			}
			if res.mappingsBackfilled != tt.wantCreated || res.mappingConflicts != tt.wantConflicts || res.errored != 0 {
				t.Errorf("counters = %+v", res)
			}
		})
	}
}

func TestReconcileCommitteeMemberVotingRejectsNonPositiveEmitRate(t *testing.T) {
	if _, err := reconcileCommitteeMemberVoting(t.Context(), reconcileCommitteeMemberVotingOpts{emitRate: 0}); err == nil {
		t.Fatal("expected an error for --emit-rate 0")
	}
}
