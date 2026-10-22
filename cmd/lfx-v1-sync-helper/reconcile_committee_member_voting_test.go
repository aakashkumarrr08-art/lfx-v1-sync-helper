// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sort"
	"testing"
	"time"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
	"golang.org/x/time/rate"
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
	reconcileTestRecordD   = "f0000000-0000-0000-0000-00000000000f"
	reconcileTestContactA  = "lfContactAAAAAAAAA"
	reconcileTestContactB  = "lfContactBBBBBBBBB"
	reconcileTestContactC  = "lfContactCCCCCCCCC"
	reconcileTestContactD  = "lfContactDDDDDDDDD"
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
		// voting in sync, companion present but forward mapping missing (partial write).
		"m9": []byte(`{"uid":"m9","committee_uid":"cu","voting":{"status":"Voting Rep"}}`),
	}
	reverse := map[string]string{
		"m1": reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestContactA,
		"m2": reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestContactB,
		"m3": reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestRecordC,
		"m5": tombstoneMarker,
		"m6": "only-one-part",
		"m7": reconcileTestProject + ":" + reconcileTestCommittee + ":lfContactZZZZZZZZZ",
		"m9": reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestContactD,
	}
	companions := map[string]string{
		"m2": reconcileTestRecordB,
		"m3": reconcileTestRecordC,
		"m9": reconcileTestRecordD,
	}
	forwards := map[string]string{
		reconcileTestRecordB: "cu:m2",
		reconcileTestRecordC: "cu:m3",
	}
	objects := map[string][]byte{
		reconcileTestRecordA: []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"` + reconcileTestContactA + `","voting_status__c":"Voting Rep"}`),
		reconcileTestRecordB: []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"` + reconcileTestContactB + `","voting_status__c":"Observer"}`),
		reconcileTestRecordC: []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"` + reconcileTestContactC + `","voting_status__c":"None"}`),
		reconcileTestRecordD: []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"` + reconcileTestContactD + `","voting_status__c":"Voting Rep"}`),
		// soft-deleted record for m7's contact: must not be joined.
		"d0000000-0000-0000-0000-00000000000d": []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"lfContactZZZZZZZZZ","_sdc_deleted_at":"2026-01-01T00:00:00Z"}`),
		"e0000000-0000-0000-0000-00000000000e": {0xc1}, // invalid in both JSON and msgpack
	}

	res := reconcileCommitteeMemberVotingResult{}
	idx := buildV1MemberVotingIndex(t.Context(), objects, &res)
	if res.v1ObjectsUndecodable != 1 || res.errored != 1 {
		t.Errorf("v1_objects_undecodable = %d, errored = %d; want 1 and 1", res.v1ObjectsUndecodable, res.errored)
	}
	items := joinVotingReconcile(t.Context(), members, reverse, companions, forwards, idx, &res)

	// errored: one undecodable v1 record (e0…) and one undecodable member (m8).
	want := reconcileCommitteeMemberVotingResult{
		inspected:            9,
		noMapping:            2,
		poisonedMapping:      1,
		noV1Record:           1,
		v1ObjectsUndecodable: 1,
		matched:              2,
		mismatched:           2,
		mappingsMissing:      2,
		errored:              2,
	}
	if res != want {
		t.Errorf("counters = %+v\nwant %+v", res, want)
	}

	sort.Slice(items, func(i, j int) bool { return items[i].memberUID < items[j].memberUID })
	if len(items) != 3 {
		t.Fatalf("items = %+v, want m1, m3 and m9", items)
	}
	m1, m3, m9 := items[0], items[1], items[2]
	if m1.memberUID != "m1" || !m1.mismatch || m1.target != v1NoVotingStatus || !m1.needsCompanion || !m1.needsForward ||
		m1.recordSFID != reconcileTestRecordA || m1.contactSFID != reconcileTestContactA || m1.projectSFID != reconcileTestProject ||
		m1.reverseMapping != reverse["m1"] {
		t.Errorf("m1 = %+v", m1)
	}
	if m3.memberUID != "m3" || !m3.mismatch || m3.target != "Voting Rep" || m3.needsCompanion || m3.needsForward ||
		m3.recordSFID != reconcileTestRecordC || m3.contactSFID != reconcileTestContactC {
		t.Errorf("m3 = %+v", m3)
	}
	if m9.memberUID != "m9" || m9.mismatch || m9.needsCompanion || !m9.needsForward || m9.recordSFID != reconcileTestRecordD {
		t.Errorf("m9 = %+v", m9)
	}
}

func TestReconcileCommitteeMemberVotingJoinCompanionMismatch(t *testing.T) {
	reconcileTestLogger(t)
	objects := map[string][]byte{
		reconcileTestRecordA: []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"` + reconcileTestContactA + `","voting_status__c":"Voting Rep"}`),
	}
	res := reconcileCommitteeMemberVotingResult{}
	idx := buildV1MemberVotingIndex(t.Context(), objects, &res)
	member := map[string][]byte{"m1": []byte(`{"uid":"m1","committee_uid":"cu","voting":{"status":""}}`)}
	reverse := map[string]string{"m1": reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestContactA}
	// The companion names record B, but the join resolves record A.
	companions := map[string]string{"m1": reconcileTestRecordB}
	forwards := map[string]string{reconcileTestRecordB: "cu:m1"}

	items := joinVotingReconcile(t.Context(), member, reverse, companions, forwards, idx, &res)
	if len(items) != 0 {
		t.Errorf("items = %+v, want none: a mismatched companion must not be repaired or PATCHed", items)
	}
	want := reconcileCommitteeMemberVotingResult{inspected: 1, companionMismatch: 1}
	if res != want {
		t.Errorf("counters = %+v\nwant %+v", res, want)
	}
}

func TestReconcileCommitteeMemberVotingResultLogFields(t *testing.T) {
	res := reconcileCommitteeMemberVotingResult{
		dryRun: true, inspected: 1, noMapping: 2, poisonedMapping: 3, noV1Record: 4, ambiguousV1Record: 5,
		companionMismatch: 6, v1ObjectsUndecodable: 7, matched: 8, mismatched: 9, patched: 10, skippedNowEqual: 11,
		mappingsMissing: 12, mappingsBackfilled: 13, mappingConflicts: 14, errored: 15,
	}
	fields := res.logFields()
	if len(fields)%2 != 0 {
		t.Fatalf("logFields() returned an odd-length slice: %d", len(fields))
	}
	got := make(map[string]any, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			t.Fatalf("logFields()[%d] = %v, want a string key", i, fields[i])
		}
		got[key] = fields[i+1]
	}
	// Every result field must be logged, so a new counter cannot be added
	// without a logFields() entry.
	if n := reflect.TypeOf(res).NumField(); len(got) != n {
		t.Errorf("logFields() has %d keys, result struct has %d fields", len(got), n)
	}
	want := map[string]any{
		"dry_run": true, "inspected": 1, "no_mapping": 2, "poisoned_mapping": 3, "no_v1_record": 4,
		"ambiguous_v1_record": 5, "companion_mismatch": 6, "v1_objects_undecodable": 7, "matched": 8,
		"mismatched": 9, "patched": 10, "skipped_now_equal": 11, "mappings_missing": 12,
		"mappings_backfilled": 13, "mapping_conflicts": 14, "errored": 15,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("logFields()[%q] = %v, want %v", k, got[k], v)
		}
	}
}

func TestReconcileCommitteeMemberVotingJoinAmbiguous(t *testing.T) {
	reconcileTestLogger(t)
	objects := map[string][]byte{
		reconcileTestRecordA: []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"` + reconcileTestContactA + `","voting_status__c":"Voting Rep"}`),
		reconcileTestRecordB: []byte(`{"collaboration_name__c":"` + reconcileTestCommittee + `","contact_name__c":"` + reconcileTestContactA + `","voting_status__c":"Observer"}`),
	}
	idx := buildV1MemberVotingIndex(t.Context(), objects, &reconcileCommitteeMemberVotingResult{})
	member := map[string][]byte{"m1": []byte(`{"uid":"m1","committee_uid":"cu","voting":{"status":""}}`)}
	tests := []struct {
		name    string
		reverse string
	}{
		{name: "contact SFID reverse mapping", reverse: reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestContactA},
		// The legacy branch selects one row by record SFID, but the PATCH path
		// takes the shared contact SFID, so it must be skipped as well.
		{name: "legacy record SFID reverse mapping", reverse: reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestRecordA},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := reconcileCommitteeMemberVotingResult{}
			items := joinVotingReconcile(t.Context(), member, map[string]string{"m1": tt.reverse}, nil, nil, idx, &res)
			if len(items) != 0 || res.ambiguousV1Record != 1 {
				t.Errorf("items = %+v, ambiguous = %d; want none and 1", items, res.ambiguousV1Record)
			}
		})
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

	const owned = "cu:m1"
	votingRep := map[string]any{"voting_status__c": "Voting Rep"}
	tests := []struct {
		name          string
		forward       string // live record-keyed forward mapping; "" means absent
		forwardErr    error
		liveV2        string
		liveV1        map[string]any
		v1Found       bool
		updateErr     error
		wantPatch     string // "" means no PATCH expected
		wantPatched   int
		wantEqual     int
		wantNoV1      int
		wantNoMapping int
		wantConflicts int
		wantErrored   int
	}{
		{name: "still mismatched patches Observer", forward: owned, liveV1: votingRep, v1Found: true, wantPatch: v1NoVotingStatus, wantPatched: 1},
		{name: "now equal is skipped", forward: owned, liveV2: "Voting Rep", liveV1: votingRep, v1Found: true, wantEqual: 1},
		{name: "v1 record gone", forward: owned, v1Found: false, wantNoV1: 1},
		{name: "v1 PATCH failure is counted", forward: owned, liveV2: "Observer", liveV1: votingRep, v1Found: true, updateErr: errors.New("boom"), wantPatch: "Observer", wantErrored: 1},
		// The forward mapping names the v2 member that owns the v1 record; no
		// other member may PATCH it.
		{name: "forward owned by another member is not patched", forward: "cu:other", liveV1: votingRep, v1Found: true, wantConflicts: 1},
		{name: "tombstoned forward is not patched", forward: tombstoneMarker, liveV1: votingRep, v1Found: true, wantConflicts: 1},
		{name: "absent forward is not patched", liveV1: votingRep, v1Found: true, wantNoMapping: 1},
		{name: "forward read failure is not patched", forward: owned, forwardErr: errors.New("store unavailable"), liveV1: votingRep, v1Found: true, wantErrored: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeMappingStore()
			orig := mappingStore
			mappingStore = store
			t.Cleanup(func() { mappingStore = orig })
			if tt.forward != "" {
				if _, err := store.Put(t.Context(), "committee_member.sfid."+reconcileTestRecordA, []byte(tt.forward)); err != nil {
					t.Fatal(err)
				}
			}
			store.nextGetErr = tt.forwardErr

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
			applyVotingReconcilePatch(t.Context(), item, "committee_member.sfid.", "platform-community__c.", &res)
			if patched != tt.wantPatch {
				t.Errorf("PATCH VotingStatus = %q, want %q", patched, tt.wantPatch)
			}
			if res.patched != tt.wantPatched || res.skippedNowEqual != tt.wantEqual || res.noV1Record != tt.wantNoV1 ||
				res.noMapping != tt.wantNoMapping || res.mappingConflicts != tt.wantConflicts || res.errored != tt.wantErrored {
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
	if !createReconcileMapping(ctx, "k.new", "v", nil, &res) || res.mappingsBackfilled != 1 {
		t.Fatalf("absent key: res = %+v", res)
	}
	if !createReconcileMapping(ctx, "k.new", "v", nil, &res) || res.mappingsBackfilled != 1 || res.mappingConflicts != 0 {
		t.Fatalf("same value: res = %+v", res)
	}
	if _, err := store.Put(ctx, "k.other", []byte("someone-else")); err != nil {
		t.Fatal(err)
	}
	if createReconcileMapping(ctx, "k.other", "v", nil, &res) || res.mappingConflicts != 1 {
		t.Fatalf("different value: res = %+v", res)
	}
	if entry, _ := store.Get(ctx, "k.other"); string(entry.Value) != "someone-else" {
		t.Errorf("conflicting key was overwritten: %q", entry.Value)
	}
	store.nextGetErr = errors.New("store unavailable")
	if createReconcileMapping(ctx, "k.other", "v", nil, &res) || res.errored != 1 || res.mappingConflicts != 1 {
		t.Fatalf("read-back failure: res = %+v", res)
	}
}

func TestReconcileCommitteeMemberVotingBackfillMappings(t *testing.T) {
	reconcileTestLogger(t)
	reverseValue := reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestContactA
	forwardKey := "committee_member.sfid." + reconcileTestRecordA
	companionKey := committeeMemberRecordSFIDKey("m1")
	reverseKey := "committee_member.uid.m1"

	tests := []struct {
		name           string
		reverse        string // live reverse mapping value; "" means absent
		companion      string // pre-existing companion value; "" means absent
		forward        string // pre-existing forward value; "" means absent
		needsCompanion bool
		createErr      error // returned by the first Create (the forward key)
		wantCompanion  string
		wantForward    string
		wantCreated    int
		wantConflicts  int
		wantNoMapping  int
		wantErrored    int
		wantOwned      bool
	}{
		{name: "absent forward writes both", reverse: reverseValue, needsCompanion: true, wantCompanion: reconcileTestRecordA, wantForward: "cu:m1", wantCreated: 2, wantOwned: true},
		{name: "matching forward writes companion only", reverse: reverseValue, forward: "cu:m1", needsCompanion: true, wantCompanion: reconcileTestRecordA, wantForward: "cu:m1", wantCreated: 1, wantOwned: true},
		{name: "forward owned by another member writes nothing", reverse: reverseValue, forward: "cu:other", needsCompanion: true, wantForward: "cu:other", wantConflicts: 1},
		{name: "tombstoned forward writes nothing", reverse: reverseValue, forward: tombstoneMarker, needsCompanion: true, wantForward: tombstoneMarker, wantConflicts: 1},
		// Partial write from an earlier run or the live create path: companion
		// written, forward write failed. Only the forward key is repaired.
		{name: "companion present repairs missing forward only", reverse: reverseValue, companion: reconcileTestRecordA, wantCompanion: reconcileTestRecordA, wantForward: "cu:m1", wantCreated: 1, wantOwned: true},
		// The forward key is the ownership claim: if it cannot be written, no
		// companion is left behind for a later v2 delete to act on.
		{name: "forward write failure leaves no companion", reverse: reverseValue, needsCompanion: true, createErr: errors.New("store unavailable"), wantErrored: 1},
		{name: "member deleted since the scan writes nothing", reverse: tombstoneMarker, needsCompanion: true, wantNoMapping: 1},
		{name: "reverse mapping gone writes nothing", needsCompanion: true, wantNoMapping: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeMappingStore()
			orig := mappingStore
			mappingStore = store
			t.Cleanup(func() { mappingStore = orig })
			ctx := t.Context()
			for key, value := range map[string]string{reverseKey: tt.reverse, companionKey: tt.companion, forwardKey: tt.forward} {
				if value == "" {
					continue
				}
				if _, err := store.Put(ctx, key, []byte(value)); err != nil {
					t.Fatal(err)
				}
			}
			store.nextCreateErr = tt.createErr

			item := votingReconcileItem{
				memberUID: "m1", committeeUID: "cu", recordSFID: reconcileTestRecordA, reverseMapping: reverseValue,
				needsCompanion: tt.needsCompanion, needsForward: true,
			}
			res := reconcileCommitteeMemberVotingResult{}
			if owned := backfillReconcileMappings(ctx, item, "committee_member.sfid.", &res); owned != tt.wantOwned {
				t.Errorf("owned = %v, want %v", owned, tt.wantOwned)
			}

			if companion, _ := store.Get(ctx, companionKey); string(companion.Value) != tt.wantCompanion {
				t.Errorf("companion = %q, want %q", companion.Value, tt.wantCompanion)
			}
			if forward, _ := store.Get(ctx, forwardKey); string(forward.Value) != tt.wantForward {
				t.Errorf("forward = %q, want %q", forward.Value, tt.wantForward)
			}
			if res.mappingsBackfilled != tt.wantCreated || res.mappingConflicts != tt.wantConflicts ||
				res.noMapping != tt.wantNoMapping || res.errored != tt.wantErrored {
				t.Errorf("counters = %+v", res)
			}
		})
	}
}

// TestReconcileCommitteeMemberVotingItemTimeout verifies that a hung v1 PATCH
// is bounded by reconcileMemberTimeout, counted as an error, and does not stop
// the remaining members from being reconciled.
func TestReconcileCommitteeMemberVotingItemTimeout(t *testing.T) {
	reconcileTestLogger(t)
	origFetch, origGet, origUpdate, origTimeout := reconcileFetchCommitteeMemberFn, reconcileGetV1ObjectFn, reconcileUpdateV1MemberFn, reconcileMemberTimeout
	t.Cleanup(func() {
		reconcileFetchCommitteeMemberFn, reconcileGetV1ObjectFn, reconcileUpdateV1MemberFn = origFetch, origGet, origUpdate
		reconcileMemberTimeout = origTimeout
	})
	reconcileMemberTimeout = 20 * time.Millisecond
	store := newFakeMappingStore()
	origStore := mappingStore
	mappingStore = store
	t.Cleanup(func() { mappingStore = origStore })
	for record, owner := range map[string]string{reconcileTestRecordA: "cu:m1", reconcileTestRecordB: "cu:m2"} {
		if _, err := store.Put(t.Context(), "committee_member.sfid."+record, []byte(owner)); err != nil {
			t.Fatal(err)
		}
	}

	reconcileFetchCommitteeMemberFn = func(context.Context, string, string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
		return &committeeservice.CommitteeMemberFullWithReadonlyAttributes{}, "etag", nil
	}
	reconcileGetV1ObjectFn = func(context.Context, string) (map[string]any, bool, error) {
		return map[string]any{"voting_status__c": "Voting Rep"}, true, nil
	}
	reconcileUpdateV1MemberFn = func(ctx context.Context, _, _, memberSFID string, _ projectServiceCommitteeMemberUpdate) error {
		if memberSFID == reconcileTestContactA {
			<-ctx.Done() // hung gateway
			return ctx.Err()
		}
		return nil
	}

	items := []votingReconcileItem{
		{memberUID: "m1", committeeUID: "cu", contactSFID: reconcileTestContactA, recordSFID: reconcileTestRecordA, mismatch: true},
		{memberUID: "m2", committeeUID: "cu", contactSFID: reconcileTestContactB, recordSFID: reconcileTestRecordB, mismatch: true},
	}
	res := reconcileCommitteeMemberVotingResult{}
	if err := applyVotingReconcileItems(t.Context(), items, rate.NewLimiter(rate.Inf, 1), "committee_member.sfid.", "platform-community__c.", &res); err != nil {
		t.Fatalf("applyVotingReconcileItems: %v", err)
	}
	if res.errored != 1 || res.patched != 1 {
		t.Errorf("counters = %+v; want errored=1 patched=1", res)
	}
}

// TestReconcileCommitteeMemberVotingForeignOwnerNotPatched verifies that a
// member whose v1 record's forward mapping belongs to another v2 member is
// never PATCHed, whether the conflict is found by the mapping backfill or by
// the pre-PATCH ownership check. Otherwise a member with no voting status
// would overwrite the owner's "Voting Rep" with "Observer".
func TestReconcileCommitteeMemberVotingForeignOwnerNotPatched(t *testing.T) {
	reconcileTestLogger(t)
	origFetch, origGet, origUpdate := reconcileFetchCommitteeMemberFn, reconcileGetV1ObjectFn, reconcileUpdateV1MemberFn
	t.Cleanup(func() {
		reconcileFetchCommitteeMemberFn, reconcileGetV1ObjectFn, reconcileUpdateV1MemberFn = origFetch, origGet, origUpdate
	})
	reconcileFetchCommitteeMemberFn = func(context.Context, string, string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
		return &committeeservice.CommitteeMemberFullWithReadonlyAttributes{}, "etag", nil
	}
	reconcileGetV1ObjectFn = func(context.Context, string) (map[string]any, bool, error) {
		return map[string]any{"voting_status__c": "Voting Rep"}, true, nil
	}
	reconcileUpdateV1MemberFn = func(_ context.Context, _, _, memberSFID string, payload projectServiceCommitteeMemberUpdate) error {
		t.Errorf("PATCHed %s to %q; a foreign-owned v1 record must never be PATCHed", memberSFID, payload.VotingStatus)
		return nil
	}

	reverseValue := reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestContactA
	base := votingReconcileItem{
		memberUID: "m1", committeeUID: "cu", reverseMapping: reverseValue, projectSFID: reconcileTestProject,
		committeeSFID: reconcileTestCommittee, contactSFID: reconcileTestContactA, recordSFID: reconcileTestRecordA,
		v1Status: "Voting Rep", target: v1NoVotingStatus, mismatch: true,
	}
	tests := []struct {
		name           string
		needsCompanion bool
		needsForward   bool
	}{
		{name: "conflict found by backfill", needsCompanion: true, needsForward: true},
		{name: "existing companion skips backfill", needsCompanion: false, needsForward: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeMappingStore()
			orig := mappingStore
			mappingStore = store
			t.Cleanup(func() { mappingStore = orig })
			ctx := t.Context()
			for key, value := range map[string]string{
				"committee_member.uid.m1":                       reverseValue,
				"committee_member.sfid." + reconcileTestRecordA: "cu:other",
			} {
				if _, err := store.Put(ctx, key, []byte(value)); err != nil {
					t.Fatal(err)
				}
			}

			item := base
			item.needsCompanion, item.needsForward = tt.needsCompanion, tt.needsForward
			res := reconcileCommitteeMemberVotingResult{}
			if err := applyVotingReconcileItems(ctx, []votingReconcileItem{item}, rate.NewLimiter(rate.Inf, 1), "committee_member.sfid.", "platform-community__c.", &res); err != nil {
				t.Fatalf("applyVotingReconcileItems: %v", err)
			}
			if res.patched != 0 || res.mappingConflicts != 1 || res.errored != 0 {
				t.Errorf("counters = %+v; want patched=0 mapping_conflicts=1 errored=0", res)
			}
		})
	}
}

func TestForwardMappingNamesMember(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		{value: "cu:m1", want: true},
		{value: "m1", want: true}, // legacy bare member UID
		{value: "cu:other", want: false},
		{value: "other", want: false},
		{value: "cx:m1", want: false},
		{value: tombstoneMarker, want: false},
		{value: "", want: false},
	}
	for _, tt := range tests {
		if got := forwardMappingNamesMember(tt.value, "cu", "m1"); got != tt.want {
			t.Errorf("forwardMappingNamesMember(%q) = %v, want %v", tt.value, got, tt.want)
		}
	}
}

func TestReconcileCommitteeMemberVotingLegacyForwardMapping(t *testing.T) {
	reconcileTestLogger(t)
	origFetch, origGet, origUpdate := reconcileFetchCommitteeMemberFn, reconcileGetV1ObjectFn, reconcileUpdateV1MemberFn
	t.Cleanup(func() {
		reconcileFetchCommitteeMemberFn, reconcileGetV1ObjectFn, reconcileUpdateV1MemberFn = origFetch, origGet, origUpdate
	})
	reconcileFetchCommitteeMemberFn = func(context.Context, string, string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
		return &committeeservice.CommitteeMemberFullWithReadonlyAttributes{}, "etag", nil
	}
	reconcileGetV1ObjectFn = func(context.Context, string) (map[string]any, bool, error) {
		return map[string]any{"voting_status__c": "Voting Rep"}, true, nil
	}

	reverseValue := reconcileTestProject + ":" + reconcileTestCommittee + ":" + reconcileTestContactA
	forwardKey := "committee_member.sfid." + reconcileTestRecordA
	base := votingReconcileItem{
		memberUID: "m1", committeeUID: "cu", reverseMapping: reverseValue, projectSFID: reconcileTestProject,
		committeeSFID: reconcileTestCommittee, contactSFID: reconcileTestContactA, recordSFID: reconcileTestRecordA,
		v1Status: "Voting Rep", target: v1NoVotingStatus, mismatch: true,
	}
	tests := []struct {
		name           string
		needsCompanion bool
		wantBackfilled int
	}{
		{name: "companion present", needsCompanion: false, wantBackfilled: 0},
		{name: "companion missing", needsCompanion: true, wantBackfilled: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeMappingStore()
			orig := mappingStore
			mappingStore = store
			t.Cleanup(func() { mappingStore = orig })
			ctx := t.Context()
			seed := map[string]string{
				"committee_member.uid.m1": reverseValue,
				forwardKey:                "m1", // legacy format: bare member UID
			}
			if !tt.needsCompanion {
				seed[committeeMemberRecordSFIDKey("m1")] = reconcileTestRecordA
			}
			for key, value := range seed {
				if _, err := store.Put(ctx, key, []byte(value)); err != nil {
					t.Fatal(err)
				}
			}
			var patched []string
			reconcileUpdateV1MemberFn = func(_ context.Context, _, _, memberSFID string, payload projectServiceCommitteeMemberUpdate) error {
				patched = append(patched, memberSFID+"="+payload.VotingStatus)
				return nil
			}

			item := base
			item.needsCompanion, item.needsForward = tt.needsCompanion, tt.needsCompanion
			res := reconcileCommitteeMemberVotingResult{}
			if err := applyVotingReconcileItems(ctx, []votingReconcileItem{item}, rate.NewLimiter(rate.Inf, 1), "committee_member.sfid.", "platform-community__c.", &res); err != nil {
				t.Fatalf("applyVotingReconcileItems: %v", err)
			}
			if res.patched != 1 || res.mappingConflicts != 0 || res.errored != 0 || res.mappingsBackfilled != tt.wantBackfilled {
				t.Errorf("counters = %+v; want patched=1 mapping_conflicts=0 errored=0 mappings_backfilled=%d", res, tt.wantBackfilled)
			}
			if want := reconcileTestContactA + "=" + v1NoVotingStatus; len(patched) != 1 || patched[0] != want {
				t.Errorf("PATCHes = %v, want [%s]", patched, want)
			}
			if entry, err := store.Get(ctx, forwardKey); err != nil || string(entry.Value) != "m1" {
				t.Errorf("legacy forward mapping = %q, %v; want it left as %q", entry.Value, err, "m1")
			}
			if entry, err := store.Get(ctx, committeeMemberRecordSFIDKey("m1")); err != nil || string(entry.Value) != reconcileTestRecordA {
				t.Errorf("companion = %q, %v; want %q", entry.Value, err, reconcileTestRecordA)
			}
		})
	}
}

func TestReconcileCommitteeMemberVotingRejectsNonPositiveEmitRate(t *testing.T) {
	if _, err := reconcileCommitteeMemberVoting(t.Context(), reconcileCommitteeMemberVotingOpts{emitRate: 0}); err == nil {
		t.Fatal("expected an error for --emit-rate 0")
	}
}
