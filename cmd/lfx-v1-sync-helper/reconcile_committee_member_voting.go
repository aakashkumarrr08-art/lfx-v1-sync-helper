// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/time/rate"
)

// reconcileCommitteeMemberVotingOpts configures a --reconcile-committee-member-voting run.
type reconcileCommitteeMemberVotingOpts struct {
	dryRun   bool
	emitRate float64
}

// reconcileCommitteeMemberVotingResult summarizes a reconciliation run.
type reconcileCommitteeMemberVotingResult struct {
	dryRun               bool
	inspected            int // v2 committee members read from KV_committee-members
	noMapping            int // no live reverse mapping (never synced to v1, or deleted)
	poisonedMapping      int // reverse mapping malformed, or no contact SFID resolvable for it
	noV1Record           int // no live platform-community__c record for the member
	ambiguousV1Record    int // more than one live v1 record for the same committee + contact
	v1ObjectsUndecodable int // platform-community__c values that could not be decoded
	matched              int // voting status already in sync
	mismatched           int // voting status differs between v2 and v1
	patched              int // v1 PATCHed to the v2 value (live run)
	skippedNowEqual      int // mismatch resolved by the time of the live re-check
	mappingsMissing      int // members without a record-sfid companion mapping
	mappingsBackfilled   int // mapping keys created (companion + forward, live run)
	mappingConflicts     int // mapping key already present with a different value
	errored              int // decode, fetch, KV or v1 API failures
}

// logFields returns the complete counter set as alternating key/value pairs
// for logger.With, so the dry-run report and the final summary log the same
// fields.
func (res reconcileCommitteeMemberVotingResult) logFields() []any {
	return []any{
		"dry_run", res.dryRun,
		"inspected", res.inspected,
		"no_mapping", res.noMapping,
		"poisoned_mapping", res.poisonedMapping,
		"no_v1_record", res.noV1Record,
		"ambiguous_v1_record", res.ambiguousV1Record,
		"v1_objects_undecodable", res.v1ObjectsUndecodable,
		"matched", res.matched,
		"mismatched", res.mismatched,
		"patched", res.patched,
		"skipped_now_equal", res.skippedNowEqual,
		"mappings_missing", res.mappingsMissing,
		"mappings_backfilled", res.mappingsBackfilled,
		"mapping_conflicts", res.mappingConflicts,
		"errored", res.errored,
	}
}

// classifyVotingReconcile compares a v2 and a v1 voting status (v2 wins).
// An empty or "None" v2 status matches a v1 status of "", "None" or
// v1NoVotingStatus, since none of those is vote-eligible; otherwise the v1
// status must equal the v2 status (case-insensitive). target is the v1 value
// to PATCH when mismatch is true.
func classifyVotingReconcile(v2Status, v1Status string) (mismatch bool, target string) {
	v2 := normalizeVotingStatus(v2Status)
	v1 := normalizeVotingStatus(v1Status)
	if v2 == "" {
		if v1 == "" || strings.EqualFold(v1, v1NoVotingStatus) {
			return false, ""
		}
		return true, v1NoVotingStatus
	}
	if strings.EqualFold(v2, v1) {
		return false, ""
	}
	return true, v2
}

// votingReconcileMemberRecord is the minimal set of fields read from the
// committee-members KV bucket (JSON-encoded by the committee service).
type votingReconcileMemberRecord struct {
	UID          string `json:"uid"`
	CommitteeUID string `json:"committee_uid"`
	Voting       *struct {
		Status string `json:"status"`
	} `json:"voting"`
}

// v1MemberVoting is the voting-relevant view of a live platform-community__c record.
type v1MemberVoting struct {
	recordSFID   string
	contactSFID  string
	votingStatus string
}

// v1MemberVotingIndex indexes live platform-community__c records by
// committee SFID + ":" + contact SFID (the join key carried by the reverse
// mapping) and by record SFID (for legacy reverse mappings that store it).
type v1MemberVotingIndex struct {
	byContact map[string]v1MemberVoting
	byRecord  map[string]v1MemberVoting
	ambiguous map[string]bool
}

// buildV1MemberVotingIndex builds the index from a key → raw value map of
// platform-community__c records (keys are record SFIDs), skipping
// soft-deleted rows. It returns the number of undecodable values.
func buildV1MemberVotingIndex(objects map[string][]byte) (v1MemberVotingIndex, int) {
	idx := v1MemberVotingIndex{
		byContact: make(map[string]v1MemberVoting, len(objects)),
		byRecord:  make(map[string]v1MemberVoting, len(objects)),
		ambiguous: map[string]bool{},
	}
	undecodable := 0
	for recordSFID, raw := range objects {
		v1Data, err := decodeV1CommitteeRow(raw)
		if err != nil {
			undecodable++
			continue
		}
		if v1RowIsSoftDeleted(v1Data) {
			continue
		}
		committeeSFID, _ := v1Data["collaboration_name__c"].(string)
		contactSFID, _ := v1Data["contact_name__c"].(string)
		votingStatus, _ := v1Data["voting_status__c"].(string)
		rec := v1MemberVoting{
			recordSFID:   recordSFID,
			contactSFID:  strings.TrimSpace(contactSFID),
			votingStatus: votingStatus,
		}
		idx.byRecord[recordSFID] = rec
		committeeSFID = strings.TrimSpace(committeeSFID)
		if committeeSFID == "" || rec.contactSFID == "" {
			continue
		}
		key := committeeSFID + ":" + rec.contactSFID
		if _, dup := idx.byContact[key]; dup {
			idx.ambiguous[key] = true
		}
		idx.byContact[key] = rec
	}
	return idx, undecodable
}

// votingReconcileItem is a v2 member joined to its v1 record that needs a
// voting PATCH, a mapping backfill, or both.
type votingReconcileItem struct {
	memberUID     string
	committeeUID  string
	projectSFID   string
	committeeSFID string
	contactSFID   string
	recordSFID    string
	v2Status      string
	v1Status      string
	target        string
	mismatch      bool
	needsMappings bool
}

// joinVotingReconcile joins each v2 committee member (members: KV key → raw
// JSON) to its reverse mapping (reverse: member UID → mapping value), its
// record-sfid companion (companions: member UID → value) and its v1 record,
// classifies the pair and updates the counters in res. It returns the
// members that need a voting PATCH or a mapping backfill.
func joinVotingReconcile(
	ctx context.Context,
	members map[string][]byte,
	reverse map[string]string,
	companions map[string]string,
	v1 v1MemberVotingIndex,
	res *reconcileCommitteeMemberVotingResult,
) []votingReconcileItem {
	var items []votingReconcileItem
	for key, raw := range members {
		if strings.HasPrefix(key, "lookup/") {
			continue
		}
		res.inspected++

		var rec votingReconcileMemberRecord
		if err := json.Unmarshal(raw, &rec); err != nil || rec.UID == "" || rec.CommitteeUID == "" {
			logger.With(errKey, err, "key", key).
				WarnContext(ctx, "reconcile: committee-member record undecodable or missing uid/committee_uid, skipping")
			res.errored++
			continue
		}

		mappingValue, ok := reverse[rec.UID]
		if !ok || isTombstonedMapping([]byte(mappingValue)) {
			res.noMapping++
			continue
		}
		projectSFID, committeeSFID, recordSFID, contactSFID, ok := parseCommitteeMemberReverseMapping(mappingValue)
		if !ok {
			logger.With("member_uid", rec.UID).WarnContext(ctx, "reconcile: malformed committee-member reverse mapping, skipping")
			res.poisonedMapping++
			continue
		}

		var v1Rec v1MemberVoting
		var found bool
		if contactSFID != "" {
			joinKey := committeeSFID + ":" + contactSFID
			if v1.ambiguous[joinKey] {
				logger.With("member_uid", rec.UID, "committee_sfid", committeeSFID).
					WarnContext(ctx, "reconcile: more than one live v1 record for this committee member, skipping")
				res.ambiguousV1Record++
				continue
			}
			v1Rec, found = v1.byContact[joinKey]
		} else {
			// Legacy reverse mapping that stores the record sfid instead of the contact SFID.
			v1Rec, found = v1.byRecord[recordSFID]
			contactSFID = v1Rec.contactSFID
		}
		if !found {
			res.noV1Record++
			continue
		}
		if contactSFID == "" {
			logger.With("member_uid", rec.UID, "record_sfid", v1Rec.recordSFID).
				WarnContext(ctx, "reconcile: no contact SFID resolvable for committee member, skipping")
			res.poisonedMapping++
			continue
		}

		item := votingReconcileItem{
			memberUID:     rec.UID,
			committeeUID:  rec.CommitteeUID,
			projectSFID:   projectSFID,
			committeeSFID: committeeSFID,
			contactSFID:   contactSFID,
			recordSFID:    v1Rec.recordSFID,
			v1Status:      v1Rec.votingStatus,
		}
		if rec.Voting != nil {
			item.v2Status = rec.Voting.Status
		}
		item.mismatch, item.target = classifyVotingReconcile(item.v2Status, item.v1Status)
		if item.mismatch {
			res.mismatched++
		} else {
			res.matched++
		}

		companion, hasCompanion := companions[rec.UID]
		if !hasCompanion || isTombstonedMapping([]byte(companion)) {
			item.needsMappings = true
			res.mappingsMissing++
		}

		if item.mismatch || item.needsMappings {
			items = append(items, item)
		}
	}
	return items
}

// Injectable for tests.
var (
	reconcileFetchCommitteeMemberFn = fetchCommitteeMember
	reconcileGetV1ObjectFn          = getV1ObjectData
	reconcileUpdateV1MemberFn       = updateV1CommitteeMember
)

// reconcileCommitteeMemberVoting compares every v2 committee member's voting
// status with its v1 platform-community__c record and, on a live run, PATCHes
// v1 to the v2 value (v2 wins; an empty v2 status becomes v1NoVotingStatus).
// A live run also writes the record-sfid companion and record-keyed forward
// mapping for members created in Self Serve before lfx-self-serve#4079, so
// their later v1 edits reach v2. Existing mapping keys are never overwritten.
//
// Enumeration uses ScanSubjectData (sequential GetMsg / next_by_subj), never a
// consumer, for the same reasons as the other one-shot jobs (see nats_scan.go).
func reconcileCommitteeMemberVoting(ctx context.Context, opts reconcileCommitteeMemberVotingOpts) (reconcileCommitteeMemberVotingResult, error) {
	const (
		committeeMembersStream     = "KV_committee-members"
		committeeMembersSubject    = "$KV.committee-members.*"
		committeeMembersSubjectPfx = "$KV.committee-members."

		mappingsStream      = "KV_v1-mappings"
		reverseSubject      = "$KV.v1-mappings.committee_member.uid.*"
		reverseSubjectPfx   = "$KV.v1-mappings.committee_member.uid."
		companionSubject    = "$KV.v1-mappings.committee_member.record_sfid.*"
		companionSubjectPfx = "$KV.v1-mappings.committee_member.record_sfid."

		v1ObjectsStream         = "KV_v1-objects"
		v1ObjectSubject         = "$KV.v1-objects.platform-community__c.*"
		v1ObjectSubjectPfx      = "$KV.v1-objects.platform-community__c."
		v1ObjectKeyPrefix       = "platform-community__c."
		forwardMappingKeyPrefix = "committee_member.sfid."
	)

	res := reconcileCommitteeMemberVotingResult{dryRun: opts.dryRun}
	if opts.emitRate <= 0 {
		return res, fmt.Errorf("--emit-rate must be greater than 0, got %v", opts.emitRate)
	}

	opTimeout := cfg.NATSFetchMaxWait
	if opTimeout <= 0 {
		opTimeout = defaultNATSFetchMaxWait
	}

	scan := func(stream, subject, prefix string) (map[string][]byte, error) {
		subjects, err := ScanSubjectData(ctx, jsContext, stream, subject, opTimeout)
		if err != nil {
			return nil, fmt.Errorf("failed to enumerate %s (%s): %w", stream, subject, err)
		}
		out := make(map[string][]byte, len(subjects))
		for s, v := range subjects {
			out[strings.TrimPrefix(s, prefix)] = v
		}
		return out, nil
	}
	toStrings := func(m map[string][]byte) map[string]string {
		out := make(map[string]string, len(m))
		for k, v := range m {
			out[k] = string(v)
		}
		return out
	}

	members, err := scan(committeeMembersStream, committeeMembersSubject, committeeMembersSubjectPfx)
	if err != nil {
		return res, err
	}
	reverseRaw, err := scan(mappingsStream, reverseSubject, reverseSubjectPfx)
	if err != nil {
		return res, err
	}
	companionRaw, err := scan(mappingsStream, companionSubject, companionSubjectPfx)
	if err != nil {
		return res, err
	}
	v1Objects, err := scan(v1ObjectsStream, v1ObjectSubject, v1ObjectSubjectPfx)
	if err != nil {
		return res, err
	}

	v1Index, undecodable := buildV1MemberVotingIndex(v1Objects)
	res.v1ObjectsUndecodable = undecodable
	items := joinVotingReconcile(ctx, members, toStrings(reverseRaw), toStrings(companionRaw), v1Index, &res)

	if opts.dryRun {
		for _, item := range items {
			logger.With(
				"member_uid", item.memberUID,
				"committee_uid", item.committeeUID,
				"record_sfid", item.recordSFID,
				"v2_voting_status", item.v2Status,
				"v1_voting_status", item.v1Status,
				"target", item.target,
				"mismatch", item.mismatch,
				"needs_mappings", item.needsMappings,
			).InfoContext(ctx, "[dry-run] reconcile: committee member would be updated")
		}
		return res, nil
	}

	limiter := rate.NewLimiter(rate.Limit(opts.emitRate), 1)
	for _, item := range items {
		if item.needsMappings {
			backfillReconcileMappings(ctx, item, forwardMappingKeyPrefix, &res)
		}
		if !item.mismatch {
			continue
		}
		if err := limiter.Wait(ctx); err != nil {
			return res, err
		}
		applyVotingReconcilePatch(ctx, item, v1ObjectKeyPrefix, &res)
	}
	return res, nil
}

// applyVotingReconcilePatch re-reads the live v2 member and v1 record, then
// PATCHes v1 to the v2 value if the two still disagree.
func applyVotingReconcilePatch(ctx context.Context, item votingReconcileItem, v1ObjectKeyPrefix string, res *reconcileCommitteeMemberVotingResult) {
	log := logger.With("member_uid", item.memberUID, "committee_uid", item.committeeUID, "record_sfid", item.recordSFID)

	member, _, err := reconcileFetchCommitteeMemberFn(ctx, item.committeeUID, item.memberUID)
	if err != nil {
		log.With(errKey, err).WarnContext(ctx, "reconcile: failed to re-read v2 committee member, skipping")
		res.errored++
		return
	}
	v2Status := ""
	if member != nil && member.Voting != nil {
		v2Status = member.Voting.Status
	}

	v1Data, found, err := reconcileGetV1ObjectFn(ctx, v1ObjectKeyPrefix+item.recordSFID)
	if err != nil {
		log.With(errKey, err).WarnContext(ctx, "reconcile: failed to re-read v1 committee member, skipping")
		res.errored++
		return
	}
	if !found {
		res.noV1Record++
		return
	}
	v1Status, _ := v1Data["voting_status__c"].(string)

	mismatch, target := classifyVotingReconcile(v2Status, v1Status)
	if !mismatch {
		res.skippedNowEqual++
		return
	}

	if err := reconcileUpdateV1MemberFn(ctx, item.projectSFID, item.committeeSFID, item.contactSFID, projectServiceCommitteeMemberUpdate{VotingStatus: target}); err != nil {
		log.With(errKey, err).WarnContext(ctx, "reconcile: failed to PATCH v1 committee member voting status")
		res.errored++
		return
	}
	log.With("v2_voting_status", v2Status, "v1_voting_status", v1Status, "target", target).
		InfoContext(ctx, "reconcile: patched v1 committee member voting status")
	res.patched++
}

// backfillReconcileMappings writes the record-sfid companion and then the
// record-keyed forward mapping for item. The forward key is checked first: a
// companion pointing at a record whose forward key belongs to another member
// would make a later v2 delete of this member (syncCommitteeMemberDeleteToV1)
// tombstone that member's forward mapping and silence its WAL delete.
func backfillReconcileMappings(ctx context.Context, item votingReconcileItem, forwardMappingKeyPrefix string, res *reconcileCommitteeMemberVotingResult) {
	forwardKey := forwardMappingKeyPrefix + item.recordSFID
	forwardValue := item.committeeUID + ":" + item.memberUID

	entry, err := mappingStore.Get(ctx, forwardKey)
	switch {
	case err == nil && string(entry.Value) != forwardValue:
		logger.With("key", forwardKey, "member_uid", item.memberUID).
			WarnContext(ctx, "reconcile: forward mapping key already exists with a different value, not writing the record sfid companion")
		res.mappingConflicts++
		return
	case err != nil && !errors.Is(err, ErrKeyNotFound):
		logger.With(errKey, err, "key", forwardKey).WarnContext(ctx, "reconcile: failed to read forward mapping")
		res.errored++
		return
	}

	if createReconcileMapping(ctx, committeeMemberRecordSFIDKey(item.memberUID), item.recordSFID, res) {
		createReconcileMapping(ctx, forwardKey, forwardValue, res)
	}
}

// createReconcileMapping creates a mapping key only if it is absent. An
// existing key with the same value counts as success; one with a different
// value is a conflict and is never overwritten. Returns true when the key
// holds the intended value afterwards.
func createReconcileMapping(ctx context.Context, key, value string, res *reconcileCommitteeMemberVotingResult) bool {
	_, err := mappingStore.Create(ctx, key, []byte(value))
	if err == nil {
		res.mappingsBackfilled++
		return true
	}
	if !errors.Is(err, ErrKeyExists) {
		logger.With(errKey, err, "key", key).WarnContext(ctx, "reconcile: failed to create mapping")
		res.errored++
		return false
	}
	entry, getErr := mappingStore.Get(ctx, key)
	if getErr != nil {
		logger.With(errKey, getErr, "key", key).WarnContext(ctx, "reconcile: failed to read back existing mapping")
		res.errored++
		return false
	}
	if string(entry.Value) == value {
		return true
	}
	logger.With("key", key).
		WarnContext(ctx, "reconcile: mapping key already exists with a different value, not overwriting")
	res.mappingConflicts++
	return false
}
