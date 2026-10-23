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
	"time"

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
	noMapping            int // no live reverse mapping at join time; reverse mapping removed or changed before the live backfill; or no record-keyed forward mapping at PATCH time
	poisonedMapping      int // reverse mapping malformed, or no contact SFID resolvable for it
	noV1Record           int // no live platform-community__c record for the member
	ambiguousV1Record    int // more than one live v1 record for the same committee + contact
	companionMismatch    int // record-sfid companion names a different v1 record than the join; needs manual repair
	v1ObjectsUndecodable int // platform-community__c values that could not be decoded
	matched              int // voting status already in sync
	mismatched           int // voting status differs between v2 and v1
	patched              int // v1 PATCHed to the v2 value (live run)
	skippedNowEqual      int // mismatch resolved by the time of the live re-check
	mappingsMissing      int // members missing the record-sfid companion or the record-keyed forward mapping
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
		"companion_mismatch", res.companionMismatch,
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
// soft-deleted rows. An undecodable value counts as both
// v1ObjectsUndecodable and errored: its member would otherwise be reported as
// no_v1_record without its voting status ever being checked.
func buildV1MemberVotingIndex(ctx context.Context, objects map[string][]byte, res *reconcileCommitteeMemberVotingResult) v1MemberVotingIndex {
	idx := v1MemberVotingIndex{
		byContact: make(map[string]v1MemberVoting, len(objects)),
		byRecord:  make(map[string]v1MemberVoting, len(objects)),
		ambiguous: map[string]bool{},
	}
	for recordSFID, raw := range objects {
		v1Data, err := decodeV1CommitteeRow(raw)
		if err != nil {
			logger.With(errKey, err, "record_sfid", recordSFID).
				WarnContext(ctx, "reconcile: v1 committee member record undecodable, skipping")
			res.v1ObjectsUndecodable++
			res.errored++
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
	return idx
}

// votingReconcileItem is a v2 member joined to its v1 record that needs a
// voting PATCH, a mapping backfill, or both.
type votingReconcileItem struct {
	memberUID      string
	committeeUID   string
	reverseMapping string // reverse mapping value at scan time, re-checked before mapping writes
	projectSFID    string
	committeeSFID  string
	contactSFID    string
	recordSFID     string
	v2Status       string
	v1Status       string
	target         string
	mismatch       bool
	needsCompanion bool // record-sfid companion missing
	needsForward   bool // record-keyed forward mapping missing
}

// joinVotingReconcile joins each v2 committee member (members: KV key → raw
// JSON) to its reverse mapping (reverse: member UID → mapping value), its
// record-sfid companion (companions: member UID → value), its record-keyed
// forward mapping (forwards: SFID → value) and its v1 record, classifies the
// pair and updates the counters in res. It returns the members that need a
// voting PATCH or a mapping backfill.
func joinVotingReconcile(
	ctx context.Context,
	members map[string][]byte,
	reverse map[string]string,
	companions map[string]string,
	forwards map[string]string,
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
			v1Rec, found = v1.byContact[committeeSFID+":"+contactSFID]
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
		// Checked after the contact SFID is resolved so the legacy record-sfid
		// join is covered too: the PATCH path takes the contact SFID and cannot
		// target one of several live records for the same committee + contact.
		if v1.ambiguous[committeeSFID+":"+contactSFID] {
			logger.With("member_uid", rec.UID, "committee_sfid", committeeSFID).
				WarnContext(ctx, "reconcile: more than one live v1 record for this committee member, skipping")
			res.ambiguousV1Record++
			continue
		}

		item := votingReconcileItem{
			memberUID:      rec.UID,
			committeeUID:   rec.CommitteeUID,
			reverseMapping: mappingValue,
			projectSFID:    projectSFID,
			committeeSFID:  committeeSFID,
			contactSFID:    contactSFID,
			recordSFID:     v1Rec.recordSFID,
			v1Status:       v1Rec.votingStatus,
		}
		if rec.Voting != nil {
			item.v2Status = rec.Voting.Status
		}

		// A companion without its forward key is a partial write (companion
		// written, forward write failed); repair the forward key alone. A
		// tombstoned forward key was deleted on purpose and is left alone.
		companion, hasCompanion := companions[rec.UID]
		switch {
		case !hasCompanion || isTombstonedMapping([]byte(companion)):
			item.needsCompanion = true
			item.needsForward = true
		case companion != item.recordSFID:
			// The companion names another record (e.g. the v1 row was
			// recreated). Claiming the joined record's forward key would split
			// the companion from the forward key, and a later v2 delete would
			// tombstone the wrong one, so this needs a manual repair.
			logger.With("member_uid", rec.UID, "companion_record_sfid", companion, "record_sfid", item.recordSFID).
				WarnContext(ctx, "reconcile: record-sfid companion names a different v1 record, skipping")
			res.companionMismatch++
			continue
		default:
			_, hasForward := forwards[item.recordSFID]
			item.needsForward = !hasForward
		}

		item.mismatch, item.target = classifyVotingReconcile(item.v2Status, item.v1Status)
		if item.mismatch {
			res.mismatched++
		} else {
			res.matched++
		}
		if item.needsCompanion || item.needsForward {
			res.mappingsMissing++
		}

		if item.mismatch || item.needsCompanion || item.needsForward {
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
		forwardSubject      = "$KV.v1-mappings.committee_member.sfid.*"
		forwardSubjectPfx   = "$KV.v1-mappings.committee_member.sfid."

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
	forwardRaw, err := scan(mappingsStream, forwardSubject, forwardSubjectPfx)
	if err != nil {
		return res, err
	}
	v1Objects, err := scan(v1ObjectsStream, v1ObjectSubject, v1ObjectSubjectPfx)
	if err != nil {
		return res, err
	}

	v1Index := buildV1MemberVotingIndex(ctx, v1Objects, &res)
	items := joinVotingReconcile(ctx, members, toStrings(reverseRaw), toStrings(companionRaw), toStrings(forwardRaw), v1Index, &res)

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
				"needs_companion", item.needsCompanion,
				"needs_forward", item.needsForward,
			).InfoContext(ctx, "[dry-run] reconcile: committee member would be updated")
		}
		return res, nil
	}

	limiter := rate.NewLimiter(rate.Limit(opts.emitRate), 1)
	err = applyVotingReconcileItems(ctx, items, limiter, forwardMappingKeyPrefix, v1ObjectKeyPrefix, &res)
	return res, err
}

// reconcileMemberTimeout bounds one member's mapping backfill, re-reads and
// PATCH, so a hung upstream call (v1HTTPClient has no client timeout) cannot
// stall the remaining members. Declared as a var (not const) so tests can
// shorten it.
var reconcileMemberTimeout = 2 * time.Minute

// applyVotingReconcileItems runs the live mapping backfill and voting PATCH
// for each item, pacing PATCHes with limiter. Per-member failures, including
// timeouts, are counted in res and do not stop the run; only cancellation of
// ctx does.
func applyVotingReconcileItems(ctx context.Context, items []votingReconcileItem, limiter *rate.Limiter, forwardMappingKeyPrefix, v1ObjectKeyPrefix string, res *reconcileCommitteeMemberVotingResult) error {
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if item.mismatch {
			if err := limiter.Wait(ctx); err != nil {
				return err
			}
		}
		itemCtx, cancel := context.WithTimeout(ctx, reconcileMemberTimeout)
		// A member that cannot claim the forward key must not PATCH the v1
		// record: it may belong to another v2 member.
		owned := true
		if item.needsCompanion || item.needsForward {
			owned = backfillReconcileMappings(itemCtx, item, forwardMappingKeyPrefix, res)
		}
		if item.mismatch && owned {
			applyVotingReconcilePatch(itemCtx, item, forwardMappingKeyPrefix, v1ObjectKeyPrefix, res)
		}
		cancel()
	}
	return nil
}

// applyVotingReconcilePatch checks that item's member owns the v1 record's
// forward mapping, re-reads the live v2 member and v1 record, then PATCHes v1
// to the v2 value if the two still disagree.
func applyVotingReconcilePatch(ctx context.Context, item votingReconcileItem, forwardMappingKeyPrefix, v1ObjectKeyPrefix string, res *reconcileCommitteeMemberVotingResult) {
	log := logger.With("member_uid", item.memberUID, "committee_uid", item.committeeUID, "record_sfid", item.recordSFID)

	// The record-keyed forward mapping names the v2 member that owns the v1
	// record. Another member's status must not overwrite the owner's.
	forwardKey := forwardMappingKeyPrefix + item.recordSFID
	forward, err := mappingStore.Get(ctx, forwardKey)
	switch {
	case errors.Is(err, ErrKeyNotFound):
		log.InfoContext(ctx, "reconcile: no forward mapping for v1 record, not patching")
		res.noMapping++
		return
	case err != nil:
		log.With(errKey, err, "key", forwardKey).WarnContext(ctx, "reconcile: failed to read forward mapping, not patching")
		res.errored++
		return
	case !forwardMappingNamesMember(string(forward.Value), item.committeeUID, item.memberUID):
		log.WarnContext(ctx, "reconcile: v1 record's forward mapping belongs to another member or is tombstoned, not patching")
		res.mappingConflicts++
		return
	}

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

// backfillReconcileMappings writes the record-keyed forward mapping and then,
// if missing, the record-sfid companion for item.
//
// The live reverse mapping is re-read first, so a member deleted in v2 after
// the scan does not regain live links; a delete landing between this read and
// the writes below is not covered.
//
// The forward key is written first because Create is an atomic ownership
// claim: the companion is written only once this member owns the forward key.
// A companion pointing at a record whose forward key belongs to another
// member would make a later v2 delete of this member
// (syncCommitteeMemberDeleteToV1) tombstone that member's forward mapping. A
// failed companion write leaves the companion missing, so a rerun retries it.
//
// Returns true when item's member owns the forward key afterwards, i.e. a
// voting PATCH for it may proceed.
func backfillReconcileMappings(ctx context.Context, item votingReconcileItem, forwardMappingKeyPrefix string, res *reconcileCommitteeMemberVotingResult) bool {
	reverseKey := "committee_member.uid." + item.memberUID
	entry, err := mappingStore.Get(ctx, reverseKey)
	switch {
	case errors.Is(err, ErrKeyNotFound), err == nil && string(entry.Value) != item.reverseMapping:
		logger.With("member_uid", item.memberUID).
			InfoContext(ctx, "reconcile: reverse mapping removed or changed since the scan, not writing mappings")
		res.noMapping++
		return false
	case err != nil:
		logger.With(errKey, err, "key", reverseKey).WarnContext(ctx, "reconcile: failed to re-read reverse mapping")
		res.errored++
		return false
	}

	forwardKey := forwardMappingKeyPrefix + item.recordSFID
	ownsForward := func(existing string) bool {
		return forwardMappingNamesMember(existing, item.committeeUID, item.memberUID)
	}
	if !createReconcileMapping(ctx, forwardKey, item.committeeUID+":"+item.memberUID, ownsForward, res) {
		return false
	}
	if item.needsCompanion {
		createReconcileMapping(ctx, committeeMemberRecordSFIDKey(item.memberUID), item.recordSFID, nil, res)
	}
	return true
}

// forwardMappingNamesMember reports whether a committee_member.sfid.* value
// names memberUID: either "committeeUID:memberUID" or the legacy bare
// memberUID, which handleCommitteeMemberUpdate still accepts (and upgrades on
// the next v1 edit). A tombstone or another member's UID does not match.
func forwardMappingNamesMember(value, committeeUID, memberUID string) bool {
	return value == committeeUID+":"+memberUID || value == memberUID
}

// createReconcileMapping creates a mapping key only if it is absent. An
// existing key with the same value, or one accepted by matches (when
// non-nil), counts as success; any other value is a conflict. An existing key
// is never overwritten. Returns true when the key holds an accepted value
// afterwards.
func createReconcileMapping(ctx context.Context, key, value string, matches func(existing string) bool, res *reconcileCommitteeMemberVotingResult) bool {
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
	if existing := string(entry.Value); existing == value || (matches != nil && matches(existing)) {
		return true
	}
	logger.With("key", key).
		WarnContext(ctx, "reconcile: mapping key already exists with a different value, not overwriting")
	res.mappingConflicts++
	return false
}
