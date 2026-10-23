// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"fmt"
	"strings"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

// fetchCommitteeBase fetches an existing committee base from the Committee Service API.
//
// Declared as a var (mirroring fetchProjectBase in client_projects.go) so
// tests can patch it to drive controlled failure paths through updateCommittee
// / deleteCommittee without standing up a full committeeClient stub.
var fetchCommitteeBase = func(ctx context.Context, committeeUID string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, "")
	if err != nil {
		return nil, "", err
	}

	result, err := committeeClient.GetCommitteeBase(ctx, &committeeservice.GetCommitteeBasePayload{
		BearerToken: &token,
		UID:         &committeeUID,
	})
	if err != nil {
		return nil, "", wrapCommitteeServiceError("failed to fetch committee base", err)
	}

	etag := ""
	if result.Etag != nil {
		etag = *result.Etag
	}

	return result.CommitteeBase, etag, nil
}

// createCommittee creates a new committee via the Committee Service API.
func createCommittee(ctx context.Context, payload *committeeservice.CreateCommitteePayload, v1Principal string) (*committeeservice.CommitteeFullWithReadonlyAttributes, error) {
	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, v1Principal)
	if err != nil {
		return nil, err
	}

	payload.BearerToken = &token

	result, err := committeeClient.CreateCommittee(ctx, payload)
	if err != nil {
		return nil, wrapCommitteeServiceError("failed to create committee", err)
	}

	return result, nil
}

// updateCommittee syncs a V1 committee record to V2 by fetching the current base,
// merging V1 fields on top (V1 wins where present; current values are preserved for
// absent V1 keys), and calling UpdateCommitteeBase only when a synced field differs.
//
// The mapper is called with currentBase as the default seed so that absent V1 keys
// do not clobber existing V2 values with the Go zero value for plain-bool payload
// fields (EnableVoting, SsoGroupEnabled, Public).
//
// Returns mutated=true iff UpdateCommitteeBase was actually issued. When the
// mapped state matches the current V2 base the API call is skipped and
// (false, nil, nil) is returned so callers can clean up any pending
// indexer-echo marker they wrote before this call — matching updateProject
// and updateCommitteeMember so all three handler cleanup sites use the same
// !mutated check.
func updateCommittee(ctx context.Context, committeeUID string, v1Data map[string]any, v1Principal string) (bool, *committeeservice.CommitteeBaseWithReadonlyAttributes, error) {
	// Fetch current committee base + ETag.
	currentBase, baseETag, err := fetchCommitteeBase(ctx, committeeUID)
	if err != nil {
		return false, nil, wrapCommitteeServiceError("failed to fetch current committee base", err)
	}

	// Build a fully-merged update payload: start from currentBase, overlay V1 fields.
	payload, err := mapV1DataToCommitteeUpdateBasePayload(ctx, committeeUID, v1Data, currentBase)
	if err != nil {
		return false, nil, err
	}

	// Snapshot the merged payload as a base struct for change detection.
	updatedBase := committeeBaseFromUpdatePayload(currentBase.UID, payload)

	// Skip the call if no synced field has changed.
	if committeeBasesEqual(currentBase, updatedBase) {
		return false, nil, nil
	}

	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, v1Principal)
	if err != nil {
		return false, nil, fmt.Errorf("failed to generate token for committee base update: %w", err)
	}

	payload.BearerToken = &token
	payload.IfMatch = stringToStringPtr(baseETag)

	result, err := committeeClient.UpdateCommitteeBase(ctx, payload)
	if err != nil {
		return false, nil, wrapCommitteeServiceError("failed to update committee base", err)
	}

	return true, result, nil
}

// committeeBaseFromUpdatePayload projects an UpdateCommitteeBasePayload into a
// CommitteeBaseWithReadonlyAttributes for change detection against the current
// server-side base.
func committeeBaseFromUpdatePayload(uid *string, payload *committeeservice.UpdateCommitteeBasePayload) *committeeservice.CommitteeBaseWithReadonlyAttributes {
	return &committeeservice.CommitteeBaseWithReadonlyAttributes{
		UID:             uid,
		Name:            stringToStringPtr(payload.Name),
		ProjectUID:      stringToStringPtr(payload.ProjectUID),
		Category:        stringToStringPtr(payload.Category),
		Description:     payload.Description,
		Website:         payload.Website,
		MailingList:     payload.MailingList,
		ChatChannel:     payload.ChatChannel,
		DisplayName:     payload.DisplayName,
		EnableVoting:    payload.EnableVoting,
		SsoGroupEnabled: payload.SsoGroupEnabled,
		Public:          payload.Public,
		ParentUID:       payload.ParentUID,
	}
}

// committeeBasesEqual compares two CommitteeBaseWithReadonlyAttributes objects across
// every field the V1→V2 sync writes. Fields not managed by the sync (e.g. RequiresReview,
// JoinMode, Repository, Scope, Deliverables, KeyDates, Calendar) are ignored
// so that admin edits made in V2 do not force redundant updates.
func committeeBasesEqual(a, b *committeeservice.CommitteeBaseWithReadonlyAttributes) bool {
	return stringPtrToString(a.Name) == stringPtrToString(b.Name) &&
		stringPtrToString(a.ProjectUID) == stringPtrToString(b.ProjectUID) &&
		stringPtrToString(a.Category) == stringPtrToString(b.Category) &&
		stringPtrToString(a.Description) == stringPtrToString(b.Description) &&
		stringPtrToString(a.Website) == stringPtrToString(b.Website) &&
		stringPtrToString(a.MailingList) == stringPtrToString(b.MailingList) &&
		stringPtrToString(a.ChatChannel) == stringPtrToString(b.ChatChannel) &&
		stringPtrToString(a.DisplayName) == stringPtrToString(b.DisplayName) &&
		stringPtrToString(a.ParentUID) == stringPtrToString(b.ParentUID) &&
		a.EnableVoting == b.EnableVoting &&
		a.SsoGroupEnabled == b.SsoGroupEnabled &&
		a.Public == b.Public
}

// createCommitteeMember creates a new committee member via the Committee Service API.
func createCommitteeMember(ctx context.Context, payload *committeeservice.CreateCommitteeMemberPayload, v1Principal string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, error) {
	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, v1Principal)
	if err != nil {
		return nil, err
	}

	payload.BearerToken = &token

	result, err := committeeClient.CreateCommitteeMember(ctx, payload)
	if err != nil {
		return nil, wrapCommitteeServiceError("failed to create committee member", err)
	}

	return result, nil
}

// fetchCommitteeMember fetches an existing committee member from the Committee Service API.
func fetchCommitteeMember(ctx context.Context, committeeUID, memberUID string) (*committeeservice.CommitteeMemberFullWithReadonlyAttributes, string, error) {
	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, "")
	if err != nil {
		return nil, "", err
	}

	result, err := committeeClient.GetCommitteeMember(ctx, &committeeservice.GetCommitteeMemberPayload{
		BearerToken: &token,
		UID:         committeeUID,
		MemberUID:   memberUID,
		Version:     "1",
	})
	if err != nil {
		return nil, "", wrapCommitteeServiceError("failed to fetch committee member", err)
	}

	etag := ""
	if result.Etag != nil {
		etag = *result.Etag
	}

	return result.Member, etag, nil
}

// updateCommitteeMember updates an existing committee member via the Committee Service API.
//
// Returns mutated=true iff UpdateCommitteeMember was actually issued. When the
// mapped state is byte-for-byte equal to the current V2 member the API call is
// skipped and (false, nil) is returned so callers can clean up any pending
// indexer-echo marker they wrote before this call — otherwise the marker
// silences the next legitimate v2 committee-member update within the freshness
// window.
func updateCommitteeMember(ctx context.Context, payload *committeeservice.UpdateCommitteeMemberPayload, v1Principal string) (bool, error) {
	// Fetch current committee member for comparison.
	currentMember, etag, err := fetchCommitteeMember(ctx, payload.UID, payload.MemberUID)
	if err != nil {
		return false, wrapCommitteeServiceError("failed to fetch current committee member", err)
	}

	preserveUnsetVotingStatus(currentMember, payload)

	// Check if member has changes (basic comparison).
	memberChanged := !committeeMembersEqual(currentMember, payload)

	if !memberChanged {
		return false, nil
	}

	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, v1Principal)
	if err != nil {
		return false, fmt.Errorf("failed to generate token for committee member update: %w", err)
	}

	payload.BearerToken = &token
	payload.IfMatch = stringToStringPtr(etag)

	_, err = committeeClient.UpdateCommitteeMember(ctx, payload)
	if err != nil {
		return false, wrapCommitteeServiceError("failed to update committee member", err)
	}

	return true, nil
}

// deleteCommittee deletes a committee by UID.
func deleteCommittee(ctx context.Context, committeeUID string, v1Principal string) error {
	// Fetch current committee base to get etag.
	_, etag, err := fetchCommitteeBase(ctx, committeeUID)
	if err != nil {
		return wrapCommitteeServiceError("failed to fetch committee base for deletion", err)
	}

	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, v1Principal)
	if err != nil {
		return fmt.Errorf("failed to generate token for committee deletion: %w", err)
	}

	payload := &committeeservice.DeleteCommitteePayload{
		BearerToken: &token,
		UID:         &committeeUID,
		IfMatch:     stringToStringPtr(etag),
	}

	err = committeeClient.DeleteCommittee(ctx, payload)
	if err != nil {
		return wrapCommitteeServiceError("failed to delete committee", err)
	}

	return nil
}

// deleteCommitteeMember deletes a committee member by committee UID and member UID.
func deleteCommitteeMember(ctx context.Context, committeeUID, memberUID string, v1Principal string, skipNotification bool) error {
	// Fetch current committee member to get etag.
	_, etag, err := fetchCommitteeMember(ctx, committeeUID, memberUID)
	if err != nil {
		return wrapCommitteeServiceError("failed to fetch committee member for deletion", err)
	}

	token, err := generateCachedJWTToken(ctx, committeeServiceAudience, v1Principal)
	if err != nil {
		return fmt.Errorf("failed to generate token for committee member deletion: %w", err)
	}

	payload := &committeeservice.DeleteCommitteeMemberPayload{
		BearerToken:      &token,
		UID:              committeeUID,
		MemberUID:        memberUID,
		Version:          "1",
		IfMatch:          stringToStringPtr(etag),
		SkipNotification: skipNotification,
	}

	err = committeeClient.DeleteCommitteeMember(ctx, payload)
	if err != nil {
		return wrapCommitteeServiceError("failed to delete committee member", err)
	}

	return nil
}

// preserveUnsetVotingStatus drops an incoming v1NoVotingStatus from update when
// the current v2 member has no voting status. The v2→v1 sync writes
// v1NoVotingStatus for such a member because v1 cannot store an empty status,
// so a later v1 edit of any other field re-emits it; without this the v2
// member would be set to "Observer" (lfx-self-serve#4079). A nil Voting is how
// the v1→v2 mappers already express "no voting status".
func preserveUnsetVotingStatus(current *committeeservice.CommitteeMemberFullWithReadonlyAttributes, update *committeeservice.UpdateCommitteeMemberPayload) {
	if update.Voting == nil || !strings.EqualFold(strings.TrimSpace(update.Voting.Status), v1NoVotingStatus) {
		return
	}
	var currentStatus string
	if current != nil && current.Voting != nil {
		currentStatus = current.Voting.Status
	}
	if normalizeVotingStatus(currentStatus) == "" {
		update.Voting = nil
	}
}

// committeeMembersEqual compares a committee member with an update payload for equality.
func committeeMembersEqual(current *committeeservice.CommitteeMemberFullWithReadonlyAttributes, update *committeeservice.UpdateCommitteeMemberPayload) bool {
	// Compare basic fields.
	if stringPtrToString(current.Username) != stringPtrToString(update.Username) ||
		stringPtrToString(current.Email) != update.Email ||
		stringPtrToString(current.FirstName) != stringPtrToString(update.FirstName) ||
		stringPtrToString(current.LastName) != stringPtrToString(update.LastName) ||
		stringPtrToString(current.JobTitle) != stringPtrToString(update.JobTitle) ||
		current.AppointedBy != update.AppointedBy ||
		current.Status != update.Status {
		return false
	}

	// Compare role information.
	if current.Role != nil && update.Role != nil {
		if current.Role.Name != update.Role.Name ||
			stringPtrToString(current.Role.StartDate) != stringPtrToString(update.Role.StartDate) ||
			stringPtrToString(current.Role.EndDate) != stringPtrToString(update.Role.EndDate) {
			return false
		}
	} else if current.Role != update.Role {
		return false
	}

	// Compare voting information. A nil Voting, an empty status and "None" all
	// mean "no voting status", so a legacy v2 "None" against an incoming nil is
	// not a change. Dates are compared only when both sides carry voting info.
	var currentVotingStatus, updateVotingStatus string
	if current.Voting != nil {
		currentVotingStatus = current.Voting.Status
	}
	if update.Voting != nil {
		updateVotingStatus = update.Voting.Status
	}
	if normalizeVotingStatus(currentVotingStatus) != normalizeVotingStatus(updateVotingStatus) {
		return false
	}
	if current.Voting != nil && update.Voting != nil {
		if stringPtrToString(current.Voting.StartDate) != stringPtrToString(update.Voting.StartDate) ||
			stringPtrToString(current.Voting.EndDate) != stringPtrToString(update.Voting.EndDate) {
			return false
		}
	}

	// Compare organization information.
	if current.Organization != nil && update.Organization != nil {
		if stringPtrToString(current.Organization.Name) != stringPtrToString(update.Organization.Name) ||
			stringPtrToString(current.Organization.Website) != stringPtrToString(update.Organization.Website) {
			return false
		}
	} else if current.Organization != update.Organization {
		return false
	}

	return true
}
