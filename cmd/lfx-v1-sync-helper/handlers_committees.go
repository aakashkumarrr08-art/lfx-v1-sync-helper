// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	sfidvalidator "github.com/linuxfoundation/lfx-v1-sync-helper/internal/sfid"
	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

// allowedCommitteeCategories defines the valid values for type__c mapping to category.
var allowedCommitteeCategories = map[string]bool{
	"Ambassador":                        true,
	"Board":                             true,
	"Code of Conduct":                   true,
	"Committers":                        true,
	"Expert Group":                      true,
	"Finance Committee":                 true,
	"Government Advisory Council":       true,
	"Legal Committee":                   true,
	"Maintainers":                       true,
	"Marketing Committee/Sub Committee": true,
	"Marketing Mailing List":            true,
	"Marketing Oversight Committee/Marketing Advisory Committee": true,
	"Newsletter":                    true,
	"Other":                         true,
	"Product Security":              true,
	"Special Interest Group":        true,
	"Technical Advisory Committee":  true,
	"Technical Mailing List":        true,
	"Technical Oversight Committee": true,
	"Technical Steering Committee":  true,
	"Working Group":                 true,
}

// allowedAppointedByValues defines the valid values for appointed_by__c mapping to appointed_by.
var allowedAppointedByValues = map[string]bool{
	"Community":                          true,
	"Membership Entitlement":             true,
	"Vote of End User Member Class":      true,
	"Vote of TSC Committee":              true,
	"Vote of TAC Committee":              true,
	"Vote of Academic Member Class":      true,
	"Vote of Lab Member Class":           true,
	"Vote of Marketing Committee":        true,
	"Vote of Governing Board":            true,
	"Vote of General Member Class":       true,
	"Vote of End User Committee":         true,
	"Vote of TOC Committee":              true,
	"Vote of Gold Member Class":          true,
	"Vote of Silver Member Class":        true,
	"Vote of Strategic Membership Class": true,
	"None":                               true,
}

// allowedRoleNames defines the valid values for role__c mapping to role name.
var allowedRoleNames = map[string]bool{
	"Chair":                  true,
	"Counsel":                true,
	"Developer Seat":         true,
	"TAC/TOC Representative": true,
	"Director":               true,
	"Lead":                   true,
	"None":                   true,
	"Secretary":              true,
	"Treasurer":              true,
	"Vice Chair":             true,
	"LF Staff":               true,
}

// mapRoleNameToValidValue filters and maps role__c to a valid role name value.
func mapRoleNameToValidValue(ctx context.Context, roleName string) string {
	if roleName == "" {
		return "None"
	}

	if allowedRoleNames[roleName] {
		return roleName
	}

	// If the value is not in the allowed list, use None as fallback.
	logger.With("original_role_name", roleName, "fallback_role_name", "None").WarnContext(ctx, "role name value not in allowed list, using fallback")
	return "None"
}

// mapAppointedByToValidValue filters and maps appointed_by__c to a valid appointed_by value.
func mapAppointedByToValidValue(ctx context.Context, appointedBy string) string {
	if appointedBy == "" {
		return "None"
	}

	if allowedAppointedByValues[appointedBy] {
		return appointedBy
	}

	// If the value is not in the allowed list, use None as fallback.
	logger.With("original_appointed_by", appointedBy, "fallback_appointed_by", "None").WarnContext(ctx, "appointed_by value not in allowed list, using fallback")
	return "None"
}

// mapTypeToCategory filters and maps type__c to category.
func mapTypeToCategory(ctx context.Context, typeVal, committeeName string) *string {
	if typeVal == "" {
		return nil
	}

	if typeVal == "Technical Oversight Committee/Technical Advisory Committee" {
		// Special case mapping.
		if strings.Contains(strings.ToLower(committeeName), "advisory") {
			mapped := "Technical Advisory Committee"
			return &mapped
		}
		if strings.Contains(strings.ToLower(committeeName), "tac") {
			mapped := "Technical Advisory Committee"
			return &mapped
		}
		mapped := "Technical Oversight Committee"
		return &mapped
	}

	if allowedCommitteeCategories[typeVal] {
		return &typeVal
	}

	// If the value is not in the allowed list, use Other as fallback.
	logger.With("original_category", typeVal, "fallback_category", "Other").WarnContext(ctx, "committee type not in allowed list, using fallback")
	fallback := "Other"
	return &fallback
}

// handleCommitteeUpdate processes a committee update from the KV bucket.
// Returns true when the message should be nack'd for redelivery — that
// is, when a *transient* mapping-store error prevented us from
// distinguishing "no existing v2 committee" from "we cannot tell right
// now". Treating a transient error as "no v2 committee" would create a
// duplicate resource on the next handler run.
func handleCommitteeUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return false
	}

	// Extract v1Principal from v1 data for JWT generation.
	v1Principal := extractV1Principal(ctx, v1Data)

	// Extract committee SFID.
	sfid, ok := v1Data["sfid"].(string)
	if !ok || sfid == "" {
		logger.With("key", key).ErrorContext(ctx, "no SFID found in committee data")
		return false
	}

	// Extract project SFID from v1 data for use in project checks and reverse mapping.
	projectSFID := ""
	if projSFID, ok := v1Data["project_name__c"].(string); ok && projSFID != "" {
		projectSFID = projSFID
	}

	// Check if we have an existing mapping using SFID. On a transient
	// store error we MUST NACK — treating it as "no mapping" would send
	// the create-path below and produce a duplicate v2 committee.
	mappingKey := fmt.Sprintf("committee.sfid.%s", sfid)
	existingUID := ""

	entry, mapErr := mappingStore.Get(ctx, mappingKey)
	switch {
	case mapErr == nil:
		if isTombstonedMapping(entry.Value) {
			logger.With("sfid", sfid).WarnContext(ctx, "skipping committee upsert - mapping is tombstoned (previously deleted)")
			return false
		}
		existingUID = string(entry.Value)
	case errors.Is(mapErr, ErrKeyNotFound):
		// Genuine miss — fall through to create path.
	default:
		logger.With(errKey, mapErr, "sfid", sfid).
			ErrorContext(ctx, "transient mapping-store failure resolving committee SFID; redelivering to avoid duplicate v2 create")
		return true
	}

	var uid string
	var err error
	var v2SSOGroupName string

	if existingUID != "" {
		// Update existing committee. updateCommittee fetches the current V2 base,
		// runs the mapper with currentBase as the seed (so absent V1 keys preserve
		// current values), and only issues the API call when a synced field differs.
		logger.With("committee_uid", existingUID, "sfid", sfid).InfoContext(ctx, "updating existing committee")

		// Record that we're about to write the v2 committee so the
		// resulting indexer event (which will fan out to
		// syncCommitteeUpdateToV1) can identify itself as our own
		// round-trip echo and skip the redundant v1 PATCH that would
		// otherwise overwrite lastmodifiedbyid with our client ID.
		// See pending_markers.go for the full contract.
		//
		// If updateCommittee reports no mutation (change-detection
		// short-circuit at client_committees.go, or a pre-mutation
		// error like a fetchCommitteeBase / JWT / API failure) we
		// delete the marker below — regardless of err. A no-mutation
		// outcome means no lfx.committee.updated indexer event will
		// fire, so the marker has no consumer and would otherwise
		// silence a genuine v2 update within the freshness window. On
		// a NACK+redeliver, the retry re-writes the marker before its
		// next v2 API attempt.
		writePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceCommittee, existingUID)

		var mutated bool
		var updateResult *committeeservice.CommitteeBaseWithReadonlyAttributes
		mutated, updateResult, err = updateCommittee(ctx, existingUID, v1Data, v1Principal)
		if !mutated {
			deletePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceCommittee, existingUID)
		}
		if updateResult != nil && updateResult.SsoGroupName != nil {
			v2SSOGroupName = *updateResult.SsoGroupName
		}
		uid = existingUID
	} else {
		// Check if parent project exists in mappings before creating new committee.
		// On transient error we NACK to avoid creating a committee whose parent
		// project we could not verify.
		if projectSFID != "" {
			projectMappingKey := fmt.Sprintf("project.sfid.%s", projectSFID)
			_, projErr := mappingStore.Get(ctx, projectMappingKey)
			switch {
			case projErr == nil:
				// parent found — proceed
			case errors.Is(projErr, ErrKeyNotFound):
				logger.With("project_sfid", projectSFID, "committee_sfid", sfid).InfoContext(ctx, "skipping committee creation - parent project not found in mappings")
				return false
			default:
				logger.With(errKey, projErr, "project_sfid", projectSFID, "committee_sfid", sfid).
					ErrorContext(ctx, "transient mapping-store failure resolving parent project SFID; redelivering")
				return true
			}
		}

		// Create new committee.
		logger.With("sfid", sfid).InfoContext(ctx, "creating new committee")

		// Map v1 data to create payload. A transient mapping-store
		// failure inside the mapper (parent project lookup) is
		// redeliverable; permanent failures (missing/invalid data)
		// are ACK'd.
		var payload *committeeservice.CreateCommitteePayload
		payload, err = mapV1DataToCommitteeCreatePayload(ctx, v1Data)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to map v1 data to create payload")
			return isTransientStoreErr(err)
		}

		var response *committeeservice.CommitteeFullWithReadonlyAttributes
		response, err = createCommittee(ctx, payload, v1Principal)
		if response != nil && response.UID != nil {
			uid = *response.UID
		}
		if response != nil && response.SsoGroupName != nil {
			v2SSOGroupName = *response.SsoGroupName
		}
	}

	if err != nil {
		logger.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to sync committee")
		// A transient mapping-store failure inside updateCommittee ->
		// mapV1DataToCommitteeUpdateBasePayload propagates via the
		// isTransientStoreErr sentinel; NACK so the message is
		// redelivered rather than silently dropped.
		return isTransientStoreErr(err)
	}

	// Store the SFID mapping and reverse mapping before the writeback PATCH.
	// This must happen first: the PATCH can trigger a v1 WAL event, and without
	// the reverse mapping the indexer would see no committee.uid.* entry and
	// attempt a duplicate v2 create.
	if uid != "" {
		if _, err := mappingStore.Put(ctx, mappingKey, []byte(uid)); err != nil {
			logger.With(errKey, err, "sfid", sfid, "uid", uid).WarnContext(ctx, "failed to store committee mapping")
		}

		// Store reverse mapping (v2 UID -> v1 project:committee SFID).
		reverseMappingKey := fmt.Sprintf("committee.uid.%s", uid)
		reverseMappingValue := fmt.Sprintf("%s:%s", projectSFID, sfid)
		if _, err := mappingStore.Put(ctx, reverseMappingKey, []byte(reverseMappingValue)); err != nil {
			logger.With(errKey, err, "committee_uid", uid, "sfid", sfid).WarnContext(ctx, "failed to store committee reverse mapping")
		}
	}

	// Write the v2-generated SSOGroupName back to the v1 project-service so PCC can display it.
	if v2SSOGroupName != "" && projectSFID != "" {
		if writeErr := updateV1Committee(ctx, projectSFID, sfid, projectServiceCommitteeUpdate{
			SSOGroupName: &v2SSOGroupName,
		}); writeErr != nil {
			logger.With(errKey, writeErr, "sfid", sfid, "sso_group_name", v2SSOGroupName).WarnContext(ctx, "failed to write SSOGroupName back to v1")
		} else {
			logger.With("sfid", sfid, "sso_group_name", v2SSOGroupName).InfoContext(ctx, "wrote SSOGroupName back to v1")
		}
	}

	logger.With("committee_uid", uid, "sfid", sfid).InfoContext(ctx, "successfully synced committee")
	return false
}

// handleCommitteeMemberDelete processes a committee member deletion.
// Returns true if the operation should be retried, false otherwise.
func handleCommitteeMemberDelete(ctx context.Context, key string, sfid string, v1Principal string) bool {
	// Suppress the WAL echo of our own v2→v1 DELETE. syncCommitteeMemberDeleteToV1
	// writes this marker (keyed by the platform-community__c record
	// SFID) before it calls the v1 project-service DELETE; the v1 side
	// then emits a soft-delete WAL event that routes here via
	// handleResourceDelete with the record SFID as its key. handleKVPut's
	// shouldSkipSync check is bypassed on the soft-delete branch, so
	// this marker is the only mechanism that closes the loop for
	// v2-originated deletes. Runs BEFORE mapping lookup so the WAL
	// echo is caught whether or not the writer has already tombstoned
	// the mapping — see linuxfoundation/lfx-self-serve#2007 for the
	// retry-until-tombstone failure mode without it.
	//
	// Edge case: when syncCommitteeMemberDeleteToV1's recordSFID is
	// empty (v2-originated member with no record-sfid companion), the
	// marker isn't written and this consume misses. The tombstone
	// check below then catches the echo if tombstoning has landed;
	// otherwise the pre-fix retry-until-tombstone-lands noise applies
	// for that specific case.
	if consumePendingMarker(ctx, markerV2ToV1, markerOpDelete, markerResourceCommitteeMember, sfid) {
		logger.With("sfid", sfid, "key", key).
			InfoContext(ctx, "skipping WAL echo of v2-originated committee member delete (fresh v2_to_v1 pending marker)")
		return false
	}

	// Check if we have an existing mapping using SFID.
	mappingKey := fmt.Sprintf("committee_member.sfid.%s", sfid)
	entry, err := mappingStore.Get(ctx, mappingKey)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			logger.With("sfid", sfid, "key", key).InfoContext(ctx, "committee member mapping not found, nothing to delete")
			return false
		}
		logger.With(errKey, err, "sfid", sfid, "key", key).ErrorContext(ctx, "failed to get committee member mapping for deletion")
		return true // Retry on error.
	}

	mappingValue := string(entry.Value)
	if mappingValue == "" || isTombstonedMapping(entry.Value) {
		logger.With("sfid", sfid, "key", key).InfoContext(ctx, "committee member mapping empty or tombstoned, nothing to delete")
		return false
	}

	// Parse the new mapping format: "{committee_uuid}:{member_uuid}".
	parts := strings.Split(mappingValue, ":")
	if len(parts) != 2 {
		// Old format (no committee ID) means we cannot delete the committee member. Tombstone it anyhow..
		logger.With("member_uid", mappingValue, "sfid", sfid, "key", key).WarnContext(ctx, "committee member deletion with old mapping format, deletion cannot be synced")
		if err := tombstoneMapping(ctx, mappingKey); err != nil {
			logger.With(errKey, err, "sfid", sfid).WarnContext(ctx, "failed to tombstone old format committee member mapping")
		}
		return false
	}

	committeeUID := parts[0]
	memberUID := parts[1]

	// Delete the committee member using the API.
	logger.With("committee_uid", committeeUID, "member_uid", memberUID, "sfid", sfid, "key", key, "v1_principal", v1Principal).InfoContext(ctx, "deleting committee member")

	// Record that we're about to delete the v2 committee member so the
	// resulting indexer event can identify itself as our own round-trip
	// echo and skip the redundant v1 DELETE. Keyed by v2 UID since the
	// consumer has the UID directly from the indexer event's ObjectID.
	writePendingMarker(ctx, markerV1ToV2, markerOpDelete, markerResourceCommitteeMember, memberUID)

	skipNotification := cfg != nil && cfg.CommitteeSkipMemberNotifications
	err = deleteCommitteeMember(ctx, committeeUID, memberUID, v1Principal, skipNotification)
	if err != nil {
		// v2 DELETE failed. Roll back the marker so it does not
		// suppress an unrelated genuine v2-native
		// committee_member.deleted event for the same UID within the
		// freshness window. See handleProjectDelete for the equivalent
		// block.
		deletePendingMarker(ctx, markerV1ToV2, markerOpDelete, markerResourceCommitteeMember, memberUID)
		logger.With(errKey, err, "committee_uid", committeeUID, "member_uid", memberUID, "sfid", sfid, "key", key).ErrorContext(ctx, "failed to delete committee member")
		return true // Retry on error.
	}

	// Tombstone mappings after successful deletion.
	if err := tombstoneMapping(ctx, mappingKey); err != nil {
		logger.With(errKey, err, "sfid", sfid).WarnContext(ctx, "failed to tombstone committee member SFID mapping")
	}

	// Tombstone reverse mapping (member UID -> v1 project:committee:member SFID).
	reverseMappingKey := fmt.Sprintf("committee_member.uid.%s", memberUID)
	if err := tombstoneMapping(ctx, reverseMappingKey); err != nil {
		logger.With(errKey, err, "committee_uid", committeeUID, "member_uid", memberUID).WarnContext(ctx, "failed to tombstone committee member UID mapping")
	}

	// Tombstone the record-sfid companion too, or it is left as a permanent live key:
	// the v2-side delete indexer event that would otherwise clean it up either finds
	// the reverse mapping already tombstoned above, or its own v1 delete call 404s
	// since this v1-originated deletion already removed the member.
	if err := tombstoneMapping(ctx, committeeMemberRecordSFIDKey(memberUID)); err != nil {
		logger.With(errKey, err, "committee_uid", committeeUID, "member_uid", memberUID).WarnContext(ctx, "failed to tombstone committee member record sfid mapping")
	}

	logger.With("committee_uid", committeeUID, "member_uid", memberUID, "sfid", sfid, "key", key).InfoContext(ctx, "successfully deleted committee member")
	return false
}

// handleCommitteeDelete processes a committee deletion.
// Returns true if the operation should be retried, false otherwise.
func handleCommitteeDelete(ctx context.Context, key string, sfid string, v1Principal string) bool {
	// Suppress the WAL echo of our own v2→v1 DELETE. syncCommitteeDeleteToV1
	// writes this marker (keyed by v1 SFID) before it calls the v1
	// project-service DELETE; handleKVPut's shouldSkipSync check is
	// bypassed on the soft-delete branch so this marker is the only
	// mechanism that closes the loop for v2-originated deletes. See
	// handlers_projects.go handleProjectDelete for the equivalent block
	// and linuxfoundation/lfx-self-serve#2007 for the failure mode
	// without it. Runs BEFORE mapping lookup for the same reason —
	// tombstoning the mapping may race the WAL echo, and a marker
	// keyed by v2 UID would leak whenever tombstoning wins.
	if consumePendingMarker(ctx, markerV2ToV1, markerOpDelete, markerResourceCommittee, sfid) {
		logger.With("sfid", sfid, "key", key).
			InfoContext(ctx, "skipping WAL echo of v2-originated committee delete (fresh v2_to_v1 pending marker)")
		return false
	}

	// Check if we have an existing mapping using SFID.
	mappingKey := fmt.Sprintf("committee.sfid.%s", sfid)
	entry, err := mappingStore.Get(ctx, mappingKey)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			logger.With("sfid", sfid, "key", key).WarnContext(ctx, "committee mapping not found, nothing to delete")
			return false
		}
		logger.With(errKey, err, "sfid", sfid, "key", key).ErrorContext(ctx, "failed to get committee mapping for deletion")
		return true // Retry on error.
	}

	existingUID := string(entry.Value)
	if existingUID == "" || isTombstonedMapping(entry.Value) {
		logger.With("sfid", sfid, "key", key).InfoContext(ctx, "committee mapping empty or tombstoned, nothing to delete")
		return false
	}

	// Delete the committee using provided v1Principal or v1-sync-helper service credentials.
	logger.With("committee_uid", existingUID, "sfid", sfid, "key", key, "v1_principal", v1Principal).InfoContext(ctx, "deleting committee")

	// Record that we're about to delete the v2 committee so the
	// resulting indexer event can identify itself as our own round-trip
	// echo and skip the redundant v1 DELETE. Keyed by v2 UID since the
	// consumer has the UID directly from the indexer event's ObjectID.
	writePendingMarker(ctx, markerV1ToV2, markerOpDelete, markerResourceCommittee, existingUID)

	err = deleteCommittee(ctx, existingUID, v1Principal)
	if err != nil {
		// v2 DELETE failed. Roll back the marker so it does not
		// suppress an unrelated genuine v2-native committee.deleted
		// event for the same UID within the freshness window. See
		// handleProjectDelete for the equivalent block.
		deletePendingMarker(ctx, markerV1ToV2, markerOpDelete, markerResourceCommittee, existingUID)
		logger.With(errKey, err, "committee_uid", existingUID, "sfid", sfid, "key", key).ErrorContext(ctx, "failed to delete committee")
		return true // Retry on error.
	}

	// Tombstone mappings after successful deletion.
	if err := tombstoneMapping(ctx, mappingKey); err != nil {
		logger.With(errKey, err, "sfid", sfid, "committee_uid", existingUID).WarnContext(ctx, "failed to tombstone committee SFID mapping")
	}

	// Also tombstone reverse mapping (v2 UID -> v1 SFID).
	reverseMappingKey := fmt.Sprintf("committee.uid.%s", existingUID)
	if err := tombstoneMapping(ctx, reverseMappingKey); err != nil {
		logger.With(errKey, err, "committee_uid", existingUID, "sfid", sfid).WarnContext(ctx, "failed to tombstone committee UID mapping")
	}

	logger.With("committee_uid", existingUID, "sfid", sfid, "key", key).InfoContext(ctx, "successfully deleted committee")
	return false
}

// mapV1DataToCommitteeCreatePayload converts v1 committee data to a CreateCommitteePayload.
func mapV1DataToCommitteeCreatePayload(ctx context.Context, v1Data map[string]any) (*committeeservice.CreateCommitteePayload, error) {
	// Extract required fields.
	name := ""
	if mailingList, ok := v1Data["mailing_list__c"].(string); ok {
		name = mailingList
	}

	projectUID := ""
	if projectSFID, ok := v1Data["project_name__c"].(string); ok && projectSFID != "" {
		// Look up the project's V2 UID from SFID mappings. A transient
		// mapping-store failure MUST propagate as *errTransientStore so
		// the outer handler NACKs instead of collapsing to
		// projectUID="" and treating the resulting "missing required
		// fields" error as permanent — that would silently drop a
		// legitimate committee create under a Postgres blip.
		projectMappingKey := fmt.Sprintf("project.sfid.%s", projectSFID)
		entry, err := mappingStore.Get(ctx, projectMappingKey)
		switch {
		case err == nil:
			projectUID = string(entry.Value)
			logger.With("project_sfid", projectSFID, "project_uid", projectUID).DebugContext(ctx, "found project UID from SFID mapping for committee")
		case errors.Is(err, ErrKeyNotFound):
			logger.With("project_sfid", projectSFID).WarnContext(ctx, "could not find project UID in mappings for committee")
		default:
			return nil, wrapTransientStoreErr(fmt.Errorf("resolving project UID for committee create (project_sfid=%s): %w", projectSFID, err))
		}
	}

	if name == "" || projectUID == "" {
		return nil, fmt.Errorf("missing required fields: name=%q, projectUID=%q", name, projectUID)
	}

	payload := &committeeservice.CreateCommitteePayload{
		Name:       name,
		ProjectUID: projectUID,
	}

	// Map optional fields.
	if desc, ok := v1Data["description__c"].(string); ok && desc != "" {
		payload.Description = &desc
	}

	if typeVal, ok := v1Data["type__c"].(string); ok && typeVal != "" {
		if category := mapTypeToCategory(ctx, typeVal, name); category != nil {
			payload.Category = *category // Map Type to Category with validation.
		}
	}

	if websiteURL, ok := v1Data["committee_website__c"].(string); ok && isValidURL(websiteURL) {
		clean := cleanURL(websiteURL)
		payload.Website = &clean
	}

	if enableVoting, ok := v1Data["enable_voting__c"].(bool); ok {
		payload.EnableVoting = enableVoting
	}

	if ssoEnabled, ok := v1Data["sso_group_enabled"].(bool); ok {
		payload.SsoGroupEnabled = ssoEnabled
	}

	// Map public enabled field.
	if public, ok := v1Data["public_enabled"].(bool); ok {
		payload.Public = public
		logger.With("public_enabled", public).DebugContext(ctx, "mapped committee public enabled field")
	}

	// Map public display name field.
	if displayName, ok := v1Data["public_name"].(string); ok && displayName != "" {
		payload.DisplayName = &displayName
		logger.With("display_name", displayName).DebugContext(ctx, "mapped committee display name field")
	}

	// Map business email required field.
	if businessEmailRequired, ok := v1Data["business_email_required__c"].(bool); ok {
		payload.BusinessEmailRequired = businessEmailRequired
		logger.With("business_email_required", businessEmailRequired).DebugContext(ctx, "mapped committee business email required field")
	}

	// Map governance fields (requires lfx-v2-committee-service >= v0.3.0 with PR #61 merged).
	if joinMode, ok := v1Data["join_mode"].(string); ok && joinMode != "" {
		payload.JoinMode = joinMode
		logger.With("join_mode", joinMode).DebugContext(ctx, "mapped committee join mode field")
	}
	if mailingListEmail, ok := v1Data["mailing_list_email"].(string); ok && mailingListEmail != "" {
		payload.MailingList = &mailingListEmail
		logger.With("mailing_list_email", mailingListEmail).DebugContext(ctx, "mapped committee mailing list email field")
	}
	if chatChannel, ok := v1Data["chat_channel"].(string); ok && chatChannel != "" {
		payload.ChatChannel = &chatChannel
		logger.With("chat_channel", chatChannel).DebugContext(ctx, "mapped committee chat channel field")
	}

	return payload, nil
}

// mapV1DataToCommitteeUpdateBasePayload converts v1 committee data to an
// UpdateCommitteeBasePayload. The payload is seeded from currentBase so that
// V1 fields absent from v1Data preserve the current V2 values instead of being
// overwritten with Go zero values (particularly important for plain-bool payload
// fields like EnableVoting, SsoGroupEnabled, and Public: their zero value `false`
// would silently clobber a current V2 `true` whenever another unrelated field
// change triggered the update).
func mapV1DataToCommitteeUpdateBasePayload(ctx context.Context, committeeUID string, v1Data map[string]any, currentBase *committeeservice.CommitteeBaseWithReadonlyAttributes) (*committeeservice.UpdateCommitteeBasePayload, error) {
	// Extract required fields.
	name := ""
	if mailingList, ok := v1Data["mailing_list__c"].(string); ok {
		name = mailingList
	}

	projectUID := ""
	if projectSFID, ok := v1Data["project_name__c"].(string); ok && projectSFID != "" {
		// Look up the project's V2 UID from SFID mappings. Same
		// transient-vs-permanent distinction as the create-payload
		// builder above: a Postgres blip must propagate as
		// *errTransientStore so the outer handler NACKs.
		projectMappingKey := fmt.Sprintf("project.sfid.%s", projectSFID)
		entry, err := mappingStore.Get(ctx, projectMappingKey)
		switch {
		case err == nil:
			projectUID = string(entry.Value)
			logger.With("project_sfid", projectSFID, "project_uid", projectUID).DebugContext(ctx, "found project UID from SFID mapping for committee")
		case errors.Is(err, ErrKeyNotFound):
			logger.With("project_sfid", projectSFID).WarnContext(ctx, "could not find project UID in mappings for committee")
		default:
			return nil, wrapTransientStoreErr(fmt.Errorf("resolving project UID for committee update (project_sfid=%s): %w", projectSFID, err))
		}
	}

	if name == "" || projectUID == "" {
		return nil, fmt.Errorf("missing required fields: name=%q, projectUID=%q", name, projectUID)
	}

	payload := &committeeservice.UpdateCommitteeBasePayload{
		UID:        &committeeUID,
		Name:       name,
		ProjectUID: projectUID,
	}
	seedCommitteeUpdateFromCurrentBase(payload, currentBase)
	overlayV1CommitteeUpdatePayload(ctx, payload, v1Data, name)

	return payload, nil
}

// seedCommitteeUpdateFromCurrentBase seeds sync-managed fields on payload from
// the currently-fetched V2 committee base so that absent V1 keys preserve current
// values instead of falling back to Go zero values. This is essential for plain-bool
// fields (EnableVoting, SsoGroupEnabled, Public) whose zero value `false` would
// otherwise clobber a current V2 `true` on any unrelated-field-triggered update.
// currentBase may be nil (defensive for unit tests); nil is a no-op.
func seedCommitteeUpdateFromCurrentBase(payload *committeeservice.UpdateCommitteeBasePayload, currentBase *committeeservice.CommitteeBaseWithReadonlyAttributes) {
	if payload == nil || currentBase == nil {
		return
	}
	payload.Category = stringPtrToString(currentBase.Category)
	payload.Description = currentBase.Description
	payload.Website = currentBase.Website
	payload.MailingList = currentBase.MailingList
	payload.ChatChannel = currentBase.ChatChannel
	payload.DisplayName = currentBase.DisplayName
	payload.EnableVoting = currentBase.EnableVoting
	payload.SsoGroupEnabled = currentBase.SsoGroupEnabled
	payload.Public = currentBase.Public
}

// overlayV1CommitteeUpdatePayload overlays V1 values on top of payload for every
// field the sync manages. Each guard leaves the pre-seeded value in place when the
// V1 key is absent, empty, or invalid. name is the required committee name already
// extracted by the caller (used for category validation error messages).
func overlayV1CommitteeUpdatePayload(ctx context.Context, payload *committeeservice.UpdateCommitteeBasePayload, v1Data map[string]any, name string) {
	if desc, ok := v1Data["description__c"].(string); ok && desc != "" {
		payload.Description = &desc
	}

	if typeVal, ok := v1Data["type__c"].(string); ok && typeVal != "" {
		if category := mapTypeToCategory(ctx, typeVal, name); category != nil {
			payload.Category = *category // Map Type to Category with validation.
		}
	}

	if websiteURL, ok := v1Data["committee_website__c"].(string); ok && isValidURL(websiteURL) {
		clean := cleanURL(websiteURL)
		payload.Website = &clean
	}

	if enableVoting, ok := v1Data["enable_voting__c"].(bool); ok {
		payload.EnableVoting = enableVoting
	}

	if ssoEnabled, ok := v1Data["sso_group_enabled"].(bool); ok {
		payload.SsoGroupEnabled = ssoEnabled
	}

	if public, ok := v1Data["public_enabled"].(bool); ok {
		payload.Public = public
		logger.With("public_enabled", public).DebugContext(ctx, "mapped committee public enabled field for update")
	}

	if displayName, ok := v1Data["public_name"].(string); ok && displayName != "" {
		payload.DisplayName = &displayName
		logger.With("display_name", displayName).DebugContext(ctx, "mapped committee display name field for update")
	}

	// Map business email required field - only available in create payload.
	// UpdateCommitteeBasePayload does not support BusinessEmailRequired field.

	// Map governance fields (requires lfx-v2-committee-service >= v0.3.0 with PR #61 merged).
	// Note: JoinMode is not available on UpdateCommitteeBasePayload.
	if mailingListEmail, ok := v1Data["mailing_list_email"].(string); ok && mailingListEmail != "" {
		payload.MailingList = &mailingListEmail
		logger.With("mailing_list_email", mailingListEmail).DebugContext(ctx, "mapped committee mailing list email field for update")
	}
	if chatChannel, ok := v1Data["chat_channel"].(string); ok && chatChannel != "" {
		payload.ChatChannel = &chatChannel
		logger.With("chat_channel", chatChannel).DebugContext(ctx, "mapped committee chat channel field for update")
	}
}

// handleCommitteeMemberUpdate processes a committee member update from platform-community__c records.
func handleCommitteeMemberUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	// Check if we should skip this sync operation.
	if shouldSkipSync(ctx, v1Data) {
		return false
	}

	// Extract v1Principal from v1 data for JWT generation.
	v1Principal := extractV1Principal(ctx, v1Data)

	// Extract committee member SFID.
	sfid, ok := v1Data["sfid"].(string)
	if !ok || sfid == "" {
		logger.With("key", key).ErrorContext(ctx, "no SFID found in committee member data")
		return false
	}

	// Extract the contact SFID (contact_name__c). This is the identifier the v1
	// project-service DELETE/PATCH .../committees/{c}/members/{MemberID} endpoints
	// match on (SQL: WHERE contact_name__c = :memberID) — it is the v1 API's
	// "MemberID", NOT the platform-community__c record sfid (the v1 API's "ID").
	// Storing the record sfid in the reverse mapping causes v1 member deletes to
	// 404 and leaves members on the committee / meeting invites (LFXV2-2673).
	contactSFID, _ := v1Data["contact_name__c"].(string)
	contactSFID = strings.TrimSpace(contactSFID)
	if contactSFID == "" {
		logger.With("sfid", sfid).WarnContext(ctx, "missing contact_name__c on committee member; not storing reverse mapping, v1 delete/update sync will not resolve the member")
	} else if !sfidvalidator.IsValid(contactSFID) {
		// parseCommitteeMemberReverseMapping rejects a non-UUID, non-SFID third field
		// as malformed, so a mapping written with an invalid contactSFID here would
		// immediately be treated as unusable by every reader. Drop it instead of
		// publishing a value the readers will reject anyway.
		logger.With("sfid", sfid, "contact_sfid", contactSFID).
			WarnContext(ctx, "invalid contact_name__c on committee member; not storing reverse mapping, v1 delete/update sync will not resolve the member")
		contactSFID = ""
	}

	// Check for blank email and skip with warning.
	email := ""
	if contactEmail, ok := v1Data["contactemail__c"].(string); ok && contactEmail != "" {
		email = contactEmail
	}
	if email == "" {
		logger.With("sfid", sfid).WarnContext(ctx, "skipping committee member with blank email")
		return false
	}

	// Extract collaboration_name__c to get committee UID.
	collaborationNameV1, ok := v1Data["collaboration_name__c"].(string)
	if !ok || collaborationNameV1 == "" {
		logger.With("key", key, "sfid", sfid).ErrorContext(ctx, "no collaboration_name__c found in committee member data")
		return false
	}

	// Check if parent committee exists in mappings. On transient
	// error we NACK — treating it as "no committee" would skip the
	// sync and drop the event silently.
	committeeMappingKey := fmt.Sprintf("committee.sfid.%s", collaborationNameV1)
	committeeEntry, committeeLookupErr := mappingStore.Get(ctx, committeeMappingKey)
	switch {
	case committeeLookupErr == nil:
		// proceed
	case errors.Is(committeeLookupErr, ErrKeyNotFound):
		logger.With("collaboration_sfid", collaborationNameV1, "member_sfid", sfid).InfoContext(ctx, "skipping committee member sync - parent committee not found in mappings")
		return false
	default:
		logger.With(errKey, committeeLookupErr, "collaboration_sfid", collaborationNameV1, "member_sfid", sfid).
			ErrorContext(ctx, "transient mapping-store failure resolving parent committee SFID; redelivering")
		return true
	}

	// Look up committee UID from collaboration_name__c mapping.
	// Note: collaboration_name__c points to the v1 SFID of the committee.
	committeeUID := string(committeeEntry.Value)
	logger.With("collaboration_sfid", collaborationNameV1, "committee_uid", committeeUID).DebugContext(ctx, "found committee UID from committee SFID mapping")

	// Check if we have an existing mapping. On transient error we
	// NACK — treating it as "no mapping" would send the create-path
	// and produce a duplicate v2 member.
	memberMappingKey := fmt.Sprintf("committee_member.sfid.%s", sfid)
	existingMemberUID := ""
	needsFormatUpgrade := false

	entry, memErr := mappingStore.Get(ctx, memberMappingKey)
	switch {
	case memErr == nil:
		if isTombstonedMapping(entry.Value) {
			logger.With("sfid", sfid).WarnContext(ctx, "skipping committee member upsert - mapping is tombstoned (previously deleted)")
			return false
		}
		mappingValue := string(entry.Value)
		// Check if it's new format (committee:member) or old format (just member).
		if strings.Contains(mappingValue, ":") {
			// New format: "{committee_uuid}:{member_uuid}".
			parts := strings.Split(mappingValue, ":")
			if len(parts) == 2 {
				existingMemberUID = parts[1]
			}
		} else {
			// Old format: just member UID - needs upgrade.
			existingMemberUID = mappingValue
			needsFormatUpgrade = true
		}
	case errors.Is(memErr, ErrKeyNotFound):
		// Genuine miss — fall through to create path.
	default:
		logger.With(errKey, memErr, "sfid", sfid).
			ErrorContext(ctx, "transient mapping-store failure resolving committee member SFID; redelivering to avoid duplicate v2 create")
		return true
	}

	var memberUID string
	var err error

	if existingMemberUID != "" {
		// Update existing committee member.
		logger.With("member_uid", existingMemberUID, "sfid", sfid, "committee_uid", committeeUID).InfoContext(ctx, "updating existing committee member")

		// Map v1 data to update payload.
		var payload *committeeservice.UpdateCommitteeMemberPayload
		payload, err = mapV1DataToCommitteeMemberUpdatePayload(ctx, committeeUID, existingMemberUID, v1Data)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to map v1 data to committee member update payload")
			return isTransientStoreErr(err)
		}

		// Record that we're about to write the v2 committee member so
		// the resulting indexer event (which will fan out to
		// syncCommitteeMemberUpdateToV1) can identify itself as our own
		// round-trip echo and skip the redundant v1 PATCH that would
		// otherwise overwrite lastmodifiedbyid with our client ID.
		// See pending_markers.go for the full contract.
		//
		// If updateCommitteeMember reports no mutation (change-detection
		// short-circuit, or a pre-mutation error like a
		// fetchCommitteeMember / JWT / API failure) we delete the
		// marker below — regardless of err. A no-mutation outcome means
		// no lfx.committee_member.updated indexer event will fire, so
		// the marker has no consumer and would otherwise silence a
		// genuine v2 update within the freshness window. On a
		// NACK+redeliver, the retry re-writes the marker before its
		// next v2 API attempt.
		writePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceCommitteeMember, existingMemberUID)

		var memberMutated bool
		memberMutated, err = updateCommitteeMember(ctx, payload, v1Principal)
		if !memberMutated {
			deletePendingMarker(ctx, markerV1ToV2, markerOpUpdate, markerResourceCommitteeMember, existingMemberUID)
		}
		memberUID = existingMemberUID
	} else {
		// Create new committee member.
		logger.With("sfid", sfid, "committee_uid", committeeUID).InfoContext(ctx, "creating new committee member")

		// Map v1 data to create payload.
		var payload *committeeservice.CreateCommitteeMemberPayload
		payload, err = mapV1DataToCommitteeMemberCreatePayload(ctx, committeeUID, v1Data)
		if err != nil {
			logger.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to map v1 data to committee member create payload")
			return isTransientStoreErr(err)
		}

		var response *committeeservice.CommitteeMemberFullWithReadonlyAttributes
		response, err = createCommitteeMember(ctx, payload, v1Principal)
		if response != nil && response.UID != nil {
			memberUID = *response.UID
		}
	}

	if err != nil {
		logger.With(errKey, err, "sfid", sfid).ErrorContext(ctx, "failed to sync committee member")
		return isTransientStoreErr(err)
	}

	// Store the member mapping.
	// Store the mapping in new format: "{committee_uuid}:{member_uuid}".
	if memberUID != "" {
		newMappingValue := fmt.Sprintf("%s:%s", committeeUID, memberUID)
		if _, err := mappingStore.Put(ctx, memberMappingKey, []byte(newMappingValue)); err != nil {
			logger.With(errKey, err, "sfid", sfid, "member_uid", memberUID).WarnContext(ctx, "failed to store committee member mapping")
		} else if needsFormatUpgrade {
			logger.With("sfid", sfid, "member_uid", memberUID, "committee_uid", committeeUID).InfoContext(ctx, "upgraded committee member mapping to new format")
		}

		// Store reverse mapping (committee:member UID -> v1 project:committee:member SFID).
		// Extract project SFID from v1 data for reverse mapping.
		projectSFID := ""
		if projSFID, ok := v1Data["project_name__c"].(string); ok && projSFID != "" {
			projectSFID = projSFID
		}

		// The third field must be the contact SFID (v1 API "MemberID"), which the v1
		// delete/update endpoints match on — not the record sfid (LFXV2-2673). Without a
		// contact SFID the mapping cannot drive a v1 delete/update at all, and would
		// otherwise overwrite any existing usable mapping, so skip the write entirely.
		//
		// The reverse mapping is kept to exactly three fields (same field count as
		// before this fix) so a rolling deploy never has an old pod (parsing this same
		// key with the previous release's splitThreeParts-based reader) misparse a
		// value written by a new pod. The record sfid needed to tombstone the forward
		// mapping "committee_member.sfid.<sfid>" on delete is instead stored in the
		// separate committeeMemberRecordSFIDKey — see parseCommitteeMemberReverseMapping
		// in ingest_indexer.go.
		if contactSFID == "" {
			logger.With("sfid", sfid, "member_uid", memberUID).
				WarnContext(ctx, "not storing committee member reverse mapping: missing contact_name__c")
		} else {
			// Write the record-sfid companion before the reverse mapping itself: once
			// the reverse mapping is published with a contact SFID, the backfill
			// classifies it as already OK and never revisits it, so a companion that
			// failed to write here would never get repaired. If this write fails,
			// skip the reverse mapping write too so it isn't left inconsistent —
			// the next update event for this v1 record will retry both.
			if _, err := mappingStore.Put(ctx, committeeMemberRecordSFIDKey(memberUID), []byte(sfid)); err != nil {
				logger.With(errKey, err, "committee_uid", committeeUID, "member_uid", memberUID).
					WarnContext(ctx, "failed to store committee member record sfid mapping, skipping reverse mapping write to avoid publishing it without a companion")
			} else {
				reverseMappingKey := fmt.Sprintf("committee_member.uid.%s", memberUID)
				reverseMappingValue := fmt.Sprintf("%s:%s:%s", projectSFID, collaborationNameV1, contactSFID)
				if _, err := mappingStore.Put(ctx, reverseMappingKey, []byte(reverseMappingValue)); err != nil {
					logger.With(errKey, err, "committee_uid", committeeUID, "member_uid", memberUID).WarnContext(ctx, "failed to store committee member reverse mapping")
				}
			}
		}
	}

	logger.With("member_uid", memberUID, "sfid", sfid, "committee_uid", committeeUID).InfoContext(ctx, "successfully synced committee member")
	return false
}

// mapV1DataToCommitteeMemberCreatePayload converts v1 platform-community__c data to a CreateCommitteeMemberPayload.
func mapV1DataToCommitteeMemberCreatePayload(ctx context.Context, committeeUID string, v1Data map[string]any) (*committeeservice.CreateCommitteeMemberPayload, error) {
	// Extract email field (already validated by caller).
	email := ""
	if contactEmail, ok := v1Data["contactemail__c"].(string); ok && contactEmail != "" {
		email = contactEmail
	}

	payload := &committeeservice.CreateCommitteeMemberPayload{
		UID:              committeeUID,
		Email:            email,
		Version:          "1",
		SkipNotification: cfg != nil && cfg.CommitteeSkipMemberNotifications,
	}

	// Map contact information.
	if contactNameV1, ok := v1Data["contact_name__c"].(string); ok && contactNameV1 != "" {
		// Look up user information from v1 API using the SFID.
		user, err := lookupMergedUser(ctx, contactNameV1)
		if err != nil {
			logger.With(errKey, err, "contact_name_sfid", contactNameV1).WarnContext(ctx, "failed to lookup user from v1 API, falling back to name-only lookup")
			// The lookup fails for members who have not yet created an LFX account
			// (no username in merged_user). Fall back to the raw DB row so we can
			// still populate first_name/last_name even without a username.
			if row, rowErr := dbLookupMergedUserRowBySFID(ctx, contactNameV1); rowErr == nil && row != nil {
				applyRowNames(row, &payload.FirstName, &payload.LastName)
			}
		} else {
			payload.Username = &user.Username
			if user.FirstName != "" {
				payload.FirstName = &user.FirstName
			}
			if user.LastName != "" {
				payload.LastName = &user.LastName
			}
		}
	}

	// Map job title.
	if title, ok := v1Data["title"].(string); ok && title != "" {
		payload.JobTitle = &title
	}

	// Map committee role information - only if role__c is set.
	if role, ok := v1Data["role__c"].(string); ok && role != "" {
		roleStruct := &struct {
			Name      string  `json:"name"`
			StartDate *string `json:"start_date,omitempty"`
			EndDate   *string `json:"end_date,omitempty"`
		}{
			Name: mapRoleNameToValidValue(ctx, role),
		}

		if startDate, ok := v1Data["start_date__c"].(string); ok && startDate != "" {
			dateOnly := extractDateOnly(startDate)
			if dateOnly != "" {
				roleStruct.StartDate = &dateOnly
			}
		}

		if endDate, ok := v1Data["end_date__c"].(string); ok && endDate != "" {
			dateOnly := extractDateOnly(endDate)
			if dateOnly != "" {
				roleStruct.EndDate = &dateOnly
			}
		}

		payload.Role = &struct {
			Name      string
			StartDate *string
			EndDate   *string
		}{
			Name:      roleStruct.Name,
			StartDate: roleStruct.StartDate,
			EndDate:   roleStruct.EndDate,
		}
	}

	// Map appointed by.
	if appointedBy, ok := v1Data["appointed_by__c"].(string); ok {
		payload.AppointedBy = mapAppointedByToValidValue(ctx, appointedBy)
	} else {
		payload.AppointedBy = "None" // Default value.
	}

	// Map status.
	if status, ok := v1Data["status__c"].(string); ok && status != "" {
		payload.Status = status
	} else {
		payload.Status = "Active" // Default status.
	}

	// Map voting information - only if voting_status__c is set.
	if votingStatus, ok := v1Data["voting_status__c"].(string); ok && votingStatus != "" {
		votingStruct := &struct {
			Status    string  `json:"status"`
			StartDate *string `json:"start_date,omitempty"`
			EndDate   *string `json:"end_date,omitempty"`
		}{
			Status: votingStatus,
		}

		if votingStartDate, ok := v1Data["voting_start_date__c"].(string); ok && votingStartDate != "" {
			dateOnly := extractDateOnly(votingStartDate)
			if dateOnly != "" {
				votingStruct.StartDate = &dateOnly
			}
		}

		if votingEndDate, ok := v1Data["voting_end_date__c"].(string); ok && votingEndDate != "" {
			dateOnly := extractDateOnly(votingEndDate)
			if dateOnly != "" {
				votingStruct.EndDate = &dateOnly
			}
		}

		payload.Voting = &struct {
			Status    string
			StartDate *string
			EndDate   *string
		}{
			Status:    votingStruct.Status,
			StartDate: votingStruct.StartDate,
			EndDate:   votingStruct.EndDate,
		}
	}

	// Map organization information.
	if accountSFID, ok := v1Data["account__c"].(string); ok && accountSFID != "" {
		// Look up organization information from v1 Organization Service.
		org, err := lookupV1Org(ctx, accountSFID)
		if err != nil {
			logger.With(errKey, err, "account_sfid", accountSFID).WarnContext(ctx, "failed to lookup organization, leaving empty")
			// Organization lookup failed, leave Organization field nil.
		} else if org.Name != "" {
			// Successfully fetched organization data.
			orgName := org.Name
			orgStruct := &struct {
				ID      *string
				Name    *string
				Website *string
			}{
				// NOTE: This is highly irregular - we are adding v1 identifiers
				// into v2. Everywhere else (except v1 meetings) we've made a
				// clean break with new UUIDs. This v1 SFID was added to the
				// service in order to implement external Data Lake queries.
				// However, as we are not expecting to migrate the v1
				// Organization Service into LFX One, this should get changed in
				// the future. There *will* be a concept of B2B-engaged
				// organizations managed in LFX One, requiring some kind of
				// role-assignment journey, and thus a service that is somewhere
				// between the v1 Organization Service and Member Service in
				// terms of functionality. However, principally-B2C engagements
				// like committee membership will be expected to use something
				// like "domain" or "Clearbit ID" as the unique identifier.
				ID:   &accountSFID,
				Name: &orgName,
			}

			// Parse website URL from Domain attribute.
			if websiteURL := parseWebsiteURL(org.Domain); websiteURL != "" {
				orgStruct.Website = &websiteURL
			}

			payload.Organization = orgStruct
		}
	}

	return payload, nil
}

// applyRowNames sets FirstName and LastName on the pointed-to *string values
// from a merged_user row when they are non-empty. Used as the no-LFX-account
// fallback when lookupMergedUser fails (no username in merged_user).
func applyRowNames(row *mergedUserRow, firstName, lastName **string) {
	if row.FirstName.String != "" {
		fn := row.FirstName.String
		*firstName = &fn
	}
	if row.LastName.String != "" {
		ln := row.LastName.String
		*lastName = &ln
	}
}

// mapV1DataToCommitteeMemberUpdatePayload converts v1 platform-community__c data to an UpdateCommitteeMemberPayload.
func mapV1DataToCommitteeMemberUpdatePayload(ctx context.Context, committeeUID string, memberUID string, v1Data map[string]any) (*committeeservice.UpdateCommitteeMemberPayload, error) {
	// Extract email field (already validated by caller).
	email := ""
	if contactEmail, ok := v1Data["contactemail__c"].(string); ok && contactEmail != "" {
		email = contactEmail
	}

	payload := &committeeservice.UpdateCommitteeMemberPayload{
		UID:       committeeUID,
		MemberUID: memberUID,
		Email:     email,
		Version:   "1",
	}

	// Map contact information.
	if contactNameV1, ok := v1Data["contact_name__c"].(string); ok && contactNameV1 != "" {
		// Look up user information from v1 API using the SFID.
		user, err := lookupMergedUser(ctx, contactNameV1)
		if err != nil {
			logger.With(errKey, err, "contact_name_sfid", contactNameV1).WarnContext(ctx, "failed to lookup user from v1 API, falling back to name-only lookup")
			// The lookup fails for members who have not yet created an LFX account
			// (no username in merged_user). Fall back to the raw DB row so we can
			// still populate first_name/last_name even without a username.
			if row, rowErr := dbLookupMergedUserRowBySFID(ctx, contactNameV1); rowErr == nil && row != nil {
				applyRowNames(row, &payload.FirstName, &payload.LastName)
			}
		} else {
			payload.Username = &user.Username
			if user.FirstName != "" {
				payload.FirstName = &user.FirstName
			}
			if user.LastName != "" {
				payload.LastName = &user.LastName
			}
		}
	}

	// Map job title.
	if title, ok := v1Data["title"].(string); ok && title != "" {
		payload.JobTitle = &title
	}

	// Map committee role information - only if role__c is set.
	if role, ok := v1Data["role__c"].(string); ok && role != "" {
		roleStruct := &struct {
			Name      string  `json:"name"`
			StartDate *string `json:"start_date,omitempty"`
			EndDate   *string `json:"end_date,omitempty"`
		}{
			Name: mapRoleNameToValidValue(ctx, role),
		}

		if startDate, ok := v1Data["start_date__c"].(string); ok && startDate != "" {
			dateOnly := extractDateOnly(startDate)
			if dateOnly != "" {
				roleStruct.StartDate = &dateOnly
			}
		}

		if endDate, ok := v1Data["end_date__c"].(string); ok && endDate != "" {
			dateOnly := extractDateOnly(endDate)
			if dateOnly != "" {
				roleStruct.EndDate = &dateOnly
			}
		}

		payload.Role = &struct {
			Name      string
			StartDate *string
			EndDate   *string
		}{
			Name:      roleStruct.Name,
			StartDate: roleStruct.StartDate,
			EndDate:   roleStruct.EndDate,
		}
	}

	// Map appointed by.
	if appointedBy, ok := v1Data["appointed_by__c"].(string); ok {
		payload.AppointedBy = mapAppointedByToValidValue(ctx, appointedBy)
	} else {
		payload.AppointedBy = "None" // Default value.
	}

	// Map status.
	if status, ok := v1Data["status__c"].(string); ok && status != "" {
		payload.Status = status
	} else {
		payload.Status = "Active" // Default status.
	}

	// Map voting information - only if voting_status__c is set.
	if votingStatus, ok := v1Data["voting_status__c"].(string); ok && votingStatus != "" {
		votingStruct := &struct {
			Status    string  `json:"status"`
			StartDate *string `json:"start_date,omitempty"`
			EndDate   *string `json:"end_date,omitempty"`
		}{
			Status: votingStatus,
		}

		if votingStartDate, ok := v1Data["voting_start_date__c"].(string); ok && votingStartDate != "" {
			dateOnly := extractDateOnly(votingStartDate)
			if dateOnly != "" {
				votingStruct.StartDate = &dateOnly
			}
		}

		if votingEndDate, ok := v1Data["voting_end_date__c"].(string); ok && votingEndDate != "" {
			dateOnly := extractDateOnly(votingEndDate)
			if dateOnly != "" {
				votingStruct.EndDate = &dateOnly
			}
		}

		payload.Voting = &struct {
			Status    string
			StartDate *string
			EndDate   *string
		}{
			Status:    votingStruct.Status,
			StartDate: votingStruct.StartDate,
			EndDate:   votingStruct.EndDate,
		}
	}

	// Map organization information.
	if accountSFID, ok := v1Data["account__c"].(string); ok && accountSFID != "" {
		// Look up organization information from v1 Organization Service.
		org, err := lookupV1Org(ctx, accountSFID)
		if err != nil {
			logger.With(errKey, err, "account_sfid", accountSFID).WarnContext(ctx, "failed to lookup organization, leaving empty")
			// Organization lookup failed, leave Organization field nil.
		} else if org.Name != "" {
			// Successfully fetched organization data.
			orgName := org.Name
			orgStruct := &struct {
				ID      *string
				Name    *string
				Website *string
			}{
				// NOTE: This is highly irregular - we are adding v1 identifiers into v2.
				// (Please see additional commentary in the corresponding code in
				// the above mapping function for the member "create" payload.)
				ID:   &accountSFID,
				Name: &orgName,
			}

			// Parse website URL from Domain attribute.
			if websiteURL := parseWebsiteURL(org.Domain); websiteURL != "" {
				orgStruct.Website = &websiteURL
			}

			payload.Organization = orgStruct
		}
	}

	return payload, nil
}
