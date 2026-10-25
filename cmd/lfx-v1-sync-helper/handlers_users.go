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
)

const (
	// v1-objects KV key prefixes as replicated by Meltano.
	v1MergedUserKVPrefix     = "salesforce-merged_user."
	v1AlternateEmailKVPrefix = "salesforce-alternate_email__c."
)

// auth0CallTimeout bounds Auth0 Management API work on handler-blocking paths.
// Chosen comfortably under the JetStream AckWait (30s, see main.go) so the
// handler always ACKs/NACKs before server-side redelivery. The 10s slack
// covers rate-limiter waits, logging, and the ACK/NACK roundtrip.
const auth0CallTimeout = 20 * time.Second

const userDeletedEventPublishTimeout = 10 * time.Second

// syncProfileToAuth0Fn is the function handleMergedUserUpdate calls to push
// a profile to Auth0. Swappable in tests; production leaves it pointing at
// the real Management-API implementation.
var syncProfileToAuth0Fn = syncProfileToAuth0

// syncAlternateEmailToAuth0 dependencies, split out so tests can inject fakes
// for the v1 platform DB reads and Auth0 identity operations without needing
// a live database or Management API.
var (
	lookupMergedUserFn    = lookupMergedUser
	linkEmailIdentityFn   = linkEmailIdentity
	unlinkEmailIdentityFn = unlinkEmailIdentity
)

// handleMergedUserDelete dependencies, split out so tests can inject fakes
// without needing a live NATS connection or database.
var (
	publishUserDeletedEventFn = publishUserDeletedEvent
	getPrimaryEmailForUserFn  = dbGetLastKnownPrimaryEmailForUser
	getDeletedUserUsernameFn  = dbGetUsernameForDeletedUser
)

// isSoleQualifyingAlternateEmailFn is injectable for tests.
var isSoleQualifyingAlternateEmailFn = dbIsSoleQualifyingAlternateEmail

// normalizeUserIdentifier normalizes a user-provided username or email for
// case-insensitive, whitespace-trimmed matching against the v1 platform
// database: TrimSpace → ToLower. No Unicode normalization is applied — the
// database column values are not normalized either, and both fields are
// expected to be ASCII in practice, so an input-only NFC pass would be
// misleading without actually guaranteeing canonical-equivalence matches.
func normalizeUserIdentifier(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// handleMergedUserUpdate processes merged user updates and syncs profile
// fields from the v1 platform DB to Auth0 user_metadata. Username and email
// lookups query the v1 platform database live, so no secondary index
// maintenance is needed here.
//
// The Auth0 profile sync runs synchronously before ACKing the JetStream
// message. Retryable Auth0 errors (429, 5xx) return true to NACK the
// message so JetStream redelivery provides backoff; non-retryable errors
// are logged and dropped. WithNoRetries is set on the management client so
// the handler controls retry behaviour directly.
//
// Targeted re-sync is handled separately by --sync-user / --sync-users-file.
func handleMergedUserUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	sfid, ok := v1Data["sfid"].(string)
	if !ok || sfid == "" {
		logger.With("key", key).WarnContext(ctx, "merged_user missing sfid, skipping")
		return false
	}

	username, _ := v1Data["username__c"].(string)
	if normalizeUserIdentifier(username) == "" {
		logger.With("key", key).DebugContext(ctx, "merged_user has no username, skipping profile sync")
		return false
	}

	auth0UserID := mapUsernameToAuthSub(username)
	return syncMergedUserProfile(ctx, key, auth0UserID, v1Data)
}

// handleMergedUserDelete processes deletion of a merged user record by
// publishing a user-deleted event so v2 committee data is scrubbed of the
// username. Lookups query the v1 platform database live, so there are no
// secondary indexes to clean up.
// Soft deletes and hard KV deletes both arrive here. For a hard KV delete,
// recover the username from the v1 database by the exact user SFID.
// Returns true if the operation should be retried, false otherwise.
func handleMergedUserDelete(ctx context.Context, key, userSfid string, v1Data map[string]any) bool {
	var username string
	if v1Data == nil {
		var err error
		username, err = getDeletedUserUsernameFn(ctx, userSfid)
		if err != nil {
			logger.With(errKey, err, "key", key, "user_sfid", userSfid).
				WarnContext(ctx, "failed to recover username for hard-deleted merged_user; retrying")
			return true
		}
	} else {
		if value, exists := v1Data["username__c"]; exists {
			// An explicitly present blank value is authoritative for this
			// delete; do not replace it with a potentially newer DB value.
			username, _ = value.(string)
		} else {
			var err error
			username, err = getDeletedUserUsernameFn(ctx, userSfid)
			if err != nil {
				logger.With(errKey, err, "key", key, "user_sfid", userSfid).
					WarnContext(ctx, "failed to recover username for deleted merged_user; retrying")
				return true
			}
			if normalizeUserIdentifier(username) == "" {
				logger.With("key", key, "user_sfid", userSfid).
					WarnContext(ctx, "merged_user delete cleanup skipped because username could not be recovered")
			}
		}
	}

	if normalizedUsername := normalizeUserIdentifier(username); normalizedUsername != "" {
		// Best-effort primary email lookup: the alternate email rows may
		// already be soft-deleted, so this queries without liveness filters.
		email, emailErr := getPrimaryEmailForUserFn(ctx, userSfid)
		if emailErr != nil {
			logger.With(errKey, emailErr, "key", key, "user_sfid", userSfid).
				DebugContext(ctx, "failed to look up primary email for deleted user; publishing user-deleted event without email")
		} else if email == "" {
			logger.With("key", key, "user_sfid", userSfid).
				DebugContext(ctx, "no primary email found for deleted user; publishing user-deleted event without email")
		}
		if err := publishUserDeletedEventFn(ctx, key, normalizedUsername, email); err != nil {
			logger.With(errKey, err, "key", key, "user_sfid", userSfid).
				WarnContext(ctx, "failed to publish user-deleted event; retrying KV message")
			return true
		}
	} else if v1Data == nil {
		logger.With("key", key, "user_sfid", userSfid).
			WarnContext(ctx, "merged_user hard-deleted but no username is available; skipping user-deleted event")
	}

	return false
}

// userDeletedEvent is the payload published to "lfx.v1-sync-helper.user.deleted" when a
// merged user is deleted. Username is the normalized LFID; Email is the deleted account's
// primary email when available so downstream scrubbers can distinguish LFID reuse.
type userDeletedEvent struct {
	Username string `json:"username"`
	Email    string `json:"email,omitempty"`
}

const v1SyncHelperUserDeletedSubject = "lfx.v1-sync-helper.user.deleted"

// publishUserDeletedEvent publishes a user-deleted NATS event. Flush confirms
// the server received the publish before the KV message is ACKed. A timeout
// can still cause a duplicate on retry if the server received the event but
// the flush response was lost; the subscriber's scrub operation is idempotent.
var natsPublishBytesFn = func(ctx context.Context, subject string, data []byte) error {
	if err := natsConn.Publish(subject, data); err != nil {
		return err
	}
	flushCtx, cancel := context.WithTimeout(ctx, userDeletedEventPublishTimeout)
	defer cancel()
	return natsConn.FlushWithContext(flushCtx)
}

func publishUserDeletedEvent(ctx context.Context, key, username, email string) error {
	payload, err := json.Marshal(userDeletedEvent{Username: username, Email: email})
	if err != nil {
		return fmt.Errorf("marshal user-deleted event: %w", err)
	}
	if err := natsPublishBytesFn(ctx, v1SyncHelperUserDeletedSubject, payload); err != nil {
		return fmt.Errorf("publish user-deleted event: %w", err)
	}
	logger.With("key", key).
		InfoContext(ctx, "published user-deleted event for committee username scrub")
	return nil
}

// syncMergedUserProfile calls syncProfileToAuth0Fn synchronously and returns
// true if the error is retryable so the caller can NACK the JetStream message.
func syncMergedUserProfile(ctx context.Context, key, auth0UserID string, v1Data map[string]any) bool {
	syncCtx, cancel := context.WithTimeout(ctx, auth0CallTimeout)
	defer cancel()

	auth0User, err := fetchAuth0User(syncCtx, auth0UserID)
	if err != nil {
		if isRetryableAuth0Error(err) {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
				WarnContext(ctx, "retryable Auth0 error fetching user for profile sync, NACKing for redelivery")
			return true
		}
		logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
			ErrorContext(ctx, "failed to fetch Auth0 user for profile sync, dropping non-retryable error")
		return false
	}

	// includeSkills=false: this live path races handleUserSkillsUpdate's own
	// live write of the same field (see syncProfileToAuth0's doc comment).
	if _, err := syncProfileToAuth0Fn(syncCtx, auth0UserID, auth0User, v1Data, false, false); err != nil {
		if isRetryableAuth0Error(err) {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
				WarnContext(ctx, "retryable Auth0 error during profile sync, NACKing for redelivery")
			return true
		}
		logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID).
			ErrorContext(ctx, "failed to sync profile to Auth0, dropping non-retryable error")
	}
	return false
}

// handleAlternateEmailUpdate processes additive alternate email updates by
// linking the email as an identity on the user's Auth0 account. Username and
// email lookups query the v1 platform database live, so no secondary index
// maintenance is needed here. Soft deletes are intercepted by handleKVPut
// before reaching this function and routed to handleAlternateEmailDelete
// instead.
// Returns true if the operation should be retried, false otherwise.
func handleAlternateEmailUpdate(ctx context.Context, key string, v1Data map[string]any) bool {
	leadorcontactid, ok := v1Data["leadorcontactid"].(string)
	if !ok || leadorcontactid == "" {
		logger.With("key", key).WarnContext(ctx, "alternate email missing leadorcontactid, skipping")
		return false
	}

	emailSfid, ok := v1Data["sfid"].(string)
	if !ok || emailSfid == "" {
		logger.With("key", key).WarnContext(ctx, "alternate email missing sfid, skipping")
		return false
	}

	// active__c=false means the user-service deactivated the email without
	// deleting the row. A ".old" domain suffix is a v1 convention for the
	// same intent without flipping active__c. Treat both the same as a soft delete.
	emailAddr, _ := v1Data["alternate_email_address__c"].(string)
	isOld := hasOldDomainSuffix(emailAddr)
	if isActive, ok := v1Data["active__c"].(bool); (ok && !isActive) || isOld {
		logger.With("key", key, "email_sfid", emailSfid, "old_domain", isOld).
			DebugContext(ctx, "alternate email inactive (active__c=false or .old domain), routing to delete handler")
		return handleAlternateEmailDelete(ctx, key, emailSfid, v1Data)
	}

	// Primary emails are not linked as Auth0 identities (they are the Auth0
	// user's own email).
	if isPrimary, _ := v1Data["primary_email__c"].(bool); isPrimary {
		return false
	}

	// If this is the user's only qualifying alternate email, treat it as
	// though it were flagged primary (see dbIsSoleQualifyingAlternateEmail):
	// v1 lazy-sync may not have created/synced the primary row yet.
	//
	// Known limitation: if this row is skipped here as sole, and a second
	// qualifying row (including a later primary-flagged row) arrives after
	// it, only the new row is considered by its own event — this row is not
	// revisited until a backfill run reconciles it. See LFXV2-2662.
	sole, err := isSoleQualifyingAlternateEmailFn(ctx, leadorcontactid)
	if err != nil {
		if errors.Is(err, errAmbiguousDefactoPrimaryEmail) {
			// Deterministic v1 data condition (multiple qualifying rows,
			// none flagged primary) — retrying will reach the same result
			// until the data is fixed, so drop rather than requesting
			// redelivery. This requires a manual data fix or a backfill
			// run once the ambiguity is resolved at the source.
			logger.With(errKey, err, "key", key, "user_sfid", leadorcontactid).
				ErrorContext(ctx, "cannot determine de-facto primary alternate email, dropping (requires manual data fix)")
			return false
		}
		// A read failure could just as easily be masking a true sole-email
		// case as a true multi-email case, so don't guess non-sole and risk
		// wrongly linking a row that should be de-facto primary. This may
		// well be transient, so request redelivery instead of silently
		// dropping the message — otherwise this valid link could go missing
		// until a backfill happens to run.
		logger.With(errKey, err, "key", key, "user_sfid", leadorcontactid).
			WarnContext(ctx, "failed to determine sole-qualifying-email status, requesting retry")
		return true
	}
	if sole {
		logger.With("key", key, "email_sfid", emailSfid, "user_sfid", leadorcontactid).
			DebugContext(ctx, "treating sole qualifying alternate email as de-facto primary, skipping Auth0 link")
		return false
	}

	// Only link verified, non-empty emails.
	isVerified, _ := v1Data["email_verified__c"].(bool)
	if !isVerified {
		logger.With("key", key, "email_sfid", emailSfid).
			DebugContext(ctx, "alternate email not verified, skipping Auth0 link")
		return false
	}
	if emailAddr == "" {
		return false
	}

	// Bound with auth0CallTimeout (< AckWait) so a stuck Auth0 request can't
	// stall the handler past JetStream's redelivery window.
	syncCtx, cancel := context.WithTimeout(ctx, auth0CallTimeout)
	defer cancel()
	if retry := linkAlternateEmailToAuth0(syncCtx, key, leadorcontactid, emailAddr); retry {
		return true
	}

	return false
}

// handleAlternateEmailDelete processes a soft delete of an alternate email
// record by unlinking the corresponding linked identity from the user's Auth0
// account. This is the only path that drives Auth0 unlinks — the update
// handler doesn't fire on soft deletes (handleKVPut routes them here).
// Returns true if the operation should be retried, false otherwise.
func handleAlternateEmailDelete(ctx context.Context, key, emailSfid string, v1Data map[string]any) bool {
	if v1Data == nil {
		// Hard KV delete — the payload is gone, so we
		// can't resolve the user or email. Soft deletes should always carry
		// a payload, so log a warning if we see this.
		logger.With("key", key, "email_sfid", emailSfid).
			WarnContext(ctx, "alternate email hard-deleted with no payload; cannot unlink Auth0 identity")
		return false
	}

	userSfid, _ := v1Data["leadorcontactid"].(string)
	emailAddr, _ := v1Data["alternate_email_address__c"].(string)

	if userSfid == "" {
		logger.With("key", key, "email_sfid", emailSfid).
			WarnContext(ctx, "alternate email delete missing leadorcontactid, skipping")
		return false
	}

	// Skip primary emails — the primary email is the Auth0 user's own email
	// field, not a linked identity, so it is out of scope for this handler.
	if isPrimary, _ := v1Data["primary_email__c"].(bool); isPrimary {
		return false
	}

	if emailAddr == "" {
		logger.With("key", key, "email_sfid", emailSfid).
			WarnContext(ctx, "alternate email delete missing address, cannot unlink Auth0 identity")
		return false
	}

	v1User, err := lookupMergedUserFn(ctx, userSfid)
	if err != nil {
		logger.With(errKey, err, "key", key, "user_sfid", userSfid).
			WarnContext(ctx, "failed to resolve v1 user for Auth0 email unlink")
		return false
	}
	if v1User.Username == "" {
		logger.With("key", key, "user_sfid", userSfid).
			WarnContext(ctx, "v1 user has no username, cannot resolve Auth0 ID for unlink")
		return false
	}
	auth0UserID := mapUsernameToAuthSub(v1User.Username)

	syncCtx, cancel := context.WithTimeout(ctx, auth0CallTimeout)
	defer cancel()

	auth0User, err := fetchAuth0User(syncCtx, auth0UserID)
	if err != nil {
		if isRetryableAuth0Error(err) {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", emailAddr).
				WarnContext(syncCtx, "retryable Auth0 error fetching user for unlink, NACKing for redelivery")
			return true
		}
		logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", emailAddr).
			ErrorContext(syncCtx, "failed to fetch Auth0 user for email unlink, dropping non-retryable error")
		return false
	}

	if err := unlinkEmailIdentityFn(syncCtx, auth0User, emailAddr); err != nil {
		if isRetryableAuth0Error(err) {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", emailAddr).
				WarnContext(syncCtx, "retryable Auth0 error during unlink, NACKing for redelivery")
			return true
		}
		logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", emailAddr).
			ErrorContext(syncCtx, "failed to unlink email identity from Auth0, dropping non-retryable error")
	}
	return false
}

// linkAlternateEmailToAuth0 links an alternate email as an Auth0 identity on
// the user's primary account. The caller is responsible for all field checks
// (primary, verified, active, non-empty address) before calling this function.
// Returns true if the operation should be retried (transient failure).
func linkAlternateEmailToAuth0(ctx context.Context, key, userSfid, email string) bool {
	v1User, err := lookupMergedUserFn(ctx, userSfid)
	if err != nil {
		logger.With(errKey, err, "key", key, "user_sfid", userSfid).
			WarnContext(ctx, "failed to resolve v1 user for Auth0 email link")
		return false
	}
	if v1User.Username == "" {
		logger.With("key", key, "user_sfid", userSfid).
			WarnContext(ctx, "v1 user has no username, cannot resolve Auth0 ID")
		return false
	}
	auth0UserID := mapUsernameToAuthSub(v1User.Username)

	auth0User, err := fetchAuth0User(ctx, auth0UserID)
	if err != nil {
		if isRetryableAuth0Error(err) {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", email).
				WarnContext(ctx, "retryable Auth0 error fetching user for link, NACKing for redelivery")
			return true
		}
		logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", email).
			ErrorContext(ctx, "failed to fetch Auth0 user for email link, dropping non-retryable error")
		return false
	}

	if _, err := linkEmailIdentityFn(ctx, auth0User, email); err != nil {
		if isRetryableAuth0Error(err) {
			logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", email).
				WarnContext(ctx, "retryable Auth0 error during link, NACKing for redelivery")
			return true
		}
		logger.With(errKey, err, "key", key, "auth0_user_id", auth0UserID, "email", email).
			ErrorContext(ctx, "failed to link email identity to Auth0, dropping non-retryable error")
	}
	return false
}

const (
	// kvObjectsStream is the JetStream stream backing the v1-objects KV bucket.
	kvObjectsStream = "KV_v1-objects"
)
