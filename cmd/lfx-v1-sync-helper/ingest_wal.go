// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
)

// ActionKind represents the type of WAL action, matching wal-listener's internal ActionKind.
type ActionKind string

// WAL action types - these constants match the wal-listener's ActionKind values.
const (
	ActionInsert   ActionKind = "INSERT"
	ActionUpdate   ActionKind = "UPDATE"
	ActionDelete   ActionKind = "DELETE"
	ActionTruncate ActionKind = "TRUNCATE"
)

// String returns the string representation of the ActionKind.
func (k ActionKind) String() string {
	return string(k)
}

// WALEvent represents the structure of a WAL listener event received from the wal_listener stream.
// This structure matches the JSON payload format emitted by the wal-listener service when
// PostgreSQL WAL changes are detected.
type WALEvent struct {
	ID         string                 `json:"id"`         // Unique event ID
	Schema     string                 `json:"schema"`     // Database schema name (e.g., "platform")
	Table      string                 `json:"table"`      // Table name (e.g., "community__c")
	Action     string                 `json:"action"`     // Action type: "INSERT", "UPDATE", or "DELETE"
	Data       map[string]interface{} `json:"data"`       // New/current record data (empty for DELETE)
	DataOld    map[string]interface{} `json:"dataOld"`    // Previous record data (used for DELETE operations)
	CommitTime string                 `json:"commitTime"` // Transaction commit timestamp
}

// ActionKind returns the parsed ActionKind from the Action field.
func (w *WALEvent) ActionKind() ActionKind {
	return ActionKind(strings.ToUpper(w.Action))
}

// IsValid checks if the WAL event has the minimum required fields.
func (w *WALEvent) IsValid() bool {
	return w.Schema != "" && w.Table != "" && w.Action != ""
}

// walPKColumnOverrides maps a table name to its primary-key column, for the
// handful of tables that aren't Salesforce __c objects and so don't carry a
// literal "sfid" column. Every other table defaults to "sfid".
var walPKColumnOverrides = map[string]string{
	"user_skills": "id",
}

// pkColumn returns the column GetSFID should read as the row's identity key
// for this event's table, defaulting to "sfid".
func (w *WALEvent) pkColumn() string {
	if col, ok := walPKColumnOverrides[w.Table]; ok {
		return col
	}
	return "sfid"
}

// walDeleteFallbackTables lists tables where handleWALDelete should build a
// tombstone directly from a DELETE event's DataOld when no v1-objects KV
// entry already exists for the row, instead of silently skipping the
// delete. This is needed both for tables without a Meltano seed and for
// merged_user, whose DataOld may be the only copy of username__c after the
// source row is physically removed. The merged-user delete handler also uses
// its exact-SFID database lookup when DataOld does not contain a username.
var walDeleteFallbackTables = map[string]bool{
	"user_skills": true,
	"merged_user": true,
}

// walTimestamplessTables lists tables that never carry a "systemmodstamp" or
// "lastmodifieddate" column (unlike Salesforce __c objects), so
// shouldUpdateBasedOnTimestamps would always find both timestamps missing on
// both sides and default to skipping the update. For these tables,
// handleWALUpsert instead compares WAL commit times via
// shouldUpdateBasedOnCommitTime, so real UPDATEs after the initial INSERT
// aren't silently dropped.
var walTimestamplessTables = map[string]bool{
	"user_skills": true,
}

// GetSFID extracts the row identity key from the appropriate data field based
// on the action. For DELETE actions, it looks in DataOld; for others, it
// looks in Data. Despite the name, the column read is table-dependent: most
// tables are Salesforce __c objects keyed by "sfid", but a few plain platform
// tables (e.g. user_skills) are keyed by "id" instead — see pkColumn.
func (w *WALEvent) GetSFID() (string, bool) {
	var dataSource map[string]interface{}

	switch w.ActionKind() {
	case ActionDelete:
		dataSource = w.DataOld
	default:
		dataSource = w.Data
	}

	if dataSource == nil {
		return "", false
	}

	sfidValue, exists := dataSource[w.pkColumn()]
	if !exists || sfidValue == nil {
		return "", false
	}

	sfid := fmt.Sprintf("%v", sfidValue)
	return sfid, sfid != ""
}

// walIngestHandler processes WAL listener events from the wal_listener stream.
// It handles INSERT, UPDATE, and DELETE operations by upserting or marking
// records as deleted in the v1-objects KV bucket. This enables real-time
// synchronization of PostgreSQL changes to the KV store for downstream consumption.
// Handles ACK/NAK logic internally based on retry conditions.
func walIngestHandler(msg jetstream.Msg) {
	ctx := context.Background()

	subject := msg.Subject()
	logger.With("subject", subject).DebugContext(ctx, "received WAL listener message")

	// Parse the WAL event.
	var walEvent WALEvent
	if err := json.Unmarshal(msg.Data(), &walEvent); err != nil {
		logger.With(errKey, err, "subject", subject).ErrorContext(ctx, "failed to unmarshal WAL event")
		if ackErr := msg.Ack(); ackErr != nil {
			logger.With(errKey, ackErr, "subject", subject).Error("failed to acknowledge WAL JetStream message")
		}
		return
	}

	// Validate the WAL event.
	if !walEvent.IsValid() {
		logger.With("subject", subject, "event", walEvent).WarnContext(ctx, "invalid WAL event, missing required fields")
		if ackErr := msg.Ack(); ackErr != nil {
			logger.With(errKey, ackErr, "subject", subject).Error("failed to acknowledge WAL JetStream message")
		}
		return
	}

	// Log the event details.
	logger.With(
		"subject", subject,
		"action", walEvent.Action,
		"table", walEvent.Table,
		"schema", walEvent.Schema,
	).DebugContext(ctx, "processing WAL event")

	// Handle different actions using typed constants.
	var shouldRetry bool
	switch walEvent.ActionKind() {
	case ActionInsert, ActionUpdate:
		shouldRetry = handleWALUpsert(ctx, &walEvent)
	case ActionDelete:
		shouldRetry = handleWALDelete(ctx, &walEvent)
	case ActionTruncate:
		logger.With("action", walEvent.Action, "table", walEvent.Table).DebugContext(ctx, "truncate action not supported, ignoring")
		shouldRetry = false
	default:
		logger.With("action", walEvent.Action, "table", walEvent.Table).WarnContext(ctx, "unknown WAL action, ignoring")
		shouldRetry = false
	}

	// Handle message acknowledgment based on retry decision.
	if shouldRetry {
		// NAK the message to trigger retry.
		if err := msg.Nak(); err != nil {
			logger.With(errKey, err, "subject", subject).Error("failed to NAK WAL JetStream message for retry")
		} else {
			logger.With("subject", subject).Debug("NAKed WAL message for retry")
		}
	} else {
		// Acknowledge the message.
		if err := msg.Ack(); err != nil {
			logger.With(errKey, err, "subject", subject).Error("failed to acknowledge WAL JetStream message")
		}
	}
}

// handleWALUpsert processes INSERT and UPDATE WAL events by upserting to v1-objects KV bucket.
// It dynamically constructs KV keys using the format "{schema}-{table}.{sfid}" (e.g., "platform-community__c.{sfid}").
// It encodes data using the configured format (JSON by default, or MessagePack if cfg.UseMsgpack is true).
// It implements conditional upsert logic that only updates records if EITHER systemmodstamp
// OR lastmodifieddate in the new data is later than the existing record. This ensures
// that only newer changes are propagated while avoiding unnecessary updates.
// Returns true if the operation should be retried (only for KV revision mismatches), false otherwise.
func handleWALUpsert(ctx context.Context, walEvent *WALEvent) bool {
	// Extract the SFID using the helper method.
	sfid, exists := walEvent.GetSFID()
	if !exists {
		logger.With("table", walEvent.Table, "action", walEvent.Action).WarnContext(ctx, "WAL event missing or empty sfid field, skipping")
		return false
	}

	// Construct the key based on schema and table name.
	keyPrefix := fmt.Sprintf("%s-%s", walEvent.Schema, walEvent.Table)
	key := fmt.Sprintf("%s.%s", keyPrefix, sfid)

	// Check if the key already exists in the KV bucket.
	existing, err := v1KV.Get(ctx, key)
	if err != nil && err != jetstream.ErrKeyNotFound {
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to get existing KV entry")
		return false
	}

	var shouldUpdate bool
	var lastRevision uint64

	if err == jetstream.ErrKeyNotFound {
		// Key doesn't exist, create it.
		shouldUpdate = true
	} else {
		// Key exists, check if we should update.
		lastRevision = existing.Revision()

		// Parse existing data. If the payload is empty or unparseable, treat
		// the entry as having no usable existing state and overwrite it with
		// the incoming WAL data (keeping lastRevision so we use Update, not
		// Create). This unwedges entries where a stray empty PUT has left the
		// key in an un-readable state and would otherwise block every future
		// update.
		var existingData map[string]interface{}
		if len(existing.Value()) == 0 {
			logger.With("key", key, "revision", lastRevision).WarnContext(ctx, "existing KV entry is empty, overwriting from WAL")
			shouldUpdate = true
		} else if unmarshalErr := json.Unmarshal(existing.Value(), &existingData); unmarshalErr != nil {
			if msgpackErr := msgpack.Unmarshal(existing.Value(), &existingData); msgpackErr != nil {
				logger.With(errKey, unmarshalErr, "msgpack_error", msgpackErr, "key", key, "revision", lastRevision).WarnContext(ctx, "existing KV entry is unparseable, overwriting from WAL")
				shouldUpdate = true
			}
		}

		if existingData != nil {
			// Check if the record was marked as deleted in the target.
			if deletedAt, exists := existingData["_sdc_deleted_at"]; exists && deletedAt != nil {
				logger.With("key", key).WarnContext(ctx, "skipping WAL upsert for deleted record")
				return false
			}

			// Compare timestamps to determine if we should update. Tables with
			// no systemmodstamp/lastmodifieddate columns fall back to
			// comparing WAL commit times instead.
			if walTimestamplessTables[walEvent.Table] {
				shouldUpdate = shouldUpdateBasedOnCommitTime(ctx, walEvent.CommitTime, existingData, key)
			} else {
				shouldUpdate = shouldUpdateBasedOnTimestamps(ctx, walEvent.Data, existingData, key)
			}
		}
	}

	if shouldUpdate {
		// Add metadata fields.
		walEvent.Data["_sdc_extracted_at"] = walEvent.CommitTime
		walEvent.Data["_sdc_received_at"] = time.Now().UTC().Format(time.RFC3339)

		// Encode the data using configured format (JSON or MessagePack).
		var dataBytes []byte
		var err error
		if cfg.UseMsgpack {
			dataBytes, err = msgpack.Marshal(walEvent.Data)
		} else {
			dataBytes, err = json.Marshal(walEvent.Data)
		}
		if err != nil {
			logger.With(errKey, err, "key", key, "use_msgpack", cfg.UseMsgpack).ErrorContext(ctx, "failed to marshal WAL data")
			return false
		}

		if lastRevision == 0 {
			// Create new entry.
			if _, err := v1KV.Create(ctx, key, dataBytes); err != nil {
				// Check if this is a revision mismatch (key already exists) that should be retried.
				if isRevisionMismatchError(err) {
					logger.With(errKey, err, "key", key).WarnContext(ctx, "KV create failed due to existing key, will retry")
					return true
				}
				logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to create KV entry from WAL event")
				return false
			}
			logger.With("key", key, "action", walEvent.Action, "encoding", getEncodingFormat()).InfoContext(ctx, "created KV entry from WAL event")
		} else {
			// Update existing entry.
			if _, err := v1KV.Update(ctx, key, dataBytes, lastRevision); err != nil {
				// Check if this is a revision mismatch that should be retried.
				if isRevisionMismatchError(err) {
					logger.With(errKey, err, "key", key, "revision", lastRevision).WarnContext(ctx, "KV revision mismatch, will retry")
					return true
				}
				logger.With(errKey, err, "key", key, "revision", lastRevision).ErrorContext(ctx, "failed to update KV entry from WAL event")
				return false
			}
			logger.With("key", key, "action", walEvent.Action, "revision", lastRevision, "encoding", getEncodingFormat()).InfoContext(ctx, "updated KV entry from WAL event")
		}
	} else {
		logger.With("key", key, "action", walEvent.Action).DebugContext(ctx, "skipping WAL upsert - existing data is newer or same")
	}

	return false
}

// handleWALDelete processes DELETE WAL events by marking records as deleted in v1-objects KV bucket.
// It uses the same dynamic key format "{schema}-{table}.{sfid}" to locate existing records.
// It encodes the updated data using the configured format (JSON by default, or MessagePack if cfg.UseMsgpack is true).
// Instead of removing the record entirely, it adds a "_sdc_deleted_at" timestamp field
// to maintain an audit trail and allow downstream systems to handle deletion appropriately.
// Returns true if the operation should be retried (only for KV revision mismatches), false otherwise.
func handleWALDelete(ctx context.Context, walEvent *WALEvent) bool {
	// Extract the SFID using the helper method (which handles DELETE action correctly).
	sfid, exists := walEvent.GetSFID()
	if !exists {
		logger.With("table", walEvent.Table, "action", walEvent.Action).WarnContext(ctx, "WAL delete event missing or empty sfid field, skipping")
		return false
	}

	// Construct the key based on schema and table name.
	keyPrefix := fmt.Sprintf("%s-%s", walEvent.Schema, walEvent.Table)
	key := fmt.Sprintf("%s.%s", keyPrefix, sfid)

	// Check if the key exists in the KV bucket.
	existing, err := v1KV.Get(ctx, key)
	var existingData map[string]interface{}
	var lastRevision uint64

	switch {
	case err == jetstream.ErrKeyNotFound:
		if !walDeleteFallbackTables[walEvent.Table] || len(walEvent.DataOld) == 0 {
			// Key doesn't exist and this table has no DataOld fallback,
			// nothing to delete.
			logger.With("key", key).DebugContext(ctx, "WAL delete event for non-existent key, skipping")
			return false
		}
		// No prior KV entry — this table isn't Meltano-backfilled, so a row
		// that predates being added to the WAL filter never got one. DataOld
		// carries the full row (REPLICA IDENTITY FULL), so build the
		// tombstone from it directly instead of dropping the delete.
		existingData = make(map[string]interface{}, len(walEvent.DataOld))
		for k, v := range walEvent.DataOld {
			existingData[k] = v
		}
		logger.With("key", key, "table", walEvent.Table).InfoContext(ctx, "WAL delete event for untracked key, creating tombstone from DataOld")
	case err != nil:
		logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to get existing KV entry for delete")
		return false
	default:
		lastRevision = existing.Revision()
		// Parse existing data.
		if unmarshalErr := json.Unmarshal(existing.Value(), &existingData); unmarshalErr != nil {
			// Try msgpack if JSON fails.
			if msgpackErr := msgpack.Unmarshal(existing.Value(), &existingData); msgpackErr != nil {
				logger.With(errKey, unmarshalErr, "msgpack_error", msgpackErr, "key", key).ErrorContext(ctx, "failed to unmarshal existing KV entry data for delete")
				return false
			}
		}
	}

	// WAL DataOld represents the row immediately before deletion and is more
	// authoritative than a potentially stale KV snapshot for merged_user's
	// username. Only overlay this field when the producer supplied it; if it
	// is absent, retain the KV value and let the delete handler's exact-SFID
	// lookup provide the final fallback.
	if walEvent.Table == "merged_user" {
		if oldUsername, exists := walEvent.DataOld["username__c"]; exists {
			existingData["username__c"] = oldUsername
		}
	}

	// Update metadata fields.
	existingData["_sdc_extracted_at"] = walEvent.CommitTime
	existingData["_sdc_received_at"] = time.Now().UTC().Format(time.RFC3339)
	// Mark the record as deleted by adding _sdc_deleted_at field.
	existingData["_sdc_deleted_at"] = walEvent.CommitTime

	// Encode the updated data using configured format (JSON or MessagePack).
	var dataBytes []byte
	if cfg.UseMsgpack {
		dataBytes, err = msgpack.Marshal(existingData)
	} else {
		dataBytes, err = json.Marshal(existingData)
	}
	if err != nil {
		logger.With(errKey, err, "key", key, "use_msgpack", cfg.UseMsgpack).ErrorContext(ctx, "failed to marshal deletion marker data")
		return false
	}

	// Update the entry with the deletion marker. If there was no existing KV
	// entry (the DataOld-fallback tombstone case above), create it instead.
	if lastRevision == 0 {
		if _, err := v1KV.Create(ctx, key, dataBytes); err != nil {
			// Check if this is a revision mismatch (key already exists) that should be retried.
			if isRevisionMismatchError(err) {
				logger.With(errKey, err, "key", key).WarnContext(ctx, "KV create failed due to existing key on delete, will retry")
				return true
			}
			logger.With(errKey, err, "key", key).ErrorContext(ctx, "failed to create tombstone KV entry for delete")
			return false
		}
	} else if _, err := v1KV.Update(ctx, key, dataBytes, lastRevision); err != nil {
		// Check if this is a revision mismatch that should be retried.
		if isRevisionMismatchError(err) {
			logger.With(errKey, err, "key", key, "revision", lastRevision).WarnContext(ctx, "KV revision mismatch on delete, will retry")
			return true
		}
		logger.With(errKey, err, "key", key, "revision", lastRevision).ErrorContext(ctx, "failed to update KV entry with deletion marker")
		return false
	}

	logger.With("key", key, "encoding", getEncodingFormat()).InfoContext(ctx, "marked KV entry as deleted from WAL event")
	return false
}

// shouldUpdateBasedOnTimestamps compares timestamps between new and existing data to determine if an update should occur.
// Returns true if EITHER systemmodstamp OR lastmodifieddate in new data is later than existing data.
// This implements the same comparison logic as the Meltano target-nats-kv plugin but allows
// updates based on either timestamp field being newer (not just the bookmark field).
func shouldUpdateBasedOnTimestamps(ctx context.Context, newData, existingData map[string]interface{}, key string) bool {
	// Extract timestamps from new data.
	newSystemModstamp := getTimestampString(newData, "systemmodstamp")
	newLastModified := getTimestampString(newData, "lastmodifieddate")

	// Extract timestamps from existing data.
	existingSystemModstamp := getTimestampString(existingData, "systemmodstamp")
	existingLastModified := getTimestampString(existingData, "lastmodifieddate")

	logger.With(
		"key", key,
		"new_systemmodstamp", newSystemModstamp,
		"new_lastmodified", newLastModified,
		"existing_systemmodstamp", existingSystemModstamp,
		"existing_lastmodified", existingLastModified,
	).DebugContext(ctx, "comparing timestamps for WAL upsert decision")

	// Parse timestamps for comparison.
	newSystemTime, newSystemErr := parseTimestamp(newSystemModstamp)
	newLastModTime, newLastModErr := parseTimestamp(newLastModified)
	existingSystemTime, existingSystemErr := parseTimestamp(existingSystemModstamp)
	existingLastModTime, existingLastModErr := parseTimestamp(existingLastModified)

	// Compare systemmodstamp if both are valid.
	if newSystemErr == nil && existingSystemErr == nil {
		if newSystemTime.After(existingSystemTime) {
			logger.With("key", key).DebugContext(ctx, "WAL upsert: new systemmodstamp is later")
			return true
		}
	}

	// Compare lastmodifieddate if both are valid.
	if newLastModErr == nil && existingLastModErr == nil {
		if newLastModTime.After(existingLastModTime) {
			logger.With("key", key).DebugContext(ctx, "WAL upsert: new lastmodifieddate is later")
			return true
		}
	}

	// If new timestamps are missing/invalid, warn and don't update.
	if newSystemErr != nil && newLastModErr != nil {
		logger.With("key", key, "systemmodstamp_err", newSystemErr, "lastmodified_err", newLastModErr).WarnContext(ctx, "WAL event has invalid timestamps, skipping upsert")
		return false
	}

	// If we have valid new timestamps but missing/invalid existing ones, update.
	if (newSystemErr == nil || newLastModErr == nil) && (existingSystemErr != nil && existingLastModErr != nil) {
		logger.With("key", key).DebugContext(ctx, "WAL upsert: new data has valid timestamps, existing data does not")
		return true
	}

	logger.With("key", key).DebugContext(ctx, "WAL upsert: existing data is newer or same, skipping")
	return false
}

// shouldUpdateBasedOnCommitTime compares the incoming WAL event's commit time
// against the existing KV entry's recorded "_sdc_extracted_at" (itself set
// from a prior event's commit time) to decide whether to update. Used for
// walTimestamplessTables, where shouldUpdateBasedOnTimestamps can't compare
// systemmodstamp/lastmodifieddate because the table never has those columns.
func shouldUpdateBasedOnCommitTime(ctx context.Context, newCommitTime string, existingData map[string]interface{}, key string) bool {
	newTime, newErr := parseTimestamp(newCommitTime)
	if newErr != nil {
		logger.With(errKey, newErr, "key", key, "commit_time", newCommitTime).WarnContext(ctx, "WAL event has invalid commit time, skipping upsert")
		return false
	}

	existingExtractedAt := getTimestampString(existingData, "_sdc_extracted_at")
	existingTime, existingErr := parseTimestamp(existingExtractedAt)
	if existingErr != nil {
		logger.With("key", key).DebugContext(ctx, "WAL upsert: existing entry has no valid commit time on record, updating")
		return true
	}

	if newTime.After(existingTime) {
		logger.With("key", key).DebugContext(ctx, "WAL upsert: new commit time is later")
		return true
	}

	logger.With("key", key).DebugContext(ctx, "WAL upsert: existing data is newer or same by commit time, skipping")
	return false
}

// getTimestampString safely extracts a timestamp string from a map.
// Returns an empty string if the field doesn't exist or is nil.
func getTimestampString(data map[string]interface{}, field string) string {
	if value, exists := data[field]; exists && value != nil {
		return fmt.Sprintf("%v", value)
	}
	return ""
}

// parseTimestamp parses a timestamp string in common formats used by Salesforce and PostgreSQL.
// It tries multiple timestamp formats to handle various datetime representations.
// For timestamps without timezone designators, it assumes UTC.
func parseTimestamp(timestampStr string) (time.Time, error) {
	if timestampStr == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}

	// Try common timestamp formats with timezone information first.
	formatsWithTZ := []string{
		time.RFC3339Nano,
		time.RFC3339,
	}

	for _, format := range formatsWithTZ {
		if t, err := time.Parse(format, timestampStr); err == nil {
			return t, nil
		}
	}

	// Try formats without timezone designators, assuming UTC.
	// This handles PostgreSQL "timestamp without time zone" columns.
	formatsWithoutTZ := []string{
		"2006-01-02T15:04:05.000000",
		"2006-01-02T15:04:05",
	}

	for _, format := range formatsWithoutTZ {
		if t, err := time.Parse(format, timestampStr); err == nil {
			// Assume UTC for zoneless timestamps.
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", timestampStr)
}

// isRevisionMismatchError checks if an error is a KV revision mismatch that should be retried.
func isRevisionMismatchError(err error) bool {
	// Attempt direct JetStreamError comparison.
	if jsErr, ok := err.(jetstream.JetStreamError); ok {
		if apiErr := jsErr.APIError(); apiErr != nil {
			return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
		}
	}

	// Check for NATS error strings containing the expected error codes.
	errStr := err.Error()
	if strings.Contains(errStr, "err_code=10071") ||
		strings.Contains(errStr, "wrong last sequence") ||
		strings.Contains(errStr, "key exists") {
		return true
	}

	return false
}

// getEncodingFormat returns a string representation of the current encoding format.
func getEncodingFormat() string {
	if cfg.UseMsgpack {
		return "msgpack"
	}
	return "json"
}
