// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// fakeKVEntry is a minimal jetstream.KeyValueEntry for tests.
type fakeKVEntry struct {
	key      string
	value    []byte
	revision uint64
}

func (e *fakeKVEntry) Bucket() string                  { return "v1-objects" }
func (e *fakeKVEntry) Key() string                     { return e.key }
func (e *fakeKVEntry) Value() []byte                   { return e.value }
func (e *fakeKVEntry) Revision() uint64                { return e.revision }
func (e *fakeKVEntry) Created() time.Time              { return time.Time{} }
func (e *fakeKVEntry) Delta() uint64                   { return 0 }
func (e *fakeKVEntry) Operation() jetstream.KeyValueOp { return jetstream.KeyValuePut }

// fakeKV is a minimal in-memory jetstream.KeyValue for testing
// handleWALDelete's KV read/write paths. It embeds the interface so any
// method not overridden panics if called, rather than silently no-oping.
type fakeKV struct {
	jetstream.KeyValue
	data map[string][]byte
	rev  map[string]uint64
}

func newFakeKV() *fakeKV {
	return &fakeKV{data: make(map[string][]byte), rev: make(map[string]uint64)}
}

func (f *fakeKV) Get(_ context.Context, key string) (jetstream.KeyValueEntry, error) {
	v, ok := f.data[key]
	if !ok {
		return nil, jetstream.ErrKeyNotFound
	}
	return &fakeKVEntry{key: key, value: v, revision: f.rev[key]}, nil
}

func (f *fakeKV) Create(_ context.Context, key string, value []byte, _ ...jetstream.KVCreateOpt) (uint64, error) {
	if _, exists := f.data[key]; exists {
		return 0, jetstream.ErrKeyExists
	}
	f.data[key] = value
	f.rev[key] = 1
	return 1, nil
}

func (f *fakeKV) Update(_ context.Context, key string, value []byte, revision uint64) (uint64, error) {
	if f.rev[key] != revision {
		return 0, jetstream.ErrKeyExists // any non-nil error is enough; isRevisionMismatchError is checked separately in other tests
	}
	f.data[key] = value
	f.rev[key]++
	return f.rev[key], nil
}

// TestHandleWALDelete_DataOldFallback covers thread 1: a delete for a table
// with no prior KV entry (never Meltano-backfilled, never previously
// upserted) builds a tombstone directly from DataOld instead of silently
// dropping the delete.
func TestHandleWALDelete_DataOldFallback(t *testing.T) {
	origCfg, origKV := cfg, v1KV
	cfg = &Config{}
	fake := newFakeKV()
	v1KV = fake
	defer func() {
		cfg = origCfg
		v1KV = origKV
	}()

	event := &WALEvent{
		Schema:     "salesforce",
		Table:      "user_skills",
		Action:     "DELETE",
		CommitTime: "2024-01-01T00:00:00Z",
		DataOld:    map[string]interface{}{"id": "usk-1", "lfid": "jdoe", "skill_id": "sk-1"},
	}

	retry := handleWALDelete(context.Background(), event)
	if retry {
		t.Fatal("expected no retry")
	}

	key := "salesforce-user_skills.usk-1"
	raw, ok := fake.data[key]
	if !ok {
		t.Fatalf("expected tombstone KV entry at %q to be created, none found", key)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("failed to unmarshal tombstone: %v", err)
	}
	if got["lfid"] != "jdoe" {
		t.Errorf("tombstone lfid = %v, want jdoe (should be preserved from DataOld)", got["lfid"])
	}
	if got["_sdc_deleted_at"] != "2024-01-01T00:00:00Z" {
		t.Errorf("tombstone _sdc_deleted_at = %v, want commit time", got["_sdc_deleted_at"])
	}
}

func TestHandleWALDelete_MergedUserDataOldFallback(t *testing.T) {
	origCfg, origKV := cfg, v1KV
	cfg = &Config{}
	fake := newFakeKV()
	v1KV = fake
	defer func() {
		cfg = origCfg
		v1KV = origKV
	}()

	event := &WALEvent{
		Schema:     "salesforce",
		Table:      "merged_user",
		Action:     "DELETE",
		CommitTime: "2024-01-01T00:00:00Z",
		DataOld: map[string]interface{}{
			"sfid":        "003xx001",
			"username__c": "alice",
		},
	}

	if retry := handleWALDelete(context.Background(), event); retry {
		t.Fatal("expected no retry")
	}

	raw, ok := fake.data["salesforce-merged_user.003xx001"]
	if !ok {
		t.Fatal("expected merged_user tombstone to be created from DataOld")
	}
	var got map[string]interface{}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("failed to unmarshal tombstone: %v", err)
	}
	if got["username__c"] != "alice" {
		t.Errorf("tombstone username__c = %v, want alice", got["username__c"])
	}
	if got["_sdc_deleted_at"] != "2024-01-01T00:00:00Z" {
		t.Errorf("tombstone _sdc_deleted_at = %v, want commit time", got["_sdc_deleted_at"])
	}
}

func TestHandleWALDelete_MergedUserPrefersDataOldUsername(t *testing.T) {
	origCfg, origKV := cfg, v1KV
	cfg = &Config{}
	fake := newFakeKV()
	v1KV = fake
	defer func() {
		cfg = origCfg
		v1KV = origKV
	}()

	key := "salesforce-merged_user.003xx002"
	existing, err := json.Marshal(map[string]interface{}{
		"sfid":        "003xx002",
		"username__c": "stale-user",
	})
	if err != nil {
		t.Fatal(err)
	}
	fake.data[key] = existing
	fake.rev[key] = 1

	event := &WALEvent{
		Schema:     "salesforce",
		Table:      "merged_user",
		Action:     "DELETE",
		CommitTime: "2024-01-01T00:00:00Z",
		DataOld: map[string]interface{}{
			"sfid":        "003xx002",
			"username__c": "deleted-user",
		},
	}

	if retry := handleWALDelete(context.Background(), event); retry {
		t.Fatal("expected no retry")
	}

	var got map[string]interface{}
	if err := json.Unmarshal(fake.data[key], &got); err != nil {
		t.Fatalf("failed to unmarshal tombstone: %v", err)
	}
	if got["username__c"] != "deleted-user" {
		t.Errorf("tombstone username__c = %v, want deleted-user (WAL DataOld must override stale KV)", got["username__c"])
	}
}

func TestHandleWALDelete_MergedUserExplicitlyBlankDataOldUsername(t *testing.T) {
	origCfg, origKV := cfg, v1KV
	origLogger := logger
	origPublish := publishUserDeletedEventFn
	origEmail := getPrimaryEmailForUserFn
	origUsername := getDeletedUserUsernameFn
	cfg = &Config{}
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	fake := newFakeKV()
	v1KV = fake
	t.Cleanup(func() {
		cfg = origCfg
		v1KV = origKV
		logger = origLogger
		publishUserDeletedEventFn = origPublish
		getPrimaryEmailForUserFn = origEmail
		getDeletedUserUsernameFn = origUsername
	})

	const (
		sfid = "003xx005"
		key  = "salesforce-merged_user." + sfid
	)
	existing, err := json.Marshal(map[string]interface{}{
		"sfid":        sfid,
		"username__c": "stale-user",
	})
	if err != nil {
		t.Fatal(err)
	}
	fake.data[key] = existing
	fake.rev[key] = 1

	published := 0
	getDeletedUserUsernameFn = func(context.Context, string) (string, error) {
		t.Fatal("database fallback must not replace an explicitly blank DataOld username")
		return "", nil
	}
	getPrimaryEmailForUserFn = func(context.Context, string) (string, error) {
		return "", nil
	}
	publishUserDeletedEventFn = func(context.Context, string, string, string) error {
		published++
		return nil
	}

	event := &WALEvent{
		Schema:     "salesforce",
		Table:      "merged_user",
		Action:     "DELETE",
		CommitTime: "2024-01-01T00:00:00Z",
		DataOld: map[string]interface{}{
			"sfid":        sfid,
			"username__c": "",
		},
	}
	if retry := handleWALDelete(context.Background(), event); retry {
		t.Fatal("WAL handler requested retry")
	}

	entry, err := fake.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("failed to read WAL tombstone: %v", err)
	}
	if retry := kvHandler(entry); retry {
		t.Fatal("KV handler should ACK an explicitly blank old username")
	}
	if published != 0 {
		t.Fatalf("published %d events for explicitly blank old username, want 0", published)
	}
}

func TestHandleWALDelete_MergedUserEndToEnd(t *testing.T) {
	origCfg, origKV := cfg, v1KV
	origLogger := logger
	origPublish := publishUserDeletedEventFn
	origEmail := getPrimaryEmailForUserFn
	origUsername := getDeletedUserUsernameFn
	cfg = &Config{}
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	fake := newFakeKV()
	v1KV = fake
	t.Cleanup(func() {
		cfg = origCfg
		v1KV = origKV
		logger = origLogger
		publishUserDeletedEventFn = origPublish
		getPrimaryEmailForUserFn = origEmail
		getDeletedUserUsernameFn = origUsername
	})

	const (
		sfid     = "003xx003"
		username = "deleted-user"
		key      = "salesforce-merged_user." + sfid
	)
	existing, err := json.Marshal(map[string]interface{}{
		"sfid":        sfid,
		"username__c": "stale-user",
	})
	if err != nil {
		t.Fatal(err)
	}
	fake.data[key] = existing
	fake.rev[key] = 1

	published := 0
	var publishedUsername string
	getDeletedUserUsernameFn = func(context.Context, string) (string, error) {
		t.Fatal("database fallback should not be needed when DataOld contains username__c")
		return "", nil
	}
	getPrimaryEmailForUserFn = func(context.Context, string) (string, error) {
		return "", nil
	}
	publishUserDeletedEventFn = func(_ context.Context, _, gotUsername, _ string) error {
		published++
		publishedUsername = gotUsername
		return nil
	}

	event := &WALEvent{
		Schema:     "salesforce",
		Table:      "merged_user",
		Action:     "DELETE",
		CommitTime: "2024-01-01T00:00:00Z",
		DataOld: map[string]interface{}{
			"sfid":        sfid,
			"username__c": username,
		},
	}
	if retry := handleWALDelete(context.Background(), event); retry {
		t.Fatal("WAL handler requested retry")
	}

	entry, err := fake.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("failed to read WAL tombstone: %v", err)
	}
	if retry := kvHandler(entry); retry {
		t.Fatal("KV handler requested retry after successful cleanup event")
	}
	if published != 1 {
		t.Fatalf("published %d cleanup events, want exactly 1", published)
	}
	if publishedUsername != username {
		t.Errorf("published username = %q, want %q from WAL DataOld", publishedUsername, username)
	}
}

func TestHandleWALDelete_MergedUserMissingDataOldUsernameAndDatabaseMiss(t *testing.T) {
	origCfg, origKV := cfg, v1KV
	origLogger := logger
	origPublish := publishUserDeletedEventFn
	origEmail := getPrimaryEmailForUserFn
	origUsername := getDeletedUserUsernameFn
	cfg = &Config{}
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	fake := newFakeKV()
	v1KV = fake
	t.Cleanup(func() {
		cfg = origCfg
		v1KV = origKV
		logger = origLogger
		publishUserDeletedEventFn = origPublish
		getPrimaryEmailForUserFn = origEmail
		getDeletedUserUsernameFn = origUsername
	})

	const (
		sfid = "003xx004"
		key  = "salesforce-merged_user." + sfid
	)
	published := 0
	getDeletedUserUsernameFn = func(_ context.Context, gotSFID string) (string, error) {
		if gotSFID != sfid {
			t.Errorf("database lookup SFID = %q, want %q", gotSFID, sfid)
		}
		return "", nil
	}
	getPrimaryEmailForUserFn = func(context.Context, string) (string, error) {
		return "", nil
	}
	publishUserDeletedEventFn = func(context.Context, string, string, string) error {
		published++
		return nil
	}

	event := &WALEvent{
		Schema:     "salesforce",
		Table:      "merged_user",
		Action:     "DELETE",
		CommitTime: "2024-01-01T00:00:00Z",
		DataOld:    map[string]interface{}{"sfid": sfid},
	}
	if retry := handleWALDelete(context.Background(), event); retry {
		t.Fatal("WAL handler requested retry")
	}

	entry, err := fake.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("failed to read WAL tombstone: %v", err)
	}
	if retry := kvHandler(entry); retry {
		t.Fatal("KV handler should ACK a definitive username miss rather than retry forever")
	}
	if published != 0 {
		t.Fatalf("published %d events without a recovered username, want 0", published)
	}
}

// TestHandleWALDelete_NonFallbackTableSkipsWhenMissing confirms tables not in
// walDeleteFallbackTables keep the original skip-on-missing-key behavior.
func TestHandleWALDelete_NonFallbackTableSkipsWhenMissing(t *testing.T) {
	origCfg, origKV := cfg, v1KV
	cfg = &Config{}
	fake := newFakeKV()
	v1KV = fake
	defer func() {
		cfg = origCfg
		v1KV = origKV
	}()

	event := &WALEvent{
		Schema:     "salesforce",
		Table:      "alternate_email__c",
		Action:     "DELETE",
		CommitTime: "2024-01-01T00:00:00Z",
		DataOld:    map[string]interface{}{"sfid": "003xx000"},
	}

	retry := handleWALDelete(context.Background(), event)
	if retry {
		t.Fatal("expected no retry")
	}
	if len(fake.data) != 0 {
		t.Errorf("expected no KV entry to be created for a non-fallback table, got %v", fake.data)
	}
}

// TestHandleWALDelete_UpdatesExistingEntry confirms the ordinary path
// (existing KV entry found) still updates it in place via the revision-gated
// Update call, unaffected by the new fallback branch.
func TestHandleWALDelete_UpdatesExistingEntry(t *testing.T) {
	origCfg, origKV := cfg, v1KV
	cfg = &Config{}
	fake := newFakeKV()
	v1KV = fake
	defer func() {
		cfg = origCfg
		v1KV = origKV
	}()

	key := "salesforce-user_skills.usk-1"
	existing, _ := json.Marshal(map[string]interface{}{"id": "usk-1", "lfid": "jdoe"})
	fake.data[key] = existing
	fake.rev[key] = 1

	event := &WALEvent{
		Schema:     "salesforce",
		Table:      "user_skills",
		Action:     "DELETE",
		CommitTime: "2024-01-02T00:00:00Z",
		DataOld:    map[string]interface{}{"id": "usk-1"},
	}

	retry := handleWALDelete(context.Background(), event)
	if retry {
		t.Fatal("expected no retry")
	}
	if fake.rev[key] != 2 {
		t.Errorf("expected revision to advance to 2, got %d", fake.rev[key])
	}
	var got map[string]interface{}
	_ = json.Unmarshal(fake.data[key], &got)
	if got["lfid"] != "jdoe" {
		t.Errorf("expected existing lfid to be preserved, got %v", got["lfid"])
	}
}
