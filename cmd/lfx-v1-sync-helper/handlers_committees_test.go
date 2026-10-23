// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

// helper: string pointer.
func sptr(s string) *string { return &s }

// TestCommitteeBasesEqual_BooleanOnlyDifferenceTriggersUpdate verifies that
// committeeBasesEqual detects differences in the boolean fields (EnableVoting,
// SsoGroupEnabled, Public) that the sync manages. Regression: prior to this fix
// the equality function only compared Name/ProjectUID/Category/Description/Website,
// so a V1 record that flipped enable_voting__c or sso_group_enabled would be
// treated as "unchanged" and the update would be silently skipped even though
// the actual purpose of the sync was to propagate that boolean change.
func TestCommitteeBasesEqual_BooleanOnlyDifferenceTriggersUpdate(t *testing.T) {
	tests := []struct {
		name string
		mut  func(b *committeeservice.CommitteeBaseWithReadonlyAttributes)
	}{
		{
			name: "EnableVoting differs",
			mut:  func(b *committeeservice.CommitteeBaseWithReadonlyAttributes) { b.EnableVoting = true },
		},
		{
			name: "SsoGroupEnabled differs",
			mut:  func(b *committeeservice.CommitteeBaseWithReadonlyAttributes) { b.SsoGroupEnabled = true },
		},
		{
			name: "Public differs",
			mut:  func(b *committeeservice.CommitteeBaseWithReadonlyAttributes) { b.Public = true },
		},
		{
			name: "MailingList differs",
			mut: func(b *committeeservice.CommitteeBaseWithReadonlyAttributes) {
				b.MailingList = sptr("list@example.org")
			},
		},
		{
			name: "ChatChannel differs",
			mut:  func(b *committeeservice.CommitteeBaseWithReadonlyAttributes) { b.ChatChannel = sptr("#committee") },
		},
		{
			name: "DisplayName differs",
			mut:  func(b *committeeservice.CommitteeBaseWithReadonlyAttributes) { b.DisplayName = sptr("New Display") },
		},
		{
			name: "ParentUID differs",
			mut:  func(b *committeeservice.CommitteeBaseWithReadonlyAttributes) { b.ParentUID = sptr("parent-uid") },
		},
	}

	baseTemplate := func() *committeeservice.CommitteeBaseWithReadonlyAttributes {
		return &committeeservice.CommitteeBaseWithReadonlyAttributes{
			UID:             sptr("cmt-uid"),
			Name:            sptr("Governing Board"),
			ProjectUID:      sptr("proj-uid"),
			Category:        sptr("Governance"),
			Description:     sptr("desc"),
			Website:         sptr("https://example.org"),
			MailingList:     nil,
			ChatChannel:     nil,
			DisplayName:     nil,
			EnableVoting:    false,
			SsoGroupEnabled: false,
			Public:          false,
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := baseTemplate()
			b := baseTemplate()
			tt.mut(b)

			if committeeBasesEqual(a, b) {
				t.Fatalf("committeeBasesEqual returned true for %s; expected update to be detected", tt.name)
			}
		})
	}
}

// TestCommitteeBasesEqual_IgnoresNonSyncedFields verifies that
// committeeBasesEqual does not report a difference for fields the sync does not
// manage (RequiresReview, JoinMode, Repository, Scope, Deliverables,
// KeyDates, Calendar). Admin edits made directly in V2 to these fields must not
// force redundant sync updates.
func TestCommitteeBasesEqual_IgnoresNonSyncedFields(t *testing.T) {
	a := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		Name:           sptr("A"),
		ProjectUID:     sptr("proj-uid"),
		Category:       sptr("Governance"),
		RequiresReview: false,
		ParentUID:      sptr(""),
		JoinMode:       "open",
	}
	b := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		Name:           sptr("A"),
		ProjectUID:     sptr("proj-uid"),
		Category:       sptr("Governance"),
		RequiresReview: true,                        // differs
		JoinMode:       "closed",                    // differs
		Repository:     sptr("https://git.example"), // differs
	}
	if !committeeBasesEqual(a, b) {
		t.Fatal("committeeBasesEqual reported inequality for non-synced fields; admin V2 edits must not force sync updates")
	}
}

// TestSeedCommitteeUpdateFromCurrentBase_PreservesCurrentValues verifies that
// seedCommitteeUpdateFromCurrentBase copies every sync-managed field from
// currentBase onto the payload. Combined with overlayV1CommitteeUpdatePayload
// (whose _, ok guards no-op when a V1 key is absent), this is what prevents the
// payload's plain-bool zero value from silently overwriting a current V2 `true`.
func TestSeedCommitteeUpdateFromCurrentBase_PreservesCurrentValues(t *testing.T) {
	currentBase := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		Category:        sptr("Governance"),
		Description:     sptr("existing description"),
		Website:         sptr("https://example.org"),
		MailingList:     sptr("list@example.org"),
		ChatChannel:     sptr("#existing"),
		DisplayName:     sptr("Existing Display"),
		EnableVoting:    true,
		SsoGroupEnabled: true,
		Public:          true,
		ParentUID:       sptr("parent-uid"),
	}
	payload := &committeeservice.UpdateCommitteeBasePayload{
		UID:        sptr("cmt-uid"),
		Name:       "Committee",
		ProjectUID: "proj-uid",
	}
	seedCommitteeUpdateFromCurrentBase(payload, currentBase)

	if payload.Category != "Governance" {
		t.Errorf("Category not seeded: got %q", payload.Category)
	}
	if payload.Description == nil || *payload.Description != "existing description" {
		t.Errorf("Description not seeded: got %v", payload.Description)
	}
	if payload.Website == nil || *payload.Website != "https://example.org" {
		t.Errorf("Website not seeded: got %v", payload.Website)
	}
	if payload.MailingList == nil || *payload.MailingList != "list@example.org" {
		t.Errorf("MailingList not seeded: got %v", payload.MailingList)
	}
	if payload.ChatChannel == nil || *payload.ChatChannel != "#existing" {
		t.Errorf("ChatChannel not seeded: got %v", payload.ChatChannel)
	}
	if payload.DisplayName == nil || *payload.DisplayName != "Existing Display" {
		t.Errorf("DisplayName not seeded: got %v", payload.DisplayName)
	}
	if !payload.EnableVoting {
		t.Error("EnableVoting not seeded from currentBase; absent V1 key would clobber current true with zero-value false")
	}
	if !payload.SsoGroupEnabled {
		t.Error("SsoGroupEnabled not seeded from currentBase; absent V1 key would clobber current true with zero-value false")
	}
	if !payload.Public {
		t.Error("Public not seeded from currentBase; absent V1 key would clobber current true with zero-value false")
	}
	if payload.ParentUID == nil || *payload.ParentUID != "parent-uid" {
		t.Errorf("ParentUID not preserved from current base: %v", payload.ParentUID)
	}
}

// TestSeedCommitteeUpdateFromCurrentBase_NilCurrentBaseIsNoOp defends against
// panics if the caller ever passes a nil currentBase (which should not happen in
// production but keeps unit tests trivial).
func TestSeedCommitteeUpdateFromCurrentBase_NilCurrentBaseIsNoOp(t *testing.T) {
	payload := &committeeservice.UpdateCommitteeBasePayload{Name: "X", ProjectUID: "Y"}
	seedCommitteeUpdateFromCurrentBase(payload, nil)
	// No fields touched; still valid struct.
	if payload.Name != "X" || payload.ProjectUID != "Y" {
		t.Error("seed with nil currentBase mutated required fields")
	}
	seedCommitteeUpdateFromCurrentBase(nil, nil) // must not panic
}

// TestOverlayV1CommitteeUpdatePayload_AbsentKeysPreserveSeed verifies that when
// a V1 payload does not include a given key, the pre-seeded value on the payload
// is preserved. This is the core regression: prior to this fix the "unrelated
// field change triggers an update that clobbers current bools" scenario was
// unprotected because the payload started as a zero-valued struct.
func TestOverlayV1CommitteeUpdatePayload_AbsentKeysPreserveSeed(t *testing.T) {
	// Seed with all current V2 values set to true / non-empty.
	payload := &committeeservice.UpdateCommitteeBasePayload{
		UID:             sptr("cmt-uid"),
		Name:            "Committee",
		ProjectUID:      "proj-uid",
		Description:     sptr("current desc"),
		EnableVoting:    true,
		SsoGroupEnabled: true,
		Public:          true,
		MailingList:     sptr("current@list.org"),
		ChatChannel:     sptr("#current"),
		DisplayName:     sptr("Current Display"),
	}

	// V1 data changes ONLY the description. All other keys are absent.
	v1Data := map[string]any{
		"description__c": "new desc from V1",
	}

	overlayV1CommitteeUpdatePayload(context.Background(), payload, v1Data, "Committee")

	if payload.Description == nil || *payload.Description != "new desc from V1" {
		t.Errorf("Description not overlaid from V1: got %v", payload.Description)
	}
	if !payload.EnableVoting {
		t.Error("EnableVoting was clobbered by absent V1 key; must be preserved from seed")
	}
	if !payload.SsoGroupEnabled {
		t.Error("SsoGroupEnabled was clobbered by absent V1 key; must be preserved from seed")
	}
	if !payload.Public {
		t.Error("Public was clobbered by absent V1 key; must be preserved from seed")
	}
	if payload.MailingList == nil || *payload.MailingList != "current@list.org" {
		t.Errorf("MailingList clobbered by absent V1 key: got %v", payload.MailingList)
	}
	if payload.ChatChannel == nil || *payload.ChatChannel != "#current" {
		t.Errorf("ChatChannel clobbered by absent V1 key: got %v", payload.ChatChannel)
	}
	if payload.DisplayName == nil || *payload.DisplayName != "Current Display" {
		t.Errorf("DisplayName clobbered by absent V1 key: got %v", payload.DisplayName)
	}
}

// TestOverlayV1CommitteeUpdatePayload_BooleanOverridesSeed verifies that when V1
// data DOES include a boolean key, the V1 value overrides the seed, including
// V1-says-false overriding a seeded true. This is the intended direction of
// truth: V1 wins when present.
func TestOverlayV1CommitteeUpdatePayload_BooleanOverridesSeed(t *testing.T) {
	tests := []struct {
		name             string
		v1Data           map[string]any
		seedEnableVoting bool
		seedSsoEnabled   bool
		seedPublic       bool
		wantEnableVoting bool
		wantSsoEnabled   bool
		wantPublic       bool
	}{
		{
			name:             "V1 true overrides seed false",
			v1Data:           map[string]any{"enable_voting__c": true, "sso_group_enabled": true, "public_enabled": true},
			seedEnableVoting: false,
			seedSsoEnabled:   false,
			seedPublic:       false,
			wantEnableVoting: true,
			wantSsoEnabled:   true,
			wantPublic:       true,
		},
		{
			name:             "V1 false overrides seed true",
			v1Data:           map[string]any{"enable_voting__c": false, "sso_group_enabled": false, "public_enabled": false},
			seedEnableVoting: true,
			seedSsoEnabled:   true,
			seedPublic:       true,
			wantEnableVoting: false,
			wantSsoEnabled:   false,
			wantPublic:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := &committeeservice.UpdateCommitteeBasePayload{
				Name:            "Committee",
				ProjectUID:      "proj-uid",
				EnableVoting:    tt.seedEnableVoting,
				SsoGroupEnabled: tt.seedSsoEnabled,
				Public:          tt.seedPublic,
			}
			overlayV1CommitteeUpdatePayload(context.Background(), payload, tt.v1Data, "Committee")
			if payload.EnableVoting != tt.wantEnableVoting {
				t.Errorf("EnableVoting: got %v want %v", payload.EnableVoting, tt.wantEnableVoting)
			}
			if payload.SsoGroupEnabled != tt.wantSsoEnabled {
				t.Errorf("SsoGroupEnabled: got %v want %v", payload.SsoGroupEnabled, tt.wantSsoEnabled)
			}
			if payload.Public != tt.wantPublic {
				t.Errorf("Public: got %v want %v", payload.Public, tt.wantPublic)
			}
		})
	}
}

// TestBooleanOnlyChangeIsDetectedEndToEnd exercises the seed → overlay →
// committeeBasesEqual pipeline that updateCommittee uses to decide whether to
// call the API. It confirms that a V1 record whose only difference from current
// V2 state is a single boolean flip is correctly detected as a change (regression
// against PR #143's incomplete fix, which left the change-detection gate blind
// to boolean-only differences).
func TestBooleanOnlyChangeIsDetectedEndToEnd(t *testing.T) {
	currentBase := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		UID:             sptr("cmt-uid"),
		Name:            sptr("Committee"),
		ProjectUID:      sptr("proj-uid"),
		Category:        sptr("Governance"),
		Description:     sptr("desc"),
		Website:         sptr("https://example.org"),
		EnableVoting:    false,
		SsoGroupEnabled: false,
		Public:          false,
	}
	payload := &committeeservice.UpdateCommitteeBasePayload{
		UID:        sptr("cmt-uid"),
		Name:       "Committee",
		ProjectUID: "proj-uid",
	}
	seedCommitteeUpdateFromCurrentBase(payload, currentBase)

	// V1 flips enable_voting__c and sso_group_enabled to true; nothing else changes.
	v1Data := map[string]any{
		"enable_voting__c":  true,
		"sso_group_enabled": true,
	}
	overlayV1CommitteeUpdatePayload(context.Background(), payload, v1Data, "Committee")

	updated := committeeBaseFromUpdatePayload(currentBase.UID, payload)
	if committeeBasesEqual(currentBase, updated) {
		t.Fatal("boolean-only change not detected as an update; committeeBasesEqual regression")
	}
	if !updated.EnableVoting || !updated.SsoGroupEnabled {
		t.Errorf("updated base did not carry the flipped V1 booleans: EnableVoting=%v SsoGroupEnabled=%v",
			updated.EnableVoting, updated.SsoGroupEnabled)
	}
}

// TestUnrelatedFieldChangeDoesNotClobberCurrentBooleans is the second scenario
// Copilot flagged: V1 data changes an unrelated field (description) and does NOT
// include the boolean keys. Prior to this fix the payload's zero-value `false`
// bools would be sent, silently overwriting current V2 `true`s. With the
// currentBase seed in place, the update is still sent (description differs) but
// the current bool values ride along unchanged.
func TestUnrelatedFieldChangeDoesNotClobberCurrentBooleans(t *testing.T) {
	currentBase := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		UID:             sptr("cmt-uid"),
		Name:            sptr("Committee"),
		ProjectUID:      sptr("proj-uid"),
		Category:        sptr("Governance"),
		Description:     sptr("old desc"),
		Website:         sptr("https://example.org"),
		EnableVoting:    true,
		SsoGroupEnabled: true,
		Public:          true,
	}
	payload := &committeeservice.UpdateCommitteeBasePayload{
		UID:        sptr("cmt-uid"),
		Name:       "Committee",
		ProjectUID: "proj-uid",
	}
	seedCommitteeUpdateFromCurrentBase(payload, currentBase)

	// V1 only carries description__c; enable_voting__c and sso_group_enabled are absent.
	v1Data := map[string]any{
		"description__c": "new desc",
	}
	overlayV1CommitteeUpdatePayload(context.Background(), payload, v1Data, "Committee")

	updated := committeeBaseFromUpdatePayload(currentBase.UID, payload)
	if committeeBasesEqual(currentBase, updated) {
		t.Fatal("description change not detected; committeeBasesEqual regression")
	}
	if !updated.EnableVoting {
		t.Error("EnableVoting was clobbered to false by absent V1 key")
	}
	if !updated.SsoGroupEnabled {
		t.Error("SsoGroupEnabled was clobbered to false by absent V1 key")
	}
	if !updated.Public {
		t.Error("Public was clobbered to false by absent V1 key")
	}
	if updated.Description == nil || *updated.Description != "new desc" {
		t.Errorf("Description not applied from V1: got %v", updated.Description)
	}
}

// TestNoChangeSkipsUpdate confirms that when V1 data reflects the current V2
// state exactly (or is a strict subset with no differences), committeeBasesEqual
// reports equality and updateCommittee will skip the API call — no wasted writes.
func TestNoChangeSkipsUpdate(t *testing.T) {
	currentBase := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		UID:             sptr("cmt-uid"),
		Name:            sptr("Committee"),
		ProjectUID:      sptr("proj-uid"),
		Category:        sptr("Governance"),
		Description:     sptr("desc"),
		Website:         sptr("https://example.org"),
		EnableVoting:    true,
		SsoGroupEnabled: true,
		Public:          false,
	}
	payload := &committeeservice.UpdateCommitteeBasePayload{
		UID:        sptr("cmt-uid"),
		Name:       "Committee",
		ProjectUID: "proj-uid",
	}
	seedCommitteeUpdateFromCurrentBase(payload, currentBase)
	// V1 restates the exact same values.
	v1Data := map[string]any{
		"description__c":    "desc",
		"enable_voting__c":  true,
		"sso_group_enabled": true,
		"public_enabled":    false,
	}
	overlayV1CommitteeUpdatePayload(context.Background(), payload, v1Data, "Committee")

	updated := committeeBaseFromUpdatePayload(currentBase.UID, payload)
	if !committeeBasesEqual(currentBase, updated) {
		t.Fatal("identical state reported as changed; would issue wasted UpdateCommitteeBase call")
	}
}

// TestMapTypeToCategory pins the v1 type__c -> v2 category mapping, including
// the Newsletter category and the Technical Oversight/Advisory special case.
func TestMapTypeToCategory(t *testing.T) {
	// Swap in a discard logger for the duration of the test: mapTypeToCategory
	// logs a warning on its fallback path, and the package-level logger is only
	// initialized by main().
	origLogger := logger
	defer func() { logger = origLogger }()
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name          string
		typeVal       string
		committeeName string
		want          *string
	}{
		{
			name:          "Newsletter passes through",
			typeVal:       "Newsletter",
			committeeName: "Weekly Newsletter",
			want:          sptr("Newsletter"),
		},
		{
			name:          "allowlisted control value passes through",
			typeVal:       "Working Group",
			committeeName: "Working Group",
			want:          sptr("Working Group"),
		},
		{
			name:          "empty type returns nil",
			typeVal:       "",
			committeeName: "anything",
			want:          nil,
		},
		{
			name:          "combined TOC/TAC value with advisory in name maps to TAC",
			typeVal:       "Technical Oversight Committee/Technical Advisory Committee",
			committeeName: "Foo Advisory Board",
			want:          sptr("Technical Advisory Committee"),
		},
		{
			name:          "combined TOC/TAC value with tac in name maps to TAC",
			typeVal:       "Technical Oversight Committee/Technical Advisory Committee",
			committeeName: "Project tac lowercase",
			want:          sptr("Technical Advisory Committee"),
		},
		{
			name:          "combined TOC/TAC value otherwise maps to TOC",
			typeVal:       "Technical Oversight Committee/Technical Advisory Committee",
			committeeName: "Governing Council",
			want:          sptr("Technical Oversight Committee"),
		},
		{
			name:          "unrecognized value falls back to Other",
			typeVal:       "Bogus Category",
			committeeName: "x",
			want:          sptr("Other"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapTypeToCategory(context.Background(), tt.typeVal, tt.committeeName)
			if (got == nil) != (tt.want == nil) {
				t.Fatalf("mapTypeToCategory() = %v, want %v", got, tt.want)
			}
			if got != nil && *got != *tt.want {
				t.Fatalf("mapTypeToCategory() = %q, want %q", *got, *tt.want)
			}
		})
	}
}

// TestNewsletterCategoryRoundTrip pins that a v1 Newsletter committee survives
// a v1 -> v2 -> v1 round trip unchanged, in contrast to the lossy-but-intentional
// Technical Oversight/Advisory collapse and the absorbing Other fallback.
func TestNewsletterCategoryRoundTrip(t *testing.T) {
	origLogger := logger
	defer func() { logger = origLogger }()
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name          string
		typeVal       string
		committeeName string
		want          string
	}{
		{
			name:          "Newsletter is the identity",
			typeVal:       "Newsletter",
			committeeName: "Weekly Newsletter",
			want:          "Newsletter",
		},
		{
			name:          "combined TOC/TAC value round-trips to itself",
			typeVal:       "Technical Oversight Committee/Technical Advisory Committee",
			committeeName: "Governing Council",
			want:          "Technical Oversight Committee/Technical Advisory Committee",
		},
		{
			name:          "unrecognized value is absorbed into Other",
			typeVal:       "Bogus Category",
			committeeName: "x",
			want:          "Other",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			category := mapTypeToCategory(context.Background(), tt.typeVal, tt.committeeName)
			if category == nil {
				t.Fatal("mapTypeToCategory() = nil, want non-nil")
			}
			got := mapV2CategoryToV1(*category)
			if got != tt.want {
				t.Fatalf("mapV2CategoryToV1(mapTypeToCategory(%q)) = %q, want %q", tt.typeVal, got, tt.want)
			}
		})
	}
}

// TestMapV1DataToCommitteeMemberVotingNone verifies that a v1 voting_status__c of
// "None" (any casing) or "" maps to a nil v2 Voting in both member mappers, since
// v2 rejects "None" on voting-enabled committees (lfx-self-serve#4079).
func TestMapV1DataToCommitteeMemberVotingNone(t *testing.T) {
	origLogger := logger
	defer func() { logger = origLogger }()
	logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	tests := []struct {
		votingStatus string
		wantStatus   string // "" means a nil Voting is expected
	}{
		{"None", ""},
		{"none", ""},
		{"", ""},
		{"Voting Rep", "Voting Rep"},
		{"Observer", "Observer"},
	}
	for _, tt := range tests {
		t.Run(tt.votingStatus, func(t *testing.T) {
			v1Data := map[string]any{
				"contactemail__c":  "member@example.com",
				"voting_status__c": tt.votingStatus,
			}

			create, err := mapV1DataToCommitteeMemberCreatePayload(ctx, "committee-uid", v1Data)
			if err != nil {
				t.Fatalf("create mapper: %v", err)
			}
			update, err := mapV1DataToCommitteeMemberUpdatePayload(ctx, "committee-uid", "member-uid", v1Data)
			if err != nil {
				t.Fatalf("update mapper: %v", err)
			}

			if tt.wantStatus == "" {
				if create.Voting != nil {
					t.Errorf("create: expected nil Voting, got %+v", create.Voting)
				}
				if update.Voting != nil {
					t.Errorf("update: expected nil Voting, got %+v", update.Voting)
				}
				return
			}
			if create.Voting == nil || create.Voting.Status != tt.wantStatus {
				t.Errorf("create: expected Voting.Status %q, got %+v", tt.wantStatus, create.Voting)
			}
			if update.Voting == nil || update.Voting.Status != tt.wantStatus {
				t.Errorf("update: expected Voting.Status %q, got %+v", tt.wantStatus, update.Voting)
			}
		})
	}
}

// TestCommitteeMembersEqualVotingNone verifies that committeeMembersEqual treats
// a nil Voting, an empty status and "None" as the same "no voting status" value,
// while a real status against nil is still a change.
func TestCommitteeMembersEqualVotingNone(t *testing.T) {
	type voting = struct {
		Status    string
		StartDate *string
		EndDate   *string
	}
	email := "member@example.com"
	current := func(v *voting) *committeeservice.CommitteeMemberFullWithReadonlyAttributes {
		return &committeeservice.CommitteeMemberFullWithReadonlyAttributes{Email: &email, Voting: v}
	}
	update := func(v *voting) *committeeservice.UpdateCommitteeMemberPayload {
		return &committeeservice.UpdateCommitteeMemberPayload{Email: email, Voting: v}
	}

	tests := []struct {
		name      string
		current   *voting
		update    *voting
		wantEqual bool
	}{
		{"nil vs nil", nil, nil, true},
		{"legacy None vs nil", &voting{Status: "None"}, nil, true},
		{"nil vs empty status", nil, &voting{Status: ""}, true},
		{"none casing vs None", &voting{Status: "none"}, &voting{Status: "None"}, true},
		{"Voting Rep vs nil", &voting{Status: "Voting Rep"}, nil, false},
		{"nil vs Voting Rep", nil, &voting{Status: "Voting Rep"}, false},
		{"Voting Rep vs Observer", &voting{Status: "Voting Rep"}, &voting{Status: "Observer"}, false},
		{"same status, different start date", &voting{Status: "Voting Rep", StartDate: sptr("2026-01-01")}, &voting{Status: "Voting Rep", StartDate: sptr("2026-02-01")}, false},
		{"same status and dates", &voting{Status: "Voting Rep", StartDate: sptr("2026-01-01")}, &voting{Status: "Voting Rep", StartDate: sptr("2026-01-01")}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := committeeMembersEqual(current(tt.current), update(tt.update)); got != tt.wantEqual {
				t.Errorf("committeeMembersEqual = %v, want %v", got, tt.wantEqual)
			}
		})
	}
}

// TestPreserveUnsetVotingStatus verifies that the v1NoVotingStatus written to
// v1 for a v2 member with no voting status is not synced back into v2 as a
// real status when an unrelated v1 field changes (lfx-self-serve#4079).
func TestPreserveUnsetVotingStatus(t *testing.T) {
	type voting = struct {
		Status    string
		StartDate *string
		EndDate   *string
	}
	email := "member@example.com"

	tests := []struct {
		name       string
		current    *voting
		update     *voting
		wantStatus string // "" means update.Voting is nil afterwards
		wantEqual  bool
	}{
		{"nil vs Observer", nil, &voting{Status: "Observer"}, "", true},
		{"empty vs observer casing", &voting{Status: ""}, &voting{Status: " observer "}, "", true},
		{"legacy None vs Observer", &voting{Status: "None"}, &voting{Status: "Observer"}, "", true},
		{"Voting Rep vs Observer is a change", &voting{Status: "Voting Rep"}, &voting{Status: "Observer"}, "Observer", false},
		{"nil vs Voting Rep is a change", nil, &voting{Status: "Voting Rep"}, "Voting Rep", false},
		{"Observer vs Observer is untouched", &voting{Status: "Observer"}, &voting{Status: "Observer"}, "Observer", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := &committeeservice.CommitteeMemberFullWithReadonlyAttributes{Email: &email, Voting: tt.current}
			update := &committeeservice.UpdateCommitteeMemberPayload{Email: email, Voting: tt.update}
			preserveUnsetVotingStatus(current, update)

			gotStatus := ""
			if update.Voting != nil {
				gotStatus = update.Voting.Status
			}
			if gotStatus != tt.wantStatus {
				t.Errorf("update voting status = %q, want %q", gotStatus, tt.wantStatus)
			}
			if got := committeeMembersEqual(current, update); got != tt.wantEqual {
				t.Errorf("committeeMembersEqual = %v, want %v", got, tt.wantEqual)
			}
		})
	}
}
