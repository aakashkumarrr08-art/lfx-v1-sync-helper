// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"encoding/json"
	"reflect"
	"testing"
)

func assertUpdateJSON(t *testing.T, payload any, want map[string]any) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PATCH body = %s, want %#v", body, want)
	}
}

func TestProjectUpdateClearVsOmit(t *testing.T) {
	for _, tt := range []struct {
		name string
		data map[string]any
		want map[string]any
	}{
		{"absent fields", map[string]any{}, map[string]any{}},
		{"empty clears", map[string]any{
			"description": "", "charter_url": "", "funding_model": []any{},
			"parent_uid": "", "legal_parent_uid": "", "formation_date": "",
			"autojoin_enabled": false,
		}, map[string]any{
			"Description": "", "CharterURL": "", "Model": []any{},
			"Parent": "", "LegalParentID": "", "StartDate": "",
			"AutoJoinEnabled": false,
		}},
		{"typed empty slice", map[string]any{"funding_model": []string{}}, map[string]any{"Model": []any{}}},
		{"whitespace parent clears", map[string]any{
			"parent_uid": "  \t", "legal_parent_uid": "\n  ",
		}, map[string]any{"Parent": "", "LegalParentID": ""}},
		{"null nullable fields", map[string]any{
			"funding_model": nil, "formation_date": nil, "entity_dissolution_date": nil,
		}, map[string]any{"Model": []any{}, "StartDate": "", "ProjectEntityDissolutionDate": ""}},
		{"null and malformed omitted", map[string]any{"description": nil, "funding_model": "wrong"}, map[string]any{}},
		{"malformed list omitted", map[string]any{"funding_model": []any{42}}, map[string]any{}},
		{"empty slug cannot clear identifier", map[string]any{"slug": "", "description": ""}, map[string]any{"Description": ""}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := mapV2DataToV1ProjectUpdatePayload(t.Context(), tt.data)
			if err != nil {
				t.Fatal(err)
			}
			assertUpdateJSON(t, payload, tt.want)
		})
	}
}

func TestCommitteeUpdateClearVsOmit(t *testing.T) {
	for _, tt := range []struct {
		name string
		data map[string]any
		want map[string]any
	}{
		{"absent fields", map[string]any{}, map[string]any{}},
		{"empty clears", map[string]any{
			"description": "", "website": "", "join_mode": "",
			"mailing_list_email": "", "chat_channel": "", "sso_group_name": "",
		}, map[string]any{
			"Description": "", "CommitteeWebsite": "", "JoinMode": "",
			"MailingList": "", "ChatChannel": "",
		}},
		{"non-empty values", map[string]any{"description": "new", "category": "Newsletter"}, map[string]any{"Description": "new", "Category": "Newsletter"}},
		{"generated SSO name passes through", map[string]any{"sso_group_name": "project-committee"}, map[string]any{"SSOGroupName": "project-committee"}},
		{"v2 mailing list key", map[string]any{"mailing_list": "list@example.invalid"}, map[string]any{"MailingList": "list@example.invalid"}},
		{"null omitted", map[string]any{"description": nil}, map[string]any{}},
		{"partial payload does not infer clears", map[string]any{
			"uid": "committee-test-uid", "project_uid": "project-test-uid",
			"name": "Committee", "category": "Other",
		}, map[string]any{"Name": "Committee", "Category": "Other"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assertUpdateJSON(t, mapV2DataToV1CommitteeUpdatePayload(tt.data), tt.want)
		})
	}
}

func TestCommitteeUpdateClearsFromSerializedSnapshot(t *testing.T) {
	// Match CommitteeBase's JSON tags in committee-service v0.4.26. Build()
	// marshals the entire resource before publishing the indexer update; its
	// omitempty fields disappear on clear rather than arriving as empty strings.
	type committeeSnapshot struct {
		UID              string  `json:"uid"`
		ProjectUID       string  `json:"project_uid"`
		Name             string  `json:"name"`
		Category         string  `json:"category"`
		CreatedAt        string  `json:"created_at"`
		UpdatedAt        string  `json:"updated_at"`
		EnableVoting     bool    `json:"enable_voting"`
		SSOGroupEnabled  bool    `json:"sso_group_enabled"`
		RequiresReview   bool    `json:"requires_review"`
		Public           bool    `json:"public"`
		TotalMembers     int     `json:"total_members"`
		TotalVotingRepos int     `json:"total_voting_repos"`
		HasMailingList   bool    `json:"has_mailing_list"`
		Description      string  `json:"description,omitempty"`
		Website          *string `json:"website,omitempty"`
		MailingList      *string `json:"mailing_list,omitempty"`
		ChatChannel      *string `json:"chat_channel,omitempty"`
		JoinMode         string  `json:"join_mode,omitempty"`
		SSOGroupName     string  `json:"sso_group_name,omitempty"`
	}
	base := committeeSnapshot{
		UID: "committee-test-uid", ProjectUID: "project-test-uid", Name: "Committee", Category: "Other",
		CreatedAt: "2026-01-01T00:00:00Z", UpdatedAt: "2026-01-02T00:00:00Z",
	}
	for _, tt := range []struct {
		name string
		data committeeSnapshot
		want map[string]any
	}{
		{"omitted cleared fields", base, map[string]any{
			"Name": "Committee", "Category": "Other", "Description": "", "CommitteeWebsite": "",
			"MailingList": "", "ChatChannel": "", "JoinMode": "",
		}},
		{"populated mailing list", func() committeeSnapshot {
			value := base
			list := "list@example.invalid"
			value.MailingList = &list
			return value
		}(), map[string]any{
			"Name": "Committee", "Category": "Other", "Description": "", "CommitteeWebsite": "",
			"MailingList": "list@example.invalid", "ChatChannel": "", "JoinMode": "",
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body, err := json.Marshal(struct {
				Data committeeSnapshot `json:"data"`
			}{Data: tt.data})
			if err != nil {
				t.Fatal(err)
			}
			var event indexingEventBody
			if err := json.Unmarshal(body, &event); err != nil {
				t.Fatal(err)
			}
			if _, ok := event.Data["description"]; ok {
				t.Fatal("empty description should be omitted by committee-service serialization")
			}
			assertUpdateJSON(t, mapV2DataToV1CommitteeUpdatePayload(event.Data), tt.want)
			for _, missing := range []string{"updated_at", "sso_group_enabled", "total_members", "total_voting_repos", "has_mailing_list"} {
				trimmed := make(map[string]any, len(event.Data))
				for key, value := range event.Data {
					if key != missing {
						trimmed[key] = value
					}
				}
				// A near-complete but truncated event must not clear absent fields.
				want := map[string]any{"Name": "Committee", "Category": "Other"}
				if list, ok := trimmed["mailing_list"].(string); ok {
					want["MailingList"] = list
				}
				assertUpdateJSON(t, mapV2DataToV1CommitteeUpdatePayload(trimmed), want)
			}
		})
	}
}

func TestResolveOrgIDFromEventData_keepsSFID(t *testing.T) {
	ctx := t.Context()
	sfid := "0014100000Te2ovAAB"
	data := map[string]any{
		"organization": map[string]any{
			"id":   sfid,
			"name": "The Linux Foundation",
		},
	}
	orgID, err := resolveOrgIDFromEventData(ctx, data)
	if err != nil {
		t.Fatalf("resolveOrgIDFromEventData() error = %v", err)
	}
	if orgID != sfid {
		t.Fatalf("resolveOrgIDFromEventData() = %q, want %q", orgID, sfid)
	}
}

func TestResolveOrgIDFromEventData_ignoresNonSFID(t *testing.T) {
	// CDP UUIDs and other non-SFID values must be dropped so they are never
	// forwarded to v1 as OrganizationID. With no name or website, resolveV1OrgID
	// returns "", nil immediately — no network call is made.
	tests := []struct {
		name  string
		orgID string
	}{
		{"CDP UUID (hyphenated)", "51fde723-67df-4e0e-91c6-936d01d59559"},
		{"CDP hex digest", "4340abc06f4e11f1944c4bb16c3aa46c"},
		{"arbitrary non-SFID", "org-not-an-sfid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			data := map[string]any{
				"organization": map[string]any{
					"id": tt.orgID,
				},
			}
			orgID, err := resolveOrgIDFromEventData(ctx, data)
			if err != nil {
				t.Fatalf("resolveOrgIDFromEventData() error = %v", err)
			}
			if orgID != "" {
				t.Fatalf("resolveOrgIDFromEventData() = %q, want empty string", orgID)
			}
		})
	}
}

// TestMapV2CategoryToV1 pins the v2 category -> v1 type__c mapping, including
// the Newsletter category and the Technical Oversight/Advisory collapse.
func TestMapV2CategoryToV1(t *testing.T) {
	tests := []struct {
		name     string
		category string
		want     string
	}{
		{"Newsletter passes through", "Newsletter", "Newsletter"},
		{"Technical Oversight Committee collapses to combined value", "Technical Oversight Committee", "Technical Oversight Committee/Technical Advisory Committee"},
		{"Technical Advisory Committee collapses to combined value", "Technical Advisory Committee", "Technical Oversight Committee/Technical Advisory Committee"},
		{"allowlisted control value passes through", "Other", "Other"},
		{"empty value falls back to Other", "", "Other"},
		{"unrecognized value falls back to Other", "Not A Category", "Other"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapV2CategoryToV1(tt.category)
			if got != tt.want {
				t.Fatalf("mapV2CategoryToV1(%q) = %q, want %q", tt.category, got, tt.want)
			}
		})
	}
}

// TestMapV2CategoryToV1_CoversAllAllowedCategories guards against the two
// hand-maintained category lists drifting apart again: every v1-allowed
// category (other than "Other" itself) must pass through mapV2CategoryToV1
// unchanged or via the known Technical Oversight/Advisory collapse, rather
// than silently falling back to "Other".
func TestMapV2CategoryToV1_CoversAllAllowedCategories(t *testing.T) {
	for category := range allowedCommitteeCategories {
		if category == "Other" {
			continue
		}
		got := mapV2CategoryToV1(category)
		if got == "Other" {
			t.Errorf("mapV2CategoryToV1(%q) = %q; category is allowed on the v1->v2 path but falls back to Other on v2->v1 - add it to mapV2CategoryToV1", category, got)
		}
	}
}

// TestExtractStringSlice pins the behavior for the three JSON-decoded shapes we
// realistically see for funding_model and similar array fields: nil, []any (from
// encoding/json into map[string]any), and []string (from a strongly typed source).
func TestExtractStringSlice(t *testing.T) {
	tests := []struct {
		name   string
		in     any
		want   []string
		wantOK bool
	}{
		{name: "nil returns not-ok", in: nil, want: nil, wantOK: false},
		{name: "empty []any returns not-ok", in: []any{}, want: nil, wantOK: false},
		{name: "empty []string returns not-ok", in: []string{}, want: nil, wantOK: false},
		{name: "[]any of strings passes through", in: []any{"Membership", "Grants"}, want: []string{"Membership", "Grants"}, wantOK: true},
		{name: "[]string passes through", in: []string{"Membership"}, want: []string{"Membership"}, wantOK: true},
		{name: "[]any with non-string items drops them", in: []any{"Membership", 42, nil, "Grants"}, want: []string{"Membership", "Grants"}, wantOK: true},
		{name: "[]any with only non-string items returns not-ok", in: []any{42, nil, true}, want: nil, wantOK: false},
		{name: "unsupported type returns not-ok", in: "not-a-slice", want: nil, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := extractStringSlice(tt.in)
			if ok != tt.wantOK {
				t.Fatalf("extractStringSlice(%#v) ok = %v, want %v", tt.in, ok, tt.wantOK)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("extractStringSlice(%#v) = %#v, want %#v", tt.in, got, tt.want)
			}
		})
	}
}

// TestMapV2DataToV1ProjectCreatePayload_HappyPath verifies that every v2
// ProjectBase field the mapper knows about lands on the correct v1 field, that
// dates are date-only, that the funding_model slice is preserved, that
// is_foundation switches ProjectType, and that no parent SFID resolution
// (which requires a live mappingsKV) is triggered when parent_uid /
// legal_parent_uid are absent.
func TestMapV2DataToV1ProjectCreatePayload_HappyPath(t *testing.T) {
	ctx := t.Context()
	autojoinTrue := true
	data := map[string]any{
		"name":                          "Prospect Foundation",
		"slug":                          "prospect-foundation",
		"description":                   "A prospect foundation for formation intake.",
		"stage":                         "Prospect",
		"category":                      "Sandbox",
		"legal_entity_type":             "Series LLC",
		"legal_entity_name":             "Prospect Foundation, LLC",
		"funding":                       "Membership",
		"funding_model":                 []any{"Membership", "Grants"},
		"charter_url":                   "https://example.org/charter.pdf",
		"autojoin_enabled":              true,
		"formation_date":                "2026-01-15T00:00:00Z",
		"logo_url":                      "https://example.org/logo.png",
		"repository_url":                "https://github.com/prospect/foundation",
		"website_url":                   "https://example.org",
		"entity_dissolution_date":       "2036-01-15T00:00:00Z",
		"entity_formation_document_url": "https://example.org/formation.pdf",
		"is_foundation":                 true,
	}

	got, err := mapV2DataToV1ProjectCreatePayload(ctx, data)
	if err != nil {
		t.Fatalf("mapV2DataToV1ProjectCreatePayload() error = %v", err)
	}

	if got.Name != "Prospect Foundation" {
		t.Errorf("Name = %q, want %q", got.Name, "Prospect Foundation")
	}
	if got.ProjectType != "Project Group" {
		t.Errorf("ProjectType = %q, want %q (is_foundation should map to Project Group)", got.ProjectType, "Project Group")
	}
	if got.Slug != "prospect-foundation" {
		t.Errorf("Slug = %q", got.Slug)
	}
	if got.Description != "A prospect foundation for formation intake." {
		t.Errorf("Description = %q", got.Description)
	}
	if got.Status != "Prospect" {
		t.Errorf("Status = %q, want %q (v2 stage → v1 Status)", got.Status, "Prospect")
	}
	if got.Category != "Sandbox" {
		t.Errorf("Category = %q", got.Category)
	}
	if got.EntityType != "Series LLC" {
		t.Errorf("EntityType = %q (v2 legal_entity_type → v1 EntityType)", got.EntityType)
	}
	if got.EntityName != "Prospect Foundation, LLC" {
		t.Errorf("EntityName = %q", got.EntityName)
	}
	if got.Funding != "Membership" {
		t.Errorf("Funding = %q", got.Funding)
	}
	if !reflect.DeepEqual(got.Model, []string{"Membership", "Grants"}) {
		t.Errorf("Model = %#v (v2 funding_model → v1 Model)", got.Model)
	}
	if got.CharterURL != "https://example.org/charter.pdf" {
		t.Errorf("CharterURL = %q", got.CharterURL)
	}
	if got.AutoJoinEnabled == nil || *got.AutoJoinEnabled != autojoinTrue {
		t.Errorf("AutoJoinEnabled = %v, want *true", got.AutoJoinEnabled)
	}
	if got.StartDate != "2026-01-15" {
		t.Errorf("StartDate = %q, want %q (must be date-only)", got.StartDate, "2026-01-15")
	}
	if got.ProjectLogo != "https://example.org/logo.png" {
		t.Errorf("ProjectLogo = %q", got.ProjectLogo)
	}
	if got.RepositoryURL != "https://github.com/prospect/foundation" {
		t.Errorf("RepositoryURL = %q", got.RepositoryURL)
	}
	if got.Website != "https://example.org" {
		t.Errorf("Website = %q", got.Website)
	}
	if got.ProjectEntityDissolutionDate != "2036-01-15" {
		t.Errorf("ProjectEntityDissolutionDate = %q, want %q (must be date-only)", got.ProjectEntityDissolutionDate, "2036-01-15")
	}
	if got.ProjectEntityFormationDocument != "https://example.org/formation.pdf" {
		t.Errorf("ProjectEntityFormationDocument = %q", got.ProjectEntityFormationDocument)
	}
	if got.Parent != "" {
		t.Errorf("Parent = %q, want empty (no parent_uid in input)", got.Parent)
	}
	if got.LegalParentID != "" {
		t.Errorf("LegalParentID = %q, want empty (no legal_parent_uid in input)", got.LegalParentID)
	}
}

// TestMapV2DataToV1ProjectCreatePayload_DefaultsProjectType verifies that when
// is_foundation is unset or false, ProjectType defaults to "Project".
func TestMapV2DataToV1ProjectCreatePayload_DefaultsProjectType(t *testing.T) {
	ctx := t.Context()
	for _, tt := range []struct {
		name string
		data map[string]any
	}{
		{"is_foundation absent", map[string]any{"name": "Regular Project"}},
		{"is_foundation=false", map[string]any{"name": "Regular Project", "is_foundation": false}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mapV2DataToV1ProjectCreatePayload(ctx, tt.data)
			if err != nil {
				t.Fatalf("mapV2DataToV1ProjectCreatePayload() error = %v", err)
			}
			if got.ProjectType != "Project" {
				t.Fatalf("ProjectType = %q, want %q", got.ProjectType, "Project")
			}
		})
	}
}

// TestMapV2DataToV1ProjectCreatePayload_RequiresName pins the guard that rejects
// events with no name — the v1 API requires Name on POST /v1/projects.
func TestMapV2DataToV1ProjectCreatePayload_RequiresName(t *testing.T) {
	ctx := t.Context()
	for _, tt := range []struct {
		name string
		data map[string]any
	}{
		{"name absent", map[string]any{"slug": "orphan"}},
		{"name empty", map[string]any{"name": ""}},
		{"name whitespace", map[string]any{"name": "   "}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := mapV2DataToV1ProjectCreatePayload(ctx, tt.data)
			if err == nil {
				t.Fatalf("mapV2DataToV1ProjectCreatePayload() should have errored on missing name")
			}
		})
	}
}

// TestMapV2DataToV1ProjectUpdatePayload_HappyPath verifies field mapping for the
// PATCH path. Same field-level assertions as create, but with the Update struct
// and without the create-only Name/ProjectType requirement.
func TestMapV2DataToV1ProjectUpdatePayload_HappyPath(t *testing.T) {
	ctx := t.Context()
	data := map[string]any{
		"name":                    "Renamed Project",
		"slug":                    "renamed-project",
		"stage":                   "Active",
		"funding_model":           []any{"Membership"},
		"autojoin_enabled":        false,
		"formation_date":          "2020-06-01T12:34:56Z",
		"entity_dissolution_date": "",
	}
	got, err := mapV2DataToV1ProjectUpdatePayload(ctx, data)
	if err != nil {
		t.Fatalf("mapV2DataToV1ProjectUpdatePayload() error = %v", err)
	}
	if got.Name == nil || *got.Name != "Renamed Project" {
		t.Errorf("Name = %v", got.Name)
	}
	if got.Slug == nil || *got.Slug != "renamed-project" {
		t.Errorf("Slug = %v", got.Slug)
	}
	if got.Status == nil || *got.Status != "Active" {
		t.Errorf("Status = %v", got.Status)
	}
	if got.Model == nil || !reflect.DeepEqual(*got.Model, []string{"Membership"}) {
		t.Errorf("Model = %#v", got.Model)
	}
	if got.AutoJoinEnabled == nil || *got.AutoJoinEnabled != false {
		t.Errorf("AutoJoinEnabled = %v, want *false (must send explicit false, not omit)", got.AutoJoinEnabled)
	}
	if got.StartDate == nil || *got.StartDate != "2020-06-01" {
		t.Errorf("StartDate = %v, want date-only", got.StartDate)
	}
	if got.ProjectEntityDissolutionDate == nil || *got.ProjectEntityDissolutionDate != "" {
		t.Errorf("ProjectEntityDissolutionDate = %v, want pointer to empty (clear)", got.ProjectEntityDissolutionDate)
	}
	// Fields not in input should be nil so PATCH omits them.
	if got.Category != nil {
		t.Errorf("Category = %v, want nil (not in input)", got.Category)
	}
}

// TestMapV2DataToV1ProjectUpdatePayload_EmptyDataYieldsEmptyPayload verifies
// that a v2 update event with no fields (unusual but possible) produces an
// empty payload rather than an error. All fields have `omitempty` so the
// resulting PATCH is a no-op on the v1 side.
func TestMapV2DataToV1ProjectUpdatePayload_EmptyDataYieldsEmptyPayload(t *testing.T) {
	ctx := t.Context()
	got, err := mapV2DataToV1ProjectUpdatePayload(ctx, map[string]any{})
	if err != nil {
		t.Fatalf("mapV2DataToV1ProjectUpdatePayload() error = %v", err)
	}
	if got == nil {
		t.Fatalf("mapV2DataToV1ProjectUpdatePayload() returned nil")
	}
	empty := &projectServiceProjectUpdate{}
	if !reflect.DeepEqual(got, empty) {
		t.Fatalf("mapV2DataToV1ProjectUpdatePayload({}) = %#v, want empty struct", got)
	}
}

// TestMapV2DataToV1ProjectUpdatePayload_IsFoundationSwitchesProjectType pins
// the update-path treatment of is_foundation added in response to Copilot
// review on PR #160: v2 → v1 sync must propagate a foundation/non-foundation
// flip via ProjectType rather than leaving v1 stale.
func TestMapV2DataToV1ProjectUpdatePayload_IsFoundationSwitchesProjectType(t *testing.T) {
	ctx := t.Context()
	tests := []struct {
		name        string
		data        map[string]any
		wantProject *string
	}{
		{name: "is_foundation=true → Project Group", data: map[string]any{"is_foundation": true}, wantProject: ptr("Project Group")},
		{name: "is_foundation=false → Project", data: map[string]any{"is_foundation": false}, wantProject: ptr("Project")},
		{name: "is_foundation absent → unset", data: map[string]any{}, wantProject: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mapV2DataToV1ProjectUpdatePayload(ctx, tt.data)
			if err != nil {
				t.Fatalf("mapV2DataToV1ProjectUpdatePayload() error = %v", err)
			}
			if !reflect.DeepEqual(got.ProjectType, tt.wantProject) {
				t.Fatalf("ProjectType = %v, want %v", got.ProjectType, tt.wantProject)
			}
		})
	}
}
