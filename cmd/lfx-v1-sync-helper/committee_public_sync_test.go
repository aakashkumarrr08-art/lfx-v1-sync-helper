// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import "testing"

func TestV2CommitteePublicWrittenToV1Create(t *testing.T) {
	tests := []struct {
		name     string
		data     map[string]any
		wantPub  any
		wantName any
	}{
		{
			name:     "public with display name",
			data:     map[string]any{"name": "Committee", "category": "Other", "public": true, "display_name": "Public Committee"},
			wantPub:  true,
			wantName: "Public Committee",
		},
		{
			name:    "private",
			data:    map[string]any{"name": "Committee", "category": "Other", "public": false},
			wantPub: false,
		},
		{
			name: "legacy event without public",
			data: map[string]any{"name": "Committee", "category": "Other"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := setupSyncTest(t, 201, `{"ID":"committee-sfid"}`)
			if err := syncCommitteeCreateToV1(t.Context(), "committee-uid", "project-a", tt.data); err != nil {
				t.Fatal(err)
			}
			requests := h.recordedRequests()
			if len(requests) != 1 {
				t.Fatalf("v1 create requests = %+v", requests)
			}
			if got := requests[0].Body["PublicEnabled"]; got != tt.wantPub {
				t.Errorf("PublicEnabled = %v, want %v", got, tt.wantPub)
			}
			if got := requests[0].Body["PublicName"]; got != tt.wantName {
				t.Errorf("PublicName = %v, want %v", got, tt.wantName)
			}
		})
	}
}

func TestV2CommitteePublicWrittenToV1Update(t *testing.T) {
	tests := []struct {
		name     string
		data     map[string]any
		wantPub  any
		wantName any
	}{
		{
			name:     "enable public",
			data:     map[string]any{"name": "Committee", "public": true, "display_name": "Public Committee"},
			wantPub:  true,
			wantName: "Public Committee",
		},
		{
			name:    "disable public",
			data:    map[string]any{"name": "Committee", "public": false},
			wantPub: false,
		},
		{
			name: "partial event leaves visibility untouched",
			data: map[string]any{"name": "Committee"},
		},
		{
			name:    "empty display name is never sent",
			data:    map[string]any{"name": "Committee", "public": true, "display_name": ""},
			wantPub: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := setupSyncTest(t, 200, `{}`)
			if err := syncCommitteeUpdateToV1(t.Context(), "committee-uid", "project-a", "committee-sfid", tt.data); err != nil {
				t.Fatal(err)
			}
			requests := h.recordedRequests()
			if len(requests) != 1 {
				t.Fatalf("v1 update requests = %+v", requests)
			}
			if got := requests[0].Body["PublicEnabled"]; got != tt.wantPub {
				t.Errorf("PublicEnabled = %v, want %v", got, tt.wantPub)
			}
			if got := requests[0].Body["PublicName"]; got != tt.wantName {
				t.Errorf("PublicName = %v, want %v", got, tt.wantName)
			}
		})
	}
}
