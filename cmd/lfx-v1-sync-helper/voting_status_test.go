// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import "testing"

func TestNormalizeVotingStatus(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"", ""},
		{"   ", ""},
		{"None", ""},
		{"none", ""},
		{" NONE ", ""},
		{"Voting Rep", "Voting Rep"},
		{" Observer ", "Observer"},
	}
	for _, tt := range tests {
		if got := normalizeVotingStatus(tt.in); got != tt.want {
			t.Errorf("normalizeVotingStatus(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestV1VotingFromV2EventData(t *testing.T) {
	tests := []struct {
		name               string
		data               map[string]any
		wantStatus         string
		wantStart, wantEnd string
		wantPresent        bool
	}{
		{name: "absent key", data: map[string]any{"email": "a@example.com"}},
		{name: "unexpected type", data: map[string]any{"voting": "Voting Rep"}},
		{name: "null voting", data: map[string]any{"voting": nil}, wantStatus: v1NoVotingStatus, wantPresent: true},
		{name: "empty object", data: map[string]any{"voting": map[string]any{}}, wantStatus: v1NoVotingStatus, wantPresent: true},
		{name: "empty status", data: map[string]any{"voting": map[string]any{"status": ""}}, wantStatus: v1NoVotingStatus, wantPresent: true},
		{name: "None status", data: map[string]any{"voting": map[string]any{"status": "None"}}, wantStatus: v1NoVotingStatus, wantPresent: true},
		{name: "lowercase none status", data: map[string]any{"voting": map[string]any{"status": "none"}}, wantStatus: v1NoVotingStatus, wantPresent: true},
		{
			name: "status with dates",
			data: map[string]any{"voting": map[string]any{
				"status":     "Voting Rep",
				"start_date": "2026-01-01",
				"end_date":   "2026-12-31",
			}},
			wantStatus:  "Voting Rep",
			wantStart:   "2026-01-01",
			wantEnd:     "2026-12-31",
			wantPresent: true,
		},
		{
			name:        "empty status keeps dates",
			data:        map[string]any{"voting": map[string]any{"status": "", "start_date": "2026-01-01"}},
			wantStatus:  v1NoVotingStatus,
			wantStart:   "2026-01-01",
			wantPresent: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, start, end, present := v1VotingFromV2EventData(tt.data)
			if status != tt.wantStatus || start != tt.wantStart || end != tt.wantEnd || present != tt.wantPresent {
				t.Errorf("got (%q, %q, %q, %v), want (%q, %q, %q, %v)",
					status, start, end, present, tt.wantStatus, tt.wantStart, tt.wantEnd, tt.wantPresent)
			}
		})
	}
}
