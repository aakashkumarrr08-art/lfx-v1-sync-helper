// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// Committee member voting-status normalization shared by the v1→v2 and v2→v1
// sync paths (lfx-self-serve#4079).
//
// v2 represents "no voting status" as an empty status; v1 may store "None"
// (legacy Salesforce picklist value) but its project-service POST/PATCH only
// accept Alternate Voting Rep, Observer, Voting Rep and Emeritus, and cannot
// clear the field. An omitted v1 status gets a v1-side default, so v2→v1 sends
// v1NoVotingStatus explicitly to keep the member non-eligible for votes.

// The lfx-v1-sync-helper service.
package main

import "strings"

// v1NoVotingStatus is the v1 voting status sent when the v2 member has no
// voting status. "Observer" is the only non-voting value v1 accepts on write.
const v1NoVotingStatus = "Observer"

// normalizeVotingStatus returns the trimmed voting status, or "" when it is
// empty or "None" (case-insensitive), so both spellings of "no voting status"
// compare equal.
func normalizeVotingStatus(s string) string {
	s = strings.TrimSpace(s)
	if strings.EqualFold(s, "None") {
		return ""
	}
	return s
}

// v1VotingFromV2EventData extracts the v1 voting fields from a v2 committee
// member indexer event. present is false when the event carries no "voting"
// key, in which case v1 must be left untouched. A null voting object or an
// empty/"None" status is translated to v1NoVotingStatus.
func v1VotingFromV2EventData(data map[string]any) (status, startDate, endDate string, present bool) {
	raw, ok := data["voting"]
	if !ok {
		return "", "", "", false
	}
	voting, isMap := raw.(map[string]any)
	if raw != nil && !isMap {
		return "", "", "", false
	}

	if vs, ok := voting["status"].(string); ok {
		status = normalizeVotingStatus(vs)
	}
	if status == "" {
		status = v1NoVotingStatus
	}
	startDate, _ = voting["start_date"].(string)
	endDate, _ = voting["end_date"].(string)
	return status, startDate, endDate, true
}
