// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
)

// scriptedMappingGet makes mapping visibility races deterministic without
// timing-dependent goroutines. Only the chosen key is delayed; all other store
// operations use the shared CAS-capable fake.
type scriptedMappingGet struct {
	MappingStore
	key   string
	errs  []error
	calls int
}

func (s *scriptedMappingGet) Get(ctx context.Context, key string) (MappingEntry, error) {
	if key == s.key {
		s.calls++
		if len(s.errs) > 0 {
			err := s.errs[0]
			s.errs = s.errs[1:]
			return MappingEntry{}, err
		}
	}
	return s.MappingStore.Get(ctx, key)
}

func committeeRetryEvent(t *testing.T, resource, action, uid string, data map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(indexingEventBody{Data: data})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(indexingEvent{ObjectID: uid, ObjectType: resource, Action: action, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

// Main committee lookups already use bounded retries. Pin every call site so
// the race mitigation cannot silently regress to a raw Get.
func TestCommitteeMappingLookupRetries(t *testing.T) {
	const contactSFID = "003000000000001AAA"
	for _, resource := range []string{"committee", "committee_member"} {
		for _, lookup := range []string{"create parent", "create loop guard", "update", "delete"} {
			for _, firstErr := range []error{ErrKeyNotFound, errors.New("store unavailable")} {
				t.Run(resource+"/"+lookup+"/"+firstErr.Error(), func(t *testing.T) {
					status, response := http.StatusCreated, `{"ID":"new-committee","MemberID":"`+contactSFID+`"}`
					action := "created"
					switch lookup {
					case "update":
						action, status, response = "updated", http.StatusOK, `{}`
					case "delete":
						action, status, response = "deleted", http.StatusNoContent, ""
					}
					h := setupSyncTest(t, status, response)
					uid, key, value := "committee-uid", "project.uid.project-uid", "project-sfid"
					data := map[string]any{"project_uid": "project-uid", "name": "Committee", "description": "Updated"}
					proc := processCommitteeIndexingEvent
					if resource == "committee_member" {
						uid, key, value = "member-uid", "committee.uid.committee-uid", "project-sfid:committee-sfid"
						data = map[string]any{"committee_uid": "committee-uid", "email": "member@example.invalid", "job_title": "Updated"}
						proc = processCommitteeMemberIndexingEvent
						if lookup == "create parent" {
							// Stop at the existing reverse-mapping guard after resolving
							// the late parent, without needing a live user database.
							h.seed(t, "committee_member.uid."+uid, "project-sfid:committee-sfid:"+contactSFID)
						}
					}
					h.seed(t, key, value)
					if lookup != "create parent" {
						key = resource + ".uid." + uid
						value = "project-sfid:committee-sfid"
						if resource == "committee_member" {
							value += ":" + contactSFID
							h.seed(t, committeeMemberRecordSFIDKey(uid), "record-sfid")
						}
						h.seed(t, key, value)
					}
					script := &scriptedMappingGet{MappingStore: h.store, key: key, errs: []error{firstErr}}
					mappingStore = script
					payload := committeeRetryEvent(t, resource, action, uid, data)
					if err := proc(t.Context(), "lfx."+resource+"."+action, payload); err != nil {
						t.Fatal(err)
					}
					if script.calls < 2 {
						t.Fatalf("mapping Get calls = %d, want at least 2", script.calls)
					}
					requests := h.recordedRequests()
					if lookup == "create loop guard" || (resource == "committee_member" && lookup == "create parent") {
						if len(requests) != 0 {
							t.Fatal("late reverse mapping did not prevent duplicate create")
						}
					} else if len(requests) != 1 {
						t.Fatalf("HTTP requests = %d, want 1", len(requests))
					}
				})
			}
		}
	}
}

func TestResolveCommitteeMemberRecordSFIDRetries(t *testing.T) {
	for _, tc := range []struct {
		name  string
		errs  []error
		value string
		want  string
		calls int
	}{
		{"late write", []error{ErrKeyNotFound}, "record-sfid", "record-sfid", 2},
		{"transient recovery", []error{errors.New("unavailable")}, "record-sfid", "record-sfid", 2},
		{"transient exhausted", []error{errors.New("unavailable"), errors.New("unavailable"), errors.New("unavailable"), errors.New("unavailable")}, "record-sfid", "", 4},
		{"absent", nil, "", "", mappingGetMaxAttempts},
		{"tombstoned", nil, tombstoneMarker, "", 1},
		{"cancelled", []error{context.Canceled}, "record-sfid", "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setupSyncTest(t, http.StatusNoContent, "")
			key := committeeMemberRecordSFIDKey("member-uid")
			if tc.value != "" {
				h.seed(t, key, tc.value)
			}
			script := &scriptedMappingGet{MappingStore: h.store, key: key, errs: tc.errs}
			mappingStore = script
			if got := resolveCommitteeMemberRecordSFID(t.Context(), "member-uid"); got != tc.want {
				t.Errorf("record SFID = %q, want %q", got, tc.want)
			}
			if script.calls != tc.calls {
				t.Errorf("Get calls = %d, want %d", script.calls, tc.calls)
			}
		})
	}
}

func TestCommitteeMemberDeleteRetriesForwardOwnershipLookup(t *testing.T) {
	const contactSFID = "003000000000001AAA"
	h := setupSyncTest(t, http.StatusNoContent, "")
	key := "committee_member.sfid." + contactSFID
	h.seed(t, key, "committee-uid:member-uid")
	h.seed(t, "committee_member.uid.member-uid", "project-sfid:committee-sfid:"+contactSFID)
	script := &scriptedMappingGet{MappingStore: h.store, key: key, errs: []error{ErrKeyNotFound}}
	mappingStore = script
	if err := syncCommitteeMemberDeleteToV1(t.Context(), "member-uid", "project-sfid", "committee-sfid", contactSFID, ""); err != nil {
		t.Fatal(err)
	}
	h.assertMapping(t, key, tombstoneMarker)
	h.assertMapping(t, "committee_member.uid.member-uid", tombstoneMarker)
	assertSyncRequest(t, h, http.MethodDelete, "/project-service/v2/projects/project-sfid/committees/committee-sfid/members/"+contactSFID, nil)
}

func TestCommitteeMappingLookupExhaustion(t *testing.T) {
	for _, resource := range []string{"committee", "committee_member"} {
		for _, action := range []string{"created", "updated", "deleted"} {
			for _, failure := range []error{ErrKeyNotFound, errors.New("store unavailable")} {
				t.Run(resource+"/"+action+"/"+failure.Error(), func(t *testing.T) {
					h := setupSyncTest(t, http.StatusOK, `{}`)
					uid := "object-uid"
					key := resource + ".uid." + uid
					data := map[string]any{"name": "Committee", "project_uid": "parent-uid", "committee_uid": "parent-uid"}
					proc := processCommitteeIndexingEvent
					if resource == "committee_member" {
						proc = processCommitteeMemberIndexingEvent
					}
					if action == "created" {
						key = "project.uid.parent-uid"
						if resource == "committee_member" {
							key = "committee.uid.parent-uid"
						}
					}
					errs := make([]error, mappingGetMaxAttempts)
					for i := range errs {
						errs[i] = failure
					}
					script := &scriptedMappingGet{MappingStore: h.store, key: key, errs: errs}
					mappingStore = script
					err := proc(t.Context(), "lfx."+resource+"."+action, committeeRetryEvent(t, resource, action, uid, data))
					if errors.Is(failure, ErrKeyNotFound) {
						if err != nil {
							t.Errorf("confirmed missing mapping should permanently skip: %v", err)
						}
					} else if !errors.Is(err, failure) {
						t.Errorf("transient error = %v, want %v for redelivery", err, failure)
					}
					if script.calls != mappingGetMaxAttempts {
						t.Errorf("Get calls = %d, want %d", script.calls, mappingGetMaxAttempts)
					}
					if len(h.recordedRequests()) != 0 {
						t.Error("failed mapping lookup sent HTTP request")
					}
				})
			}
		}
	}
}

func TestCommitteeMemberDeleteForwardLookupExhaustionStillCleansReverse(t *testing.T) {
	const contactSFID = "003000000000001AAA"
	h := setupSyncTest(t, http.StatusNoContent, "")
	forwardKey := "committee_member.sfid." + contactSFID
	reverseKey := "committee_member.uid.member-uid"
	h.seed(t, forwardKey, "committee-uid:member-uid")
	h.seed(t, reverseKey, "project-sfid:committee-sfid:"+contactSFID)
	failure := errors.New("store unavailable")
	errs := make([]error, mappingGetMaxAttempts)
	for i := range errs {
		errs[i] = failure
	}
	script := &scriptedMappingGet{MappingStore: h.store, key: forwardKey, errs: errs}
	mappingStore = script
	if err := syncCommitteeMemberDeleteToV1(t.Context(), "member-uid", "project-sfid", "committee-sfid", contactSFID, ""); err != nil {
		t.Fatal(err)
	}
	if script.calls != mappingGetMaxAttempts {
		t.Errorf("Get calls = %d, want %d", script.calls, mappingGetMaxAttempts)
	}
	assertSyncRequest(t, h, http.MethodDelete, "/project-service/v2/projects/project-sfid/committees/committee-sfid/members/"+contactSFID, nil)
	h.assertMapping(t, forwardKey, "committee-uid:member-uid")
	h.assertMapping(t, reverseKey, tombstoneMarker)
}
