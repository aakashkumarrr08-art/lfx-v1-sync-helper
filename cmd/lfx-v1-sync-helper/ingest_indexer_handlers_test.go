// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"

	nats "github.com/nats-io/nats.go"
)

// These cases enter through the real dispatchers, including the committee
// JetStream wrapper, rather than substituting the sync functions themselves.
type indexerSyncCase struct {
	resource markerResource
	uid      string
	sfid     string
	path     string
	reverse  string
	create   map[string]any
	wantPost map[string]any
}

func indexerSyncCases() []indexerSyncCase {
	return []indexerSyncCase{
		{
			resource: markerResourceProject, uid: "project-uid", sfid: "project-sfid",
			path: "/project-service/v1/projects", reverse: "project-sfid",
			create:   map[string]any{"name": "Project", "slug": "project", "description": "Created", "autojoin_enabled": false},
			wantPost: map[string]any{"Name": "Project", "Slug": "project", "ProjectType": "Project", "Description": "Created", "AutoJoinEnabled": false},
		},
		{
			resource: markerResourceCommittee, uid: "committee-uid", sfid: "committee-sfid",
			path: "/project-service/v2/projects/project-sfid/committees", reverse: "project-sfid:committee-sfid",
			create:   map[string]any{"project_uid": "project-uid", "name": "Committee", "category": "Other", "description": "Created"},
			wantPost: map[string]any{"Name": "Committee", "Category": "Other", "Description": "Created"},
		},
	}
}

func (c indexerSyncCase) dispatch(t *testing.T, action string, data map[string]any) bool {
	t.Helper()
	body, err := json.Marshal(indexingEventBody{Data: data})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(indexingEvent{ObjectID: c.uid, ObjectType: string(c.resource), Action: action, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	subject := "lfx." + string(c.resource) + "." + action
	if c.resource == markerResourceProject {
		projectIndexerEventHandler(&nats.Msg{Subject: subject, Data: payload})
		return true // Core NATS project handler has no ACK/NAK result.
	}
	msg := &fakeMsg{subject: subject, data: payload}
	newCommitteeEventsIngestHandler(processCommitteeIndexingEvent, processCommitteeMemberIndexingEvent)(msg)
	if msg.ackCalls+msg.nakCalls != 1 {
		t.Fatalf("committee delivery must ACK or NAK exactly once: %+v", msg)
	}
	return msg.acked
}

func (c indexerSyncCase) seedParent(t *testing.T, h *syncTestHarness) {
	t.Helper()
	if c.resource == markerResourceCommittee {
		h.seed(t, "project.uid.project-uid", "project-sfid")
	}
}

func assertSyncRequest(t *testing.T, h *syncTestHarness, method, path string, body map[string]any) {
	t.Helper()
	requests := h.recordedRequests()
	if len(requests) != 1 {
		t.Fatalf("got %d HTTP requests, want 1: %+v", len(requests), requests)
	}
	got := requests[0]
	if got.Method != method || got.Path != path || !reflect.DeepEqual(got.Body, body) {
		t.Errorf("request = %+v; want %s %s body=%v", got, method, path, body)
	}
	if method != http.MethodDelete && got.ContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got.ContentType)
	}
}

func TestIndexerSyncCreate(t *testing.T) {
	for _, c := range indexerSyncCases() {
		t.Run(string(c.resource), func(t *testing.T) {
			for _, initial := range []string{"missing", "live", "tombstoned"} {
				t.Run(initial, func(t *testing.T) {
					h := setupSyncTest(t, http.StatusCreated, `{"ID":"`+c.sfid+`"}`)
					c.seedParent(t, h)
					reverseKey := string(c.resource) + ".uid." + c.uid
					switch initial {
					case "live":
						h.seed(t, reverseKey, c.reverse)
					case "tombstoned":
						h.seed(t, reverseKey, tombstoneMarker)
					}
					if !c.dispatch(t, "created", c.create) {
						t.Fatal("successful create should ACK")
					}
					if initial == "live" {
						if requests := h.recordedRequests(); len(requests) != 0 {
							t.Fatalf("loop guard sent HTTP requests: %+v", requests)
						}
						h.assertMapping(t, reverseKey, c.reverse)
						return
					}
					assertSyncRequest(t, h, http.MethodPost, c.path, c.wantPost)
					h.assertMapping(t, string(c.resource)+".sfid."+c.sfid, c.uid)
					h.assertMapping(t, reverseKey, c.reverse)
					// Replaying the same event must not create a duplicate.
					if !c.dispatch(t, "created", c.create) {
						t.Fatal("replayed create should ACK")
					}
					if len(h.recordedRequests()) != 1 {
						t.Fatal("replayed create bypassed loop guard")
					}
				})
			}
		})
	}
}

func TestIndexerSyncUpdate(t *testing.T) {
	for _, c := range indexerSyncCases() {
		t.Run(string(c.resource), func(t *testing.T) {
			h := setupSyncTest(t, http.StatusOK, `{}`)
			h.seed(t, string(c.resource)+".uid."+c.uid, c.reverse)
			if !c.dispatch(t, "updated", map[string]any{"name": "Renamed", "description": ""}) {
				t.Fatal("successful update should ACK")
			}
			assertSyncRequest(t, h, http.MethodPatch, c.path+"/"+c.sfid, map[string]any{"Name": "Renamed", "Description": ""})
			h.assertMapping(t, string(c.resource)+".uid."+c.uid, c.reverse)
		})
	}
}

func TestIndexerSyncDelete(t *testing.T) {
	for _, c := range indexerSyncCases() {
		t.Run(string(c.resource), func(t *testing.T) {
			for _, status := range []int{http.StatusNoContent, http.StatusNotFound, http.StatusServiceUnavailable} {
				t.Run(http.StatusText(status), func(t *testing.T) {
					h := setupSyncTest(t, status, "")
					forwardKey, reverseKey := string(c.resource)+".sfid."+c.sfid, string(c.resource)+".uid."+c.uid
					h.seed(t, forwardKey, c.uid)
					h.seed(t, reverseKey, c.reverse)
					acked := c.dispatch(t, "deleted", nil)
					assertSyncRequest(t, h, http.MethodDelete, c.path+"/"+c.sfid, nil)
					if status == http.StatusServiceUnavailable {
						h.assertMapping(t, forwardKey, c.uid)
						h.assertMapping(t, reverseKey, c.reverse)
						if c.resource == markerResourceCommittee && acked {
							t.Error("transient delete failure should NAK")
						}
						if consumePendingMarker(t.Context(), markerV2ToV1, markerOpDelete, c.resource, c.sfid) {
							t.Error("failed delete left a pending WAL marker")
						}
						return
					}
					if !acked {
						t.Error("successful/idempotent delete should ACK")
					}
					h.assertMapping(t, forwardKey, tombstoneMarker)
					h.assertMapping(t, reverseKey, tombstoneMarker)
					if !consumePendingMarker(t.Context(), markerV2ToV1, markerOpDelete, c.resource, c.sfid) {
						t.Error("successful delete did not leave a WAL echo marker")
					}
				})
			}
		})
	}
}

func TestIndexerSyncSkipsEchoes(t *testing.T) {
	for _, c := range indexerSyncCases() {
		for _, action := range []string{"updated", "deleted"} {
			t.Run(string(c.resource)+"/"+action, func(t *testing.T) {
				h := setupSyncTest(t, http.StatusOK, `{}`)
				op := markerOpUpdate
				value := c.reverse
				if action == "deleted" {
					op, value = markerOpDelete, tombstoneMarker
				}
				h.seed(t, string(c.resource)+".uid."+c.uid, value)
				writePendingMarker(t.Context(), markerV1ToV2, op, c.resource, c.uid)
				if !c.dispatch(t, action, map[string]any{"description": "echo"}) {
					t.Fatal("echo should ACK")
				}
				if len(h.recordedRequests()) != 0 {
					t.Error("echo sent an HTTP request")
				}
				key := pendingMarkerKey(markerV1ToV2, op, c.resource, c.uid)
				if _, err := h.store.Get(t.Context(), key); !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("echo marker not deleted: %v", err)
				}
			})
		}
	}
}

func TestIndexerSyncHTTPFailures(t *testing.T) {
	for _, c := range indexerSyncCases() {
		for _, action := range []string{"created", "updated"} {
			t.Run(string(c.resource)+"/"+action, func(t *testing.T) {
				h := setupSyncTest(t, http.StatusServiceUnavailable, `{"error":"unavailable"}`)
				c.seedParent(t, h)
				reverseKey := string(c.resource) + ".uid." + c.uid
				if action == "updated" {
					h.seed(t, reverseKey, c.reverse)
				}
				acked := c.dispatch(t, action, c.create)
				if len(h.recordedRequests()) != 1 {
					t.Fatal("expected one failed HTTP request")
				}
				if c.resource == markerResourceCommittee && acked {
					t.Error("transient API error should NAK")
				}
				if action == "updated" {
					h.assertMapping(t, reverseKey, c.reverse)
				} else if _, err := h.store.Get(t.Context(), reverseKey); !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("failed create persisted reverse mapping: %v", err)
				}
				if _, err := h.store.Get(t.Context(), string(c.resource)+".sfid."+c.sfid); !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("failed request persisted forward mapping: %v", err)
				}
			})
		}
	}
}

func TestIndexerSyncSkipsUnmappedAndUnknownEvents(t *testing.T) {
	for _, c := range indexerSyncCases() {
		for _, action := range []string{"updated", "deleted", "unknown"} {
			t.Run(string(c.resource)+"/"+action, func(t *testing.T) {
				h := setupSyncTest(t, http.StatusOK, `{}`)
				if !c.dispatch(t, action, c.create) {
					t.Error("permanent skip should ACK")
				}
				if len(h.recordedRequests()) != 0 {
					t.Error("unmapped or unknown event sent HTTP request")
				}
			})
		}
	}
}
