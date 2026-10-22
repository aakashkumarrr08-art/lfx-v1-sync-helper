// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"testing"

	committeeservice "github.com/linuxfoundation/lfx-v2-committee-service/gen/committee_service"
)

func TestV1CommitteeParentCreateAndUpdate(t *testing.T) {
	h := setupSyncTest(t, 200, "{}")
	h.seed(t, "project.sfid.project-a", "project-uid")
	h.seed(t, "committee.sfid.parent-a", "parent-uid")
	h.seed(t, "committee.uid.parent-uid", "project-a:parent-a")
	data := map[string]any{"mailing_list__c": "Subgroup", "project_name__c": "project-a", "committee_id": "parent-a"}
	created, err := mapV1DataToCommitteeCreatePayload(t.Context(), data)
	if err != nil || created.ParentUID == nil || *created.ParentUID != "parent-uid" {
		t.Fatalf("create parent = %v, err = %v", created, err)
	}
	base := &committeeservice.CommitteeBaseWithReadonlyAttributes{
		Name: sptr("Subgroup"), ProjectUID: sptr("project-uid"), ParentUID: sptr("old-parent"),
	}
	updated, err := mapV1DataToCommitteeUpdateBasePayload(t.Context(), "child-uid", data, base)
	if err != nil || updated.ParentUID == nil || *updated.ParentUID != "parent-uid" {
		t.Fatalf("update parent = %v, err = %v", updated, err)
	}
	if committeeBasesEqual(base, committeeBaseFromUpdatePayload(sptr("child-uid"), updated)) {
		t.Fatal("parent-only change must trigger a v2 update")
	}
	delete(data, "committee_id")
	updated, err = mapV1DataToCommitteeUpdateBasePayload(t.Context(), "child-uid", data, base)
	if err != nil || updated.ParentUID == nil || *updated.ParentUID != "old-parent" {
		t.Fatalf("absent parent should preserve existing parent: %v, %v", updated, err)
	}
	data["committee_id"] = ""
	updated, err = mapV1DataToCommitteeUpdateBasePayload(t.Context(), "child-uid", data, base)
	if err != nil || updated.ParentUID != nil {
		t.Fatalf("empty parent should clear: %v, %v", updated, err)
	}
	data["committee_id"] = nil
	updated, err = mapV1DataToCommitteeUpdateBasePayload(t.Context(), "child-uid", data, base)
	if err != nil || updated.ParentUID != nil {
		t.Fatalf("null parent should clear: %v, %v", updated, err)
	}
}

func TestV2CommitteeParentWrittenToV1(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		h := setupSyncTest(t, 201, `{"ID":"child-sfid"}`)
		h.seed(t, "committee.uid.parent-uid", "project-a:parent-a")
		err := syncCommitteeCreateToV1(t.Context(), "child-uid", "project-a", map[string]any{
			"name": "Child", "category": "Other", "parent_uid": "parent-uid",
		})
		if err != nil {
			t.Fatal(err)
		}
		requests := h.recordedRequests()
		if len(requests) != 1 || requests[0].Body["CommitteeID"] != "parent-a" {
			t.Fatalf("v1 create requests = %+v", requests)
		}
	})
	t.Run("update and clear", func(t *testing.T) {
		h := setupSyncTest(t, 200, `{}`)
		h.seed(t, "committee.uid.parent-uid", "project-a:parent-a")
		for _, data := range []map[string]any{
			{"name": "Child", "parent_uid": "parent-uid"},
			{"name": "Child", "parent_uid": ""},
			{
				"uid": "child-uid", "project_uid": "project-uid", "name": "Child", "category": "Other",
				"created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-02T00:00:00Z",
				"enable_voting": false, "sso_group_enabled": false, "requires_review": false,
				"public": false, "total_members": float64(0), "total_voting_repos": float64(0), "has_mailing_list": false,
			},
		} {
			if err := syncCommitteeUpdateToV1(t.Context(), "child-uid", "project-a", "child-sfid", data); err != nil {
				t.Fatal(err)
			}
		}
		requests := h.recordedRequests()
		if len(requests) != 3 || requests[0].Body["CommitteeID"] != "parent-a" || requests[1].Body["CommitteeID"] != "NONE" || requests[2].Body["CommitteeID"] != "NONE" {
			t.Fatalf("v1 update requests = %+v", requests)
		}
	})
}

func TestV1CommitteeParentUnmappedOrTombstoned(t *testing.T) {
	h := setupSyncTest(t, 200, "{}")
	data := map[string]any{"committee_id": "not-yet-mapped"}
	_, err := resolveV1CommitteeParentUID(t.Context(), data)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("missing parent error = %v", err)
	}
	h.seed(t, "committee.sfid.not-yet-mapped", "!del")
	if _, err := resolveV1CommitteeParentUID(t.Context(), data); err == nil {
		t.Fatal("tombstoned parent must not be used")
	}
	h.store.nextGetErr = errors.New("store unavailable")
	if _, err := resolveV1CommitteeParentUID(t.Context(), data); !isTransientStoreErr(err) {
		t.Fatalf("transient lookup error should NACK: %v", err)
	}
	data["committee_id"] = 12
	if _, err := resolveV1CommitteeParentUID(t.Context(), data); err == nil {
		t.Fatal("invalid parent type must not silently clear the link")
	}
}

func TestV1CommitteeParentRejectsSelfOrCrossProject(t *testing.T) {
	h := setupSyncTest(t, 200, "{}")
	h.seed(t, "committee.sfid.parent-a", "parent-uid")
	h.seed(t, "committee.uid.parent-uid", "other-project:parent-a")
	data := map[string]any{
		"sfid": "parent-a", "project_name__c": "project-a", "committee_id": "parent-a",
	}
	if _, err := resolveV1CommitteeParentUID(t.Context(), data); err == nil {
		t.Fatal("self-parent must be rejected")
	}
	data["sfid"] = "child-a"
	if _, err := resolveV1CommitteeParentUID(t.Context(), data); err == nil {
		t.Fatal("parent from another project must be rejected")
	}
	h.seed(t, "committee.uid.parent-uid", "project-a:parent-a")
	if uid, err := resolveV1CommitteeParentUID(t.Context(), data); err != nil || uid == nil || *uid != "parent-uid" {
		t.Fatalf("valid same-project parent = %v, err = %v", uid, err)
	}
}

func TestHandleCommitteeUpdateRetriesUnmappedParent(t *testing.T) {
	fake := setupHandlerMarkerTest(t)
	const childSFID = "child-a"
	if _, err := fake.Put(t.Context(), "committee.sfid."+childSFID, []byte("child-uid")); err != nil {
		t.Fatal(err)
	}
	if _, err := fake.Put(t.Context(), "project.sfid.project-a", []byte("project-uid")); err != nil {
		t.Fatal(err)
	}
	fetchCommitteeBase = func(_ context.Context, _ string) (*committeeservice.CommitteeBaseWithReadonlyAttributes, string, error) {
		return &committeeservice.CommitteeBaseWithReadonlyAttributes{UID: sptr("child-uid")}, "etag", nil
	}
	v1Data := map[string]any{
		"sfid": childSFID, "mailing_list__c": "Child", "project_name__c": "project-a",
		"committee_id": "new-parent", "lastmodifiedbyid": "v1-editor",
	}
	if !handleCommitteeUpdate(t.Context(), "platform-collaboration__c."+childSFID, v1Data) {
		t.Fatal("missing parent on update must NACK for redelivery")
	}
	marker := pendingMarkerKey(markerV1ToV2, markerOpUpdate, markerResourceCommittee, "child-uid")
	if _, err := fake.Get(t.Context(), marker); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("failed update left a pending marker: %v", err)
	}
}

func TestV2CommitteeParentResolution(t *testing.T) {
	h := setupSyncTest(t, 200, "{}")
	h.seed(t, "committee.uid.parent-uid", "project-a:parent-a")
	for _, tc := range []struct {
		name string
		data map[string]any
		full bool
		want *string
	}{
		{"set", map[string]any{"parent_uid": "parent-uid"}, false, sptr("parent-a")},
		{"explicit clear", map[string]any{"parent_uid": ""}, false, sptr("NONE")},
		{"full snapshot clear", map[string]any{}, true, sptr("NONE")},
		{"partial absent", map[string]any{}, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveV2CommitteeParentSFID(t.Context(), "project-a", tc.data, tc.full)
			if err != nil || stringPtrToString(got) != stringPtrToString(tc.want) || (got == nil) != (tc.want == nil) {
				t.Fatalf("parent SFID = %v, err = %v; want %v", got, err, tc.want)
			}
		})
	}
	if _, err := resolveV2CommitteeParentSFID(context.Background(), "other-project", map[string]any{"parent_uid": "parent-uid"}, false); err == nil {
		t.Fatal("cross-project parent must not be written to v1")
	}
	if _, err := resolveV2CommitteeParentSFID(t.Context(), "project-a", map[string]any{"parent_uid": "missing"}, false); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("unmapped parent must retry: %v", err)
	}
	if _, err := resolveV2CommitteeParentSFID(t.Context(), "project-a", map[string]any{"parent_uid": 12}, false); err == nil {
		t.Fatal("invalid v2 parent type must not clear v1")
	}
}
