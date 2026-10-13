// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

//go:build integration

// Integration tests for the v1 committee client functions.
// These tests hit the real LFX API gateway and require credentials to be set
// in the environment (same variables used by the service itself).
//
// Run with:
//
//	source cmd/lfx-v1-sync-helper/.env && go test -tags=integration -v -run TestCommitteeClientIntegration ./cmd/lfx-v1-sync-helper/
//	source cmd/lfx-v1-sync-helper/.env && go test -tags=integration -v -run TestCommitteeMemberClientIntegration ./cmd/lfx-v1-sync-helper/

package main

import (
	"context"
	"log/slog"
	"net/url"
	"os"
	"testing"
)

const (
	// integrationTestProjectSFID is the dev environment project used for integration testing.
	// We use TLF project because it is one of the only projects that gets indexed into the V2 system.
	integrationTestProjectSFID = "a0941000002wBz9AAE"
)

// setupIntegrationTest initialises the package-level globals that the client
// functions depend on (cfg and v1HTTPClient), mirroring what main() does.
func setupIntegrationTest(t *testing.T) {
	t.Helper()

	logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	auth0Tenant := os.Getenv("AUTH0_TENANT")
	auth0ClientID := os.Getenv("AUTH0_CLIENT_ID")
	auth0PrivateKey := os.Getenv("AUTH0_PRIVATE_KEY")
	lfxAPIGW := os.Getenv("LFX_API_GW")

	if auth0Tenant == "" || auth0ClientID == "" || auth0PrivateKey == "" {
		t.Skip("AUTH0_TENANT, AUTH0_CLIENT_ID, AUTH0_PRIVATE_KEY must be set to run integration tests")
	}

	if lfxAPIGW == "" {
		lfxAPIGW = "https://api-gw.dev.platform.linuxfoundation.org/"
	}

	gatewayURL, err := url.Parse(lfxAPIGW)
	if err != nil {
		t.Fatalf("failed to parse LFX_API_GW: %v", err)
	}

	cfg = &Config{
		Auth0Tenant:     auth0Tenant,
		Auth0ClientID:   auth0ClientID,
		Auth0PrivateKey: auth0PrivateKey,
		LFXAPIGateway:   gatewayURL,
	}

	if err := initV1Client(cfg); err != nil {
		t.Fatalf("failed to init v1 client: %v", err)
	}
}

// TestCommitteeClientIntegration exercises createV1Committee, updateV1Committee,
// and deleteV1Committee against the real dev API, cleaning up after itself.
func TestCommitteeClientIntegration(t *testing.T) {
	setupIntegrationTest(t)
	ctx := context.Background()

	// --- Create ---
	publicEnabled := true
	ssoGroupEnabled := false
	created, err := createV1Committee(ctx, integrationTestProjectSFID, projectServiceCommitteeCreate{
		Name:            "v1-sync-helper integration test committee",
		Category:        "Working Group",
		Description:     "Temporary committee created by integration test — safe to delete",
		Website:         "https://example.com/test-committee",
		PublicEnabled:   &publicEnabled,
		SSOGroupEnabled: &ssoGroupEnabled,
	})
	if err != nil {
		t.Fatalf("createV1Committee: %v", err)
	}
	t.Logf("created committee: ID=%s Name=%q Category=%s PublicEnabled=%v SSOGroupEnabled=%v",
		created.ID, created.Name, created.Category, created.PublicEnabled, created.SSOGroupEnabled)

	if created.ID == "" {
		t.Fatal("expected non-empty committee ID in create response")
	}

	// Always clean up, even if the update step fails.
	t.Cleanup(func() {
		if err := deleteV1Committee(ctx, integrationTestProjectSFID, created.ID); err != nil {
			t.Errorf("deleteV1Committee cleanup: %v", err)
		} else {
			t.Logf("deleted committee %s", created.ID)
		}
	})

	// --- Update ---
	publicDisabled := false
	updatedName := "v1-sync-helper integration test committee (updated)"
	updatedDescription := "Updated by integration test"
	publicName := "Test Committee Public Name"
	if err := updateV1Committee(ctx, integrationTestProjectSFID, created.ID, projectServiceCommitteeUpdate{
		Name:          &updatedName,
		Description:   &updatedDescription,
		PublicEnabled: &publicDisabled,
		PublicName:    &publicName,
	}); err != nil {
		t.Fatalf("updateV1Committee: %v", err)
	}
	t.Logf("updated committee %s", created.ID)
}

// TestCommitteeMemberClientIntegration exercises createV1CommitteeMember, updateV1CommitteeMember,
// and deleteV1CommitteeMember against the real dev API, cleaning up after itself.
func TestCommitteeMemberClientIntegration(t *testing.T) {
	setupIntegrationTest(t)
	ctx := context.Background()

	// Create a parent committee to use for member operations.
	committee, err := createV1Committee(ctx, integrationTestProjectSFID, projectServiceCommitteeCreate{
		Name:     "v1-sync-helper member test committee",
		Category: "Working Group",
	})
	if err != nil {
		t.Fatalf("createV1Committee (setup): %v", err)
	}
	t.Logf("created parent committee: ID=%s", committee.ID)
	t.Cleanup(func() {
		if err := deleteV1Committee(ctx, integrationTestProjectSFID, committee.ID); err != nil {
			t.Errorf("deleteV1Committee cleanup: %v", err)
		} else {
			t.Logf("deleted parent committee %s", committee.ID)
		}
	})

	// --- Create member ---
	member, err := createV1CommitteeMember(ctx, integrationTestProjectSFID, committee.ID, projectServiceCommitteeMemberCreate{
		Email:     "integration-test@example.com",
		FirstName: "Integration",
		LastName:  "Test",
		Role:      "None",
		Status:    "Active",
	})
	if err != nil {
		t.Fatalf("createV1CommitteeMember: %v", err)
	}
	t.Logf("created member: ID=%s Email=%s", member.ID, member.Email)

	if member.ID == "" {
		t.Fatal("expected non-empty member ID in create response")
	}

	// Always clean up the member, even if the update step fails.
	t.Cleanup(func() {
		if err := deleteV1CommitteeMember(ctx, integrationTestProjectSFID, committee.ID, member.ID); err != nil {
			t.Errorf("deleteV1CommitteeMember cleanup: %v", err)
		} else {
			t.Logf("deleted member %s", member.ID)
		}
	})

	// --- Update member ---
	if err := updateV1CommitteeMember(ctx, integrationTestProjectSFID, committee.ID, member.ID, projectServiceCommitteeMemberUpdate{
		Role:   "Chair",
		Status: "Active",
	}); err != nil {
		t.Fatalf("updateV1CommitteeMember: %v", err)
	}
	t.Logf("updated member %s", member.ID)
}
