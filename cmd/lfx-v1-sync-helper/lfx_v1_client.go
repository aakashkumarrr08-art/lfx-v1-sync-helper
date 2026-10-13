// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// Auth0 authentication and HTTP client for LFX v1 API Gateway calls
//
// This client handles:
// 1. Auth0 private key JWT authentication for LFX v1 API access
// 2. User lookup via v1-objects KV bucket (replicated by Meltano from salesforce-merged_user)
// 3. Machine user detection (platform IDs ending with "@clients")
// 4. Organization lookup via v1 Organization Service API with intelligent caching
//
// User Types:
// - Machine users: platform IDs with "@clients" suffix (no lookup required)
// - Platform users: regular platform IDs looked up from v1-objects KV bucket

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/auth0/go-auth0/authentication"
	"github.com/auth0/go-auth0/authentication/oauth"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/oauth2"
)

const (
	// Cache settings for organization lookups
	orgCacheKeyPrefix         = "v1_org."
	orgLockKeyPrefix          = "v1_org_lock."
	orgCacheExpiry            = 30 * time.Minute // Treat org data as fresh for 30 minutes
	orgCacheStaleWhileRefresh = 6 * time.Hour    // Use stale data up to 6 hours with background refresh
	orgLockTimeout            = 10 * time.Second // Lock timeout for concurrent requests
	orgLockRetryInterval      = 1 * time.Second  // Retry interval when lock exists
	orgLockRetryAttempts      = 3                // Number of lock acquisition retry attempts
)

var (
	v1HTTPClient *http.Client
)

// V1User represents a user from the v1-objects KV bucket. Used for both salesforce-merged_user (b2c)
// and salesforce_b2b-User (b2b) records — FirstName, LastName, and Email are present in both schemas.
// For b2b users, Avatar is mapped from FullPhotoUrl; Username is not set since b2b Salesforce usernames
// are not LFID Auth0 identities.
type V1User struct {
	ID        string `json:"ID"`
	Username  string `json:"Username"`
	Email     string `json:"Email"`
	FirstName string `json:"FirstName"`
	LastName  string `json:"LastName"`
	Avatar    string `json:"Avatar"`
}

// V1Organization represents an organization from the LFX v1 Organization Service
type V1Organization struct {
	ID          string    `json:"ID"`
	Name        string    `json:"Name"`
	Domain      string    `json:"Domains"`
	LastFetched time.Time `json:"_last_fetched"` // Internal field for cache management
}

// V1OrganizationResponse represents the API response from v1 Organization Service
type V1OrganizationResponse struct {
	ID     string `json:"ID"`
	Name   string `json:"Name"`
	Domain string `json:"Domains"`
}

// V1OrganizationListResponse represents the list response from GET /v1/orgs/search
type V1OrganizationListResponse struct {
	Data []V1OrganizationResponse `json:"Data"`
}

// V1OrganizationCreateRequest is the request body for POST /v1/orgs
type V1OrganizationCreateRequest struct {
	Name    string `json:"Name"`
	Website string `json:"Website"`
}

// ClientCredentialsTokenSource implements oauth2.TokenSource for Auth0 private key JWT
type ClientCredentialsTokenSource struct {
	ctx        context.Context
	authConfig *authentication.Authentication
	audience   string
}

// v1TokenFetchTimeout bounds a single Auth0 client-credentials token fetch.
// Matches auth0CallTimeout (the other live Auth0 bound) in duration.
const v1TokenFetchTimeout = 20 * time.Second

// Token implements the oauth2.TokenSource interface to return a new access token
func (c *ClientCredentialsTokenSource) Token() (*oauth2.Token, error) {
	ctx := c.ctx
	if ctx == nil {
		ctx = context.TODO()
	}
	// Bound the fetch: oauth2.Transport calls Token() synchronously before the
	// base RoundTripper sees the request, so the caller's request context can
	// never interrupt a stalled Auth0 call. c.ctx is context.Background() in
	// production, making this the only bound on token acquisition.
	ctx, cancel := context.WithTimeout(ctx, v1TokenFetchTimeout)
	defer cancel()

	// Build and issue a request using Auth0 client credentials flow
	body := oauth.LoginWithClientCredentialsRequest{
		Audience: c.audience,
	}

	tokenSet, err := c.authConfig.OAuth.LoginWithClientCredentials(ctx, body, oauth.IDTokenValidationOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get Auth0 token: %w", err)
	}

	// Convert the Auth0 response to an oauth2.Token with leeway for clock skew
	const leeway = 60 * time.Second
	token := &oauth2.Token{
		AccessToken: tokenSet.AccessToken,
		TokenType:   tokenSet.TokenType,
		Expiry:      time.Now().Add(time.Duration(tokenSet.ExpiresIn)*time.Second - leeway),
	}

	return token, nil
}

// initV1Client initializes the Auth0 authentication and HTTP client for v1 API calls
func initV1Client(cfg *Config) error {
	// Create Auth0 client configuration with private key JWT
	authConfig, err := authentication.New(
		context.Background(),
		fmt.Sprintf("%s.auth0.com", cfg.Auth0Tenant),
		authentication.WithClientID(cfg.Auth0ClientID),
		authentication.WithClientAssertion(cfg.Auth0PrivateKey, "RS256"),
	)
	if err != nil {
		return fmt.Errorf("failed to create Auth0 client configuration: %w", err)
	}

	// Create HTTP client with Auth0 token source
	tokenSource := &ClientCredentialsTokenSource{
		ctx:        context.Background(),
		authConfig: authConfig,
		audience:   cfg.LFXAPIGateway.String(),
	}

	v1HTTPClient = oauth2.NewClient(context.Background(), tokenSource)

	return nil
}

// lookupMergedUser fetches user information live from the v1 platform
// database (salesforce.merged_user), including the primary email from
// salesforce.alternate_email__c.
func lookupMergedUser(ctx context.Context, platformID string) (*V1User, error) {
	row, err := dbLookupMergedUserRowBySFID(ctx, platformID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user data: %w", err)
	}
	if row == nil {
		return nil, fmt.Errorf("user %s not found or is deleted in v1 platform database", platformID)
	}
	return mergedUserRowToV1User(ctx, row)
}

// mergedUserRowToV1User converts a merged_user row to a V1User, enforcing the
// username validity rules and enriching with the user's primary email.
func mergedUserRowToV1User(ctx context.Context, row *mergedUserRow) (*V1User, error) {
	user := &V1User{
		ID: row.SFID,
	}

	// A username is required for Auth0 mapping, so bail out early if it's
	// missing. Values containing a space or "@" are bogus (e.g. an email
	// address stored where a username belongs by problematic SCORM user
	// syncing) and are rejected rather than synced verbatim into v2 as a
	// literal identifier (committee members, project/org FGA writer and
	// auditor lists, etc.).
	username := row.Username.String
	if username == "" {
		return nil, fmt.Errorf("user %s has no username in merged_user record", row.SFID)
	}
	if strings.ContainsAny(username, " @") {
		return nil, fmt.Errorf("user %s has an invalid username in merged_user record", row.SFID)
	}
	user.Username = username
	user.FirstName = row.FirstName.String
	user.LastName = row.LastName.String

	// Map avatar from photo_url__c (LF platform profile picture).
	user.Avatar = row.PhotoURL.String

	// Look up user's primary email from the alternate email table.
	if email, emailErr := getPrimaryEmailForUser(ctx, row.SFID); emailErr == nil && email != "" {
		user.Email = email
	} else if emailErr != nil {
		logger.With("platform_id", row.SFID, "error", emailErr).DebugContext(ctx, "failed to lookup primary email for user")
	}

	return user, nil
}

// lookupB2BUser fetches user information from the salesforce_b2b-User table via v1-objects KV bucket.
// B2B users (e.g. opportunity owners) live in the Salesforce B2B org. V1User is reused here because
// the fields we care about (FirstName, LastName, Email, Username, FullPhotoUrl→Avatar) exist in both schemas.
func lookupB2BUser(ctx context.Context, b2bUserID string) (*V1User, error) {
	userKey := fmt.Sprintf("salesforce_b2b-User.%s", b2bUserID)

	userData, exists, err := getV1ObjectData(ctx, userKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get b2b user data: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("b2b user %s not found or is deleted in v1-objects KV bucket", b2bUserID)
	}

	user := &V1User{ID: b2bUserID}

	if firstName, ok := userData["FirstName"].(string); ok {
		user.FirstName = firstName
	}
	if lastName, ok := userData["LastName"].(string); ok {
		user.LastName = lastName
	}
	if email, ok := userData["Email"].(string); ok {
		user.Email = email
	}
	if username, ok := userData["Username"].(string); ok {
		user.Username = username
	}
	if photoURL, ok := userData["FullPhotoUrl"].(string); ok {
		user.Avatar = photoURL
	}

	return user, nil
}

// getPrimaryEmailForUser retrieves the primary email address for a user from
// the v1 platform database: the active row flagged primary_email__c, falling
// back to the first active row when none is flagged.
func getPrimaryEmailForUser(ctx context.Context, userSfid string) (string, error) {
	email, err := dbGetPrimaryEmailForUser(ctx, userSfid)
	if err != nil {
		return "", err
	}
	if email == "" {
		return "", fmt.Errorf("no valid emails found for user %s", userSfid)
	}
	return email, nil
}

// errAmbiguousDefactoPrimaryEmail indicates that more than one of a user's
// alternate email rows qualifies (active and verified) and none is flagged
// primary, so which row (if any) should be treated as the de-facto primary
// is genuinely ambiguous. This is a deterministic v1 data condition, not a
// transient failure: retrying the same read will reach the same result
// until the underlying data is fixed, so callers should not request
// redelivery/retry for it, unlike other errors from
// unlike other errors from dbIsSoleQualifyingAlternateEmail.
var errAmbiguousDefactoPrimaryEmail = errors.New("ambiguous de-facto primary alternate email")

// ResolveV1UserSFIDByUsername looks up a v1 user SFID by username live in the
// v1 platform database.
// Returns (sfid, nil) on success, ("", nil) on miss, or ("", error) on failure.
func ResolveV1UserSFIDByUsername(ctx context.Context, username string) (string, error) {
	return dbResolveUserSFIDByUsername(ctx, username)
}

// resolveV1UserSFIDByUsernameFn is injectable for tests: it lets
// handleUserProfileUpdated be driven end-to-end (including its per-sfid
// skills-reconcile ordering guarantee) without a live v1 platform DB.
var resolveV1UserSFIDByUsernameFn = ResolveV1UserSFIDByUsername

// lookupUserByUsername looks up a username live in the v1 platform database
// and returns the resolved V1User and SFID in a single operation.
// Returns (nil, "") on any miss or error.
func lookupUserByUsername(ctx context.Context, username string) (*V1User, string) {
	row, err := dbLookupMergedUserRowByUsername(ctx, username)
	if err != nil {
		logger.With(errKey, err, "username", username).
			WarnContext(ctx, "failed to query v1 user by username for enrichment")
		return nil, ""
	}
	if row == nil {
		return nil, ""
	}
	user, err := mergedUserRowToV1User(ctx, row)
	if err != nil {
		logger.With(errKey, err, "username", username, "sfid", row.SFID).
			WarnContext(ctx, "failed to build v1 user for enrichment")
		return nil, ""
	}
	return user, row.SFID
}

// lookupUserByUsernameForACS is the v1 username resolver used during ACS project
// settings merge. Tests may replace it to stub v1 lookups.
var lookupUserByUsernameForACS = lookupUserByUsername

// ResolveV1UserSFIDByEmail looks up a v1 user SFID by email live in the v1
// platform database (matching any active alternate email address).
// Returns (sfid, nil) on success, ("", nil) on miss, or ("", error) on failure.
func ResolveV1UserSFIDByEmail(ctx context.Context, email string) (string, error) {
	return dbResolveUserSFIDByEmail(ctx, email)
}

// getOrganizationFromV1API fetches organization information from the LFX v1 Organization Service
func getV1OrganizationFromOrgSvc(ctx context.Context, sfid string) (*V1Organization, error) {
	url := fmt.Sprintf("%sorganization-service/v1/orgs/%s", cfg.LFXAPIGateway.String(), sfid)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("v1 Organization Service returned status %d: %s", resp.StatusCode, string(body))
	}

	var orgResponse V1OrganizationResponse
	if err := json.Unmarshal(body, &orgResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal organization response: %w", err)
	}

	// Convert to internal organization format with cache timestamp
	org := &V1Organization{
		ID:          orgResponse.ID,
		Name:        orgResponse.Name,
		Domain:      orgResponse.Domain,
		LastFetched: time.Now().UTC(),
	}

	return org, nil
}

// normalizeDomain strips protocol, www prefix, and any path component from a website
// string and lowercases it, producing a bare hostname suitable for exact domain comparison.
func normalizeDomain(website string) string {
	s := strings.ToLower(strings.TrimSpace(website))
	for _, prefix := range []string{"https://", "http://"} {
		s = strings.TrimPrefix(s, prefix)
	}
	s = strings.TrimPrefix(s, "www.")
	if i := strings.Index(s, "/"); i != -1 {
		s = s[:i]
	}
	return s
}

// searchV1OrgsByWebsite searches for organizations in the v1 Organization Service by website.
// Returns the organization whose domain exactly matches the search website, or nil if none found.
// The org-service search uses a substring LIKE query, so results may include unrelated orgs
// whose website merely contains the search term; this function filters to an exact domain match.
func searchV1OrgsByWebsite(ctx context.Context, website string) (*V1Organization, error) {
	baseURL := fmt.Sprintf("%sorganization-service/v1/orgs/search", cfg.LFXAPIGateway.String())
	params := url.Values{}
	params.Set("website", website)
	fullURL := baseURL + "?" + params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create org search request: %w", err)
	}

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send org search request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read org search response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("org service returned status %d searching by website=%q: %s", resp.StatusCode, website, string(body))
	}

	var listResp V1OrganizationListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal org search response: %w", err)
	}

	searchDomain := normalizeDomain(website)
	for _, org := range listResp.Data {
		if normalizeDomain(org.Domain) == searchDomain {
			return &V1Organization{
				ID:          org.ID,
				Name:        org.Name,
				Domain:      org.Domain,
				LastFetched: time.Now().UTC(),
			}, nil
		}
	}

	logger.With("website", website, "result_count", len(listResp.Data)).
		DebugContext(ctx, "org search returned no exact domain match, falling through to org creation")
	return nil, nil
}

// createV1OrgInOrgSvc creates a new organization in the v1 Organization Service.
func createV1OrgInOrgSvc(ctx context.Context, name, website string) (*V1Organization, error) {
	apiURL := fmt.Sprintf("%sorganization-service/v1/orgs", cfg.LFXAPIGateway.String())

	reqBody, err := json.Marshal(V1OrganizationCreateRequest{Name: name, Website: website})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal org create request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create org create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send org create request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read org create response: %w", err)
	}

	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("org service returned status %d creating org name=%q website=%q: %s", resp.StatusCode, name, website, string(body))
	}

	var orgResp V1OrganizationResponse
	if err := json.Unmarshal(body, &orgResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal org create response: %w", err)
	}

	return &V1Organization{
		ID:          orgResp.ID,
		Name:        orgResp.Name,
		Domain:      orgResp.Domain,
		LastFetched: time.Now().UTC(),
	}, nil
}

// resolveV1OrgID resolves a v1 Organization SFID by searching by website first.
// If not found and both name and website are provided, it creates a new org.
// Returns empty string (no error) if the org cannot be found and there is insufficient data to create one.
func resolveV1OrgID(ctx context.Context, name, website string) (string, error) {
	if website != "" {
		org, err := searchV1OrgsByWebsite(ctx, website)
		if err != nil {
			return "", fmt.Errorf("org search failed: %w", err)
		}
		if org != nil {
			return org.ID, nil
		}
	}

	if name == "" || website == "" {
		logger.With("name", name, "website", website).
			DebugContext(ctx, "insufficient data to create v1 organization, skipping org resolution")
		return "", nil
	}

	created, err := createV1OrgInOrgSvc(ctx, name, website)
	if err != nil {
		return "", fmt.Errorf("org create failed: %w", err)
	}
	return created.ID, nil
}

// getCachedV1Org retrieves an organization from the mappings cache
func getCachedV1Org(ctx context.Context, sfid string) (*V1Organization, error) {
	cacheKey := orgCacheKeyPrefix + sfid

	entry, err := mappingStore.Get(ctx, cacheKey)
	if err != nil {
		return nil, err // No cached entry
	}

	var org V1Organization
	if err := json.Unmarshal(entry.Value, &org); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cached organization: %w", err)
	}

	return &org, nil
}

// setCachedOrg stores an organization in the mappings cache
func setCachedV1Org(ctx context.Context, sfid string, org *V1Organization) error {
	cacheKey := orgCacheKeyPrefix + sfid

	data, err := json.Marshal(org)
	if err != nil {
		return fmt.Errorf("failed to marshal organization for cache: %w", err)
	}

	_, err = mappingStore.Put(ctx, cacheKey, data)
	return err
}

// acquireOrgLock attempts to acquire a lock for organization refresh operations with retries
// Returns (acquired, waited) where waited indicates if any retry attempts were made
func acquireV1OrgLock(ctx context.Context, sfid string, maxRetries int) (bool, bool) {
	lockKey := orgLockKeyPrefix + sfid
	var waited bool

	for attempt := 1; attempt <= maxRetries; attempt++ {
		lockValue := strconv.FormatInt(time.Now().Unix(), 10)

		// Try to create the lock (will fail if it already exists)
		_, err := mappingStore.Create(ctx, lockKey, []byte(lockValue))
		if err == nil {
			return true, waited // Successfully acquired lock
		}

		// Check if lock already exists and if it's stale
		if entry, getErr := mappingStore.Get(ctx, lockKey); getErr == nil {
			if lockTimestamp, parseErr := strconv.ParseInt(string(entry.Value), 10, 64); parseErr == nil {
				lockTime := time.Unix(lockTimestamp, 0)
				if time.Since(lockTime) > orgLockTimeout {
					// Lock is stale, try to update it
					if _, updateErr := mappingStore.Put(ctx, lockKey, []byte(lockValue)); updateErr == nil {
						return true, waited
					}
				}
			}
		}

		// If this isn't the last attempt, wait before retrying
		if attempt < maxRetries {
			waited = true
			time.Sleep(orgLockRetryInterval)
		}
	}

	return false, waited // Failed to acquire lock after all attempts
}

// releaseOrgLock releases an organization refresh lock
func releaseV1OrgLock(ctx context.Context, sfid string) error {
	lockKey := orgLockKeyPrefix + sfid
	return mappingStore.Delete(ctx, lockKey)
}

// refreshOrgInBackground refreshes organization data in the background
func refreshV1OrgInBackground(ctx context.Context, sfid string) {
	go func() {
		// Acquire lock for this refresh operation
		acquired, _ := acquireV1OrgLock(ctx, sfid, 1)
		if !acquired {
			return // Another process is already refreshing
		}

		defer func() {
			if releaseErr := releaseV1OrgLock(ctx, sfid); releaseErr != nil {
				logger.With(errKey, releaseErr, "org_sfid", sfid).WarnContext(ctx, "failed to release organization cache lock")
			}
		}()

		// Fetch fresh organization data
		org, err := getV1OrganizationFromOrgSvc(ctx, sfid)
		if err != nil {
			logger.With(errKey, err, "org_sfid", sfid).WarnContext(ctx, "background organization refresh failed")
			return
		}

		// Update cache
		if err := setCachedV1Org(ctx, sfid, org); err != nil {
			logger.With(errKey, err, "org_sfid", sfid).WarnContext(ctx, "failed to update organization cache after refresh")
		} else {
			logger.With("org_sfid", sfid, "name", org.Name).DebugContext(ctx, "organization cache refreshed in background")
		}
	}()
}

// lookupOrg retrieves organization information with caching and refresh logic
func lookupV1Org(ctx context.Context, sfid string) (*V1Organization, error) {
	if sfid == "" {
		return nil, fmt.Errorf("organization SFID cannot be empty")
	}

	// Try to get from cache first
	cachedOrg, err := getCachedV1Org(ctx, sfid)
	if err == nil {
		age := time.Since(cachedOrg.LastFetched)
		// See if cache is still within the "stale" window.
		if age <= orgCacheStaleWhileRefresh {
			if age > orgCacheExpiry {
				// Cache is stale: refresh in background.
				refreshV1OrgInBackground(ctx, sfid)
			}
			return cachedOrg, nil
		}
		// Fall through if cache is *too* old (past "stale" window).
	}

	// Try to acquire lock.
	acquired, waited := acquireV1OrgLock(ctx, sfid, orgLockRetryAttempts)

	if acquired {
		// We got the lock, set up defer to release it
		defer func() {
			if releaseErr := releaseV1OrgLock(ctx, sfid); releaseErr != nil {
				logger.With(errKey, releaseErr, "org_sfid", sfid).WarnContext(ctx, "failed to release organization lookup lock")
			}
		}()
	}

	// If we waited, check cache again - another process might have populated it
	if waited {
		if freshOrg, cacheErr := getCachedV1Org(ctx, sfid); cacheErr == nil {
			if time.Since(freshOrg.LastFetched) <= orgCacheExpiry {
				// Cache is now fresh, return it
				return freshOrg, nil
			}
		}
		// Fall through to fetch fresh data.
	}

	// Fetch from API
	org, err := getV1OrganizationFromOrgSvc(ctx, sfid)
	if err != nil {
		// Cache the error state to avoid repeated failed lookups
		errorOrg := &V1Organization{
			ID:          sfid,
			Name:        "", // Empty name indicates error state
			Domain:      "",
			LastFetched: time.Now().UTC(),
		}
		if cacheErr := setCachedV1Org(ctx, sfid, errorOrg); cacheErr != nil {
			logger.With(errKey, cacheErr, "org_sfid", sfid).WarnContext(ctx, "failed to cache error state for organization")
		}
		return nil, err
	}

	// Validate required fields
	if org.Name == "" {
		logger.With("org_sfid", sfid).WarnContext(ctx, "v1 organization has empty name")
		// Cache the invalid state
		invalidOrg := &V1Organization{
			ID:          sfid,
			Name:        "", // Empty name indicates invalid state
			Domain:      "",
			LastFetched: time.Now().UTC(),
		}
		if cacheErr := setCachedV1Org(ctx, sfid, invalidOrg); cacheErr != nil {
			logger.With(errKey, cacheErr, "org_sfid", sfid).WarnContext(ctx, "failed to cache invalid state for organization")
		}
		return nil, fmt.Errorf("organization %s has invalid data (empty name)", sfid)
	}

	// Cache the valid organization data
	if err := setCachedV1Org(ctx, sfid, org); err != nil {
		logger.With(errKey, err, "org_sfid", sfid).WarnContext(ctx, "failed to cache organization data")
	}

	return org, nil
}

// projectServiceCommitteeCreate is the request body for POST /v2/projects/{projectId}/committees.
type projectServiceCommitteeCreate struct {
	Name            string `json:"Name"`
	Category        string `json:"Category"`
	Description     string `json:"Description,omitempty"`
	Website         string `json:"CommitteeWebsite,omitempty"`
	CommitteeID     string `json:"CommitteeID,omitempty"` // parent committee ID if creating a subcommittee
	SSOGroupEnabled *bool  `json:"SSOGroupEnabled,omitempty"`
	PublicEnabled   *bool  `json:"PublicEnabled,omitempty"`
	PublicName      string `json:"PublicName,omitempty"`
	SSOGroupName    string `json:"SSOGroupName,omitempty"`
	JoinMode        string `json:"JoinMode,omitempty"`
	MailingList     string `json:"MailingList,omitempty"`
	ChatChannel     string `json:"ChatChannel,omitempty"`
}

// projectServiceCommitteeUpdate is the request body for PATCH /v2/projects/{projectId}/committees/{committeeID}.
type projectServiceCommitteeUpdate struct {
	Name            *string `json:"Name,omitempty"`
	Category        *string `json:"Category,omitempty"`
	Description     *string `json:"Description,omitempty"`
	Website         *string `json:"CommitteeWebsite,omitempty"`
	CommitteeID     *string `json:"CommitteeID,omitempty"` // parent committee ID if creating a subcommittee
	SSOGroupEnabled *bool   `json:"SSOGroupEnabled,omitempty"`
	PublicEnabled   *bool   `json:"PublicEnabled,omitempty"`
	PublicName      *string `json:"PublicName,omitempty"`
	SSOGroupName    *string `json:"SSOGroupName,omitempty"`
	JoinMode        *string `json:"JoinMode,omitempty"`
	MailingList     *string `json:"MailingList,omitempty"`
	ChatChannel     *string `json:"ChatChannel,omitempty"`
}

// projectServiceCommitteeResponse is the response from the project service for creating and updating committees.
type projectServiceCommitteeResponse struct {
	ID              string `json:"ID"`
	Name            string `json:"Name"`
	Category        string `json:"Category"`
	Description     string `json:"Description"`
	Website         string `json:"CommitteeWebsite"`
	CommitteeID     string `json:"CommitteeID"`
	SSOGroupEnabled bool   `json:"SSOGroupEnabled"`
	PublicEnabled   bool   `json:"PublicEnabled"`
	PublicName      string `json:"PublicName"`
	SSOGroupName    string `json:"SSOGroupName"`
}

// createCommittee creates a committee in Postgres via the Project Service v2 API.
// projectSFID is the v1 Salesforce ID of the parent project.
func createV1Committee(ctx context.Context, projectSFID string, payload projectServiceCommitteeCreate) (*projectServiceCommitteeResponse, error) {
	apiURL := fmt.Sprintf("%sproject-service/v2/projects/%s/committees", cfg.LFXAPIGateway.String(), projectSFID)

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal create committee payload: %w", err)
	}

	logger.DebugContext(ctx, "createV1Committee request", "url", apiURL, "payload", string(body))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send create committee request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read create committee response: %w", err)
	}

	logger.DebugContext(ctx, "createV1Committee response", "status", resp.StatusCode, "url", resp.Request.URL.String(), "body", string(respBody))

	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("project service returned status %d creating committee for project %s: %s", resp.StatusCode, projectSFID, string(respBody))
	}

	var result projectServiceCommitteeResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal create committee response: %w", err)
	}

	return &result, nil
}

// updateV1Committee updates a committee in Postgres via the Project Service v2 API.
// projectSFID is the v1 Salesforce ID of the parent project.
// committeeSFID is the v1 Salesforce ID of the committee.
func updateV1Committee(ctx context.Context, projectSFID, committeeSFID string, payload projectServiceCommitteeUpdate) error {
	apiURL := fmt.Sprintf("%sproject-service/v2/projects/%s/committees/%s", cfg.LFXAPIGateway.String(), projectSFID, committeeSFID)

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal update committee payload: %w", err)
	}

	logger.DebugContext(ctx, "updateV1Committee request", "url", apiURL, "payload", string(body))

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, apiURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send update committee request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read update committee response: %w", err)
	}

	logger.DebugContext(ctx, "updateV1Committee response", "status", resp.StatusCode, "body", string(respBody))

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("project service returned status %d updating committee %s for project %s: %s", resp.StatusCode, committeeSFID, projectSFID, string(respBody))
	}

	return nil
}

// deleteV1Committee deletes a committee in Postgres via the Project Service v2 API.
// projectSFID is the v1 Salesforce ID of the parent project.
// committeeSFID is the v1 Salesforce ID of the committee.
func deleteV1Committee(ctx context.Context, projectSFID, committeeSFID string) error {
	apiURL := fmt.Sprintf("%sproject-service/v2/projects/%s/committees/%s", cfg.LFXAPIGateway.String(), projectSFID, committeeSFID)

	logger.DebugContext(ctx, "deleteV1Committee request", "url", apiURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, apiURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send delete committee request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, _ := io.ReadAll(resp.Body)
	logger.DebugContext(ctx, "deleteV1Committee response", "status", resp.StatusCode, "body", string(respBody))

	// Treat 404 as already-deleted (idempotent — safe for JetStream redelivery when V1
	// completed the delete but its response was lost).
	if resp.StatusCode == http.StatusNotFound {
		logger.With("project_sfid", projectSFID, "committee_sfid", committeeSFID).
			InfoContext(ctx, "v1 committee already absent on delete, treating as success")
		return nil
	}
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("project service returned status %d deleting committee %s for project %s: %s", resp.StatusCode, committeeSFID, projectSFID, string(respBody))
	}

	return nil
}

// projectServiceProjectCreate is the request body for POST /project-service/v1/projects.
// Field names mirror the v1 project-service HTTP API contract (verified against the
// TypeScript consumers in linuxfoundation/lfx-pcc — packages/lfx-pcc/interfaces/project.d.ts
// and apps/v1-backend/src/helpers/environment-reset/data/data-type.d.ts).
//
// omitempty is used everywhere except Name and ProjectType so unset v2 fields don't
// overwrite v1 defaults or existing values. Only fields the v2 project model actually
// exposes are represented here; the richer v1-only legal metadata (MSA URLs, industry
// sector, etc.) is intentionally left unset per LFXV2 self-serve #2003.
type projectServiceProjectCreate struct {
	Name                           string   `json:"Name"`
	ProjectType                    string   `json:"ProjectType"`
	Slug                           string   `json:"Slug,omitempty"`
	Description                    string   `json:"Description,omitempty"`
	Parent                         string   `json:"Parent,omitempty"` // parent project SFID
	Status                         string   `json:"Status,omitempty"` // v2 stage
	Category                       string   `json:"Category,omitempty"`
	EntityName                     string   `json:"EntityName,omitempty"`
	EntityType                     string   `json:"EntityType,omitempty"` // v2 legal_entity_type
	Funding                        string   `json:"Funding,omitempty"`
	Model                          []string `json:"Model,omitempty"` // v2 funding_model
	CharterURL                     string   `json:"CharterURL,omitempty"`
	AutoJoinEnabled                *bool    `json:"AutoJoinEnabled,omitempty"`
	StartDate                      string   `json:"StartDate,omitempty"` // v2 formation_date, date-only
	ProjectLogo                    string   `json:"ProjectLogo,omitempty"`
	RepositoryURL                  string   `json:"RepositoryURL,omitempty"`
	Website                        string   `json:"Website,omitempty"`
	ProjectEntityDissolutionDate   string   `json:"ProjectEntityDissolutionDate,omitempty"`   // date-only
	ProjectEntityFormationDocument string   `json:"ProjectEntityFormationDocument,omitempty"` // URL
	LegalParentID                  string   `json:"LegalParentID,omitempty"`                  // legal parent SFID
}

// projectServiceProjectUpdate is the request body for PATCH /project-service/v1/projects/{sfid}.
// Nil fields are omitted (leave v1 untouched); non-nil empty values are sent
// explicitly so the v1 PATCH can clear a field.
type projectServiceProjectUpdate struct {
	Name                           *string   `json:"Name,omitempty"`
	Slug                           *string   `json:"Slug,omitempty"`
	Description                    *string   `json:"Description,omitempty"`
	Parent                         *string   `json:"Parent,omitempty"`
	ProjectType                    *string   `json:"ProjectType,omitempty"` // v2 is_foundation: true→"Project Group", false→"Project"
	Status                         *string   `json:"Status,omitempty"`
	Category                       *string   `json:"Category,omitempty"`
	EntityName                     *string   `json:"EntityName,omitempty"`
	EntityType                     *string   `json:"EntityType,omitempty"`
	Funding                        *string   `json:"Funding,omitempty"`
	Model                          *[]string `json:"Model,omitempty"`
	CharterURL                     *string   `json:"CharterURL,omitempty"`
	AutoJoinEnabled                *bool     `json:"AutoJoinEnabled,omitempty"`
	StartDate                      *string   `json:"StartDate,omitempty"`
	ProjectLogo                    *string   `json:"ProjectLogo,omitempty"`
	RepositoryURL                  *string   `json:"RepositoryURL,omitempty"`
	Website                        *string   `json:"Website,omitempty"`
	ProjectEntityDissolutionDate   *string   `json:"ProjectEntityDissolutionDate,omitempty"`
	ProjectEntityFormationDocument *string   `json:"ProjectEntityFormationDocument,omitempty"`
	LegalParentID                  *string   `json:"LegalParentID,omitempty"`
}

// projectServiceProjectResponse is the relevant subset of the response returned by
// POST /project-service/v1/projects. The v1 API returns the full project record but we
// only need the SFID (ID) to persist the v1↔v2 mapping.
type projectServiceProjectResponse struct {
	ID   string `json:"ID"`
	Name string `json:"Name"`
	Slug string `json:"Slug"`
}

// createV1Project creates a project in v1 via POST /project-service/v1/projects.
// Called on receipt of an lfx.project.created indexer event whose reverse mapping
// is absent (i.e. the v2 project did not originate in v1).
func createV1Project(ctx context.Context, payload projectServiceProjectCreate) (*projectServiceProjectResponse, error) {
	apiURL := fmt.Sprintf("%sproject-service/v1/projects", cfg.LFXAPIGateway.String())

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal create project payload: %w", err)
	}

	logger.DebugContext(ctx, "createV1Project request", "url", apiURL, "payload", string(body))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send create project request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read create project response: %w", err)
	}

	logger.DebugContext(ctx, "createV1Project response", "status", resp.StatusCode, "url", resp.Request.URL.String(), "body", string(respBody))

	// The v1 project-service returns 201 Created on success (verified via the
	// lfx-pcc data-generator consumer). Accept 200 too in case the shape ever
	// changes; anything else is an error.
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("project service returned status %d creating project: %s", resp.StatusCode, string(respBody))
	}

	var result projectServiceProjectResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal create project response: %w", err)
	}

	return &result, nil
}

// updateV1Project patches a project in v1 via PATCH /project-service/v1/projects/{sfid}.
func updateV1Project(ctx context.Context, projectSFID string, payload projectServiceProjectUpdate) error {
	apiURL := fmt.Sprintf("%sproject-service/v1/projects/%s", cfg.LFXAPIGateway.String(), projectSFID)

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal update project payload: %w", err)
	}

	logger.DebugContext(ctx, "updateV1Project request", "url", apiURL, "payload", string(body))

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, apiURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send update project request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read update project response: %w", err)
	}

	logger.DebugContext(ctx, "updateV1Project response", "status", resp.StatusCode, "body", string(respBody))

	// Accept 200 (typical PATCH) and 204 (No Content).
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("project service returned status %d updating project %s: %s", resp.StatusCode, projectSFID, string(respBody))
	}

	return nil
}

// deleteV1Project deletes a project in v1 via DELETE /project-service/v1/projects/{sfid}.
func deleteV1Project(ctx context.Context, projectSFID string) error {
	apiURL := fmt.Sprintf("%sproject-service/v1/projects/%s", cfg.LFXAPIGateway.String(), projectSFID)

	logger.DebugContext(ctx, "deleteV1Project request", "url", apiURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, apiURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send delete project request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, _ := io.ReadAll(resp.Body)
	logger.DebugContext(ctx, "deleteV1Project response", "status", resp.StatusCode, "body", string(respBody))

	// Accept 204 (No Content — standard for DELETE) and 200. Treat 404 as
	// already-deleted (idempotent — safe for delete retries and for the case
	// where the v1 record was removed out of band).
	if resp.StatusCode == http.StatusNotFound {
		logger.With("project_sfid", projectSFID).InfoContext(ctx, "v1 project already absent on delete, treating as success")
		return nil
	}
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("project service returned status %d deleting project %s: %s", resp.StatusCode, projectSFID, string(respBody))
	}

	return nil
}

// projectServiceCommitteeMemberCreate is the request body for POST /v2/projects/{projectId}/committees/{committeeID}/members.
type projectServiceCommitteeMemberCreate struct {
	Email           string `json:"Email"`
	MemberID        string `json:"MemberID,omitempty"`
	OrganizationID  string `json:"OrganizationID,omitempty"`
	FirstName       string `json:"FirstName,omitempty"`
	LastName        string `json:"LastName,omitempty"`
	Title           string `json:"Title,omitempty"`
	Role            string `json:"Role,omitempty"`
	RoleStartDate   string `json:"RoleStartDate,omitempty"`
	RoleEndDate     string `json:"RoleEndDate,omitempty"`
	Status          string `json:"Status,omitempty"`
	AppointedBy     string `json:"AppointedBy,omitempty"`
	VotingStatus    string `json:"VotingStatus,omitempty"`
	VotingStartDate string `json:"VotingStartDate,omitempty"`
	VotingEndDate   string `json:"VotingEndDate,omitempty"`
	Agency          string `json:"Agency,omitempty"`
	Country         string `json:"Country,omitempty"`
}

// projectServiceCommitteeMemberUpdate is the request body for PATCH /v2/projects/{projectId}/committees/{committeeID}/members/{memberID}.
type projectServiceCommitteeMemberUpdate struct {
	Email           string `json:"Email,omitempty"`
	OrganizationID  string `json:"OrganizationID,omitempty"`
	Title           string `json:"Title,omitempty"`
	Role            string `json:"Role,omitempty"`
	RoleStartDate   string `json:"RoleStartDate,omitempty"`
	RoleEndDate     string `json:"RoleEndDate,omitempty"`
	Status          string `json:"Status,omitempty"`
	AppointedBy     string `json:"AppointedBy,omitempty"`
	VotingStatus    string `json:"VotingStatus,omitempty"`
	VotingStartDate string `json:"VotingStartDate,omitempty"`
	VotingEndDate   string `json:"VotingEndDate,omitempty"`
	Agency          string `json:"Agency,omitempty"`
	Country         string `json:"Country,omitempty"`
}

// projectServiceCommitteeMemberResponse is the relevant subset of the response returned
// by the create committee member operation.
type projectServiceCommitteeMemberResponse struct {
	ID             string `json:"ID"`
	MemberID       string `json:"MemberID"`
	OrganizationID string `json:"OrganizationID"`
	Email          string `json:"Email"`
	FirstName      string `json:"FirstName"`
	LastName       string `json:"LastName"`
	Role           string `json:"Role"`
	Status         string `json:"Status"`
	VotingStatus   string `json:"VotingStatus"`
}

// createV1CommitteeMember adds a member to a committee via the Project Service v2 API.
func createV1CommitteeMember(ctx context.Context, projectSFID, committeeSFID string, payload projectServiceCommitteeMemberCreate) (*projectServiceCommitteeMemberResponse, error) {
	apiURL := fmt.Sprintf("%sproject-service/v2/projects/%s/committees/%s/members", cfg.LFXAPIGateway.String(), projectSFID, committeeSFID)

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal create committee member payload: %w", err)
	}

	logger.DebugContext(ctx, "createV1CommitteeMember request", "url", apiURL, "payload", string(body))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send create committee member request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read create committee member response: %w", err)
	}

	logger.DebugContext(ctx, "createV1CommitteeMember response", "status", resp.StatusCode, "body", string(respBody))

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("project service returned status %d creating member for committee %s: %s", resp.StatusCode, committeeSFID, string(respBody))
	}

	var result projectServiceCommitteeMemberResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal create committee member response: %w", err)
	}

	return &result, nil
}

// updateV1CommitteeMember updates a committee member via the Project Service v2 API.
func updateV1CommitteeMember(ctx context.Context, projectSFID, committeeSFID, memberSFID string, payload projectServiceCommitteeMemberUpdate) error {
	apiURL := fmt.Sprintf("%sproject-service/v2/projects/%s/committees/%s/members/%s", cfg.LFXAPIGateway.String(), projectSFID, committeeSFID, memberSFID)

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal update committee member payload: %w", err)
	}

	logger.DebugContext(ctx, "updateV1CommitteeMember request", "url", apiURL, "payload", string(body))

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, apiURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send update committee member request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read update committee member response: %w", err)
	}

	logger.DebugContext(ctx, "updateV1CommitteeMember response", "status", resp.StatusCode, "body", string(respBody))

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("project service returned status %d updating member %s in committee %s: %s", resp.StatusCode, memberSFID, committeeSFID, string(respBody))
	}

	return nil
}

// deleteV1CommitteeMember removes a member from a committee via the Project Service v2 API.
func deleteV1CommitteeMember(ctx context.Context, projectSFID, committeeSFID, memberSFID string) error {
	apiURL := fmt.Sprintf("%sproject-service/v2/projects/%s/committees/%s/members/%s", cfg.LFXAPIGateway.String(), projectSFID, committeeSFID, memberSFID)

	logger.DebugContext(ctx, "deleteV1CommitteeMember request", "url", apiURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, apiURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := v1HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send delete committee member request: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, _ := io.ReadAll(resp.Body)
	logger.DebugContext(ctx, "deleteV1CommitteeMember response", "status", resp.StatusCode, "body", string(respBody))

	// Treat 404 as already-deleted (idempotent — safe for JetStream redelivery when V1
	// completed the delete but its response was lost).
	if resp.StatusCode == http.StatusNotFound {
		logger.With("project_sfid", projectSFID, "committee_sfid", committeeSFID, "member_sfid", memberSFID).
			InfoContext(ctx, "v1 committee member already absent on delete, treating as success")
		return nil
	}
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("project service returned status %d deleting member %s from committee %s: %s", resp.StatusCode, memberSFID, committeeSFID, string(respBody))
	}

	return nil
}

// parseWebsiteURL attempts to parse and normalize a website URL from organization website data.
// Returns empty string if no valid URL can be constructed.
func parseWebsiteURL(website string) string {
	websiteTrimmed := strings.TrimSpace(website)
	if websiteTrimmed != "" {
		// The website attribute typically contains just a domain name
		websiteURL := "http://" + websiteTrimmed
		if parsedURL, err := url.Parse(websiteURL); err == nil {
			return parsedURL.String()
		}
	}

	return ""
}

// getV1ObjectData retrieves and unmarshals data from the v1-objects KV bucket with dual-format support.
// It attempts JSON decoding first, then falls back to msgpack if JSON fails.
// Returns (data, exists, error) where exists indicates if the record exists and is not deleted/tombstoned.
// This abstraction should be used for all v1-objects bucket reads to ensure consistent
// dual-format handling across the codebase.
func getV1ObjectData(ctx context.Context, key string) (map[string]any, bool, error) {
	entry, err := v1KV.Get(ctx, key)
	if err != nil {
		if err == jetstream.ErrKeyNotFound || err == jetstream.ErrKeyDeleted {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to get data from v1-objects KV bucket: %w", err)
	}

	// Check if this is a tombstone marker.
	if isTombstonedMapping(entry.Value()) {
		return nil, false, nil
	}

	var data map[string]any
	if err := json.Unmarshal(entry.Value(), &data); err != nil {
		// Try msgpack if JSON fails.
		if msgpackErr := msgpack.Unmarshal(entry.Value(), &data); msgpackErr != nil {
			return nil, false, fmt.Errorf("failed to unmarshal data (json: %w, msgpack: %w)", err, msgpackErr)
		}
	}

	// Check for WAL-based soft deletes first: a non-empty _sdc_deleted_at means
	// the source DB row was physically deleted and the WAL handler preserved the
	// last-known image with this marker. If this is set there is no need to also
	// check the SFDC-semantic isdeleted flag.
	if deletedAt, ok := data["_sdc_deleted_at"]; ok {
		if s, okStr := deletedAt.(string); (okStr && strings.TrimSpace(s) != "") || (!okStr && deletedAt != nil) {
			return nil, false, nil
		}
	}

	// Check for the Salesforce-semantic soft deletion flag. isdeleted is rarely
	// set in LFX (SFDC soft-deletes shouldn't be seen outside the
	// salesforce_b2b schema, and perhaps not even there), but we check for
	// exhaustiveness so the caller never sees a logically deleted record.
	if isDeleted, ok := data["isdeleted"].(bool); ok && isDeleted {
		return nil, false, nil
	}

	return data, true, nil
}
