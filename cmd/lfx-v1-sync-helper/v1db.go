// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// Live PostgreSQL access to the LFX v1 platform database (replicated
// Salesforce schema). This replaces the NATS KV secondary indexes
// (v1-user.username.*, v1-user.email.*, v1-merged-user.alternate-emails.*,
// v1-user.primary-email.*) that previously required event-driven maintenance
// and periodic full rebuilds. Queries here are read-only; the tables are
// owned by the v1 platform replication pipeline, not this service.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

// v1DB is the bun handle for the v1 platform database, initialized by initV1DB.
var v1DB *bun.DB

// v1DBQueryTimeout bounds every db* query below with its own deadline,
// independent of the caller's context. NATS lookup handlers invoke these
// resolvers with context.Background(), so without a bounded deadline here a
// stalled database query would hang the handler goroutine indefinitely
// instead of returning an error.
const v1DBQueryTimeout = 10 * time.Second

// withQueryTimeout returns a context bounded by v1DBQueryTimeout and its
// cancel function. Callers must defer the returned cancel.
func withQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, v1DBQueryTimeout)
}

// mergedUserRow maps the columns we need from salesforce.merged_user.
type mergedUserRow struct {
	bun.BaseModel `bun:"table:salesforce.merged_user,alias:mu"`

	SFID      string         `bun:"sfid"`
	Username  sql.NullString `bun:"username__c"`
	FirstName sql.NullString `bun:"firstname"`
	LastName  sql.NullString `bun:"lastname"`
	PhotoURL  sql.NullString `bun:"photo_url__c"`
	IsDeleted sql.NullBool   `bun:"isdeleted"`
}

type mergedUserUsernameRow struct {
	bun.BaseModel `bun:"table:salesforce.merged_user,alias:mu"`

	Username sql.NullString `bun:"username__c"`
}

// alternateEmailRow maps the columns we need from salesforce.alternate_email__c.
type alternateEmailRow struct {
	bun.BaseModel `bun:"table:salesforce.alternate_email__c,alias:ae"`

	SFID             string         `bun:"sfid"`
	LeadOrContactID  sql.NullString `bun:"leadorcontactid"`
	EmailAddress     sql.NullString `bun:"alternate_email_address__c"`
	IsPrimary        sql.NullBool   `bun:"primary_email__c"`
	IsVerified       sql.NullBool   `bun:"email_verified__c"`
	IsActive         sql.NullBool   `bun:"active__c"`
	IsDeleted        sql.NullBool   `bun:"isdeleted"`
	LastModifiedDate sql.NullTime   `bun:"lastmodifieddate"`
}

// userSkillRow maps the columns we need from salesforce.user_skills joined to
// salesforce.skills. It is keyed by lfid, not sfid: user_skills is a plain
// platform table, not a Salesforce __c object.
type userSkillRow struct {
	bun.BaseModel `bun:"table:salesforce.user_skills,alias:usk"`

	ID   string `bun:"id"`
	Name string `bun:"sk_name"`
}

// initV1DB opens a pgx connection pool against the v1 platform database and
// wires bun on top of it. The DSN comes from cfg.DatabaseURL (DATABASE_URL).
func initV1DB(ctx context.Context, cfg *Config) error {
	if cfg.DatabaseURL == "" {
		return errors.New("DATABASE_URL environment variable is required")
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("failed to create pgx pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("failed to ping v1 platform database: %w", err)
	}

	// Wrap the pgx pool as a *sql.DB for bun.
	sqlDB := stdlib.OpenDBFromPool(pool)
	v1DB = bun.NewDB(sqlDB, pgdialect.New())
	return nil
}

// activeEmailFilter appends the shared liveness conditions for alternate
// email rows: not soft-deleted, active__c true, not a ".old"-suffixed
// address (a v1 convention for deactivating an address without flipping
// active__c), and belonging to a merged_user that is itself not
// soft-deleted (otherwise a soft-deleted user's stale email rows could
// still resolve an SFID).
func activeEmailFilter(q *bun.SelectQuery) *bun.SelectQuery {
	return q.
		Join("JOIN salesforce.merged_user AS mu ON mu.sfid = ae.leadorcontactid").
		Where("ae.isdeleted IS NOT TRUE").
		Where("ae.active__c IS TRUE").
		Where("LOWER(ae.alternate_email_address__c) NOT LIKE '%.old'").
		Where("mu.isdeleted IS NOT TRUE")
}

// dbResolveUserSFIDByUsername resolves a v1 user SFID by username with
// case-insensitive matching. The input is normalized with
// normalizeUserIdentifier (trim + lower) in Go; column values are matched via
// LOWER(...) only (no TRIM: confirmed via direct query against dev/staging/prod
// that no rows have leading/trailing whitespace in username__c or
// alternate_email_address__c, and wrapping the column in TRIM() prevents the
// planner from using the existing lower(...) functional indexes, forcing a
// full sequential scan). Returns ("", nil) on miss.
func dbResolveUserSFIDByUsername(ctx context.Context, username string) (string, error) {
	row, err := dbLookupMergedUserRowByUsername(ctx, username)
	if err != nil || row == nil {
		return "", err
	}
	return row.SFID, nil
}

// dbLookupMergedUserRowByUsername fetches the live merged_user row for a
// username. Returns (nil, nil) on miss (including blank input and deleted rows).
func dbLookupMergedUserRowByUsername(ctx context.Context, username string) (*mergedUserRow, error) {
	normalized := normalizeUserIdentifier(username)
	if normalized == "" {
		return nil, nil
	}
	row := &mergedUserRow{}
	qCtx, cancel := withQueryTimeout(ctx)
	defer cancel()
	err := v1DB.NewSelect().Model(row).
		Where("LOWER(mu.username__c) = ?", normalized).
		Where("mu.isdeleted IS NOT TRUE").
		Limit(1).
		Scan(qCtx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query merged_user by username: %w", err)
	}
	return row, nil
}

// lookupMergedUserRowByUsernameFn is injectable for tests.
var lookupMergedUserRowByUsernameFn = dbLookupMergedUserRowByUsername

// dbLookupMergedUserRowBySFID fetches the live merged_user row for an SFID.
// Returns (nil, nil) on miss (including deleted rows).
func dbLookupMergedUserRowBySFID(ctx context.Context, sfid string) (*mergedUserRow, error) {
	if sfid == "" {
		return nil, nil
	}
	row := &mergedUserRow{}
	qCtx, cancel := withQueryTimeout(ctx)
	defer cancel()
	err := v1DB.NewSelect().Model(row).
		Where("mu.sfid = ?", sfid).
		Where("mu.isdeleted IS NOT TRUE").
		Limit(1).
		Scan(qCtx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query merged_user by sfid: %w", err)
	}
	return row, nil
}

// dbGetUsernameForDeletedUser looks up username__c by the exact SFID without
// filtering soft-deleted rows. This recovers the username for a hard KV delete
// when the source row is still retained; a physically removed row is a safe miss.
func dbGetUsernameForDeletedUser(ctx context.Context, sfid string) (string, error) {
	if sfid == "" {
		return "", nil
	}
	row := &mergedUserUsernameRow{}
	qCtx, cancel := withQueryTimeout(ctx)
	defer cancel()
	err := v1DB.NewSelect().Model(row).
		Where("mu.sfid = ?", sfid).
		Limit(1).
		Scan(qCtx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to query deleted merged_user username: %w", err)
	}
	return row.Username.String, nil
}

// dbResolveUserSFIDByEmail resolves a v1 user SFID by any of the user's
// active alternate email addresses. Returns ("", nil) on miss.
func dbResolveUserSFIDByEmail(ctx context.Context, email string) (string, error) {
	normalized := normalizeUserIdentifier(email)
	if normalized == "" {
		return "", nil
	}
	row := &alternateEmailRow{}
	qCtx, cancel := withQueryTimeout(ctx)
	defer cancel()
	err := activeEmailFilter(v1DB.NewSelect().Model(row)).
		Where("LOWER(ae.alternate_email_address__c) = ?", normalized).
		Where("ae.leadorcontactid IS NOT NULL").
		// Prefer the primary-flagged row when the same address appears on
		// multiple rows, then newest SFID for determinism.
		OrderExpr("ae.primary_email__c DESC NULLS LAST, ae.sfid DESC").
		Limit(1).
		Scan(qCtx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to query alternate_email__c by address: %w", err)
	}
	return row.LeadOrContactID.String, nil
}

// dbGetAlternateEmailsForUser returns all live (not soft-deleted) alternate
// email rows for a user, primary-flagged rows first. Inactive rows are
// included so callers can apply their own active/verified policies.
func dbGetAlternateEmailsForUser(ctx context.Context, userSfid string) ([]alternateEmailRow, error) {
	if userSfid == "" {
		return nil, nil
	}
	var rows []alternateEmailRow
	qCtx, cancel := withQueryTimeout(ctx)
	defer cancel()
	err := v1DB.NewSelect().Model(&rows).
		Where("ae.leadorcontactid = ?", userSfid).
		Where("ae.isdeleted IS NOT TRUE").
		OrderExpr("ae.primary_email__c DESC NULLS LAST, ae.sfid ASC").
		Scan(qCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to query alternate_email__c by user: %w", err)
	}
	return rows, nil
}

// dbGetSkillsForUser returns the names of every skill a v1 user has, ordered
// alphabetically by name. v1's own query (GetUserSkills, user-service) has no
// ORDER BY, which would otherwise make the resulting Auth0 write nondeterministic
// and cause churn on every sync.
func dbGetSkillsForUser(ctx context.Context, lfid string) ([]string, error) {
	normalized := normalizeUserIdentifier(lfid)
	if normalized == "" {
		return nil, nil
	}
	var rows []userSkillRow
	qCtx, cancel := withQueryTimeout(ctx)
	defer cancel()
	err := v1DB.NewSelect().Model(&rows).
		ColumnExpr("usk.id AS id").
		ColumnExpr(`sk."name" AS sk_name`).
		Join(`JOIN salesforce.skills AS sk ON sk.id = usk.skill_id`).
		Where("LOWER(usk.lfid) = ?", normalized).
		OrderExpr(`sk."name" ASC`).
		Scan(qCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to query user_skills by lfid: %w", err)
	}
	names := make([]string, 0, len(rows))
	for _, row := range rows {
		names = append(names, row.Name)
	}
	return names, nil
}

// emailRowIsActive reports whether an alternate email row is usable: active
// and not carrying a ".old" deactivation suffix. Mirrors activeEmailFilter
// for callers that fetch all rows and filter in Go.
func emailRowIsActive(row *alternateEmailRow) bool {
	if !row.IsActive.Valid || !row.IsActive.Bool {
		return false
	}
	return !hasOldDomainSuffix(row.EmailAddress.String)
}

// hasOldDomainSuffix reports whether an email carries the v1 ".old" domain
// deactivation convention (e.g. "user@example.com.old").
func hasOldDomainSuffix(email string) bool {
	return strings.HasSuffix(strings.ToLower(email), ".old")
}

// dbGetPrimaryEmailForUser returns the user's primary email address: the
// active row flagged primary_email__c, falling back to the first active row
// when none is flagged. Returns ("", nil) when the user has no active emails.
func dbGetPrimaryEmailForUser(ctx context.Context, userSfid string) (string, error) {
	rows, err := dbGetAlternateEmailsForUser(ctx, userSfid)
	if err != nil {
		return "", err
	}
	return selectPrimaryEmailFromRows(rows), nil
}

// selectPrimaryEmailFromRows is the pure selection logic behind
// dbGetPrimaryEmailForUser, extracted so it can be unit tested without a
// database. Rows must already be filtered to a single user.
func selectPrimaryEmailFromRows(rows []alternateEmailRow) string {
	var fallback string
	for i := range rows {
		row := &rows[i]
		if !emailRowIsActive(row) || row.EmailAddress.String == "" {
			continue
		}
		if row.IsPrimary.Valid && row.IsPrimary.Bool {
			return row.EmailAddress.String
		}
		if fallback == "" {
			fallback = row.EmailAddress.String
		}
	}
	return fallback
}

// dbGetLastKnownPrimaryEmailForUser returns the user's most plausible primary
// email including soft-deleted and inactive rows. Used on the user-deletion
// path, where the alternate email rows are typically deleted before or
// alongside the merged_user row (this replaces the KV primary-email cache
// that previously worked around that ordering). Preference order: primary
// flag, then not-deleted, then active, then most recently modified
// (lastmodifieddate — the replication key configured for this table in
// meltano/meltano.yml, so it reflects the latest replicated state).
func dbGetLastKnownPrimaryEmailForUser(ctx context.Context, userSfid string) (string, error) {
	if userSfid == "" {
		return "", nil
	}
	row := &alternateEmailRow{}
	qCtx, cancel := withQueryTimeout(ctx)
	defer cancel()
	err := v1DB.NewSelect().Model(row).
		Where("ae.leadorcontactid = ?", userSfid).
		Where("ae.alternate_email_address__c != ''").
		OrderExpr("ae.primary_email__c DESC NULLS LAST, ae.isdeleted ASC NULLS FIRST, ae.active__c DESC NULLS LAST, ae.lastmodifieddate DESC NULLS LAST").
		Limit(1).
		Scan(qCtx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to query last-known primary email: %w", err)
	}
	return row.EmailAddress.String, nil
}

// dbIsSoleQualifyingAlternateEmail reports whether userSfid has at most one
// "qualifying" alternate email — active (post .old-domain override) and
// either verified or flagged primary. See handleAlternateEmailUpdate
// call sites for the LFXV2-2662 rationale. Returns
// errAmbiguousDefactoPrimaryEmail (wrapped) when multiple rows qualify and
// none is flagged primary.
func dbIsSoleQualifyingAlternateEmail(ctx context.Context, userSfid string) (bool, error) {
	rows, err := dbGetAlternateEmailsForUser(ctx, userSfid)
	if err != nil {
		return false, err
	}
	qualifying, sawPrimary := countQualifyingAlternateEmails(rows)
	if qualifying <= 1 {
		return true, nil
	}
	if !sawPrimary {
		return false, fmt.Errorf("user %s has %d qualifying alternate emails and none is flagged primary: %w", userSfid, qualifying, errAmbiguousDefactoPrimaryEmail)
	}
	return false, nil
}

// countQualifyingAlternateEmails is the pure counting logic behind
// dbIsSoleQualifyingAlternateEmail, extracted so it can be unit tested
// without a database. Rows must already be filtered to a single user.
func countQualifyingAlternateEmails(rows []alternateEmailRow) (qualifying int, sawPrimary bool) {
	for i := range rows {
		row := &rows[i]
		isPrimary := row.IsPrimary.Valid && row.IsPrimary.Bool
		isVerified := row.IsVerified.Valid && row.IsVerified.Bool
		if emailRowIsActive(row) && (isVerified || isPrimary) {
			qualifying++
			if isPrimary {
				sawPrimary = true
			}
		}
	}
	return qualifying, sawPrimary
}
