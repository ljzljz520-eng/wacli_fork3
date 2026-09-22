package store

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

// ShadowSuffix is appended to every table and view name for shadow rebuilds.
const ShadowSuffix = "_shadow"

// ShadowTables lists the materialized tables rebuilt as shadow targets, in
// parent-first creation order.
var ShadowTables = []string{
	ChatsTable,
	"contacts",
	GroupsTable,
	"group_participants",
	MessagesTable,
	MessageLocationsTable,
	"message_payload_purges",
	MessageLocalMediaAliasesTable,
	PollsTable,
	PollVotesTable,
	StatusMessagesTable,
	CallEventsTable,
	"starred",
}

// shadowDropOrder is child-first so FOREIGN KEY enforcement never blocks a drop.
var shadowDropOrder = []string{
	MessageLocalMediaAliasesTable,
	MessageLocationsTable,
	"message_payload_purges",
	PollVotesTable,
	PollsTable,
	MessagesTable,
	"group_participants",
	GroupsTable,
	StatusMessagesTable,
	CallEventsTable,
	"starred",
	"contacts",
	ChatsTable,
}

const shadowFTSCreateSQL = `
CREATE VIRTUAL TABLE messages_fts_shadow USING fts5(
    text,
    media_caption,
    filename,
    chat_name,
    sender_name,
    display_text
)`

// ResetShadowSchema drops every shadow table (including the shadow FTS table)
// and recreates the set from the active table definitions, then clears shadow
// checkpoints and view state. It does not touch active tables.
func (d *DB) ResetShadowSchema(ctx context.Context, withFTS bool) error {
	// Drop existing shadow objects. The virtual table drop also removes its
	// FTS side tables.
	if _, err := d.sql.ExecContext(ctx, `DROP TABLE IF EXISTS messages_fts`+ShadowSuffix); err != nil {
		return fmt.Errorf("drop shadow fts: %w", err)
	}
	for _, base := range shadowDropOrder {
		if _, err := d.sql.ExecContext(ctx, `DROP TABLE IF EXISTS `+base+ShadowSuffix); err != nil {
			return fmt.Errorf("drop shadow %s: %w", base, err)
		}
	}

	tableDDL, indexDDL, err := d.readShadowDDL(ctx)
	if err != nil {
		return err
	}
	// Tables: ShadowTables is already parent-first; sqlite tolerates either
	// order at CREATE time anyway.
	for _, base := range ShadowTables {
		ddl, ok := tableDDL[base]
		if !ok {
			return fmt.Errorf("missing active table definition for %s", base)
		}
		if _, err := d.sql.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("create shadow table %s: %w", base, err)
		}
	}
	for _, ddl := range indexDDL {
		if _, err := d.sql.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("create shadow index: %w", err)
		}
	}

	if withFTS {
		if _, err := d.sql.ExecContext(ctx, shadowFTSCreateSQL); err != nil {
			return fmt.Errorf("create shadow fts: %w", err)
		}
	}

	if _, err := d.sql.ExecContext(ctx,
		`DELETE FROM projector_checkpoints WHERE view LIKE '%\_shadow' ESCAPE '\'`); err != nil {
		return fmt.Errorf("clear shadow checkpoints: %w", err)
	}
	if _, err := d.sql.ExecContext(ctx,
		`DELETE FROM ledger_view_state WHERE view LIKE '%\_shadow' ESCAPE '\'`); err != nil {
		return fmt.Errorf("clear shadow view state: %w", err)
	}
	return nil
}

// readShadowDDL derives CREATE statements for the shadow set from the active
// sqlite_master definitions, rewriting every materialized table name to its
// shadow target.
func (d *DB) readShadowDDL(ctx context.Context) (map[string]string, []string, error) {
	placeholders := strings.Repeat("?,", len(ShadowTables))
	placeholders = placeholders[:len(placeholders)-1]
	args := make([]any, len(ShadowTables))
	for i, name := range ShadowTables {
		args[i] = name
	}
	rows, err := d.sql.QueryContext(ctx, `
		SELECT type, name, tbl_name, COALESCE(sql, '')
		FROM sqlite_master
		WHERE type IN ('table','index')
			AND tbl_name IN (`+placeholders+`)
		ORDER BY type DESC`, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("read active table definitions: %w", err)
	}
	defer rows.Close()

	tables := make(map[string]string)
	var indexes []string
	for rows.Next() {
		var typ, name, tblName, ddl string
		if err := rows.Scan(&typ, &name, &tblName, &ddl); err != nil {
			return nil, nil, err
		}
		if strings.TrimSpace(ddl) == "" {
			continue // auto index: recreated by the table constraint
		}
		if typ == "table" {
			// Shadow tables drop FK constraints: replay order across views is
			// not parent-first (snapshot events precede their parent tables),
			// and referential integrity is checked by the invariant checker
			// instead (message→chat, participant→group, ...).
			tables[tblName] = rewriteShadowDDL(stripShadowFK(ddl))
			continue
		}
		// Rename the index itself, then rewrite referenced table names.
		ddl = strings.Replace(ddl, name, name+ShadowSuffix, 1)
		indexes = append(indexes, rewriteShadowDDL(ddl))
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return tables, indexes, nil
}

// rewriteShadowDDL rewrites every materialized table identifier in a DDL
// statement to its shadow target (whole-word replacement).
func rewriteShadowDDL(ddl string) string {
	for _, base := range ShadowTables {
		ddl = RetargetTable(ddl, base, base+ShadowSuffix)
	}
	return ddl
}

// shadowFKClause matches one inline FOREIGN KEY table constraint, including
// the referenced table/columns and trailing ON DELETE/UPDATE actions.
var shadowFKClause = regexp.MustCompile(
	`(?is),\s*FOREIGN KEY\s*\([^)]*\)\s*REFERENCES\s+("[A-Za-z_][\w"]*"|[A-Za-z_][\w]*)\s*(\([^)]*\))?\s*(ON\s+(DELETE|UPDATE)\s+\w+\s*)*`)

// stripShadowFK removes FOREIGN KEY constraints from a CREATE TABLE statement.
func stripShadowFK(ddl string) string {
	return strings.TrimRightFunc(shadowFKClause.ReplaceAllString(ddl, ""), unicode.IsSpace)
}
