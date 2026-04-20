// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package gnar provides a catalog implementation that extends the SQL catalog
// with outbox support and multi-table transactions. Every mutating operation
// (CreateNamespace, CreateTable, CommitTable, CommitTransaction) writes an
// outbox message in the same database transaction as the catalog change,
// enabling reliable event-driven synchronization to external systems.
package gnar

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/mssqldialect"
	"github.com/uptrace/bun/dialect/mysqldialect"
	"github.com/uptrace/bun/dialect/oracledialect"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/schema"
)

// Gnar is the catalog type for the gnar catalog.
const Gnar catalog.Type = "gnar"

// createDialect creates a bun schema.Dialect for the given SQL dialect.
// Forked from the sql catalog package (unexported getDialect/createDialect).
func createDialect(d sqlcat.SupportedDialect) schema.Dialect {
	switch d {
	case sqlcat.Postgres:
		return pgdialect.New()
	case sqlcat.MySQL:
		return mysqldialect.New()
	case sqlcat.SQLite:
		return sqlitedialect.New()
	case sqlcat.MSSQL:
		return mssqldialect.New()
	case sqlcat.Oracle:
		return oracledialect.New()
	default:
		panic("unsupported sql dialect")
	}
}

func init() {
	catalog.Register("gnar", catalog.RegistrarFunc(func(ctx context.Context, name string, p iceberg.Properties) (c catalog.Catalog, err error) {
		driver, ok := p[sqlcat.DriverKey]
		if !ok {
			return nil, errors.New("must provide driver to pass to sql.Open")
		}

		dialect := strings.ToLower(p[sqlcat.DialectKey])
		if dialect == "" {
			return nil, errors.New("must provide sql dialect to use")
		}

		uri := strings.TrimPrefix(p.Get("uri", ""), "sql://")
		sqldb, err := sql.Open(driver, uri)
		if err != nil {
			return nil, err
		}

		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("failed to create Gnar catalog: %v", r)
			}
		}()

		return NewCatalog(p.Get(name, "gnar"), sqldb, sqlcat.SupportedDialect(dialect), p)
	}))
}

// Duplicated bun models for the standard iceberg tables, matching those in
// the sql catalog package (which are unexported).
type sqlIcebergTable struct {
	bun.BaseModel `bun:"table:iceberg_tables"`

	CatalogName              string `bun:",pk"`
	TableNamespace           string `bun:",pk"`
	TableName                string `bun:",pk"`
	IcebergType              string
	MetadataLocation         sql.NullString
	PreviousMetadataLocation sql.NullString
}

type sqlIcebergNamespaceProps struct {
	bun.BaseModel `bun:"table:iceberg_namespace_properties"`

	CatalogName   string `bun:",pk"`
	Namespace     string `bun:",pk"`
	PropertyKey   string `bun:",pk"`
	PropertyValue sql.NullString
}

const (
	tableTypeTable = "TABLE"
)

var (
	_ catalog.Catalog              = (*Catalog)(nil)
	_ catalog.TransactionalCatalog = (*Catalog)(nil)
	_ table.CatalogIO              = (*Catalog)(nil)

	minimalNamespaceProps = iceberg.Properties{"exists": "true"}
)

// Catalog is a gnar catalog that extends the SQL catalog with outbox support
// and multi-table transaction capabilities. All mutating operations write
// an outbox message in the same database transaction as the catalog change.
type Catalog struct {
	// Embed the SQL catalog for read-only operations and delegated methods
	// (DropTable, ListTables, ListNamespaces, etc.)
	*sqlcat.Catalog

	db    *bun.DB
	name  string
	props iceberg.Properties
}

// NewCatalog creates a new gnar catalog. It first creates the underlying SQL
// catalog, then wraps it with outbox capabilities.
//
// The gnar catalog creates its own bun.DB wrapper over the same sql.DB to
// manage transactions that include outbox writes.
func NewCatalog(name string, sqldb *sql.DB, dialect sqlcat.SupportedDialect, props iceberg.Properties) (*Catalog, error) {
	// Create the underlying SQL catalog which handles table creation and
	// all the standard catalog operations.
	sqlCat, err := sqlcat.NewCatalog(name, sqldb, dialect, props)
	if err != nil {
		return nil, fmt.Errorf("failed to create underlying SQL catalog: %w", err)
	}

	// Create our own bun.DB wrapper for outbox transactions.
	bunDB := bun.NewDB(sqldb, createDialect(dialect))

	cat := &Catalog{
		Catalog: sqlCat,
		db:      bunDB,
		name:    name,
		props:   props,
	}

	if props.GetBool("init_catalog_tables", true) {
		if err := cat.ensureOutboxTableExists(); err != nil {
			return nil, err
		}
	}

	return cat, nil
}

func (c *Catalog) CatalogType() catalog.Type {
	return Gnar
}

func (c *Catalog) ensureOutboxTableExists() error {
	return c.CreateOutboxTable(context.Background())
}

// CreateOutboxTable creates the outbox table if it does not exist.
func (c *Catalog) CreateOutboxTable(ctx context.Context) error {
	_, err := c.db.NewCreateTable().Model((*sqlIcebergOutboxMessage)(nil)).
		IfNotExists().Exec(ctx)
	return err
}

// DropOutboxTable drops the outbox table if it exists.
func (c *Catalog) DropOutboxTable(ctx context.Context) error {
	_, err := c.db.NewDropTable().Model((*sqlIcebergOutboxMessage)(nil)).
		IfExists().Exec(ctx)
	return err
}

// --- Namespace operations with outbox ---

func (c *Catalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	if err := checkValidNamespace(namespace); err != nil {
		return err
	}

	nsStr := strings.Join(namespace, ".")
	exists, err := c.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("%w: %s", catalog.ErrNamespaceAlreadyExists, nsStr)
	}

	if len(props) == 0 {
		props = minimalNamespaceProps
	}

	return withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		// Insert namespace properties
		toInsert := make([]sqlIcebergNamespaceProps, 0, len(props))
		for k, v := range props {
			toInsert = append(toInsert, sqlIcebergNamespaceProps{
				CatalogName:   c.name,
				Namespace:     nsStr,
				PropertyKey:   k,
				PropertyValue: sql.NullString{String: v, Valid: true},
			})
		}

		if _, err := tx.NewInsert().Model(&toInsert).Exec(ctx); err != nil {
			return fmt.Errorf("error inserting namespace properties for namespace '%s': %w", namespace, err)
		}

		// Insert outbox message in same transaction
		return insertOutboxMessage(ctx, tx, OutboxMessageTypeCreateNamespace, nsStr, "", nil)
	})
}

// --- Table operations with outbox ---

func (c *Catalog) CreateTable(ctx context.Context, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	staged, err := internal.CreateStagedTable(ctx, c.props, c.Catalog.LoadNamespaceProperties, ident, sc, opts...)
	if err != nil {
		return nil, err
	}

	nsIdent := catalog.NamespaceFromIdent(ident)
	tblIdent := catalog.TableNameFromIdent(ident)
	ns := strings.Join(nsIdent, ".")

	exists, err := c.CheckNamespaceExists(ctx, nsIdent)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
	}

	afs, err := staged.FS(ctx)
	if err != nil {
		return nil, err
	}
	wfs, ok := afs.(io.WriteFileIO)
	if !ok {
		return nil, errors.New("loaded filesystem IO does not support writing")
	}

	compression := staged.Table.Properties().Get(table.MetadataCompressionKey, table.MetadataCompressionDefault)
	if err := internal.WriteTableMetadata(staged.Metadata(), wfs, staged.MetadataLocation(), compression); err != nil {
		return nil, err
	}

	err = withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		// Insert table record
		if _, err := tx.NewInsert().Model(&sqlIcebergTable{
			CatalogName:      c.name,
			TableNamespace:   ns,
			TableName:        tblIdent,
			MetadataLocation: sql.NullString{String: staged.MetadataLocation(), Valid: true},
			IcebergType:      tableTypeTable,
		}).Exec(ctx); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		// Insert outbox message in same transaction
		return insertOutboxMessage(ctx, tx, OutboxMessageTypeCreateTable, ns, tblIdent, &OutboxMessageData{
			MetadataLocation: staged.MetadataLocation(),
		})
	})
	if err != nil {
		return nil, err
	}

	return c.LoadTable(ctx, ident)
}

func (c *Catalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	ns := catalog.NamespaceFromIdent(ident)
	tblName := catalog.TableNameFromIdent(ident)

	current, err := c.LoadTable(ctx, ident)
	if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, "", err
	}

	staged, err := internal.UpdateAndStageTable(ctx, current, ident, reqs, updates, c)
	if err != nil {
		return nil, "", err
	}

	if current != nil && staged.Metadata().Equals(current.Metadata()) {
		// no changes, do nothing
		return current.Metadata(), current.MetadataLocation(), nil
	}

	if err := internal.WriteMetadata(ctx, staged.Metadata(), staged.MetadataLocation(), staged.Properties()); err != nil {
		return nil, "", err
	}

	var previousMetadataLocation string
	if current != nil {
		previousMetadataLocation = current.MetadataLocation()
	}

	err = withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		if current != nil {
			res, err := tx.NewUpdate().Model(&sqlIcebergTable{
				CatalogName:              c.name,
				TableNamespace:           strings.Join(ns, "."),
				TableName:                tblName,
				IcebergType:              tableTypeTable,
				MetadataLocation:         sql.NullString{Valid: true, String: staged.MetadataLocation()},
				PreviousMetadataLocation: sql.NullString{Valid: true, String: current.MetadataLocation()},
			}).WherePK().Where("metadata_location = ?", current.MetadataLocation()).
				Where("iceberg_type = ?", tableTypeTable).
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("error updating table information: %w", err)
			}

			n, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("error updating table information: %w", err)
			}

			if n == 0 {
				return fmt.Errorf("table has been updated by another process: %s.%s", strings.Join(ns, "."), tblName)
			}
		} else {
			if _, err := tx.NewInsert().Model(&sqlIcebergTable{
				CatalogName:      c.name,
				TableNamespace:   strings.Join(ns, "."),
				TableName:        tblName,
				IcebergType:      tableTypeTable,
				MetadataLocation: sql.NullString{Valid: true, String: staged.MetadataLocation()},
			}).Exec(ctx); err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}
		}

		// Insert outbox message in same transaction
		return insertOutboxMessage(ctx, tx, OutboxMessageTypeCommitTable, strings.Join(ns, "."), tblName, &OutboxMessageData{
			PreviousMetadataLocation: previousMetadataLocation,
			MetadataLocation:         staged.MetadataLocation(),
		})
	})
	if err != nil {
		return nil, "", err
	}

	return staged.Metadata(), staged.MetadataLocation(), nil
}

// --- LoadTable override to pass gnar catalog as CatalogIO ---

func (c *Catalog) LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error) {
	ns := catalog.NamespaceFromIdent(identifier)
	tbl := catalog.TableNameFromIdent(identifier)

	result, err := withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) (*sqlIcebergTable, error) {
		t := new(sqlIcebergTable)
		err := tx.NewSelect().Model(t).
			Where("catalog_name = ?", c.name).
			Where("table_namespace = ?", strings.Join(ns, ".")).
			Where("table_name = ?", tbl).
			Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchTable, identifier)
		}

		if err != nil {
			return nil, fmt.Errorf("error encountered loading table %s: %w", identifier, err)
		}

		return t, nil
	})
	if err != nil {
		return nil, err
	}

	if !result.MetadataLocation.Valid {
		return nil, fmt.Errorf("%w: %s, metadata location is missing", catalog.ErrNoSuchTable, identifier)
	}

	return table.NewFromLocation(
		ctx,
		identifier,
		result.MetadataLocation.String,
		io.LoadFSFunc(c.props, result.MetadataLocation.String),
		c, // pass gnar catalog as CatalogIO so table commits go through outbox
	)
}

// --- RenameTable override to ensure returned table has gnar as CatalogIO ---

func (c *Catalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	// Delegate the rename operation to the embedded SQL catalog
	if _, err := c.Catalog.RenameTable(ctx, from, to); err != nil {
		return nil, err
	}

	// Re-load via our LoadTable so the table has gnar catalog as CatalogIO
	return c.LoadTable(ctx, to)
}

// --- CheckTableExists override to use our LoadTable ---

func (c *Catalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	_, err := c.LoadTable(ctx, identifier)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// --- Multi-table transaction support ---

// CommitTransaction implements catalog.TransactionalCatalog. It processes all
// table commits atomically in a single DB transaction with outbox entries for
// each table commit.
func (c *Catalog) CommitTransaction(ctx context.Context, commits []table.TableCommit) error {
	if len(commits) == 0 {
		return catalog.ErrEmptyCommitList
	}

	for _, commit := range commits {
		if len(commit.Identifier) == 0 {
			return catalog.ErrMissingIdentifier
		}
	}

	// Phase 1: Stage all table changes and write metadata files (outside DB transaction).
	type stagedCommit struct {
		ident                    table.Identifier
		staged                   *table.StagedTable
		current                  *table.Table
		previousMetadataLocation string
		ns                       string
		tblName                  string
	}

	stagedCommits := make([]stagedCommit, 0, len(commits))

	for _, commit := range commits {
		ns := catalog.NamespaceFromIdent(commit.Identifier)
		tblName := catalog.TableNameFromIdent(commit.Identifier)

		current, err := c.LoadTable(ctx, commit.Identifier)
		if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
			return fmt.Errorf("failed to load table %s: %w", commit.Identifier, err)
		}

		staged, err := internal.UpdateAndStageTable(ctx, current, commit.Identifier, commit.Requirements, commit.Updates, c)
		if err != nil {
			return fmt.Errorf("failed to stage table %s: %w", commit.Identifier, err)
		}

		if current != nil && staged.Metadata().Equals(current.Metadata()) {
			continue // no changes for this table
		}

		if err := internal.WriteMetadata(ctx, staged.Metadata(), staged.MetadataLocation(), staged.Properties()); err != nil {
			return fmt.Errorf("failed to write metadata for table %s: %w", commit.Identifier, err)
		}

		var prevMetaLoc string
		if current != nil {
			prevMetaLoc = current.MetadataLocation()
		}

		stagedCommits = append(stagedCommits, stagedCommit{
			ident:                    commit.Identifier,
			staged:                   staged,
			current:                  current,
			previousMetadataLocation: prevMetaLoc,
			ns:                       strings.Join(ns, "."),
			tblName:                  tblName,
		})
	}

	if len(stagedCommits) == 0 {
		return nil // no actual changes
	}

	// Phase 2: Single DB transaction for all table updates + outbox messages.
	return withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		for _, sc := range stagedCommits {
			if sc.current != nil {
				res, err := tx.NewUpdate().Model(&sqlIcebergTable{
					CatalogName:              c.name,
					TableNamespace:           sc.ns,
					TableName:                sc.tblName,
					IcebergType:              tableTypeTable,
					MetadataLocation:         sql.NullString{Valid: true, String: sc.staged.MetadataLocation()},
					PreviousMetadataLocation: sql.NullString{Valid: true, String: sc.current.MetadataLocation()},
				}).WherePK().Where("metadata_location = ?", sc.current.MetadataLocation()).
					Where("iceberg_type = ?", tableTypeTable).
					Exec(ctx)
				if err != nil {
					return fmt.Errorf("error updating table %s.%s: %w", sc.ns, sc.tblName, err)
				}

				n, err := res.RowsAffected()
				if err != nil {
					return fmt.Errorf("error updating table %s.%s: %w", sc.ns, sc.tblName, err)
				}

				if n == 0 {
					return fmt.Errorf("table has been updated by another process: %s.%s", sc.ns, sc.tblName)
				}
			} else {
				if _, err := tx.NewInsert().Model(&sqlIcebergTable{
					CatalogName:      c.name,
					TableNamespace:   sc.ns,
					TableName:        sc.tblName,
					IcebergType:      tableTypeTable,
					MetadataLocation: sql.NullString{Valid: true, String: sc.staged.MetadataLocation()},
				}).Exec(ctx); err != nil {
					return fmt.Errorf("failed to create table %s.%s: %w", sc.ns, sc.tblName, err)
				}
			}

			// Insert outbox message for each table in same transaction
			if err := insertOutboxMessage(ctx, tx, OutboxMessageTypeCommitTable, sc.ns, sc.tblName, &OutboxMessageData{
				PreviousMetadataLocation: sc.previousMetadataLocation,
				MetadataLocation:         sc.staged.MetadataLocation(),
			}); err != nil {
				return err
			}
		}

		return nil
	})
}

// --- Helpers ---

func checkValidNamespace(ident table.Identifier) error {
	if len(ident) < 1 {
		return fmt.Errorf("%w: empty namespace identifier", catalog.ErrNoSuchNamespace)
	}
	return nil
}

func withReadTx[R any](ctx context.Context, db *bun.DB, fn func(context.Context, bun.Tx) (R, error)) (result R, err error) {
	err = db.RunInTx(ctx, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx bun.Tx) error {
		result, err = fn(ctx, tx)
		return err
	})
	return result, err
}

func withWriteTx(ctx context.Context, db *bun.DB, fn func(context.Context, bun.Tx) error) error {
	return db.RunInTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault}, func(ctx context.Context, tx bun.Tx) error {
		return fn(ctx, tx)
	})
}
