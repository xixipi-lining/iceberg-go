// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  See the License for the specific language
// governing permissions and limitations under the License.

package sql

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/uptrace/bun"
)

type txCatalog struct {
	*Catalog
	tx bun.Tx
}

func (tc *txCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	return tc.Catalog.loadTableInTx(ctx, tc.tx, ident)
}

func (tc *txCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	return nil, "", errors.New("CommitTable not supported on txCatalog")
}

func (c *Catalog) namespaceExistsInTx(ctx context.Context, tx bun.Tx, ns string) (bool, error) {
	exists, err := tx.NewSelect().Model((*sqlIcebergTable)(nil)).
		Where("catalog_name = ?", c.name).
		Where("table_namespace = ?", ns).
		Limit(1).Exists(ctx)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}

	return tx.NewSelect().Model((*sqlIcebergNamespaceProps)(nil)).
		Where("catalog_name = ?", c.name).Where("namespace = ?", ns).
		Limit(1).Exists(ctx)
}

func (c *Catalog) loadNamespacePropertiesInTx(ctx context.Context, tx bun.Tx, namespace table.Identifier) (iceberg.Properties, error) {
	if err := checkValidNamespace(namespace); err != nil {
		return nil, err
	}

	nsToLoad := strings.Join(namespace, ".")
	exists, err := c.namespaceExistsInTx(ctx, tx, nsToLoad)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, nsToLoad)
	}

	var props []sqlIcebergNamespaceProps
	err = tx.NewSelect().Model(&props).
		Where("catalog_name = ?", c.name).
		Where("namespace = ?", nsToLoad).Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("error loading namespace properties for '%s': %w", namespace, err)
	}

	result := make(iceberg.Properties)
	for _, p := range props {
		result[p.PropertyKey] = p.PropertyValue.String
	}

	return result, nil
}

func (c *Catalog) loadTableInTx(ctx context.Context, tx bun.Tx, identifier table.Identifier) (*table.Table, error) {
	ns := catalog.NamespaceFromIdent(identifier)
	tbl := catalog.TableNameFromIdent(identifier)

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

	if !t.MetadataLocation.Valid {
		return nil, fmt.Errorf("%w: %s, metadata location is missing", catalog.ErrNoSuchTable, identifier)
	}

	return table.NewFromLocation(
		ctx,
		identifier,
		t.MetadataLocation.String,
		io.LoadFSFunc(c.props, t.MetadataLocation.String),
		c,
	)
}

// CreateNamespaceTx copy CreateNamspace except the return value
func (c *Catalog) CreateNamespaceTx(ctx context.Context, namespace table.Identifier, props iceberg.Properties) (func(context.Context, bun.Tx) error, error) {
	if err := checkValidNamespace(namespace); err != nil {
		return nil, err
	}

	if len(props) == 0 {
		props = minimalNamespaceProps
	}

	nsToCreate := strings.Join(namespace, ".")

	return func(ctx context.Context, tx bun.Tx) error {
		exists, err := c.namespaceExistsInTx(ctx, tx, nsToCreate)
		if err != nil {
			return err
		}

		if exists {
			return fmt.Errorf("%w: %s", catalog.ErrNamespaceAlreadyExists, nsToCreate)
		}

		toInsert := make([]sqlIcebergNamespaceProps, 0, len(props))
		for k, v := range props {
			toInsert = append(toInsert, sqlIcebergNamespaceProps{
				CatalogName:   c.name,
				Namespace:     nsToCreate,
				PropertyKey:   k,
				PropertyValue: sql.NullString{String: v, Valid: true},
			})
		}

		_, err = tx.NewInsert().Model(&toInsert).Exec(ctx)
		if err != nil {
			return fmt.Errorf("error inserting namespace properties for namespace '%s': %w", namespace, err)
		}

		return nil
	}, nil
}

// CreateTableInTx copy CreateTable except the return value
func (c *Catalog) CreateTableInTx(ctx context.Context, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (func(context.Context, bun.Tx) error, error) {
	return func(ctx context.Context, tx bun.Tx) error {
		staged, err := internal.CreateStagedTable(ctx, c.props, func(ctx context.Context, ns table.Identifier) (iceberg.Properties, error) {
			return c.loadNamespacePropertiesInTx(ctx, tx, ns)
		}, ident, sc, opts...)
		if err != nil {
			return err
		}

		nsIdent := catalog.NamespaceFromIdent(ident)
		tblIdent := catalog.TableNameFromIdent(ident)
		ns := strings.Join(nsIdent, ".")
		exists, err := c.namespaceExistsInTx(ctx, tx, ns)
		if err != nil {
			return err
		}

		if !exists {
			return fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
		}

		afs, err := staged.FS(ctx)
		if err != nil {
			return err
		}
		wfs, ok := afs.(io.WriteFileIO)
		if !ok {
			return errors.New("loaded filesystem IO does not support writing")
		}

		compression := staged.Table.Properties().Get(table.MetadataCompressionKey, table.MetadataCompressionDefault)
		if err := internal.WriteTableMetadata(staged.Metadata(), wfs, staged.MetadataLocation(), compression); err != nil {
			return err
		}

		_, err = tx.NewInsert().Model(&sqlIcebergTable{
			CatalogName:      c.name,
			TableNamespace:   ns,
			TableName:        tblIdent,
			MetadataLocation: sql.NullString{String: staged.MetadataLocation(), Valid: true},
			IcebergType:      TableType,
		}).Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		return insertOutboxMessage(ctx, tx, c.name, ns, tblIdent, OutboxMessageTypeCreateTable, &OutboxMessageData{
			MetadataLocation: staged.MetadataLocation(),
		})
	}, nil
}

// CommitTableInTx copy CommitTable except the return value
func (c *Catalog) CommitTableInTx(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (func(context.Context, bun.Tx) error, error) {
	return func(ctx context.Context, tx bun.Tx) error {
		ns := catalog.NamespaceFromIdent(ident)
		tblName := catalog.TableNameFromIdent(ident)

		current, err := c.loadTableInTx(ctx, tx, ident)
		if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
			return err
		}

		staged, err := internal.UpdateAndStageTable(ctx, current, ident, reqs, updates, &txCatalog{Catalog: c, tx: tx})
		if err != nil {
			return err
		}

		if current != nil && staged.Metadata().Equals(current.Metadata()) {
			// no changes, do nothing
			return nil
		}

		if err := internal.WriteMetadata(ctx, staged.Metadata(), staged.MetadataLocation(), staged.Properties()); err != nil {
			return err
		}

		if current != nil {
			res, err := tx.NewUpdate().Model(&sqlIcebergTable{
				CatalogName:              c.name,
				TableNamespace:           strings.Join(ns, "."),
				TableName:                tblName,
				IcebergType:              TableType,
				MetadataLocation:         sql.NullString{Valid: true, String: staged.MetadataLocation()},
				PreviousMetadataLocation: sql.NullString{Valid: true, String: current.MetadataLocation()},
			}).WherePK().Where("metadata_location = ?", current.MetadataLocation()).
				Where("iceberg_type = ?", TableType).
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

			return insertOutboxMessage(ctx, tx, c.name, strings.Join(ns, "."), tblName, OutboxMessageTypeCommitTable, &OutboxMessageData{
				PreviousMetadataLocation: "",
				MetadataLocation:         staged.MetadataLocation(),
			})
		}

		_, err = tx.NewInsert().Model(&sqlIcebergTable{
			CatalogName:      c.name,
			TableNamespace:   strings.Join(ns, "."),
			TableName:        tblName,
			IcebergType:      TableType,
			MetadataLocation: sql.NullString{Valid: true, String: staged.MetadataLocation()},
		}).Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		return insertOutboxMessage(ctx, tx, c.name, strings.Join(ns, "."), tblName, OutboxMessageTypeCreateTable, &OutboxMessageData{
			PreviousMetadataLocation: "",
			MetadataLocation:         staged.MetadataLocation(),
		})
	}, nil
}

// CommitTransactionInTx copy CommitTransaction except the return value
func (c *Catalog) CommitTransactionInTx(ctx context.Context, commits []table.TableCommit) (func(context.Context, bun.Tx) error, error) {
	if len(commits) == 0 {
		return nil, catalog.ErrEmptyCommitList
	}

	seen := make(map[string]struct{})

	for _, commit := range commits {
		if len(commit.Identifier) == 0 {
			return nil, catalog.ErrMissingIdentifier
		}

		key := strings.Join(commit.Identifier, ".")
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("duplicate table in commit list: %s", key)
		}
		seen[key] = struct{}{}
	}

	return func(ctx context.Context, tx bun.Tx) error {
		// Phase 1: Load current state and stage all table updates.
		type stagedCommit struct {
			ident   table.Identifier
			current *table.Table
			ns      string
			tblName string
			staged  *table.StagedTable
		}

		stagedCommits := make([]stagedCommit, 0, len(commits))
		for _, commit := range commits {
			ns := catalog.NamespaceFromIdent(commit.Identifier)
			tblName := catalog.TableNameFromIdent(commit.Identifier)

			current, err := c.loadTableInTx(ctx, tx, commit.Identifier)
			if err != nil {
				return err
			}

			staged, err := internal.UpdateAndStageTable(ctx, current, commit.Identifier, commit.Requirements, commit.Updates, &txCatalog{Catalog: c, tx: tx})
			if err != nil {
				return err
			}

			// Skip tables with no actual changes.
			if current != nil && staged.Metadata().Equals(current.Metadata()) {
				continue
			}

			// Write the metadata file.
			if err := internal.WriteMetadata(ctx, staged.Metadata(), staged.MetadataLocation(), staged.Properties()); err != nil {
				return err
			}

			stagedCommits = append(stagedCommits, stagedCommit{
				ident:   commit.Identifier,
				current: current,
				ns:      strings.Join(ns, "."),
				tblName: tblName,
				staged:  staged,
			})
		}

		if len(stagedCommits) == 0 {
			return nil // all tables had no changes
		}

		// Sort stagedCommits by identifier to prevent deadlocks.
		slices.SortFunc(stagedCommits, func(a, b stagedCommit) int {
			return cmp.Compare(strings.Join(a.ident, "."), strings.Join(b.ident, "."))
		})

		// Phase 2: Apply all DB changes atomically.
		for _, sc := range stagedCommits {
			res, err := tx.NewUpdate().Model(&sqlIcebergTable{
				CatalogName:              c.name,
				TableNamespace:           sc.ns,
				TableName:                sc.tblName,
				IcebergType:              TableType,
				MetadataLocation:         sql.NullString{Valid: true, String: sc.staged.MetadataLocation()},
				PreviousMetadataLocation: sql.NullString{Valid: true, String: sc.current.MetadataLocation()},
			}).WherePK().Where("metadata_location = ?", sc.current.MetadataLocation()).
				Where("iceberg_type = ?", TableType).
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("%w: error updating table information: %v", catalog.ErrCommitFailed, err)
			}

			n, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("%w: error updating table information: %v", catalog.ErrCommitFailed, err)
			}

			if n == 0 {
				return fmt.Errorf("%w: table has been updated by another process: %s.%s", catalog.ErrCommitFailed, sc.ns, sc.tblName)
			}

			if err := insertOutboxMessage(ctx, tx, c.name, sc.ns, sc.tblName, OutboxMessageTypeCommitTable, &OutboxMessageData{
				PreviousMetadataLocation: sc.current.MetadataLocation(),
				MetadataLocation:         sc.staged.MetadataLocation(),
			}); err != nil {
				return err
			}
		}

		return nil
	}, nil
}