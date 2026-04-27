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

package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/uptrace/bun"
)

// CreateNamespaceTx copy CreateNamspace except the return value
func (c *Catalog) CreateNamespaceTx(ctx context.Context, namespace table.Identifier, props iceberg.Properties) (func(context.Context, bun.Tx) error, error) {
	if err := checkValidNamespace(namespace); err != nil {
		return nil, err
	}

	exists, err := c.namespaceExists(ctx, strings.Join(namespace, "."))
	if err != nil {
		return nil, err
	}

	if exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNamespaceAlreadyExists, strings.Join(namespace, "."))
	}

	if len(props) == 0 {
		props = minimalNamespaceProps
	}

	nsToCreate := strings.Join(namespace, ".")

	return func(ctx context.Context, tx bun.Tx) error {
		toInsert := make([]sqlIcebergNamespaceProps, 0, len(props))
		for k, v := range props {
			toInsert = append(toInsert, sqlIcebergNamespaceProps{
				CatalogName:   c.name,
				Namespace:     nsToCreate,
				PropertyKey:   k,
				PropertyValue: sql.NullString{String: v, Valid: true},
			})
		}

		_, err := tx.NewInsert().Model(&toInsert).Exec(ctx)
		if err != nil {
			return fmt.Errorf("error inserting namespace properties for namespace '%s': %w", namespace, err)
		}

		return nil
	}, nil
}

// CreateTableInTx copy CreateTable except the return value
func (c *Catalog) CreateTableInTx(ctx context.Context, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (func(context.Context, bun.Tx) error , error)  {
	staged, err := internal.CreateStagedTable(ctx, c.props, c.LoadNamespaceProperties, ident, sc, opts...)
	if err != nil {
		return nil, err
	}

	nsIdent := catalog.NamespaceFromIdent(ident)
	tblIdent := catalog.TableNameFromIdent(ident)
	ns := strings.Join(nsIdent, ".")
	exists, err := c.namespaceExists(ctx, ns)
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

	return func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewInsert().Model(&sqlIcebergTable{
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
	ns := catalog.NamespaceFromIdent(ident)
	tblName := catalog.TableNameFromIdent(ident)

	current, err := c.LoadTable(ctx, ident)
	if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, err
	}

	staged, err := internal.UpdateAndStageTable(ctx, current, ident, reqs, updates, c)
	if err != nil {
		return nil, err
	}

	if current != nil && staged.Metadata().Equals(current.Metadata()) {
		// no changes, do nothing
		return nil, nil
	}

	if err := internal.WriteMetadata(ctx, staged.Metadata(), staged.MetadataLocation(), staged.Properties()); err != nil {
		return nil, err
	}

	return func(ctx context.Context, tx bun.Tx) error {
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

		_, err := tx.NewInsert().Model(&sqlIcebergTable{
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
			PreviousMetadataLocation: current.MetadataLocation(),
			MetadataLocation:         staged.MetadataLocation(),
		})
	}, nil
}