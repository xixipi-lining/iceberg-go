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

var ErrNoChanges = errors.New("no changes")

type sqlIcebergKVSidecarItem struct {
	bun.BaseModel `bun:"table:iceberg_kv_sidecar"`

	Name  string `bun:",pk"`
	Value string `bun:",notnull"`
}

type TransactionCatalog struct {
	*Catalog
}

func NewTransactionCatalog(db *bun.DB, props iceberg.Properties) (catalog.TransactionCatalog, error) {
	cat := &Catalog{db: db, name: "", props: props}
	tcat := &TransactionCatalog{cat}

	if props.GetBool(initCatalogTablesKey, true) {
		if err := cat.ensureTablesExist(); err != nil {
			return nil, err
		}

		if err := tcat.ensureTablesExist(); err != nil {
			return nil, err
		}
	}

	return tcat, nil
}

func (c *TransactionCatalog) CreateKVSidecarTable(ctx context.Context) error {
	_, err := c.db.NewCreateTable().Model((*sqlIcebergKVSidecarItem)(nil)).
		IfNotExists().Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c *TransactionCatalog) ensureTablesExist() error {
	return c.CreateSQLTables(context.Background())
}

func (c *TransactionCatalog) SetKVSidecar(ctx context.Context, key, value string) error {
	err := withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		item := &sqlIcebergKVSidecarItem{
			Name:  key,
			Value: value,
		}
		_, err := tx.NewInsert().
			Model(item).
			On("CONFLICT (name) DO UPDATE").
			Set("value = EXCLUDED.value").
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to set kv sidecar: %w", err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (c *TransactionCatalog) GetKVSidecar(ctx context.Context, key string) (string, error) {
	result, err := withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) (*sqlIcebergKVSidecarItem, error) {
		item := new(sqlIcebergKVSidecarItem)
		err := tx.NewSelect().Model(&item).Where("name = ?", key).Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("failed to get kv sidecar: %w", err)
		}

		return item, nil
	})
	if err != nil {
		return "", err
	}

	if result == nil {
		return "", nil
	}

	return result.Value, nil
}

func (c *TransactionCatalog) stageCreateTable(ctx context.Context, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.StagedTable, error) {
	staged, err := internal.CreateStagedTable(ctx, c.props, c.LoadNamespaceProperties, ident, sc, opts...)
	if err != nil {
		return nil, err
	}

	afs, err := staged.FS(ctx)
	if err != nil {
		return nil, err
	}
	wfs, ok := afs.(io.WriteFileIO)
	if !ok {
		return nil, errors.New("loaded filesystem IO does not support writing")
	}

	if err := internal.WriteTableMetadata(staged.Metadata(), wfs, staged.MetadataLocation()); err != nil {
		return nil, err
	}

	return &staged, nil
}

func (c *TransactionCatalog) stageCommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (*table.Table, *table.StagedTable, error) {
	current, err := c.LoadTable(ctx, ident)
	if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, nil, err
	}

	staged, err := internal.UpdateAndStageTable(ctx, current, ident, reqs, updates, c)
	if err != nil {
		return nil, nil, err
	}

	if current != nil && staged.Metadata().Equals(current.Metadata()) {
		return current, staged, ErrNoChanges
	}

	if err := internal.WriteMetadata(ctx, staged.Metadata(), staged.MetadataLocation(), staged.Properties()); err != nil {
		return nil, nil, err
	}

	return current, staged, nil
}

func (c *TransactionCatalog) Transaction(ctx context.Context, operations []catalog.Operation) error {
	op, err := c.TransactionTx(ctx, operations)
	if err != nil {
		return err
	}

	err = withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		err := op(ctx, tx)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *TransactionCatalog) TransactionTx(ctx context.Context, operations []catalog.Operation) (func(context.Context, bun.Tx) error, error) {
	ops := make([]func(context.Context, bun.Tx) error, len(operations))

	for i, op := range operations {
		switch req := op.(type) {
		case *catalog.OperationCreateTable:
			nsIdent := catalog.NamespaceFromIdent(req.Identifier)
			tblIdent := catalog.TableNameFromIdent(req.Identifier)
			ns := strings.Join(nsIdent, ".")
			exists, err := c.namespaceExists(ctx, ns)
			if err != nil {
				return nil, err
			}
			if !exists {
				return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
			}

			staged, err := c.stageCreateTable(ctx, req.Identifier, req.Schema, req.Opts...)
			if err != nil {
				return nil, err
			}

			ops[i] = func(ctx context.Context, tx bun.Tx) error {
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

				return nil
			}

		case *catalog.OperationCommitTable:
			nsIdent := catalog.NamespaceFromIdent(req.Identifier)
			tblIdent := catalog.TableNameFromIdent(req.Identifier)
			ns := strings.Join(nsIdent, ".")
			current, staged, err := c.stageCommitTable(ctx, req.Identifier, req.Requirements, req.Updates)
			if err != nil {
				if errors.Is(err, ErrNoChanges) {
					continue
				}
				return nil, err
			}

			ops[i] = func(ctx context.Context, tx bun.Tx) error {
				if current != nil {
					res, err := tx.NewUpdate().Model(&sqlIcebergTable{
						CatalogName:              c.name,
						TableNamespace:           ns,
						TableName:                tblIdent,
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
						return fmt.Errorf("table has been updated by another process: %s.%s", ns, tblIdent)
					}

					return nil
				}

				_, err := tx.NewInsert().Model(&sqlIcebergTable{
					CatalogName:      c.name,
					TableNamespace:   ns,
					TableName:        tblIdent,
					IcebergType:      TableType,
					MetadataLocation: sql.NullString{Valid: true, String: staged.MetadataLocation()},
				}).Exec(ctx)
				if err != nil {
					return fmt.Errorf("failed to create table: %w", err)
				}

				return nil
			}

		case *catalog.OperationSetKVSidecar:
			ops[i] = func(ctx context.Context, tx bun.Tx) error {
				item := &sqlIcebergKVSidecarItem{
					Name:  req.Key,
					Value: req.Value,
				}
				_, err := tx.NewInsert().
					Model(item).
					On("CONFLICT (name) DO UPDATE").
					Set("value = EXCLUDED.value").
					Exec(ctx)
				if err != nil {
					return fmt.Errorf("failed to set kv sidecar: %w", err)
				}

				return nil
			}
		}
	}

	op := func(ctx context.Context, tx bun.Tx) error {
		for _, op := range ops {
			err := op(ctx, tx)
			if err != nil {
				return err
			}
		}

		return nil
	}

	return op, nil
}
