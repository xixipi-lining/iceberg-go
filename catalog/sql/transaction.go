package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/uptrace/bun"
)

var ErrNoChanges = errors.New("no changes")

type sqlIcebergQueueOffset struct {
	bun.BaseModel `bun:"table:iceberg_queue_offset"`

	QueueId   string    `bun:",pk"`
	Offset    string    `bun:",notnull"`
	CreatedAt time.Time `bun:"created_at,notnull,default:current_timestamp"`
	UpdatedAt time.Time `bun:"updated_at,notnull,default:current_timestamp"`
}

func (o *sqlIcebergQueueOffset) BeforeUpdate(ctx context.Context, query *bun.UpdateQuery) error {
	o.UpdatedAt = time.Now()
	return nil
}

type TransactionCatalog struct {
	mx          sync.Mutex
	operations  []func(context.Context, bun.Tx) error
	followerOps []func() error
	committed   chan struct{}
	err         error

	*Catalog
	follower catalog.FollowerCatalog
}

func NewTransactionCatalog(db *bun.DB, props iceberg.Properties, follower catalog.FollowerCatalog) (*TransactionCatalog, error) {
	cat := &Catalog{db: db, name: "", props: props}
	tcat := &TransactionCatalog{
		Catalog:  cat,
		follower: follower,
	}

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

func (c *TransactionCatalog) CreateQueueOffsetTable(ctx context.Context) error {
	_, err := c.db.NewCreateTable().Model((*sqlIcebergQueueOffset)(nil)).
		IfNotExists().Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c *TransactionCatalog) ensureTablesExist() error {
	return c.CreateQueueOffsetTable(context.Background())
}

func (c *TransactionCatalog) SetQueueOffset(ctx context.Context, queueId, offset string) error {
	return withWriteTx(ctx, c.db, c.setQueueOffset(queueId, offset))
}

func (c *TransactionCatalog) setQueueOffset(queueId, offset string) func(context.Context, bun.Tx) error {
	return func(ctx context.Context, tx bun.Tx) error {
		item := &sqlIcebergQueueOffset{
			QueueId: queueId,
			Offset:  offset,
		}
		_, err := tx.NewInsert().
			Model(item).
			On("CONFLICT (queue_id) DO UPDATE").
			Set("offset = EXCLUDED.offset").
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to set queue offset: %w", err)
		}

		return nil
	}
}

func (c *TransactionCatalog) GetQueueOffset(ctx context.Context, queueId string) (string, error) {
	return withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) (string, error) {
		var offset string
		err := tx.NewSelect().Column("offset").Model((*sqlIcebergQueueOffset)(nil)).Where("queue_id = ?", queueId).Scan(ctx, &offset)
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		if err != nil {
			return "", fmt.Errorf("failed to get queue offset: %w", err)
		}

		return offset, nil
	})
}

func (c *TransactionCatalog) Commit(ctx context.Context) error {
	c.mx.Lock()
	defer c.mx.Unlock()

	if len(c.operations) == 0 {
		return errors.New("no operations to commit")
	}

	err := withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		for _, op := range c.operations {
			err := op(ctx, tx)
			if err != nil {
				return err
			}
		}

		return nil
	})

	c.err = err
	close(c.committed)
	return err
}

func (c *TransactionCatalog) SetQueueOffsetInTx(ctx context.Context, queueId, offset string) error {
	c.mx.Lock()
	if c.operations == nil {
		c.operations = make([]func(context.Context, bun.Tx) error, 0)
		c.followerOps = make([]func() error, 0)
		c.committed = make(chan struct{})
	}
	c.operations = append(c.operations, c.setQueueOffset(queueId, offset))
	c.mx.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.committed:
	}

	if c.err != nil {
		return fmt.Errorf("transaction commit error %w", c.err)
	}
	return nil
}

func (c *TransactionCatalog) CreateTableInTx(ctx context.Context, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	staged, err := c.stageCreateTable(ctx, ident, sc, opts...)
	if err != nil {
		return nil, err
	}

	nsIdent := catalog.NamespaceFromIdent(ident)
	tblIdent := catalog.TableNameFromIdent(ident)
	ns := strings.Join(nsIdent, ".")
	dbOp := func(ctx context.Context, tx bun.Tx) error {
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

	c.mx.Lock()
	if c.operations == nil {
		c.operations = make([]func(context.Context, bun.Tx) error, 0)
		c.followerOps = make([]func() error, 0)
		c.committed = make(chan struct{})
	}
	c.operations = append(c.operations, dbOp)
	c.followerOps = append(c.followerOps, func() error {
		return c.follower.FollowCreateTable(ctx, staged)
	})
	c.mx.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.committed:
	}

	if c.err != nil {
		return nil, fmt.Errorf("transaction commit error %w", c.err)
	}
	return c.LoadTable(ctx, ident)
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

func (c *TransactionCatalog) CommitTableInTx(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	ns := catalog.NamespaceFromIdent(ident)
	tblName := catalog.TableNameFromIdent(ident)

	current, staged, err := c.stageCommitTable(ctx, ident, reqs, updates)
	if err != nil {
		return nil, "", err
	}

	dbOp := func(ctx context.Context, tx bun.Tx) error {
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

			return nil
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

		return nil
	}

	c.mx.Lock()
	if c.operations == nil {
		c.operations = make([]func(context.Context, bun.Tx) error, 0)
		c.followerOps = make([]func() error, 0)
		c.committed = make(chan struct{})
	}
	c.operations = append(c.operations, dbOp)
	c.followerOps = append(c.followerOps, func() error {
		previousMetadataLocation := ""
		if current != nil {
			previousMetadataLocation = current.MetadataLocation()
		}

		return c.follower.FollowCommitTable(ctx, &previousMetadataLocation, staged)
	})
	c.mx.Unlock()

	select {
	case <-ctx.Done():
		return nil, "", ctx.Err()
	case <-c.committed:
	}

	if c.err != nil {
		return nil, "", fmt.Errorf("transaction commit error %w", c.err)
	}
	return staged.Metadata(), staged.MetadataLocation(), nil
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

	props := make(iceberg.Properties)
	maps.Copy(props, staged.Properties())
	maps.Copy(props, c.Catalog.props)
	if err := internal.WriteMetadata(ctx, staged.Metadata(), staged.MetadataLocation(), props); err != nil {
		return nil, nil, err
	}

	return current, staged, nil
}
