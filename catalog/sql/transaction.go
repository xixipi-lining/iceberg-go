package sql

import (
	"context"
	"database/sql"
	"encoding/json"
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

type OutboxMessageType string

const (
	OutboxMessageTypeCreateNamespace OutboxMessageType = "create_namespace"
	OutboxMessageTypeCreateTable     OutboxMessageType = "create_table"
	OutboxMessageTypeCommitTable     OutboxMessageType = "commit_table"
)

type OutboxMessageData struct {
	PreviousMetadataLocation string `json:"previous_metadata_location"`
	MetadataLocation         string `json:"metadata_location"`
}

type OutboxMessageStatus int8

const (
	OutboxMessageStatusPending OutboxMessageStatus = 0
	OutboxMessageStatusDone    OutboxMessageStatus = 1
)

type sqlIcebergOutboxMessage struct {
	bun.BaseModel `bun:"table:iceberg_outbox_messages"`

	Id        int64     `bun:",pk,autoincrement"`
	CreatedAt time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
	UpdatedAt time.Time `bun:"updated_at,nullzero,notnull,default:current_timestamp"`

	MessageType OutboxMessageType
	Status      OutboxMessageStatus
	Namespace   string
	TableName   string
	Message     []byte
}

type TransactionCatalog struct {
	*Catalog
}

type MultiTableTransaction struct {
	*TransactionCatalog

	mx         sync.Mutex
	wg         sync.WaitGroup
	operations []func(context.Context, bun.Tx) error
	committed  chan struct{}
	err        error
}

func NewTransactionCatalog(db *bun.DB, props iceberg.Properties) (*TransactionCatalog, error) {
	cat := &Catalog{db: db, name: "", props: props}
	tcat := &TransactionCatalog{
		Catalog: cat,
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

func (c *TransactionCatalog) NewMultiTableTransaction(count int) *MultiTableTransaction {
	tx := &MultiTableTransaction{
		TransactionCatalog: c,

		operations: make([]func(context.Context, bun.Tx) error, 0),
		committed:  make(chan struct{}),
	}

	tx.wg.Add(count)
	return tx
}

func (c *TransactionCatalog) ensureTablesExist() error {
	_, err := c.db.NewCreateTable().Model((*sqlIcebergOutboxMessage)(nil)).
		IfNotExists().Exec(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (c *MultiTableTransaction) Commit(ctx context.Context) error {
	return withWriteTx(ctx, c.db, c.commit())
}

func (c *MultiTableTransaction) commit() func(context.Context, bun.Tx) error {
	return func(ctx context.Context, tx bun.Tx) error {
		c.wg.Wait()

		c.mx.Lock()
		defer c.mx.Unlock()

		if len(c.operations) == 0 {
			return errors.New("no operations to commit")
		}

		defer close(c.committed)

		for _, op := range c.operations {
			err := op(ctx, tx)
			if err != nil {
				c.err = err
				return err
			}
		}

		return nil
	}
}

func (c *MultiTableTransaction) CommitTx() func(context.Context, bun.Tx) error {
	return c.commit()
}

func (c *MultiTableTransaction) CreateNamespaceInTx(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	done := false
	defer func() {
		if !done {
			c.wg.Done()
		}
	}()

	if err := checkValidNamespace(namespace); err != nil {
		return err
	}

	exists, err := c.namespaceExists(ctx, strings.Join(namespace, "."))
	if err != nil {
		return err
	}

	if exists {
		return fmt.Errorf("%w: %s", catalog.ErrNamespaceAlreadyExists, strings.Join(namespace, "."))
	}

	if len(props) == 0 {
		props = minimalNamespaceProps
	}

	nsToCreate := strings.Join(namespace, ".")

	dbOp := func(ctx context.Context, tx bun.Tx) error {
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
	}

	msgOp := func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewInsert().Model(&sqlIcebergOutboxMessage{
			MessageType: OutboxMessageTypeCreateNamespace,
			Namespace:   nsToCreate,
		}).Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to create outbox message: %w", err)
		}
		return nil
	}

	c.mx.Lock()
	if c.operations == nil {
		c.operations = make([]func(context.Context, bun.Tx) error, 0)
		c.committed = make(chan struct{})
	}
	c.operations = append(c.operations, dbOp, msgOp)
	c.mx.Unlock()

	done = true
	c.wg.Done()

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

func (c *MultiTableTransaction) CreateTableInTx(ctx context.Context, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	done := false
	defer func() {
		if !done {
			c.wg.Done()
		}
	}()

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

	msgOp := func(ctx context.Context, tx bun.Tx) error {
		msg, err := json.Marshal(OutboxMessageData{
			PreviousMetadataLocation: staged.MetadataLocation(),
			MetadataLocation:         staged.MetadataLocation(),
		})
		if err != nil {
			return fmt.Errorf("failed to marshal create table message: %w", err)
		}

		_, err = tx.NewInsert().Model(&sqlIcebergOutboxMessage{
			MessageType: OutboxMessageTypeCreateTable,
			Message:     msg,
			Namespace:   ns,
			TableName:   tblIdent,
		}).Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to create outbox message: %w", err)
		}
		return nil
	}

	c.mx.Lock()
	if c.operations == nil {
		c.operations = make([]func(context.Context, bun.Tx) error, 0)
		c.committed = make(chan struct{})
	}
	c.operations = append(c.operations, dbOp, msgOp)
	c.mx.Unlock()

	done = true
	c.wg.Done()

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

func (c *MultiTableTransaction) stageCreateTable(ctx context.Context, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.StagedTable, error) {
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

func (c *MultiTableTransaction) CommitTableInTx(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	done := false
	defer func() {
		if !done {
			c.wg.Done()
		}
	}()
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

	msgOp := func(ctx context.Context, tx bun.Tx) error {
		msg, err := json.Marshal(OutboxMessageData{
			PreviousMetadataLocation: current.MetadataLocation(),
			MetadataLocation:         staged.MetadataLocation(),
		})
		if err != nil {
			return fmt.Errorf("failed to marshal commit table message: %w", err)
		}

		_, err = tx.NewInsert().Model(&sqlIcebergOutboxMessage{
			MessageType: OutboxMessageTypeCommitTable,
			Message:     msg,
			Namespace:   strings.Join(ns, "."),
			TableName:   tblName,
		}).Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to create outbox message: %w", err)
		}
		return nil
	}

	c.mx.Lock()
	if c.operations == nil {
		c.operations = make([]func(context.Context, bun.Tx) error, 0)
		c.committed = make(chan struct{})
	}
	c.operations = append(c.operations, dbOp, msgOp)
	c.mx.Unlock()

	done = true
	c.wg.Done()

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

func (c *MultiTableTransaction) stageCommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (*table.Table, *table.StagedTable, error) {
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

type OutboxMessage struct {
	Id          int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
	MessageType OutboxMessageType
	Namespace   string
	TableName   string
	Message     OutboxMessageData
}

func (c *TransactionCatalog) ListOutboxMessages(ctx context.Context, count int) ([]OutboxMessage, error) {
	msgs, err := withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) ([]sqlIcebergOutboxMessage, error) {
		var messages []sqlIcebergOutboxMessage
		err := tx.NewSelect().Model(&messages).Where("status = ?", OutboxMessageStatusPending).Limit(count).Order("created_at").Scan(ctx)
		if err != nil {
			return nil, err
		}
		return messages, nil
	})
	if err != nil {
		return nil, err
	}

	ret := make([]OutboxMessage, len(msgs))
	for i, msg := range msgs {
		om := OutboxMessage{
			Id:          msg.Id,
			CreatedAt:   msg.CreatedAt,
			UpdatedAt:   msg.UpdatedAt,
			MessageType: msg.MessageType,
			Namespace:   msg.Namespace,
			TableName:   msg.TableName,
		}
		switch msg.MessageType {
		case OutboxMessageTypeCreateNamespace:
		case OutboxMessageTypeCreateTable, OutboxMessageTypeCommitTable:
			err := json.Unmarshal(msg.Message, &om.Message)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal outbox message: %w", err)
			}
		default:
			return nil, fmt.Errorf("unsupported outbox message type: %s", msg.MessageType)
		}
		ret[i] = om
	}
	return ret, nil
}

func (c *TransactionCatalog) MarkOutboxMessagesDone(ctx context.Context, id int64) error {
	return withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.NewUpdate().Model((*sqlIcebergOutboxMessage)(nil)).
			Set("status = ?", OutboxMessageStatusDone).
			Where("id = ?", id).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to mark outbox messages done: %w", err)
		}
		return nil
	})
}
