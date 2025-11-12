package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"sync"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

type TransactionCatalog struct {
	*Catalog
}

type MultiTableTransaction struct {
	*TransactionCatalog

	mx        sync.Mutex
	wg        sync.WaitGroup
	requests  []transactionRequest
	idx       int
	committed chan struct{}
	err       error
	resp      []transactionResponse
}

func NewTransactionCatalog(cat *Catalog) (catalog.TransactionCatalog, error) {
	return &TransactionCatalog{Catalog: cat}, nil
}

type queueOffset struct {
	QueueId string `json:"queue_id"`
	Offset  string `json:"offset"`
}

func (c *TransactionCatalog) SetQueueOffset(ctx context.Context, queueId, offset string) error {
	payload := queueOffset{
		QueueId: queueId,
		Offset:  offset,
	}
	_, err := doPost[queueOffset, struct{}](ctx, c.baseURI, []string{"queue-offset"}, payload, c.cl, nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *TransactionCatalog) GetQueueOffset(ctx context.Context, queueId string) (string, error) {
	uri := c.baseURI.JoinPath("queue-offset")
	v := url.Values{}
	v.Set("queue_id", queueId)
	uri.RawQuery = v.Encode()

	rsp, err := doGet[queueOffset](ctx, uri, []string{}, c.cl, nil)
	if err != nil {
		return "", err
	}
	return rsp.Offset, nil
}

func (c *TransactionCatalog) NewMultiTableTransaction(count int) catalog.MultiTableTransaction {
	tx := &MultiTableTransaction{
		TransactionCatalog: c,
		requests:           make([]transactionRequest, 0),
		idx:                0,
		committed:          make(chan struct{}),
	}
	tx.wg.Add(count)
	return tx
}

func (c *MultiTableTransaction) Commit(ctx context.Context) error {
	c.wg.Wait()

	c.mx.Lock()
	defer c.mx.Unlock()

	if len(c.requests) == 0 {
		return errors.New("no requests to commit")
	}

	defer close(c.committed)

	ret, err := doPost[[]transactionRequest, []transactionResponse](ctx, c.baseURI, []string{"transaction"}, c.requests, c.cl, nil)
	if err != nil {
		return err
	}

	c.resp = ret
	c.err = err
	return err
}

func (c *MultiTableTransaction) SetQueueOffsetInTx(ctx context.Context, queueId, offset string) error {
	payload := queueOffset{
		QueueId: queueId,
		Offset:  offset,
	}
	c.mx.Lock()
	if c.requests == nil {
		c.requests = make([]transactionRequest, 0)
		c.committed = make(chan struct{})
		c.idx = 0
	}
	c.requests = append(c.requests, transactionRequest{
		SetQueueOffset: &payload,
	})
	c.idx++
	c.mx.Unlock()
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

type updateTableRequest struct {
	Identifier   identifier         `json:"identifier"`
	Requirements table.Requirements `json:"requirements"`
	Updates      table.Updates      `json:"updates"`
}

type transactionRequest struct {
	CreateTable *struct {
		createTableRequest
		Namespace string `json:"namespace"`
	} `json:"create_table"`
	UpdateTable    *updateTableRequest `json:"update_table"`
	SetQueueOffset *queueOffset        `json:"set_queue_offset"`
}

type transactionResponse = json.RawMessage

func (c *MultiTableTransaction) CreateTableInTx(ctx context.Context, ident table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	ns, tbl, err := splitIdentForPath(ident)
	if err != nil {
		return nil, err
	}

	var cfg catalog.CreateTableCfg
	for _, o := range opts {
		o(&cfg)
	}

	freshSchema, err := iceberg.AssignFreshSchemaIDs(schema, nil)
	if err != nil {
		return nil, err
	}

	freshPartitionSpec, err := iceberg.AssignFreshPartitionSpecIDs(cfg.PartitionSpec, schema, freshSchema)
	if err != nil {
		return nil, err
	}

	freshSortOrder, err := table.AssignFreshSortOrderIDs(cfg.SortOrder, schema, freshSchema)
	if err != nil {
		return nil, err
	}

	payload := createTableRequest{
		Name:          tbl,
		Schema:        freshSchema,
		Location:      cfg.Location,
		PartitionSpec: &freshPartitionSpec,
		WriteOrder:    &freshSortOrder,
		StageCreate:   false,
		Props:         cfg.Properties,
	}

	c.mx.Lock()
	if c.requests == nil {
		c.requests = make([]transactionRequest, 0)
		c.committed = make(chan struct{})
		c.idx = 0
	}
	idx := c.idx
	c.requests = append(c.requests, transactionRequest{
		CreateTable: &struct {
			createTableRequest
			Namespace string `json:"namespace"`
		}{
			createTableRequest: payload,
			Namespace:          ns,
		},
	})
	c.idx++
	c.mx.Unlock()
	c.wg.Done()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.committed:
	}

	if c.err != nil {
		return nil, fmt.Errorf("transaction commit error %w", c.err)
	}

	var ret loadTableResponse
	if err := json.Unmarshal(c.resp[idx], &ret); err != nil {
		return nil, fmt.Errorf("failed to unmarshal loadTableResponse: %w", err)
	}

	config := maps.Clone(c.Catalog.props)
	maps.Copy(config, ret.Metadata.Properties())
	maps.Copy(config, ret.Config)

	return c.tableFromResponse(ctx, ident, ret.Metadata, ret.MetadataLoc, config)
}

func (c *MultiTableTransaction) CommitTableInTx(ctx context.Context, ident table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.mx.Lock()
	if c.requests == nil {
		c.requests = make([]transactionRequest, 0)
		c.committed = make(chan struct{})
		c.idx = 0
	}
	idx := c.idx
	c.requests = append(c.requests, transactionRequest{
		UpdateTable: &updateTableRequest{
			Identifier: identifier{
				Namespace: catalog.NamespaceFromIdent(ident),
				Name:      catalog.TableNameFromIdent(ident),
			},
			Requirements: requirements,
			Updates:      updates,
		},
	})
	c.idx++
	c.mx.Unlock()
	c.wg.Done()

	select {
	case <-ctx.Done():
		return nil, "", ctx.Err()
	case <-c.committed:
	}

	if c.err != nil {
		return nil, "", fmt.Errorf("transaction commit error %w", c.err)
	}

	var ret commitTableResponse
	if err := json.Unmarshal(c.resp[idx], &ret); err != nil {
		return nil, "", fmt.Errorf("failed to unmarshal commitTableResponse: %w", err)
	}

	config := maps.Clone(c.Catalog.props)
	maps.Copy(config, ret.Metadata.Properties())

	return ret.Metadata, ret.MetadataLoc, nil
}
