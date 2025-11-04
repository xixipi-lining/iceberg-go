package rest

import (
	"context"
	"net/url"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

type TransactionCatalog struct {
	*Catalog
}

func NewTransactionCatalog(cat *Catalog) (catalog.TransactionCatalog, error) {
	return &TransactionCatalog{cat}, nil
}

type kv struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (c *TransactionCatalog) SetKVSidecar(ctx context.Context, key, value string) error {
	payload := kv{
		Key:   key,
		Value: value,
	}
	_, err := doPost[kv, struct{}](ctx, c.baseURI, []string{"kv-sidecar"}, payload, c.cl, nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *TransactionCatalog) GetKVSidecar(ctx context.Context, key string) (string, error) {
	uri := c.baseURI.JoinPath("kv-sidecar")
	v := url.Values{}
	v.Set("key", key)
	uri.RawQuery = v.Encode()

	rsp, err := doGet[kv](ctx, uri, []string{}, c.cl, nil)
	if err != nil {
		return "", err
	}
	return rsp.Value, nil
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
	UpdateTable  *updateTableRequest `json:"update_table"`
	SetKVSidecar *kv                 `json:"set_kv_sidecar"`
}

func (c *TransactionCatalog) Transaction(ctx context.Context, operations []catalog.Operation) error {
	payload := make([]transactionRequest, len(operations))
	for i, op := range operations {
		switch req := op.(type) {
		case *catalog.OperationCreateTable:
			ns, tbl, err := splitIdentForPath(req.Identifier)
			if err != nil {
				return err
			}

			var cfg catalog.CreateTableCfg
			for _, o := range req.Opts {
				o(&cfg)
			}

			freshSchema, err := iceberg.AssignFreshSchemaIDs(req.Schema, nil)
			if err != nil {
				return err
			}

			freshPartitionSpec, err := iceberg.AssignFreshPartitionSpecIDs(cfg.PartitionSpec, req.Schema, freshSchema)
			if err != nil {
				return err
			}

			freshSortOrder, err := table.AssignFreshSortOrderIDs(cfg.SortOrder, req.Schema, freshSchema)
			if err != nil {
				return err
			}

			payload[i].CreateTable = &struct {
				createTableRequest
				Namespace string `json:"namespace"`
			}{
				createTableRequest: createTableRequest{
					Name:          tbl,
					Schema:        freshSchema,
					Location:      cfg.Location,
					PartitionSpec: &freshPartitionSpec,
					WriteOrder:    &freshSortOrder,
					StageCreate:   false,
					Props:         cfg.Properties,
				},
				Namespace: ns,
			}

		case *catalog.OperationCommitTable:
			_, tbl, err := splitIdentForPath(req.Identifier)
			if err != nil {
				return err
			}

			payload[i].UpdateTable = &updateTableRequest{
				Identifier: identifier{
					Namespace: catalog.NamespaceFromIdent(req.Identifier),
					Name:      tbl,
				},
				Requirements: req.Requirements,
				Updates:      req.Updates,
			}
		case *catalog.OperationSetKVSidecar:
			payload[i].SetKVSidecar = &kv{
				Key:   req.Key,
				Value: req.Value,
			}
		}
	}

	_, err := doPost[[]transactionRequest, struct{}](ctx, c.baseURI, []string{"transaction"}, payload, c.cl, nil)
	if err != nil {
		return err
	}
	return nil
}
