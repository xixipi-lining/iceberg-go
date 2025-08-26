package rest

import (
	"context"
	"fmt"
	"net/url"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

var _ catalog.TransactionCatalog = (*Catalog)(nil)

type kv struct {
	Key string `json:"key"`
	Value string `json:"value"`
}

func (r *Catalog) SetKVSidecar(ctx context.Context, key, value string) error {
	payload := kv{
		Key: key,
		Value: value,
	}
	_, err := doPost[kv, struct{}](ctx, r.baseURI, []string{"kvsidecar"}, payload, r.cl, nil)
	if err != nil {
		return err
	}
	return nil
}

func (r *Catalog) GetKVSidecar(ctx context.Context, key string) (string, error) {
	uri := r.baseURI.JoinPath("kvsidecar")
	v := url.Values{}
	v.Set("key", key)
	uri.RawQuery = v.Encode()

	rsp, err := doGet[kv](ctx, uri, []string{}, r.cl, nil)
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
	UpdateTable *updateTableRequest `json:"update_table"`
	SetKVSidecar *kv `json:"set_kv_sidecar"`
}

func (r *Catalog) Transaction(ctx context.Context, reqs []catalog.TransactionRequest, followers ...catalog.FollowerCatalog) error {
	if len(followers) != 0 {
		return fmt.Errorf("followers are not supported for REST catalog")
	}

	payload := make([]transactionRequest, len(reqs))
	for i, req := range reqs {
		switch req := req.(type) {
		case *catalog.CreateTableRequest:
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

		case *catalog.CommitTableRequest:
			_, tbl, err := splitIdentForPath(req.Identifier)
			if err != nil {
				return err
			}

			payload[i].UpdateTable = &updateTableRequest{
				Identifier: identifier{
					Namespace: catalog.NamespaceFromIdent(req.Identifier),
					Name: tbl,
				},
				Requirements: req.Requirements,
				Updates: req.Updates,
			}
		case *catalog.SetKVSidecarRequest:
			payload[i].SetKVSidecar = &kv{
				Key: req.Key,
				Value: req.Value,
			}
		}
	}

	_, err := doPost[[]transactionRequest, struct{}](ctx, r.baseURI, []string{"transaction"}, payload, r.cl, nil)
	if err != nil {
		return err
	}
	return nil
}