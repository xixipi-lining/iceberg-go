package catalog

import (
	"context"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

type TransactionCatalog interface {
	Catalog

	SetQueueOffset(ctx context.Context, queueId, offset string) error
	GetQueueOffset(ctx context.Context, queueId string) (string, error)

	NewMultiTableTransaction(int) MultiTableTransaction
}

type MultiTableTransaction interface {
	CreateTableInTx(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...CreateTableOpt) (*table.Table, error)
	CommitTableInTx(ctx context.Context, identifier table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error)
	SetQueueOffsetInTx(ctx context.Context, queueId, offset string) error

	Commit(ctx context.Context) error
}

type FollowerCatalog interface {
	FollowCreateTable(ctx context.Context, staged *table.StagedTable) error
	FollowCommitTable(ctx context.Context, previousMetadataLocation *string, staged *table.StagedTable) error
}
