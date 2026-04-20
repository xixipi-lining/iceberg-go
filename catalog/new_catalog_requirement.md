# gnar catalog

gnar catalog 是基于 sql catalog 的一个新 catalog. 与 sql catalog 不同的地方在于：

1. 支持通过 outbox 的方式，将 namespace 的创建、table 的创建、table 的 commit 操作同步到外部系统。
2. 支持 [多表事务接口](./multi_table_transaction.go)。同样的多表事务中的 commit 也会同步到 outbox 表

outbox 目前的设计如下，可以根据实际情况进行调整：

```go
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
```
