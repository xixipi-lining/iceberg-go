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
	"encoding/json"
	"fmt"
	"time"

	"github.com/uptrace/bun"
)

// OutboxMessageType indicates the type of catalog operation recorded in the outbox.
type OutboxMessageType string

const (
	OutboxMessageTypeCreateNamespace OutboxMessageType = "create_namespace"
	OutboxMessageTypeCreateTable     OutboxMessageType = "create_table"
	OutboxMessageTypeCommitTable     OutboxMessageType = "commit_table"
)

// OutboxMessageData carries metadata location information for outbox messages.
type OutboxMessageData struct {
	PreviousMetadataLocation string `json:"previous_metadata_location"`
	MetadataLocation         string `json:"metadata_location"`
}

// OutboxMessageStatus represents the processing state of an outbox message.
type OutboxMessageStatus int8

const (
	OutboxMessageStatusPending OutboxMessageStatus = 0
	OutboxMessageStatusDone    OutboxMessageStatus = 1
)

// sqlIcebergOutboxMessage is the bun model for the iceberg_outbox_messages table.
type sqlIcebergOutboxMessage struct {
	bun.BaseModel `bun:"table:iceberg_outbox_messages"`

	Id        int64     `bun:",pk,autoincrement"`
	CreatedAt time.Time `bun:"created_at,nullzero,notnull,default:current_timestamp"`
	UpdatedAt time.Time `bun:"updated_at,nullzero,notnull,default:current_timestamp"`
	Status    OutboxMessageStatus

	CatalogName    string
	TableNamespace string
	TableName      string

	MessageType OutboxMessageType
	Message     []byte
}

// insertOutboxMessage inserts an outbox message within the given transaction.
func insertOutboxMessage(ctx context.Context, tx bun.Tx, catalogName, tableNamespace, tableName string, msgType OutboxMessageType, data *OutboxMessageData) error {
	var message []byte

	if data != nil {
		var err error
		message, err = json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal outbox message data: %w", err)
		}
	}

	_, err := tx.NewInsert().Model(&sqlIcebergOutboxMessage{
		Status:         OutboxMessageStatusPending,
		CatalogName:    catalogName,
		TableNamespace: tableNamespace,
		TableName:      tableName,
		MessageType:    msgType,
		Message:        message,
	}).Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to insert outbox message: %w", err)
	}

	return nil
}

func listOutboxMessages(ctx context.Context, tx bun.Tx, catalogName, tableNamespace string, limit int) ([]sqlIcebergOutboxMessage, error) {
	var outboxMessages []sqlIcebergOutboxMessage
	err := tx.NewSelect().Model(&outboxMessages).
		Where("catalog_name = ?", catalogName).
		Where("table_namespace = ?", tableNamespace).
		Where("status = ?", OutboxMessageStatusPending).
		Limit(limit).
		Scan(ctx)
	return outboxMessages, err
}

func markOutboxMessageAsDone(ctx context.Context, tx bun.Tx, catalogName, tableNamespace string, id int64) error {
	res, err := tx.NewUpdate().Model(&sqlIcebergOutboxMessage{}).
		Where("catalog_name = ?", catalogName).
		Where("table_namespace = ?", tableNamespace).
		Where("id = ?", id).
		Set("status = ?", OutboxMessageStatusDone).
		Exec(ctx)

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to mark outbox message as done: %v", err)
	}
	if n == 0 {
		return fmt.Errorf("outbox message not found: %v", id)
	}
	return nil
}

type OutboxMessage struct {
	Id             int64
	MessageType    OutboxMessageType
	TableNamespace string
	TableName      string
	Data           OutboxMessageData
}

func (c *Catalog) ListOutboxMessages(ctx context.Context, tableNamespace string, limit int) ([]OutboxMessage, error) {
	msgs, err := withReadTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) ([]sqlIcebergOutboxMessage, error) {
		return listOutboxMessages(ctx, tx, c.name, tableNamespace, limit)
	})
	if err != nil {
		return nil, err
	}

	ret := make([]OutboxMessage, len(msgs))
	for i, msg := range msgs {
		var data OutboxMessageData
		var err error
		if msg.Message != nil {
			err = json.Unmarshal(msg.Message, &data)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal outbox message data: %w", err)
			}
		}
		ret[i] = OutboxMessage{
			Id:             msg.Id,
			MessageType:    msg.MessageType,
			TableNamespace: msg.TableNamespace,
			TableName:      msg.TableName,
			Data:           data,
		}
	}
	return ret, nil
}

func (c *Catalog) MarkOutboxMessageAsDone(ctx context.Context, tableNamespace string, id int64) error {
	err := withWriteTx(ctx, c.db, func(ctx context.Context, tx bun.Tx) error {
		return markOutboxMessageAsDone(ctx, tx, c.name, tableNamespace, id)
	})
	return err
}
