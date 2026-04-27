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

	MessageType OutboxMessageType
	Status      OutboxMessageStatus
	Namespace   string
	TableName   string
	Message     []byte
}

// insertOutboxMessage inserts an outbox message within the given transaction.
func insertOutboxMessage(ctx context.Context, tx bun.Tx, msgType OutboxMessageType, namespace, tableName string, data *OutboxMessageData) error {
	var message []byte

	if data != nil {
		var err error
		message, err = json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal outbox message data: %w", err)
		}
	}

	_, err := tx.NewInsert().Model(&sqlIcebergOutboxMessage{
		MessageType: msgType,
		Status:      OutboxMessageStatusPending,
		Namespace:   namespace,
		TableName:   tableName,
		Message:     message,
	}).Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to insert outbox message: %w", err)
	}

	return nil
}
