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

package gnar_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	gnarcat "github.com/apache/iceberg-go/catalog/gnar"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/driver/sqliteshim"
)

var tableSchemaNested = iceberg.NewSchemaWithIdentifiers(1,
	[]int{1},
	iceberg.NestedField{
		ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false,
	},
	iceberg.NestedField{
		ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	},
	iceberg.NestedField{
		ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false,
	},
)

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	var b strings.Builder
	b.Grow(n)
	for range n {
		b.WriteByte(letters[rand.IntN(len(letters))])
	}
	return b.String()
}

type GnarCatalogTestSuite struct {
	suite.Suite

	warehouse string
}

func (s *GnarCatalogTestSuite) SetupTest() {
	var err error
	s.warehouse, err = os.MkdirTemp(os.TempDir(), "test_gnar_*")
	s.Require().NoError(err)
}

func (s *GnarCatalogTestSuite) TearDownTest() {
	s.Require().NoError(os.RemoveAll(s.warehouse))
}

func (s *GnarCatalogTestSuite) catalogUri() string {
	return "file://" + filepath.Join(s.warehouse, "gnar-catalog.db")
}

func (s *GnarCatalogTestSuite) getDB() *sql.DB {
	sqldb, err := sql.Open(sqliteshim.ShimName, s.catalogUri())
	s.Require().NoError(err)
	return sqldb
}

func (s *GnarCatalogTestSuite) getCatalog() *gnarcat.Catalog {
	sqldb := s.getDB()
	cat, err := gnarcat.NewCatalog("default", sqldb, sqlcat.SQLite, iceberg.Properties{
		"warehouse": "file://" + s.warehouse,
	})
	s.Require().NoError(err)
	return cat
}

func (s *GnarCatalogTestSuite) getCatalogViaRegistry() catalog.Catalog {
	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":             s.catalogUri(),
		sqlcat.DriverKey:  sqliteshim.ShimName,
		sqlcat.DialectKey: string(sqlcat.SQLite),
		"type":            "gnar",
		"warehouse":       "file://" + s.warehouse,
	})
	s.Require().NoError(err)
	return cat
}

func (s *GnarCatalogTestSuite) randomTableIdentifier() table.Identifier {
	dbname := "db-" + randomString(10)
	tablename := "tbl-" + randomString(10)
	s.Require().NoError(os.MkdirAll(filepath.Join(s.warehouse, dbname+".db", tablename, "metadata"), 0o755))
	return table.Identifier{dbname, tablename}
}

// queryOutboxMessages returns all outbox messages from the database.
func (s *GnarCatalogTestSuite) queryOutboxMessages(db *sql.DB) []outboxRow {
	bunDB := bun.NewDB(db, sqlitedialect.New())
	var rows []outboxRow
	err := bunDB.NewSelect().
		TableExpr("iceberg_outbox_messages").
		Column("message_type", "status", "namespace", "table_name", "message").
		OrderExpr("id ASC").
		Scan(context.Background(), &rows)
	s.Require().NoError(err)
	return rows
}

type outboxRow struct {
	MessageType string `bun:"message_type"`
	Status      int8   `bun:"status"`
	Namespace   string `bun:"namespace"`
	TableName   string `bun:"table_name"`
	Message     []byte `bun:"message"`
}

func TestGnarCatalogTestSuite(t *testing.T) {
	suite.Run(t, new(GnarCatalogTestSuite))
}

// --- Tests ---

func (s *GnarCatalogTestSuite) TestGnarCatalogType() {
	cat := s.getCatalog()
	s.Equal(gnarcat.Gnar, cat.CatalogType())
}

func (s *GnarCatalogTestSuite) TestGnarCatalogRegistry() {
	cat := s.getCatalogViaRegistry()
	s.Equal(gnarcat.Gnar, cat.CatalogType())
}

func (s *GnarCatalogTestSuite) TestOutboxTableCreation() {
	sqldb := s.getDB()
	cat, err := gnarcat.NewCatalog("default", sqldb, sqlcat.SQLite, iceberg.Properties{
		"warehouse": "file://" + s.warehouse,
	})
	s.Require().NoError(err)
	s.NotNil(cat)

	// Verify the outbox table was created
	rows, err := sqldb.Query("SELECT name FROM sqlite_master WHERE type='table' AND name='iceberg_outbox_messages'")
	s.Require().NoError(err)
	defer rows.Close()

	s.True(rows.Next(), "iceberg_outbox_messages table should exist")
}

func (s *GnarCatalogTestSuite) TestCreateNamespaceOutbox() {
	sqldb := s.getDB()
	cat, err := gnarcat.NewCatalog("default", sqldb, sqlcat.SQLite, iceberg.Properties{
		"warehouse": "file://" + s.warehouse,
	})
	s.Require().NoError(err)

	ctx := context.Background()
	ns := table.Identifier{"test_ns"}

	// Create namespace
	s.Require().NoError(cat.CreateNamespace(ctx, ns, iceberg.Properties{"key": "val"}))

	// Verify namespace was created
	exists, err := cat.CheckNamespaceExists(ctx, ns)
	s.Require().NoError(err)
	s.True(exists)

	// Verify outbox message
	outbox := s.queryOutboxMessages(sqldb)
	s.Require().Len(outbox, 1)
	s.Equal("create_namespace", outbox[0].MessageType)
	s.Equal(int8(0), outbox[0].Status) // pending
	s.Equal("test_ns", outbox[0].Namespace)
	s.Empty(outbox[0].TableName)
}

func (s *GnarCatalogTestSuite) TestCreateTableOutbox() {
	sqldb := s.getDB()
	cat, err := gnarcat.NewCatalog("default", sqldb, sqlcat.SQLite, iceberg.Properties{
		"warehouse": "file://" + s.warehouse,
	})
	s.Require().NoError(err)

	ctx := context.Background()
	tblID := s.randomTableIdentifier()
	ns := catalog.NamespaceFromIdent(tblID)
	tblName := catalog.TableNameFromIdent(tblID)

	// Create namespace first
	s.Require().NoError(cat.CreateNamespace(ctx, ns, nil))

	// Create table
	tbl, err := cat.CreateTable(ctx, tblID, tableSchemaNested)
	s.Require().NoError(err)
	s.NotNil(tbl)

	// Verify outbox messages (1 for namespace + 1 for table)
	outbox := s.queryOutboxMessages(sqldb)
	s.Require().Len(outbox, 2)

	// First message: create_namespace
	s.Equal("create_namespace", outbox[0].MessageType)

	// Second message: create_table with metadata location
	s.Equal("create_table", outbox[1].MessageType)
	s.Equal(int8(0), outbox[1].Status)
	s.Equal(strings.Join(ns, "."), outbox[1].Namespace)
	s.Equal(tblName, outbox[1].TableName)

	// Verify outbox message data contains metadata location
	var msgData gnarcat.OutboxMessageData
	s.Require().NoError(json.Unmarshal(outbox[1].Message, &msgData))
	s.NotEmpty(msgData.MetadataLocation)
	s.Empty(msgData.PreviousMetadataLocation)
}

func (s *GnarCatalogTestSuite) TestCommitTableOutbox() {
	sqldb := s.getDB()
	cat, err := gnarcat.NewCatalog("default", sqldb, sqlcat.SQLite, iceberg.Properties{
		"warehouse": "file://" + s.warehouse,
	})
	s.Require().NoError(err)

	ctx := context.Background()
	tblID := s.randomTableIdentifier()
	ns := catalog.NamespaceFromIdent(tblID)
	tblName := catalog.TableNameFromIdent(tblID)

	// Create namespace and table
	s.Require().NoError(cat.CreateNamespace(ctx, ns, nil))
	tbl, err := cat.CreateTable(ctx, tblID, tableSchemaNested)
	s.Require().NoError(err)
	originalMetadataLocation := tbl.MetadataLocation()

	// Commit a change (set properties)
	tx := tbl.NewTransaction()
	s.Require().NoError(tx.SetProperties(map[string]string{"key": "value"}))
	_, err = tx.Commit(ctx)
	s.Require().NoError(err)

	// Verify outbox messages (1 ns + 1 create table + 1 commit table)
	outbox := s.queryOutboxMessages(sqldb)
	s.Require().Len(outbox, 3)

	// Third message: commit_table with both locations
	s.Equal("commit_table", outbox[2].MessageType)
	s.Equal(int8(0), outbox[2].Status)
	s.Equal(strings.Join(ns, "."), outbox[2].Namespace)
	s.Equal(tblName, outbox[2].TableName)

	var msgData gnarcat.OutboxMessageData
	s.Require().NoError(json.Unmarshal(outbox[2].Message, &msgData))
	s.Equal(originalMetadataLocation, msgData.PreviousMetadataLocation)
	s.NotEmpty(msgData.MetadataLocation)
	s.NotEqual(originalMetadataLocation, msgData.MetadataLocation)
}

func (s *GnarCatalogTestSuite) TestCommitTransactionOutbox() {
	sqldb := s.getDB()
	cat, err := gnarcat.NewCatalog("default", sqldb, sqlcat.SQLite, iceberg.Properties{
		"warehouse": "file://" + s.warehouse,
	})
	s.Require().NoError(err)

	ctx := context.Background()
	tblID1 := s.randomTableIdentifier()
	tblID2 := s.randomTableIdentifier()
	ns1 := catalog.NamespaceFromIdent(tblID1)
	ns2 := catalog.NamespaceFromIdent(tblID2)

	// Create namespaces and tables
	s.Require().NoError(cat.CreateNamespace(ctx, ns1, nil))
	s.Require().NoError(cat.CreateNamespace(ctx, ns2, nil))

	tbl1, err := cat.CreateTable(ctx, tblID1, tableSchemaNested)
	s.Require().NoError(err)
	tbl2, err := cat.CreateTable(ctx, tblID2, tableSchemaNested)
	s.Require().NoError(err)

	// Build multi-table transaction
	mtx, err := catalog.NewMultiTableTransaction(cat)
	s.Require().NoError(err)

	tx1 := tbl1.NewTransaction()
	s.Require().NoError(tx1.SetProperties(map[string]string{"pipeline": "v2"}))
	s.Require().NoError(mtx.AddTransaction(tx1))

	tx2 := tbl2.NewTransaction()
	s.Require().NoError(tx2.SetProperties(map[string]string{"pipeline": "v2"}))
	s.Require().NoError(mtx.AddTransaction(tx2))

	// Commit atomically
	s.Require().NoError(mtx.Commit(ctx))

	// Verify outbox messages:
	// 2 create_namespace + 2 create_table + 2 commit_table (from CommitTransaction)
	outbox := s.queryOutboxMessages(sqldb)
	s.Require().Len(outbox, 6)

	// Last two should be commit_table from the multi-table transaction
	s.Equal("commit_table", outbox[4].MessageType)
	s.Equal("commit_table", outbox[5].MessageType)

	// Verify both tables have outbox entries with correct metadata
	commitOutbox := outbox[4:]
	for _, row := range commitOutbox {
		var msgData gnarcat.OutboxMessageData
		s.Require().NoError(json.Unmarshal(row.Message, &msgData))
		s.NotEmpty(msgData.MetadataLocation)
		s.NotEmpty(msgData.PreviousMetadataLocation)
	}
}

func (s *GnarCatalogTestSuite) TestTransactionalCatalogInterface() {
	cat := s.getCatalog()

	// Verify gnar catalog implements TransactionalCatalog
	_, ok := catalog.Catalog(cat).(catalog.TransactionalCatalog)
	s.True(ok, "gnar catalog should implement TransactionalCatalog")
}

func (s *GnarCatalogTestSuite) TestLoadTableHasGnarAsCatalogIO() {
	cat := s.getCatalog()
	ctx := context.Background()

	tblID := s.randomTableIdentifier()
	ns := catalog.NamespaceFromIdent(tblID)

	s.Require().NoError(cat.CreateNamespace(ctx, ns, nil))
	tbl, err := cat.CreateTable(ctx, tblID, tableSchemaNested)
	s.Require().NoError(err)

	// Load the table and commit via transaction - this should
	// go through the gnar catalog's CommitTable (with outbox)
	loaded, err := cat.LoadTable(ctx, tblID)
	s.Require().NoError(err)
	s.Equal(tbl.MetadataLocation(), loaded.MetadataLocation())

	// A commit through the loaded table's transaction should
	// produce an outbox entry
	tx := loaded.NewTransaction()
	s.Require().NoError(tx.SetProperties(map[string]string{"via": "transaction"}))
	_, err = tx.Commit(ctx)
	s.Require().NoError(err)

	// Should have outbox for: create_namespace + create_table + commit_table
	sqldb := s.getDB()
	outbox := s.queryOutboxMessages(sqldb)
	s.Require().Len(outbox, 3)
	s.Equal("commit_table", outbox[2].MessageType)
}

func (s *GnarCatalogTestSuite) TestDelegatedOperationsStillWork() {
	cat := s.getCatalog()
	ctx := context.Background()

	tblID := s.randomTableIdentifier()
	ns := catalog.NamespaceFromIdent(tblID)

	// Test delegated operations (these go through the embedded SQL catalog)
	s.Require().NoError(cat.CreateNamespace(ctx, ns, nil))

	_, err := cat.CreateTable(ctx, tblID, tableSchemaNested)
	s.Require().NoError(err)

	// ListTables (delegated)
	var tables []table.Identifier
	for tbl, err := range cat.ListTables(ctx, ns) {
		s.Require().NoError(err)
		tables = append(tables, tbl)
	}
	s.Len(tables, 1)

	// ListNamespaces (delegated)
	nsList, err := cat.ListNamespaces(ctx, nil)
	s.Require().NoError(err)
	s.Len(nsList, 1)

	// DropTable (delegated)
	s.NoError(cat.DropTable(ctx, tblID))

	// DropNamespace (delegated)
	s.NoError(cat.DropNamespace(ctx, ns))
}

func (s *GnarCatalogTestSuite) TestCreateDuplicateNamespace() {
	cat := s.getCatalog()
	ctx := context.Background()
	ns := table.Identifier{"dup_ns"}

	s.Require().NoError(cat.CreateNamespace(ctx, ns, nil))
	err := cat.CreateNamespace(ctx, ns, nil)
	s.ErrorIs(err, catalog.ErrNamespaceAlreadyExists)
}

func (s *GnarCatalogTestSuite) TestCreateTableNonExistingNamespace() {
	cat := s.getCatalog()
	ctx := context.Background()

	_, err := cat.CreateTable(ctx, table.Identifier{"nonexistent", "tbl"}, tableSchemaNested)
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}
