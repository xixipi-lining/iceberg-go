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

//go:build integration

package dynamodb_test

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	dynamodbcat "github.com/apache/iceberg-go/catalog/dynamodb"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/modules/compose"
)

const composeContent = `
services:
  dynamodb-local:
    image: amazon/dynamodb-local
    networks:
      iceberg_net:
    ports:
      - 8000
    working_dir: /home/dynamodblocal
    command: ["-jar", "DynamoDBLocal.jar", "-sharedDb", "-inMemory"]
  minio:
    image: minio/minio
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
      - MINIO_DOMAIN=minio
    networks:
      iceberg_net:
        aliases:
          - warehouse.minio
    ports:
      - 9001
      - 9000
    command: ["server", "/data", "--console-address", ":9001"]
  mc:
    depends_on:
      - minio
    image: minio/mc
    networks:
      iceberg_net:
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    entrypoint: >
      /bin/sh -c "
      until (/usr/bin/mc alias set minio http://minio:9000 admin password) do echo '...waiting...' && sleep 1; done;
      /usr/bin/mc rm -r --force minio/warehouse;
      /usr/bin/mc mb minio/warehouse;
      /usr/bin/mc policy set public minio/warehouse;
      echo 'BUCKET_READY';
      tail -f /dev/null
      "
networks:
  iceberg_net:
`

const (
	TestNamespaceIdent = "TEST_NS"
	location           = "s3://warehouse/iceberg"
)

var tableSchemaSimple = iceberg.NewSchemaWithIdentifiers(1, []int{2},
	iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.StringType{}, Required: false},
	iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false},
)

var tableSchemaNestedTest = iceberg.NewSchemaWithIdentifiers(1,
	[]int{1},
	iceberg.NestedField{
		ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: true},
	iceberg.NestedField{
		ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	iceberg.NestedField{
		ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false},
	iceberg.NestedField{
		ID: 4, Name: "qux", Required: true, Type: &iceberg.ListType{
			ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: true}},
	iceberg.NestedField{
		ID: 6, Name: "quux",
		Type: &iceberg.MapType{
			KeyID:   7,
			KeyType: iceberg.PrimitiveTypes.String,
			ValueID: 8,
			ValueType: &iceberg.MapType{
				KeyID:         9,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       10,
				ValueType:     iceberg.PrimitiveTypes.Int32,
				ValueRequired: true,
			},
			ValueRequired: true,
		},
		Required: true},
	iceberg.NestedField{
		ID: 11, Name: "location", Type: &iceberg.ListType{
			ElementID: 12, Element: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 13, Name: "latitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
					{ID: 14, Name: "longitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
				},
			},
			ElementRequired: true},
		Required: true},
	iceberg.NestedField{
		ID:   15,
		Name: "person",
		Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 16, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
				{ID: 17, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
			},
		},
		Required: false,
	},
)

type DynamoDBIntegrationSuite struct {
	suite.Suite

	ctx   context.Context
	cat   *dynamodbcat.Catalog
	stack *compose.DockerCompose

	dynamodbEndpoint string
	s3Endpoint       string
}

func (s *DynamoDBIntegrationSuite) SetupSuite() {
	s.ctx = s.T().Context()
	stack, err := compose.NewDockerComposeWith(compose.WithStackReaders(strings.NewReader(composeContent)))
	s.Require().NoError(err)
	s.stack = stack
	s.Require().NoError(stack.Up(s.ctx))

	dynamodbContainer, err := s.stack.ServiceContainer(s.ctx, "dynamodb-local")
	s.Require().NoError(err)

	endpoint, err := dynamodbContainer.PortEndpoint(s.ctx, "8000", "http")
	s.Require().NoError(err)
	s.Require().NotNil(endpoint)
	s.dynamodbEndpoint = endpoint

	minioContainer, err := s.stack.ServiceContainer(s.ctx, "minio")
	s.Require().NoError(err)
	s.Require().NotNil(minioContainer)

	endpoint, err = minioContainer.PortEndpoint(s.ctx, "9000", "http")
	s.Require().NoError(err)
	s.Require().NotNil(endpoint)
	s.s3Endpoint = endpoint
}

func (s *DynamoDBIntegrationSuite) TearDownSuite() {
	if s.stack != nil {
		s.stack.Down(s.ctx, compose.RemoveOrphans(true), compose.RemoveImagesLocal)
	}
}

func (s *DynamoDBIntegrationSuite) SetupTest() {
	cfg, err := config.LoadDefaultConfig(s.ctx,
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint(s.dynamodbEndpoint),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("admin", "password", "token"),
		),
		config.WithRetryMaxAttempts(10),
		config.WithRetryMode(aws.RetryModeStandard),
	)
	s.Require().NoError(err)

	cat, err := dynamodbcat.NewCatalog("test-table", dynamodb.NewFromConfig(cfg), iceberg.Properties{
		io.S3Region:          "us-east-1",
		io.S3AccessKeyID:     "admin",
		io.S3SecretAccessKey: "password",
		io.S3EndpointURL:     s.s3Endpoint,
		"warehouse":          location,
	})
	s.Require().NoError(err)
	s.cat = cat
}

func (s *DynamoDBIntegrationSuite) TearDownTest() {
	// Clean up is handled by compose teardown
}

func (s *DynamoDBIntegrationSuite) ensureNamespace() {
	exists, err := s.cat.CheckNamespaceExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent))
	s.Require().NoError(err)
	if exists {
		s.Require().NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent)))
	}

	s.NoError(s.cat.CreateNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent),
		iceberg.Properties{"foo": "bar", "prop": "yes"}))
}

func (s *DynamoDBIntegrationSuite) TestCreateNamespace() {
	exists, err := s.cat.CheckNamespaceExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent))
	s.Require().NoError(err)
	if exists {
		s.Require().NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent)))
	}

	s.NoError(s.cat.CreateNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent), nil))
	s.NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent)))
}

func (s *DynamoDBIntegrationSuite) TestLoadNamespaceProps() {
	s.ensureNamespace()

	props, err := s.cat.LoadNamespaceProperties(s.ctx, catalog.ToIdentifier(TestNamespaceIdent))
	s.Require().NoError(err)
	s.Require().NotNil(props)
	s.Equal("bar", props["foo"])
	s.Equal("yes", props["prop"])
}

func (s *DynamoDBIntegrationSuite) TestUpdateNamespaceProps() {
	s.ensureNamespace()

	summary, err := s.cat.UpdateNamespaceProperties(s.ctx, catalog.ToIdentifier(TestNamespaceIdent),
		[]string{"abc"}, iceberg.Properties{"prop": "no"})
	s.Require().NoError(err)
	s.Equal(catalog.PropertiesUpdateSummary{
		Removed: []string{},
		Updated: []string{"prop"},
		Missing: []string{"abc"},
	}, summary)
}

func (s *DynamoDBIntegrationSuite) TestCreateTable() {
	s.ensureNamespace()

	tbl, err := s.cat.CreateTable(s.ctx,
		catalog.ToIdentifier(TestNamespaceIdent, "test-table"),
		tableSchemaSimple, catalog.WithProperties(iceberg.Properties{"foobar": "baz"}),
		catalog.WithLocation(location))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	s.Equal(location, tbl.Location())
	s.Equal("baz", tbl.Properties()["foobar"])

	exists, err := s.cat.CheckTableExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-table"))
	s.Require().NoError(err)
	s.True(exists)

	s.Require().NoError(s.cat.DropTable(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-table")))
}

func (s *DynamoDBIntegrationSuite) TestRenameTable() {
	s.ensureNamespace()

	// Create source table
	tbl, err := s.cat.CreateTable(s.ctx,
		catalog.ToIdentifier(TestNamespaceIdent, "source-table"),
		tableSchemaSimple, catalog.WithLocation(location))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	// Rename table
	renamedTbl, err := s.cat.RenameTable(s.ctx,
		catalog.ToIdentifier(TestNamespaceIdent, "source-table"),
		catalog.ToIdentifier(TestNamespaceIdent, "renamed-table"))
	s.Require().NoError(err)
	s.Require().NotNil(renamedTbl)

	// Verify source table no longer exists
	exists, err := s.cat.CheckTableExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "source-table"))
	s.Require().NoError(err)
	s.False(exists)

	// Verify renamed table exists
	exists, err = s.cat.CheckTableExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "renamed-table"))
	s.Require().NoError(err)
	s.True(exists)

	// Clean up
	s.Require().NoError(s.cat.DropTable(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "renamed-table")))
}

func (s *DynamoDBIntegrationSuite) TestListTables() {
	s.ensureNamespace()

	// Create multiple tables
	tableNames := []string{"table1", "table2", "table3"}
	for _, name := range tableNames {
		tbl, err := s.cat.CreateTable(s.ctx,
			catalog.ToIdentifier(TestNamespaceIdent, name),
			tableSchemaSimple, catalog.WithLocation(location))
		s.Require().NoError(err)
		s.Require().NotNil(tbl)
	}

	// List tables
	var foundTables []string
	for tblIdent, err := range s.cat.ListTables(s.ctx, catalog.ToIdentifier(TestNamespaceIdent)) {
		s.Require().NoError(err)
		foundTables = append(foundTables, tblIdent[1]) // Get table name part
	}

	// Verify all tables are found
	s.Require().Len(foundTables, len(tableNames))
	for _, name := range tableNames {
		s.Contains(foundTables, name)
	}

	// Clean up
	for _, name := range tableNames {
		s.Require().NoError(s.cat.DropTable(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, name)))
	}
}

func (s *DynamoDBIntegrationSuite) TestWriteCommitTable() {
	s.ensureNamespace()

	tbl, err := s.cat.CreateTable(s.ctx,
		catalog.ToIdentifier(TestNamespaceIdent, "test-table-2"),
		tableSchemaNestedTest, catalog.WithLocation(location))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	defer func() {
		s.Require().NoError(s.cat.DropTable(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-table-2")))
	}()

	s.Equal(location, tbl.Location())

	arrSchema, err := table.SchemaToArrowSchema(tableSchemaNestedTest, nil, false, false)
	s.Require().NoError(err)

	table, err := array.TableFromJSON(memory.DefaultAllocator, arrSchema,
		[]string{`[
		{
			"foo": "foo_string",
			"bar": 123,
			"baz": true,
			"qux": ["a", "b", "c"],
			"quux": [{"key": "gopher", "value": [
				{"key": "golang", "value": "1337"}]}],
			"location": [{"latitude": 37.7749, "longitude": -122.4194}],
			"person": {"name": "gopher", "age": 10}
		}
	]`})
	s.Require().NoError(err)
	defer table.Release()

	pqfile, err := url.JoinPath(location, "data", "test_commit_table_data", "test.parquet")
	s.Require().NoError(err)

	fs, err := tbl.FS(s.ctx)
	s.Require().NoError(err)
	fw, err := fs.(io.WriteFileIO).Create(pqfile)
	s.Require().NoError(err)
	s.Require().NoError(pqarrow.WriteTable(table, fw, table.NumRows(),
		nil, pqarrow.DefaultWriterProps()))
	defer func(fs io.IO, name string) {
		if removeErr := fs.Remove(name); removeErr != nil {
			s.T().Logf("Failed to remove test file %s: %v", name, removeErr)
		}
	}(fs, pqfile)

	txn := tbl.NewTransaction()
	s.Require().NoError(txn.AddFiles(s.ctx, []string{pqfile}, nil, false))
	updated, err := txn.Commit(s.ctx)
	s.Require().NoError(err)

	mf := []iceberg.ManifestFile{}
	for m, err := range updated.AllManifests(s.ctx) {
		s.Require().NoError(err)
		s.Require().NotNil(m)
		mf = append(mf, m)
	}

	s.Len(mf, 1)
	s.EqualValues(1, mf[0].AddedDataFiles())
	updatedFS, err := updated.FS(s.ctx)
	s.Require().NoError(err)

	entries, err := mf[0].FetchEntries(updatedFS, false)
	s.Require().NoError(err)

	s.Len(entries, 1)
	s.Equal(pqfile, entries[0].DataFile().FilePath())
}

func (s *DynamoDBIntegrationSuite) TestListNamespaces() {
	// Create multiple namespaces
	namespaces := []string{"ns1", "ns2", "ns3"}
	for _, ns := range namespaces {
		err := s.cat.CreateNamespace(s.ctx, catalog.ToIdentifier(ns), nil)
		s.Require().NoError(err)
	}

	// List namespaces
	foundNamespaces, err := s.cat.ListNamespaces(s.ctx, nil)
	s.Require().NoError(err)

	// Verify all namespaces are found
	for _, ns := range namespaces {
		found := false
		for _, foundNs := range foundNamespaces {
			if len(foundNs) == 1 && foundNs[0] == ns {
				found = true
				break
			}
		}
		s.True(found, "namespace %s not found", ns)
	}

	// Clean up
	for _, ns := range namespaces {
		s.Require().NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(ns)))
	}
}

func TestDynamoDBIntegration(t *testing.T) {
	suite.Run(t, new(DynamoDBIntegrationSuite))
}
