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

package dynamodb

import (
	"context"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type mockDynamoDBAPI struct {
	mock.Mock
}

func (m *mockDynamoDBAPI) DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*dynamodb.DescribeTableOutput), args.Error(1)
}

func (m *mockDynamoDBAPI) CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*dynamodb.CreateTableOutput), args.Error(1)
}

func (m *mockDynamoDBAPI) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*dynamodb.GetItemOutput), args.Error(1)
}

func (m *mockDynamoDBAPI) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*dynamodb.UpdateItemOutput), args.Error(1)
}

func (m *mockDynamoDBAPI) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*dynamodb.PutItemOutput), args.Error(1)
}

func (m *mockDynamoDBAPI) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*dynamodb.DeleteItemOutput), args.Error(1)
}

func (m *mockDynamoDBAPI) TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*dynamodb.TransactWriteItemsOutput), args.Error(1)
}

func (m *mockDynamoDBAPI) Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*dynamodb.QueryOutput), args.Error(1)
}

func TestDynamoDBCatalogNewCatalog(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)
	assert.NotNil(cat)
	assert.Equal("test-table", cat.tableName)
}

func TestDynamoDBCatalogNewCatalogTableNotExists(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}

	// First call to DescribeTable returns ResourceNotFoundException (table doesn't exist)
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{}, &types.ResourceNotFoundException{}).Once()

	// CreateTable call
	mockDDB.On("CreateTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.CreateTableOutput{}, nil).Once()

	// Subsequent calls to DescribeTable from the waiter return active table
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)
	assert.NotNil(cat)
}

func TestDynamoDBCreateNamespace(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	mockDDB.On("PutItem", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.PutItemOutput{}, nil)

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	props := iceberg.Properties{
		"comment":  "Test namespace",
		"location": "s3://test-bucket/test-namespace",
	}

	err = cat.CreateNamespace(context.Background(), table.Identifier{"test_namespace"}, props)
	assert.NoError(err)

	mockDDB.AssertExpectations(t)
}

func TestDynamoDBCreateNamespaceAlreadyExists(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	mockDDB.On("PutItem", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.PutItemOutput{}, &types.ConditionalCheckFailedException{})

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	err = cat.CreateNamespace(context.Background(), table.Identifier{"test_namespace"}, nil)
	assert.Error(err)
	assert.ErrorIs(err, catalog.ErrNamespaceAlreadyExists)
}

func TestDynamoDBDropNamespace(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	mockDDB.On("DeleteItem", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DeleteItemOutput{}, nil)

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	err = cat.DropNamespace(context.Background(), table.Identifier{"test_namespace"})
	assert.NoError(err)

	mockDDB.AssertExpectations(t)
}

func TestDynamoDBDropNamespaceNotExists(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	mockDDB.On("DeleteItem", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DeleteItemOutput{}, &types.ConditionalCheckFailedException{})

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	err = cat.DropNamespace(context.Background(), table.Identifier{"nonexistent_namespace"})
	assert.Error(err)
	assert.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func TestDynamoDBCheckNamespaceExists(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	mockDDB.On("GetItem", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.GetItemOutput{
			Item: map[string]types.AttributeValue{
				"identifier": &types.AttributeValueMemberS{Value: "NAMESPACE"},
				"namespace":  &types.AttributeValueMemberS{Value: "test_namespace"},
			},
		}, nil)

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	exists, err := cat.CheckNamespaceExists(context.Background(), table.Identifier{"test_namespace"})
	assert.NoError(err)
	assert.True(exists)
}

func TestDynamoDBCheckNamespaceNotExists(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	mockDDB.On("GetItem", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.GetItemOutput{Item: nil}, nil)

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	exists, err := cat.CheckNamespaceExists(context.Background(), table.Identifier{"nonexistent_namespace"})
	assert.NoError(err)
	assert.False(exists)
}

func TestDynamoDBLoadNamespaceProperties(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	testProps := map[string]string{
		"comment":  "Test namespace",
		"location": "s3://test-bucket/test-namespace",
	}

	mockDDB.On("GetItem", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.GetItemOutput{
			Item: map[string]types.AttributeValue{
				"identifier": &types.AttributeValueMemberS{Value: "NAMESPACE"},
				"namespace":  &types.AttributeValueMemberS{Value: "test_namespace"},
				"properties": &types.AttributeValueMemberM{
					Value: map[string]types.AttributeValue{
						"comment":  &types.AttributeValueMemberS{Value: "Test namespace"},
						"location": &types.AttributeValueMemberS{Value: "s3://test-bucket/test-namespace"},
					},
				},
			},
		}, nil)

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	props, err := cat.LoadNamespaceProperties(context.Background(), table.Identifier{"test_namespace"})
	assert.NoError(err)
	assert.Equal(iceberg.Properties(testProps), props)
}

func TestDynamoDBUpdateNamespaceProperties(t *testing.T) {
	tests := []struct {
		name        string
		initial     map[string]string
		updates     map[string]string
		removals    []string
		expected    catalog.PropertiesUpdateSummary
		shouldError bool
	}{
		{
			name: "Happy path with updates and removals",
			initial: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key4": "value4",
			},
			updates: map[string]string{
				"key2": "new_value2",
				"key3": "value3",
			},
			removals: []string{"key4"},
			expected: catalog.PropertiesUpdateSummary{
				Removed: []string{"key4"},
				Updated: []string{"key2", "key3"},
				Missing: []string{},
			},
			shouldError: false,
		},
		{
			name: "Some keys in removals are missing",
			initial: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			updates: map[string]string{
				"key3": "value3",
			},
			removals: []string{"key4"},
			expected: catalog.PropertiesUpdateSummary{
				Removed: []string{},
				Updated: []string{"key3"},
				Missing: []string{"key4"},
			},
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := require.New(t)

			mockDDB := &mockDynamoDBAPI{}
			mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
				&dynamodb.DescribeTableOutput{
					Table: &types.TableDescription{
						TableName:   aws.String("test-table"),
						TableStatus: types.TableStatusActive,
					},
				}, nil)

			// Mock LoadNamespaceProperties
			propsAV := make(map[string]types.AttributeValue)
			for k, v := range tt.initial {
				propsAV[k] = &types.AttributeValueMemberS{Value: v}
			}

			mockDDB.On("GetItem", mock.Anything, mock.Anything, mock.Anything).Return(
				&dynamodb.GetItemOutput{
					Item: map[string]types.AttributeValue{
						"identifier": &types.AttributeValueMemberS{Value: "NAMESPACE"},
						"namespace":  &types.AttributeValueMemberS{Value: "test_namespace"},
						"properties": &types.AttributeValueMemberM{Value: propsAV},
					},
				}, nil)

			if !tt.shouldError {
				mockDDB.On("UpdateItem", mock.Anything, mock.Anything, mock.Anything).Return(
					&dynamodb.UpdateItemOutput{}, nil)
			}

			cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
			assert.NoError(err)

			summary, err := cat.UpdateNamespaceProperties(context.Background(), table.Identifier{"test_namespace"}, tt.removals, tt.updates)
			if tt.shouldError {
				assert.Error(err)
			} else {
				assert.NoError(err)
				assert.ElementsMatch(tt.expected.Removed, summary.Removed)
				assert.ElementsMatch(tt.expected.Updated, summary.Updated)
				assert.ElementsMatch(tt.expected.Missing, summary.Missing)
			}
		})
	}
}

func TestDynamoDBListNamespaces(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	mockDDB.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.QueryOutput{
			Items: []map[string]types.AttributeValue{
				{
					"identifier": &types.AttributeValueMemberS{Value: "NAMESPACE"},
					"namespace":  &types.AttributeValueMemberS{Value: "test_namespace1"},
				},
				{
					"identifier": &types.AttributeValueMemberS{Value: "NAMESPACE"},
					"namespace":  &types.AttributeValueMemberS{Value: "test_namespace2"},
				},
			},
		}, nil)

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	namespaces, err := cat.ListNamespaces(context.Background(), nil)
	assert.NoError(err)
	assert.Len(namespaces, 2)
	assert.Contains(namespaces, table.Identifier{"test_namespace1"})
	assert.Contains(namespaces, table.Identifier{"test_namespace2"})
}

func TestDynamoDBCheckTableExists(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	mockDDB.On("GetItem", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.GetItemOutput{
			Item: map[string]types.AttributeValue{
				"identifier":        &types.AttributeValueMemberS{Value: "test_namespace.test_table"},
				"namespace":         &types.AttributeValueMemberS{Value: "test_namespace"},
				"metadata_location": &types.AttributeValueMemberS{Value: "s3://test-bucket/metadata/test.json"},
				"created_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
				"updated_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
			},
		}, nil)

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	exists, err := cat.CheckTableExists(context.Background(), table.Identifier{"test_namespace", "test_table"})
	assert.NoError(err)
	assert.True(exists)
}

func TestDynamoDBCheckTableNotExists(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	mockDDB.On("GetItem", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.GetItemOutput{Item: nil}, nil)

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	exists, err := cat.CheckTableExists(context.Background(), table.Identifier{"test_namespace", "nonexistent_table"})
	assert.NoError(err)
	assert.False(exists)
}

func TestDynamoDBDropTable(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	mockDDB.On("DeleteItem", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DeleteItemOutput{}, nil)

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	err = cat.DropTable(context.Background(), table.Identifier{"test_namespace", "test_table"})
	assert.NoError(err)

	mockDDB.AssertExpectations(t)
}

func TestDynamoDBListTables(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	mockDDB.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.QueryOutput{
			Items: []map[string]types.AttributeValue{
				{
					"identifier":        &types.AttributeValueMemberS{Value: "test_namespace.test_table1"},
					"namespace":         &types.AttributeValueMemberS{Value: "test_namespace"},
					"metadata_location": &types.AttributeValueMemberS{Value: "s3://test-bucket/metadata/test1.json"},
					"created_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
					"updated_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
				},
				{
					"identifier":        &types.AttributeValueMemberS{Value: "test_namespace.test_table2"},
					"namespace":         &types.AttributeValueMemberS{Value: "test_namespace"},
					"metadata_location": &types.AttributeValueMemberS{Value: "s3://test-bucket/metadata/test2.json"},
					"created_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
					"updated_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
				},
			},
		}, nil)

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	var tables []table.Identifier
	var lastErr error
	iter := cat.ListTables(context.Background(), table.Identifier{"test_namespace"})
	for tbl, err := range iter {
		tables = append(tables, tbl)
		if err != nil {
			lastErr = err
		}
	}

	assert.NoError(lastErr)
	assert.Len(tables, 2)
	assert.Contains(tables, table.Identifier{"test_namespace", "test_table1"})
	assert.Contains(tables, table.Identifier{"test_namespace", "test_table2"})
}

func TestDynamoDBListTablesPagination(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	// First page
	mockDDB.On("Query", mock.Anything, mock.MatchedBy(func(input *dynamodb.QueryInput) bool {
		return input.ExclusiveStartKey == nil
	}), mock.Anything).Return(
		&dynamodb.QueryOutput{
			Items: []map[string]types.AttributeValue{
				{
					"identifier":        &types.AttributeValueMemberS{Value: "test_namespace.test_table1"},
					"namespace":         &types.AttributeValueMemberS{Value: "test_namespace"},
					"metadata_location": &types.AttributeValueMemberS{Value: "s3://test-bucket/metadata/test1.json"},
					"created_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
					"updated_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
				},
			},
			LastEvaluatedKey: map[string]types.AttributeValue{
				"identifier": &types.AttributeValueMemberS{Value: "test_namespace.test_table1"},
				"namespace":  &types.AttributeValueMemberS{Value: "test_namespace"},
			},
		}, nil).Once()

	// Second page
	mockDDB.On("Query", mock.Anything, mock.MatchedBy(func(input *dynamodb.QueryInput) bool {
		return input.ExclusiveStartKey != nil
	}), mock.Anything).Return(
		&dynamodb.QueryOutput{
			Items: []map[string]types.AttributeValue{
				{
					"identifier":        &types.AttributeValueMemberS{Value: "test_namespace.test_table2"},
					"namespace":         &types.AttributeValueMemberS{Value: "test_namespace"},
					"metadata_location": &types.AttributeValueMemberS{Value: "s3://test-bucket/metadata/test2.json"},
					"created_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
					"updated_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
				},
			},
		}, nil).Once()

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	var tables []table.Identifier
	var lastErr error
	iter := cat.ListTables(context.Background(), table.Identifier{"test_namespace"})
	for tbl, err := range iter {
		tables = append(tables, tbl)
		if err != nil {
			lastErr = err
		}
	}

	assert.NoError(lastErr)
	assert.Len(tables, 2)
	assert.Contains(tables, table.Identifier{"test_namespace", "test_table1"})
	assert.Contains(tables, table.Identifier{"test_namespace", "test_table2"})

	mockDDB.AssertExpectations(t)
}

func TestDynamoDBRenameTable(t *testing.T) {
	assert := require.New(t)

	mockDDB := &mockDynamoDBAPI{}
	mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	// Mock namespace existence check
	mockDDB.On("GetItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.GetItemInput) bool {
		key := input.Key
		if idAttr, ok := key["identifier"]; ok {
			if s, ok := idAttr.(*types.AttributeValueMemberS); ok {
				return s.Value == "NAMESPACE"
			}
		}
		return false
	}), mock.Anything).Return(
		&dynamodb.GetItemOutput{
			Item: map[string]types.AttributeValue{
				"identifier": &types.AttributeValueMemberS{Value: "NAMESPACE"},
				"namespace":  &types.AttributeValueMemberS{Value: "test_namespace"},
			},
		}, nil)

	// Mock getTable for source table
	mockDDB.On("GetItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.GetItemInput) bool {
		key := input.Key
		if idAttr, ok := key["identifier"]; ok {
			if s, ok := idAttr.(*types.AttributeValueMemberS); ok {
				return s.Value == "test_namespace.old_table"
			}
		}
		return false
	}), mock.Anything).Return(
		&dynamodb.GetItemOutput{
			Item: map[string]types.AttributeValue{
				"identifier":        &types.AttributeValueMemberS{Value: "test_namespace.old_table"},
				"namespace":         &types.AttributeValueMemberS{Value: "test_namespace"},
				"metadata_location": &types.AttributeValueMemberS{Value: "s3://test-bucket/metadata/test.json"},
				"created_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
				"updated_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
			},
		}, nil)

	mockDDB.On("TransactWriteItems", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.TransactWriteItemsOutput{}, nil)

	// Mock LoadTable for the renamed table
	mockDDB.On("GetItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.GetItemInput) bool {
		key := input.Key
		if idAttr, ok := key["identifier"]; ok {
			if s, ok := idAttr.(*types.AttributeValueMemberS); ok {
				return s.Value == "test_namespace.new_table"
			}
		}
		return false
	}), mock.Anything).Return(
		&dynamodb.GetItemOutput{
			Item: map[string]types.AttributeValue{
				"identifier":        &types.AttributeValueMemberS{Value: "test_namespace.new_table"},
				"namespace":         &types.AttributeValueMemberS{Value: "test_namespace"},
				"metadata_location": &types.AttributeValueMemberS{Value: "s3://test-bucket/metadata/test.json"},
				"created_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
				"updated_at":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
			},
		}, nil)

	cat, err := NewCatalog("test-table", mockDDB, iceberg.Properties{})
	assert.NoError(err)

	// Mock the file system for LoadTable
	cat.props = iceberg.Properties{
		"warehouse": "file:///tmp/test",
	}

	// This would normally fail because we can't actually load the table metadata,
	// but we're testing the DynamoDB operations
	_, err = cat.RenameTable(context.Background(),
		table.Identifier{"test_namespace", "old_table"},
		table.Identifier{"test_namespace", "new_table"})

	// We expect this to fail at the LoadTable step since we don't have real metadata
	assert.Error(err)

	// Verify the TransactWriteItems was called with correct parameters
	mockDDB.AssertExpectations(t)
}

type DynamoDBCatalogTestSuite struct {
	suite.Suite
	mockDDB *mockDynamoDBAPI
	catalog *Catalog
}

func (s *DynamoDBCatalogTestSuite) SetupTest() {
	s.mockDDB = &mockDynamoDBAPI{}
	s.mockDDB.On("DescribeTable", mock.Anything, mock.Anything, mock.Anything).Return(
		&dynamodb.DescribeTableOutput{
			Table: &types.TableDescription{
				TableName:   aws.String("test-table"),
				TableStatus: types.TableStatusActive,
			},
		}, nil)

	var err error
	s.catalog, err = NewCatalog("test-table", s.mockDDB, iceberg.Properties{})
	s.Require().NoError(err)
}

func (s *DynamoDBCatalogTestSuite) TestDynamoDBIcebergNamespaceKey() {
	ns := &dynamodbIcebergNamespace{
		Namespace: "test_namespace",
	}

	key := ns.Key()
	s.Equal("NAMESPACE", key["identifier"].(*types.AttributeValueMemberS).Value)
	s.Equal("test_namespace", key["namespace"].(*types.AttributeValueMemberS).Value)
}

func (s *DynamoDBCatalogTestSuite) TestDynamoDBIcebergNamespaceMarshalUnmarshal() {
	original := &dynamodbIcebergNamespace{
		Namespace:  "test_namespace",
		CreatedAt:  time.Now().Truncate(time.Second),
		UpdatedAt:  time.Now().Truncate(time.Second),
		Properties: map[string]string{"key": "value"},
	}

	marshaled, err := original.MarshalMap()
	s.Require().NoError(err)

	unmarshaled := &dynamodbIcebergNamespace{}
	err = unmarshaled.UnmarshalMap(marshaled)
	s.Require().NoError(err)

	s.Equal(original.Namespace, unmarshaled.Namespace)
	s.Equal(original.Properties, unmarshaled.Properties)
}

func (s *DynamoDBCatalogTestSuite) TestDynamoDBIcebergTableKey() {
	tbl := &dynamodbIcebergTable{
		TableNamespace: "test_namespace",
		TableName:      "test_table",
	}

	key := tbl.Key()
	s.Equal("test_namespace.test_table", key["identifier"].(*types.AttributeValueMemberS).Value)
	s.Equal("test_namespace", key["namespace"].(*types.AttributeValueMemberS).Value)
}

func (s *DynamoDBCatalogTestSuite) TestDynamoDBIcebergTableMarshalUnmarshal() {
	now := time.Now().Truncate(time.Second)
	original := &dynamodbIcebergTable{
		TableNamespace:           "test_namespace",
		TableName:                "test_table",
		CreatedAt:                now,
		UpdatedAt:                now,
		MetadataLocation:         "s3://test-bucket/metadata/test.json",
		PreviousMetadataLocation: "s3://test-bucket/metadata/prev.json",
	}

	marshaled, err := original.MarshalMap()
	s.Require().NoError(err)

	unmarshaled := &dynamodbIcebergTable{}
	err = unmarshaled.UnmarshalMap(marshaled)
	s.Require().NoError(err)

	s.Equal(original.TableNamespace, unmarshaled.TableNamespace)
	s.Equal(original.TableName, unmarshaled.TableName)
	s.Equal(original.MetadataLocation, unmarshaled.MetadataLocation)
	s.Equal(original.PreviousMetadataLocation, unmarshaled.PreviousMetadataLocation)

	// The CreatedAt and UpdatedAt should be set during UnmarshalMap
	s.NotZero(unmarshaled.CreatedAt)
	s.NotZero(unmarshaled.UpdatedAt)
}

func (s *DynamoDBCatalogTestSuite) TestDynamoDBIcebergTableValidation() {
	tbl := &dynamodbIcebergTable{
		TableNamespace: "test_namespace",
		TableName:      "test_table",
		CreatedAt:      time.Now(),
	}

	err := tbl.Valid()
	s.NoError(err)

	// Test invalid table
	invalidTbl := &dynamodbIcebergTable{
		TableNamespace: "",
		TableName:      "test_table",
		CreatedAt:      time.Now(),
	}

	err = invalidTbl.Valid()
	s.Error(err)
}

func TestDynamoDBCatalogSuite(t *testing.T) {
	suite.Run(t, new(DynamoDBCatalogTestSuite))
}
