package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	dynamodbColumnIdentifier = "identifier"
	dynamodbColumnNamespace  = "namespace"

	dynamodbNamespace = "NAMESPACE"

	dynamodbNamespaceGSI = "namespace-identifier"
)

var (
	dynamodbItemNotFound = errors.New("item not found")
)

var (
	createCatalogAttributeDefinitions = []types.AttributeDefinition{
		{
			AttributeName: aws.String(dynamodbColumnIdentifier),
			AttributeType: types.ScalarAttributeTypeS,
		},
		{
			AttributeName: aws.String(dynamodbColumnNamespace),
			AttributeType: types.ScalarAttributeTypeS,
		},
	}

	createCatalogKeySchema = []types.KeySchemaElement{
		{
			AttributeName: aws.String(dynamodbColumnIdentifier),
			KeyType:       types.KeyTypeHash,
		},
		{
			AttributeName: aws.String(dynamodbColumnNamespace),
			KeyType:       types.KeyTypeRange,
		},
	}

	createCatalogGlobalSecondaryIndexes = []types.GlobalSecondaryIndex{
		{
			IndexName: aws.String(dynamodbNamespaceGSI),
			KeySchema: []types.KeySchemaElement{
				{
					AttributeName: aws.String(dynamodbColumnNamespace),
					KeyType:       types.KeyTypeHash,
				},
				{
					AttributeName: aws.String(dynamodbColumnIdentifier),
					KeyType:       types.KeyTypeRange,
				},
			},
			Projection: &types.Projection{
				ProjectionType: types.ProjectionTypeKeysOnly,
			},
		},
	}
)

type dynamodbModel struct {
	Identifier               string            `dynamodbav:"identifier"`
	Namespace                string            `dynamodbav:"namespace"`
	CreatedAt                time.Time         `dynamodbav:"created_at"`
	UpdatedAt                time.Time         `dynamodbav:"updated_at"`
	MetadataLocation         string            `dynamodbav:"metadata_location,omitempty"`
	PreviousMetadataLocation string            `dynamodbav:"previous_metadata_location,omitempty"`
	Properties               map[string]string `dynamodbav:"properties,omitempty"`
}

type dynamodbIcebergNamespace struct {
	Namespace  string
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Properties map[string]string
}

func (m *dynamodbIcebergNamespace) Key() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		dynamodbColumnIdentifier: &types.AttributeValueMemberS{Value: dynamodbNamespace},
		dynamodbColumnNamespace: &types.AttributeValueMemberS{Value: m.Namespace},
	}
}

func (m *dynamodbIcebergNamespace) MarshalMap() (map[string]types.AttributeValue, error) {
	return attributevalue.MarshalMap(dynamodbModel{
		Identifier: dynamodbNamespace,
		Namespace:  m.Namespace,
		CreatedAt:  m.CreatedAt,
		UpdatedAt:  m.UpdatedAt,
		Properties: m.Properties,
	})
}

func (m *dynamodbIcebergNamespace) UnmarshalMap(avMap map[string]types.AttributeValue) error {
	var model dynamodbModel

	err := attributevalue.UnmarshalMap(avMap, &model)
	if err != nil {
		return err
	}
	m.Namespace = model.Namespace
	m.CreatedAt = model.CreatedAt
	m.UpdatedAt = model.UpdatedAt
	m.Properties = model.Properties
	return nil
}

type dynamodbIcebergTable struct {
	TableNamespace           string
	TableName                string
	MetadataLocation         string
	PreviousMetadataLocation string
}

func (m *dynamodbIcebergTable) Key() map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		dynamodbColumnIdentifier: &types.AttributeValueMemberS{Value: m.TableNamespace + "." + m.TableName},
		dynamodbColumnNamespace: &types.AttributeValueMemberS{Value: m.TableNamespace},
	}
}

func (m *dynamodbIcebergTable) MarshalMap() (map[string]types.AttributeValue, error) {
	return attributevalue.MarshalMap(dynamodbModel{
		Identifier:               m.TableNamespace + "." + m.TableName,
		Namespace:                m.TableNamespace,
		MetadataLocation:         m.MetadataLocation,
		PreviousMetadataLocation: m.PreviousMetadataLocation,
	})
}

func (m *dynamodbIcebergTable) UnmarshalMap(avMap map[string]types.AttributeValue) error {
	var model dynamodbModel
	err := attributevalue.UnmarshalMap(avMap, &model)
	if err != nil {
		return err
	}
	m.TableNamespace = model.Namespace
	prefix := model.Namespace + "."
	if !strings.HasPrefix(model.Identifier, prefix) {
		return fmt.Errorf("invalid table identifier: %s for namespace %s", model.Identifier, model.Namespace)
	}
	m.TableName = model.Identifier[len(prefix):]
	m.MetadataLocation = model.MetadataLocation
	m.PreviousMetadataLocation = model.PreviousMetadataLocation
	return nil
}

type dynamodbAPI interface {
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
}

type Catalog struct {
	tableName      string
	dynamodb       dynamodbAPI
	dynamodbClient *dynamodb.Client
}

func NewCatalog(tableName string, dynamodb dynamodbAPI) (*Catalog, error) {
	c := &Catalog{tableName: tableName, dynamodb: dynamodb}

	c.ensureCatalogTableExistsOrCreate(context.Background())

	return c, nil
}

func (c *Catalog) ensureCatalogTableExistsOrCreate(ctx context.Context) error {
	if exists, err := c.dynamodbTableExists(ctx); err != nil {
		return err
	} else if exists {
		return nil
	}

	return c.createDynamodbTable(ctx)
}

func (c *Catalog) dynamodbTableExists(ctx context.Context) (bool, error) {
	res, err := c.dynamodb.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(c.tableName),
	})
	if err != nil {
		var notFoundErr *types.ResourceNotFoundException
		if errors.As(err, &notFoundErr) {
			return false, nil
		}
		return false, err
	}
	if res.Table.TableStatus != types.TableStatusActive {
		return false, fmt.Errorf("DynamoDB table for catalog %s is not %s", c.tableName, types.TableStatusActive)
	}
	return true, nil
}

func (c *Catalog) createDynamodbTable(ctx context.Context) error {
	_, err := c.dynamodb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:              aws.String(c.tableName),
		AttributeDefinitions:   createCatalogAttributeDefinitions,
		KeySchema:              createCatalogKeySchema,
		GlobalSecondaryIndexes: createCatalogGlobalSecondaryIndexes,
		BillingMode:            types.BillingModePayPerRequest,
	})
	if err != nil {
		return fmt.Errorf("failed to create DynamoDB table %s: %w", c.tableName, err)
	}

	waiter := dynamodb.NewTableExistsWaiter(c.dynamodb)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(c.tableName),
	}, 10*time.Second); err != nil {
		return fmt.Errorf("failed to wait for DynamoDB table %s to be created: %w", c.tableName, err)
	}

	return nil
}

func checkValidNamespace(ident table.Identifier) error {
	if len(ident) < 1 {
		return fmt.Errorf("%w: empty namespace identifier", catalog.ErrNoSuchNamespace)
	}

	return nil
}

func (c *Catalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	if err := checkValidNamespace(namespace); err != nil {
		return err
	}

	current_time := time.Now()
	ns := dynamodbIcebergNamespace{
		Namespace:  strings.Join(namespace, "."),
		CreatedAt:  current_time,
		UpdatedAt:  current_time,
		Properties: props,
	}
	item, err := ns.MarshalMap()
	if err != nil {
		return fmt.Errorf("failed to marshal item: %w", err)
	}
	expr, err := expression.NewBuilder().WithCondition(expression.AttributeNotExists(expression.Name(dynamodbColumnNamespace))).Build()
	if err != nil {
		return fmt.Errorf("failed to build expression: %w", err)
	}
	_, err = c.dynamodbClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(c.tableName),
		Item:                      item,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		var conditionalCheckFailedException *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalCheckFailedException) {
			return fmt.Errorf("%w: %s", catalog.ErrNamespaceAlreadyExists, namespace)
		}
		return fmt.Errorf("failed to create namespace: %w", err)
	}

	return nil
}

func (c *Catalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	if err := checkValidNamespace(namespace); err != nil {
		return err
	}

	ns := dynamodbIcebergNamespace{
		Namespace: strings.Join(namespace, "."),
	}

	expr, err := expression.NewBuilder().WithCondition(expression.AttributeExists(expression.Name(dynamodbColumnNamespace))).Build()
	if err != nil {
		return fmt.Errorf("failed to build expression: %w", err)
	}
	_, err = c.dynamodbClient.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(c.tableName),
		Key: ns.Key(),
		ConditionExpression: expr.Condition(),
		ExpressionAttributeNames: expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		var conditionalCheckFailedException *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalCheckFailedException) {
			return fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, namespace)
		}
		return fmt.Errorf("failed to delete namespace: %w", err)
	}

	return nil
}

func (c *Catalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	return c.namespaceExists(ctx, strings.Join(namespace, "."))
}

func (c *Catalog) namespaceExists(ctx context.Context, ns string) (bool, error) {
	// todo: implement
	return false, nil
}

func(c *Catalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	// todo: implement
	return nil, nil	
}

func (c *Catalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	// todo: implement
	return catalog.PropertiesUpdateSummary{}, nil
}


func (c *Catalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	// todo: implement
	return nil, nil
}
