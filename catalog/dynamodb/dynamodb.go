package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	_ "unsafe"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/io"
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
		dynamodbColumnNamespace:  &types.AttributeValueMemberS{Value: m.Namespace},
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
		dynamodbColumnNamespace:  &types.AttributeValueMemberS{Value: m.TableNamespace},
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
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

type Catalog struct {
	name           string
	tableName      string
	dynamodb       dynamodbAPI
	props          iceberg.Properties
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
	_, err = c.dynamodb.PutItem(ctx, &dynamodb.PutItemInput{
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
	_, err = c.dynamodb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:                 aws.String(c.tableName),
		Key:                       ns.Key(),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
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
	ns := dynamodbIcebergNamespace{
		Namespace: strings.Join(namespace, "."),
	}
	res, err := c.dynamodb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(c.tableName),
		Key:       ns.Key(),
	})
	if err != nil {
		return false, err
	}

	if res.Item == nil {
		return false, nil
	}
	return true, nil
}

func (c *Catalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	ns := dynamodbIcebergNamespace{
		Namespace: strings.Join(namespace, "."),
	}

	res, err := c.dynamodb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(c.tableName),
		Key:       ns.Key(),
	})
	if err != nil {
		return nil, err
	}
	if res.Item == nil {
		return nil, catalog.ErrNoSuchNamespace
	}

	if err := ns.UnmarshalMap(res.Item); err != nil {
		return nil, fmt.Errorf("failed to unmarshal namespace: %w", err)
	}

	return ns.Properties, nil
}

//go:linkname getUpdatedPropsAndUpdateSummary github.com/apache/iceberg-go/catalog.getUpdatedPropsAndUpdateSummary
func getUpdatedPropsAndUpdateSummary(currentProps iceberg.Properties, removals []string, updates iceberg.Properties) (iceberg.Properties, catalog.PropertiesUpdateSummary, error)

func (c *Catalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {

	properties, err := c.LoadNamespaceProperties(ctx, namespace)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	updatedProperties, propertiesUpdateSummary, err := getUpdatedPropsAndUpdateSummary(properties, removals, updates)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	ns := dynamodbIcebergNamespace{
		Namespace: strings.Join(namespace, "."),
	}

	expr, err := expression.NewBuilder().WithUpdate(
		expression.Set(expression.Name("properties"), expression.Value(updatedProperties)),
	).Build()
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, fmt.Errorf("failed to build expression: %w", err)
	}

	_, err = c.dynamodb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(c.tableName),
		Key:                       ns.Key(),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, fmt.Errorf("failed to update namespace properties: %w", err)
	}

	return propertiesUpdateSummary, nil
}

func (c *Catalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	staged, err := internal.CreateStagedTable(ctx, c.props, c.LoadNamespaceProperties, identifier, schema, opts...)
	if err != nil {
		return nil, err
	}

	nsIdent := catalog.NamespaceFromIdent(identifier)
	tblIdent := catalog.TableNameFromIdent(identifier)
	ns := strings.Join(nsIdent, ".")
	exists, err := c.CheckNamespaceExists(ctx, nsIdent)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
	}

	afs, err := staged.FS(ctx)
	if err != nil {
		return nil, err
	}
	wfs, ok := afs.(io.WriteFileIO)
	if !ok {
		return nil, errors.New("loaded filesystem IO does not support writing")
	}

	if err := internal.WriteTableMetadata(staged.Metadata(), wfs, staged.MetadataLocation()); err != nil {
		return nil, err
	}

	tbl := dynamodbIcebergTable{
		TableNamespace:           ns,
		TableName:                tblIdent,
		MetadataLocation:         staged.MetadataLocation(),
		PreviousMetadataLocation: staged.MetadataLocation(),
	}

	item, err := tbl.MarshalMap()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal table: %w", err)
	}

	expr, err := expression.NewBuilder().WithCondition(expression.AttributeNotExists(expression.Name(dynamodbColumnIdentifier))).Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build expression: %w", err)
	}

	_, err = c.dynamodb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(c.tableName),
		Item:                      item,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		var conditionalCheckFailedException *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalCheckFailedException) {
			return nil, fmt.Errorf("%w: %s", catalog.ErrTableAlreadyExists, identifier)
		}
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return c.LoadTable(ctx, identifier, staged.Properties())
}

func (c *Catalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
	// todo: implement
	return nil, nil
}
