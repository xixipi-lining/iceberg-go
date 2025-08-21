package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"strconv"
	"strings"
	"time"
	_ "unsafe"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	TableName       = "table-name"
	AccessKeyID     = "dynamodb.access-key-id"
	SecretAccessKey = "dynamodb.secret-access-key"
	SessionToken    = "dynamodb.session-token"
	Region          = "dynamodb.region"

	Endpoint   = "dynamodb.endpoint"
	MaxRetries = "dynamodb.max-retries"
	RetryMode  = "dynamodb.retry-mode"
)

var ErrNoChanges = errors.New("no changes")

var _ catalog.Catalog = (*Catalog)(nil)

func init() {
	catalog.Register("dynamodb", catalog.RegistrarFunc(func(ctx context.Context, _ string, props iceberg.Properties) (catalog.Catalog, error) {
		awsConfig, err := toAwsConfig(ctx, props)
		if err != nil {
			return nil, err
		}

		tableName, ok := props[TableName]
		if !ok {
			return nil, fmt.Errorf("%w: %s", catalog.ErrCatalogNotFound, "table-name is required")
		}

		dynamodbCli := dynamodb.NewFromConfig(awsConfig)
		return NewCatalog(tableName, dynamodbCli, props)
	}))
}

func toAwsConfig(ctx context.Context, props iceberg.Properties) (aws.Config, error) {
	opts := make([]func(*config.LoadOptions) error, 0)

	for k, v := range props {
		switch k {
		case Region:
			opts = append(opts, config.WithRegion(v))
		case Endpoint:
			opts = append(opts, config.WithBaseEndpoint(v))
		case MaxRetries:
			maxRetry, err := strconv.Atoi(v)
			if err != nil {
				return aws.Config{}, err
			}
			opts = append(opts, config.WithRetryMaxAttempts(maxRetry))
		case RetryMode:
			m, err := aws.ParseRetryMode(v)
			if err != nil {
				return aws.Config{}, err
			}
			opts = append(opts, config.WithRetryMode(m))
		}
	}

	key, secret, token := props[AccessKeyID], props[SecretAccessKey], props[SessionToken]
	if key != "" || secret != "" || token != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(key, secret, token),
		))
	}

	return config.LoadDefaultConfig(ctx, opts...)
}

type dynamodbAPI interface {
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

type Catalog struct {
	tableName string
	dynamodb  dynamodbAPI
	props     iceberg.Properties
}

func NewCatalog(tableName string, dynamodbCli dynamodbAPI, props iceberg.Properties) (*Catalog, error) {
	c := &Catalog{
		tableName: tableName,
		dynamodb:  dynamodbCli,
		props:     props,
	}

	if err := c.ensureCatalogTableExistsOrCreate(context.Background()); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Catalog) CatalogType() catalog.Type {
	return catalog.DynamoDB
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

	ns := dynamodbIcebergNamespace{
		Namespace:  strings.Join(namespace, "."),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		Properties: props,
	}
	item, err := ns.MarshalMap()
	if err != nil {
		return fmt.Errorf("failed to marshal item: %w", err)
	}
	expr, err := expression.NewBuilder().WithCondition(
		expression.And(
			expression.AttributeNotExists(expression.Name(dynamodbColumnIdentifier)),
			expression.AttributeNotExists(expression.Name(dynamodbColumnNamespace)),
		),
	).Build()
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

	expr, err := expression.NewBuilder().WithCondition(
		expression.And(
			expression.AttributeExists(expression.Name(dynamodbColumnIdentifier)),
			expression.AttributeExists(expression.Name(dynamodbColumnNamespace)),
		),
	).Build()
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
	return c.namespaceExists(ctx, strings.Join(namespace, "."))
}

func (c *Catalog) namespaceExists(ctx context.Context, ns string) (bool, error) {
	res, err := c.dynamodb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(c.tableName),
		Key:       (&dynamodbIcebergNamespace{Namespace: ns}).Key(),
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
		TableName:      aws.String(c.tableName),
		Key:            ns.Key(),
		ConsistentRead: aws.Bool(true),
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
		expression.Set(expression.Name(dynamodbColumnProperties), expression.Value(updatedProperties)),
	).WithUpdate(
		expression.Set(expression.Name(dynamodbColumnUpdatedAt), expression.Value(time.Now())),
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

func (c *Catalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	var keyCondition expression.KeyConditionBuilder
	if parent == nil {
		// Query all namespaces when parent is nil
		keyCondition = expression.Key(dynamodbColumnIdentifier).Equal(expression.Value(dynamodbNamespace))
	} else {
		// Query namespaces with specific parent prefix
		keyCondition = expression.KeyAnd(
			expression.Key(dynamodbColumnIdentifier).Equal(expression.Value(dynamodbNamespace)),
			expression.Key(dynamodbColumnNamespace).BeginsWith(strings.Join(parent, ".")+"."),
		)
	}

	expr, err := expression.NewBuilder().WithKeyCondition(keyCondition).Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build expression: %w", err)
	}

	paginator := dynamodb.NewQueryPaginator(c.dynamodb, &dynamodb.QueryInput{
		TableName:                 aws.String(c.tableName),
		ConsistentRead:            aws.Bool(true),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})

	namespaces := make([]table.Identifier, 0)
	for paginator.HasMorePages() {
		res, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to query namespaces: %w", err)
		}

		for _, item := range res.Items {
			ns := dynamodbIcebergNamespace{}
			if err := ns.UnmarshalMap(item); err != nil {
				return nil, fmt.Errorf("failed to unmarshal namespace: %w", err)
			}
			namespaces = append(namespaces, table.Identifier(strings.Split(ns.Namespace, ".")))
		}
	}

	return namespaces, nil
}

func (c *Catalog) ListNamespacesPaginated(ctx context.Context, parent table.Identifier, pageToken *string, pageSize *int) ([]table.Identifier, *string, error) {
	namespaces, err := c.ListNamespaces(ctx, parent)
	if err != nil {
		return nil, nil, err
	}
	return namespaces, nil, nil
}

func (c *Catalog) stageCreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.StagedTable, error) {
	staged, err := internal.CreateStagedTable(ctx, c.props, c.LoadNamespaceProperties, identifier, schema, opts...)
	if err != nil {
		return nil, err
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

	return &staged, nil
}

func (c *Catalog) getCreateTablePutItem(ctx context.Context, staged *table.StagedTable) (*dynamodb.PutItemInput, error) {
	item, err := (&dynamodbIcebergTable{
		TableNamespace:   strings.Join(catalog.NamespaceFromIdent(staged.Identifier()), "."),
		TableName:        catalog.TableNameFromIdent(staged.Identifier()),
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
		MetadataLocation: staged.MetadataLocation(),
	}).MarshalMap()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal table: %w", err)
	}

	expr, err := expression.NewBuilder().WithCondition(
		expression.And(
			expression.AttributeNotExists(expression.Name(dynamodbColumnIdentifier)),
			expression.AttributeNotExists(expression.Name(dynamodbColumnNamespace)),
		),
	).Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build expression: %w", err)
	}

	return &dynamodb.PutItemInput{
		TableName:                 aws.String(c.tableName),
		Item:                      item,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}, nil
}

func (c *Catalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	ns := strings.Join(catalog.NamespaceFromIdent(identifier), ".")
	exists, err := c.namespaceExists(ctx, ns)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
	}

	staged, err := c.stageCreateTable(ctx, identifier, schema, opts...)
	if err != nil {
		return nil, err
	}

	putItem, err := c.getCreateTablePutItem(ctx, staged)
	if err != nil {
		return nil, err
	}

	_, err = c.dynamodb.PutItem(ctx, putItem)
	if err != nil {
		var conditionalCheckFailedException *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalCheckFailedException) {
			return nil, fmt.Errorf("%w: %s", catalog.ErrTableAlreadyExists, staged.Identifier())
		}
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return c.LoadTable(ctx, identifier, staged.Properties())
}

func (c *Catalog) getTable(ctx context.Context, identifier table.Identifier) (*dynamodbIcebergTable, error) {
	nsIdent := catalog.NamespaceFromIdent(identifier)
	tblIdent := catalog.TableNameFromIdent(identifier)

	tbl := dynamodbIcebergTable{
		TableNamespace: strings.Join(nsIdent, "."),
		TableName:      tblIdent,
	}

	res, err := c.dynamodb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(c.tableName),
		Key:            tbl.Key(),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if res.Item == nil {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchTable, identifier)
	}

	if err := tbl.UnmarshalMap(res.Item); err != nil {
		return nil, fmt.Errorf("failed to unmarshal table: %w", err)
	}

	if err := tbl.Valid(); err != nil {
		return nil, fmt.Errorf("invalid table: %w", err)
	}

	return &tbl, nil
}

func (c *Catalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
	if props == nil {
		props = iceberg.Properties{}
	}

	tbl, err := c.getTable(ctx, identifier)
	if err != nil {
		return nil, err
	}

	tblProps := maps.Clone(c.props)
	maps.Copy(props, tblProps)

	return table.NewFromLocation(ctx, identifier, tbl.MetadataLocation, io.LoadFSFunc(tblProps, tbl.MetadataLocation), c)
}

func (c *Catalog) stageCommitTable(ctx context.Context, tbl *table.Table, requirements []table.Requirement, updates []table.Update) (*table.Table, *table.StagedTable, error) {
	current, err := c.LoadTable(ctx, tbl.Identifier(), nil)
	if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, nil, err
	}
	staged, err := internal.UpdateAndStageTable(ctx, tbl, tbl.Identifier(), requirements, updates, c)
	if err != nil {
		return nil, nil, err
	}

	if current != nil && staged.Metadata().Equals(current.Metadata()) {
		return current, staged, ErrNoChanges
	}

	if err := internal.WriteMetadata(ctx, staged.Metadata(), staged.MetadataLocation(), staged.Properties()); err != nil {
		return nil, nil, err
	}

	return current, staged, nil
}

func (c *Catalog) getCommitTableUpdateItem(ctx context.Context, current *table.Table, staged *table.StagedTable) (*dynamodb.UpdateItemInput, error) {
	expr, err := expression.NewBuilder().WithCondition(
		expression.Equal(expression.Name(dynamodbColumnMetadataLocation), expression.Value(current.MetadataLocation())),
	).WithUpdate(
		expression.Set(expression.Name(dynamodbColumnMetadataLocation), expression.Value(staged.MetadataLocation())),
	).WithUpdate(
		expression.Set(expression.Name(dynamodbColumnPreviousMetadataLocation), expression.Value(current.MetadataLocation())),
	).WithUpdate(
		expression.Set(expression.Name(dynamodbColumnUpdatedAt), expression.Value(time.Now())),
	).Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build expression: %w", err)
	}
	return &dynamodb.UpdateItemInput{
		TableName: aws.String(c.tableName),
		Key: (&dynamodbIcebergTable{
			TableNamespace: strings.Join(catalog.NamespaceFromIdent(staged.Identifier()), "."),
			TableName:      catalog.TableNameFromIdent(staged.Identifier()),
		}).Key(),
		ConditionExpression:       expr.Condition(),
		UpdateExpression:          expr.Update(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}, nil

}

func (c *Catalog) CommitTable(ctx context.Context, tbl *table.Table, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	current, staged, err := c.stageCommitTable(ctx, tbl, requirements, updates)
	if err != nil {
		if errors.Is(err, ErrNoChanges) {
			return current.Metadata(), current.MetadataLocation(), nil
		}
		return nil, "", err
	}

	updateItem, err := c.getCommitTableUpdateItem(ctx, current, staged)
	if err != nil {
		return nil, "", err
	}

	_, err = c.dynamodb.UpdateItem(ctx, updateItem)
	if err != nil {
		return nil, "", fmt.Errorf("failed to update table: %w", err)
	}

	return staged.Metadata(), staged.MetadataLocation(), nil
}

func (c *Catalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	nsIdent := catalog.NamespaceFromIdent(identifier)
	tblIdent := catalog.TableNameFromIdent(identifier)

	expr, err := expression.NewBuilder().WithCondition(
		expression.And(
			expression.AttributeExists(expression.Name(dynamodbColumnIdentifier)),
			expression.AttributeExists(expression.Name(dynamodbColumnNamespace)),
		),
	).Build()
	if err != nil {
		return fmt.Errorf("failed to build expression: %w", err)
	}

	_, err = c.dynamodb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(c.tableName),
		Key: (&dynamodbIcebergTable{
			TableNamespace: strings.Join(nsIdent, "."),
			TableName:      tblIdent,
		}).Key(),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		return fmt.Errorf("failed to delete table: %w", err)
	}

	return nil
}

func (c *Catalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	toNSIdent := catalog.NamespaceFromIdent(to)
	toTblIdent := catalog.TableNameFromIdent(to)

	exists, err := c.namespaceExists(ctx, strings.Join(toNSIdent, "."))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, toNSIdent)
	}

	fromTbl, err := c.getTable(ctx, from)
	if err != nil {
		return nil, err
	}

	toTbl := dynamodbIcebergTable{
		TableNamespace:           strings.Join(toNSIdent, "."),
		TableName:                toTblIdent,
		MetadataLocation:         fromTbl.MetadataLocation,
		PreviousMetadataLocation: fromTbl.PreviousMetadataLocation,
		CreatedAt:                fromTbl.CreatedAt,
		UpdatedAt:                time.Now(),
	}
	item, err := toTbl.MarshalMap()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal table: %w", err)
	}

	putExpr, err := expression.NewBuilder().WithCondition(
		expression.And(
			expression.AttributeNotExists(expression.Name(dynamodbColumnIdentifier)),
			expression.AttributeNotExists(expression.Name(dynamodbColumnNamespace)),
		),
	).Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build expression: %w", err)
	}

	delExpr, err := expression.NewBuilder().WithCondition(
		expression.And(
			expression.AttributeExists(expression.Name(dynamodbColumnIdentifier)),
			expression.AttributeExists(expression.Name(dynamodbColumnNamespace)),
		),
	).Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build expression: %w", err)
	}

	_, err = c.dynamodb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName:                 aws.String(c.tableName),
					Item:                      item,
					ConditionExpression:       putExpr.Condition(),
					ExpressionAttributeNames:  putExpr.Names(),
					ExpressionAttributeValues: putExpr.Values(),
				},
			},
			{
				Delete: &types.Delete{
					TableName:                 aws.String(c.tableName),
					Key:                       fromTbl.Key(),
					ConditionExpression:       delExpr.Condition(),
					ExpressionAttributeNames:  delExpr.Names(),
					ExpressionAttributeValues: delExpr.Values(),
				},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to rename table: %w", err)
	}

	return c.LoadTable(ctx, to, nil)
}

func (c *Catalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	_, err := c.getTable(ctx, identifier)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *Catalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		expr, err := expression.NewBuilder().WithKeyCondition(
			expression.Key(dynamodbColumnNamespace).Equal(expression.Value(strings.Join(namespace, "."))),
		).Build()
		if err != nil {
			yield(table.Identifier{}, fmt.Errorf("failed to build expression: %w", err))
			return
		}

		paginator := dynamodb.NewQueryPaginator(c.dynamodb, &dynamodb.QueryInput{
			TableName:                 aws.String(c.tableName),
			IndexName:                 aws.String(dynamodbNamespaceGSI),
			KeyConditionExpression:    expr.KeyCondition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
		})

		for paginator.HasMorePages() {
			res, err := paginator.NextPage(ctx)
			if err != nil {
				yield(table.Identifier{}, fmt.Errorf("failed to query tables: %w", err))
				return
			}

			for _, item := range res.Items {
				tbl := &dynamodbIcebergTable{}
				if err := tbl.UnmarshalMap(item); err != nil {
					if errors.Is(err, ErrNamespaceIsNotATableIdentifier) {
						continue
					}
					yield(table.Identifier{}, fmt.Errorf("failed to unmarshal table: %w", err))
					return
				}

				if !yield(table.Identifier(append(strings.Split(tbl.TableNamespace, "."), tbl.TableName)), nil) {
					return
				}
			}
		}
	}
}

func (c *Catalog) ListTablesPaginated(ctx context.Context, namespace table.Identifier, pageToken *string, pageSize *int) ([]table.Identifier, *string, error) {
	expr, err := expression.NewBuilder().WithKeyCondition(
		expression.Key(dynamodbColumnNamespace).Equal(expression.Value(strings.Join(namespace, "."))),
	).Build()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build expression: %w", err)
	}

	queryInput := &dynamodb.QueryInput{
		TableName:                 aws.String(c.tableName),
		IndexName:                 aws.String(dynamodbNamespaceGSI),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}
	if pageSize != nil {
		queryInput.Limit = aws.Int32(int32(*pageSize))
	} else {
		queryInput.Limit = aws.Int32(10)
	}

	if pageToken != nil {
		decoded, err := c.decodedPageToken(*pageToken)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode page token: %w", err)
		}
		queryInput.ExclusiveStartKey = decoded
	}

	res, err := c.dynamodb.Query(ctx, queryInput)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query tables: %w", err)
	}

	tables := make([]table.Identifier, len(res.Items))
	for i, item := range res.Items {
		tbl := &dynamodbIcebergTable{}
		if err := tbl.UnmarshalMap(item); err != nil {
			if errors.Is(err, ErrNamespaceIsNotATableIdentifier) {
				continue
			}
			return nil, nil, fmt.Errorf("failed to unmarshal table: %w", err)
		}
		tables[i] = append(strings.Split(tbl.TableNamespace, "."), tbl.TableName)
	}

	token, err := c.encodePageToken(res.LastEvaluatedKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode page token: %w", err)
	}

	return tables, &token, nil
}

func (c *Catalog) encodePageToken(data map[string]types.AttributeValue) (string, error) {
	token, err := attributevalue.MarshalMapJSON(data)
	if err != nil {
		return "", err
	}
	return string(token), nil
}

func (c *Catalog) decodedPageToken(token string) (map[string]types.AttributeValue, error) {
	return attributevalue.UnmarshalMapJSON([]byte(token))
}

func (c *Catalog) SetKVSidecar(ctx context.Context, key, value string) error {
	item, err := (&dynamodbKVSidecarItem{
		Name:  key,
		Value: value,
	}).MarshalMap()
	if err != nil {
		return fmt.Errorf("failed to marshal item: %w", err)
	}

	_, err = c.dynamodb.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(c.tableName),
	})
	if err != nil {
		return fmt.Errorf("failed to put item: %w", err)
	}

	return nil
}

func (c *Catalog) GetKVSidecar(ctx context.Context, key string) (string, error) {
	res, err := c.dynamodb.GetItem(ctx, &dynamodb.GetItemInput{
		Key: (&dynamodbKVSidecarItem{
			Name: key,
		}).Key(),
		TableName: aws.String(c.tableName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get item: %w", err)
	}

	item := &dynamodbKVSidecarItem{}
	if err := item.UnmarshalMap(res.Item); err != nil {
		return "", fmt.Errorf("failed to unmarshal item: %w", err)
	}

	return item.Value, nil
}

type CreateTableRequest struct {
	Identifier table.Identifier
	Schema     *iceberg.Schema
	opts       []catalog.CreateTableOpt
}

type CommitTableRequest struct {
	Table        *table.Table
	Requirements []table.Requirement
	updates      []table.Update
}

type SetKVSidecarRequest struct {
	Key   string
	Value string
}

type TransactionRequest interface {
	transactionRequest()
}

func (c *CreateTableRequest) transactionRequest()  {}
func (c *CommitTableRequest) transactionRequest()  {}
func (c *SetKVSidecarRequest) transactionRequest() {}

func (c *Catalog) Transaction(ctx context.Context, reqs ...TransactionRequest) error {
	transactItems := make([]types.TransactWriteItem, len(reqs))

	for _, req := range reqs {
		switch req := req.(type) {
		case *CreateTableRequest:
			ns := strings.Join(catalog.NamespaceFromIdent(req.Identifier), ".")
			exists, err := c.namespaceExists(ctx, ns)
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, ns)
			}

			staged, err := c.stageCreateTable(ctx, req.Identifier, req.Schema, req.opts...)
			if err != nil {
				return err
			}

			item, err := c.getCreateTablePutItem(ctx, staged)
			if err != nil {
				return err
			}

			transactItems = append(transactItems, types.TransactWriteItem{
				Put: &types.Put{
					TableName:                 item.TableName,
					Item:                      item.Item,
					ConditionExpression:       item.ConditionExpression,
					ExpressionAttributeNames:  item.ExpressionAttributeNames,
					ExpressionAttributeValues: item.ExpressionAttributeValues,
				},
			})
		case *CommitTableRequest:
			current, staged, err := c.stageCommitTable(ctx, req.Table, req.Requirements, req.updates)
			if err != nil {
				if errors.Is(err, ErrNoChanges) {
					continue
				}
				return err
			}

			updateItem, err := c.getCommitTableUpdateItem(ctx, current, staged)
			if err != nil {
				return err
			}

			transactItems = append(transactItems, types.TransactWriteItem{
				Update: &types.Update{
					TableName:                 updateItem.TableName,
					Key:                       updateItem.Key,
					ConditionExpression:       updateItem.ConditionExpression,
					UpdateExpression:          updateItem.UpdateExpression,
					ExpressionAttributeNames:  updateItem.ExpressionAttributeNames,
					ExpressionAttributeValues: updateItem.ExpressionAttributeValues,
				},
			})
		case *SetKVSidecarRequest:
			item, err := (&dynamodbKVSidecarItem{
				Name:  req.Key,
				Value: req.Value,
			}).MarshalMap()
			if err != nil {
				return fmt.Errorf("failed to marshal item: %w", err)
			}
			transactItems = append(transactItems, types.TransactWriteItem{
				Put: &types.Put{
					TableName: aws.String(c.tableName),
					Item:      item,
				},
			})
		}
	}

	_, err := c.dynamodb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	if err != nil {
		return fmt.Errorf("failed to transact: %w", err)
	}

	return nil
}
