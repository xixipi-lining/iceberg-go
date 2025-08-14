package dynamodb

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	dynamodbColumnIdentifier               = "identifier"
	dynamodbColumnNamespace                = "namespace"
	dynamodbColumnCreatedAt                = "created_at"
	dynamodbColumnUpdatedAt                = "updated_at"
	dynamodbColumnMetadataLocation         = "metadata_location"
	dynamodbColumnPreviousMetadataLocation = "previous_metadata_location"
	dynamodbColumnProperties               = "properties"

	dynamodbNamespace = "NAMESPACE"

	dynamodbNamespaceGSI = "namespace-identifier"
)

var (
	ErrNamespaceIsNotATableIdentifier = errors.New("NAMESPACE is not a valid table identifier")
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
	CreatedAt                time.Time
	UpdatedAt                time.Time
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
		CreatedAt:                m.CreatedAt,
		UpdatedAt:                m.UpdatedAt,
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
	if model.Identifier == dynamodbNamespace {
		return ErrNamespaceIsNotATableIdentifier
	}
	m.TableNamespace = model.Namespace
	prefix := model.Namespace + "."
	if !strings.HasPrefix(model.Identifier, prefix) {
		return fmt.Errorf("invalid table identifier: %s for namespace %s", model.Identifier, model.Namespace)
	}
	m.TableName = model.Identifier[len(prefix):]
	m.MetadataLocation = model.MetadataLocation
	m.PreviousMetadataLocation = model.PreviousMetadataLocation
	m.CreatedAt = model.CreatedAt
	m.UpdatedAt = model.UpdatedAt

	return nil
}

func (m *dynamodbIcebergTable) Valid() error {
	if m.TableName == "" {
		return fmt.Errorf("table name is empty")
	}
	if m.TableNamespace == "" {
		return fmt.Errorf("table namespace is empty")
	}
	if m.CreatedAt.IsZero() {
		return fmt.Errorf("created at is zero")
	}
	return nil
}
