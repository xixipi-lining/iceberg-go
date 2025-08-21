package glue

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
)

func (c *Catalog) CreateTableByTable(ctx context.Context, staged *table.StagedTable) error {
	database, tableName, err := identifierToGlueTable(staged.Identifier())
	if err != nil {
		return err
	}

	_, err = c.glueSvc.CreateTable(ctx, &glue.CreateTableInput{
		CatalogId:    c.catalogId,
		DatabaseName: aws.String(database),
		TableInput:   constructTableInput(tableName, staged.Table, nil),
	})
	
	return err
}

func (c *Catalog) CommitTableByTable(ctx context.Context, current *table.Table, staged *table.StagedTable) error {
	database, tableName, err := identifierToGlueTable(staged.Identifier())
	if err != nil {
		return err
	}

	currentGlueTable, err := c.getTable(ctx, database, tableName)
	if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
		return err
	}

	if current == nil {
		_, err = c.glueSvc.CreateTable(ctx, &glue.CreateTableInput{
			CatalogId:    c.catalogId,
			DatabaseName: aws.String(database),
			TableInput:   constructTableInput(tableName, staged.Table, nil),
		})

		return err
	}

	if currentGlueTable.VersionId == nil {
		return fmt.Errorf("cannot commit table %s.%s: because Glue table version id is missing", database, tableName)
	}

	_, err = c.glueSvc.UpdateTable(ctx, &glue.UpdateTableInput{
		CatalogId:    c.catalogId,
		DatabaseName: aws.String(database),
		TableInput:   constructTableInput(tableName, staged.Table, currentGlueTable),
		VersionId:    currentGlueTable.VersionId,
		SkipArchive:  aws.Bool(c.props.GetBool(SkipArchive, SkipArchiveDefault)),
	})

	return err
}

var _ catalog.FollowerCatalog = (*Catalog)(nil)