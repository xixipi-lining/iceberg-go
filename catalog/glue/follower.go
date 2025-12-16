package glue

import (
	"context"
	"fmt"

	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
)

func (c *Catalog) FollowCommitTable(ctx context.Context, identifier table.Identifier, previousMetadataLocation, metadataLocation string) error {
	database, tableName, err := identifierToGlueTable(identifier)
	if err != nil {
		return err
	}

	currentGlueTable, err := c.getTable(ctx, database, tableName)
	if err != nil {
		return err
	}

	if *currentGlueTable.StorageDescriptor.Location != previousMetadataLocation {
		return fmt.Errorf("previous metadata location %s does not match current metadata location %s", previousMetadataLocation, *currentGlueTable.StorageDescriptor.Location)
	}

	ctx = utils.WithAwsConfig(ctx, c.awsCfg)

	tbl, err := table.NewFromLocation(
		ctx,
		[]string{tableName},
		metadataLocation,
		io.LoadFSFunc(nil, metadataLocation),
		c,
	)
	if err != nil {
		return fmt.Errorf("failed to create iceberg table from location %s: %w", metadataLocation, err)
	}

	_, err = c.glueSvc.UpdateTable(ctx, &glue.UpdateTableInput{
		CatalogId:    c.catalogId,
		DatabaseName: aws.String(database),
		TableInput:   constructTableInput(tableName, tbl, currentGlueTable),
		VersionId:    currentGlueTable.VersionId,
		SkipArchive:  aws.Bool(c.props.GetBool(SkipArchive, SkipArchiveDefault)),
	})
	if err != nil {
		return fmt.Errorf("failed to update glue table %s.%s: %w", database, tableName, err)
	}

	return nil
}
