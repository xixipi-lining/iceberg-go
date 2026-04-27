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

package glue

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
)

func (c *Catalog) FollowCommitTable(ctx context.Context, identifier table.Identifier, prevMetadataLocation, metadataLocation string) error {
	if prevMetadataLocation == "" {
		return fmt.Errorf("cannot commit table without previouse metadata location")
	}

	database, tableName, err := identifierToGlueTable(identifier)
	if err != nil {
		return err
	}

	currentGlueTable, err := c.getTable(ctx, database, tableName)
	if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
		return err
	}
	if currentGlueTable == nil {
		return fmt.Errorf("table %s not found in catalog", identifier)
	}
	if currentGlueTable.VersionId == nil {
		return fmt.Errorf("cannot commit table %s.%s: because Glue table version id is missing", database, tableName)
	}

	tbl, err := table.NewFromLocation(
		utils.WithAwsConfig(ctx, c.awsCfg),
		identifier,
		metadataLocation,
		io.LoadFSFunc(nil, metadataLocation),
		c,
	)
	if err != nil {
		return fmt.Errorf("failed to read table metadata from %s: %w", metadataLocation, err)
	}

	_, err = c.glueSvc.UpdateTable(ctx, &glue.UpdateTableInput{
		CatalogId:    c.catalogId,
		DatabaseName: aws.String(database),
		TableInput:   constructTableInput(tableName, tbl, currentGlueTable),
		VersionId:    currentGlueTable.VersionId,
		SkipArchive:  aws.Bool(c.props.GetBool(SkipArchive, SkipArchiveDefault)),
	})
	if err != nil {
		return err
	}

	return nil
}
