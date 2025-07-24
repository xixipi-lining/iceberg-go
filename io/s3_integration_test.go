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

package io_test

import (
	"context"
	"strings"

	"github.com/testcontainers/testcontainers-go/modules/compose"

	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun/driver/sqliteshim"
	"github.com/xixipi-lining/iceberg-go"
	"github.com/xixipi-lining/iceberg-go/catalog"
	sqlcat "github.com/xixipi-lining/iceberg-go/catalog/sql"
	"github.com/xixipi-lining/iceberg-go/io"
)

const s3ComposeContent = `
services:
  minio:
    image: minio/minio
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
      - MINIO_DOMAIN=minio
    ports:
      - 9001
      - 9000
    command: ["server", "/data", "--console-address", ":9001"]
  mc:
    depends_on:
      - minio
    image: minio/mc
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
      tail -f /dev/null
      "
`

type S3IOTestSuite struct {
	suite.Suite

	ctx           context.Context
	stack         compose.ComposeStack
	minioEndpoint string
}

func (s *S3IOTestSuite) SetupSuite() {
	stack, err := compose.NewDockerComposeWith(compose.WithStackReaders(strings.NewReader(s3ComposeContent)))
	s.Require().NoError(err)
	s.stack = stack
	s.Require().NoError(stack.Up(s.T().Context()))

	svc, err := stack.ServiceContainer(s.T().Context(), "minio")
	s.Require().NoError(err)
	s.Require().NotNil(svc)

	endpoint, err := svc.PortEndpoint(s.T().Context(), "9000", "http")
	s.Require().NoError(err)
	s.Require().NotNil(endpoint)
	s.minioEndpoint = endpoint
}

func (s *S3IOTestSuite) TearDownSuite() {
	s.Require().NoError(s.stack.Down(s.T().Context()))
}

func (s *S3IOTestSuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *S3IOTestSuite) TestMinioWarehouse() {
	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":                ":memory:",
		sqlcat.DriverKey:     sqliteshim.ShimName,
		sqlcat.DialectKey:    string(sqlcat.SQLite),
		"type":               "sql",
		"warehouse":          "s3a://warehouse/iceberg/",
		io.S3Region:          "local",
		io.S3AccessKeyID:     "admin",
		io.S3SecretAccessKey: "password",
		io.S3EndpointURL:     s.minioEndpoint,
	})
	s.Require().NoError(err)

	s.Require().NotNil(cat)

	c := cat.(*sqlcat.Catalog)
	ctx := s.ctx
	s.Require().NoError(c.CreateNamespace(ctx, catalog.ToIdentifier("iceberg-test-2"), nil))

	tbl, err := c.CreateTable(ctx,
		catalog.ToIdentifier("iceberg-test-2", "test-table-2"),
		iceberg.NewSchema(0, iceberg.NestedField{
			Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, ID: 1,
		}), catalog.WithLocation("s3a://warehouse/iceberg/iceberg-test-2/test-table-2"))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)
}

func (s *S3IOTestSuite) TestMinioWarehouseNoLocation() {
	cat, err := catalog.Load(context.Background(), "default", iceberg.Properties{
		"uri":                ":memory:",
		sqlcat.DriverKey:     sqliteshim.ShimName,
		sqlcat.DialectKey:    string(sqlcat.SQLite),
		"type":               "sql",
		"warehouse":          "s3a://warehouse/iceberg/",
		io.S3Region:          "local",
		io.S3AccessKeyID:     "admin",
		io.S3SecretAccessKey: "password",
		io.S3EndpointURL:     s.minioEndpoint,
	})
	s.Require().NoError(err)

	s.Require().NotNil(cat)

	c := cat.(*sqlcat.Catalog)
	ctx := s.ctx
	s.Require().NoError(c.CreateNamespace(ctx, catalog.ToIdentifier("iceberg-test-2"), nil))

	tbl, err := c.CreateTable(ctx,
		catalog.ToIdentifier("iceberg-test-2", "test-table-2"),
		iceberg.NewSchema(0, iceberg.NestedField{
			Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, ID: 1,
		}))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)
}
