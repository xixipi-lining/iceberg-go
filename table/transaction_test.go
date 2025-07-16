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

package table_test

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/modules/compose"
)

const transactionTestComposeContent = `
services:
  spark-iceberg:
    image: pyiceberg-spark
    build:
      context: .
      dockerfile_inline: |
        FROM tabulario/spark-iceberg

        RUN pip3 install -q ipython
        RUN pip3 install pyiceberg[s3fs,hive]
        RUN pip3 install pyarrow

        ENTRYPOINT ["./entrypoint.sh"]
        CMD ["notebook"]
    networks:
      iceberg_net:
    depends_on:
      - rest
      - minio
    volumes:
      - ./warehouse:/home/iceberg/warehouse
      - ./notebooks:/home/iceberg/notebooks/notebooks
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
    ports:
      - 8888
      - 8080
      - 10000
      - 10001
  rest:
    image: apache/iceberg-rest-fixture
    networks:
      iceberg_net:
    ports:
      - 8181
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - CATALOG_WAREHOUSE=s3://warehouse/
      - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
      - CATALOG_S3_ENDPOINT=http://minio:9000
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
      tail -f /dev/null
      "
networks:
  iceberg_net:
`

type SparkIntegrationTestSuite struct {
	suite.Suite

	ctx   context.Context
	cat   catalog.Catalog
	props iceberg.Properties

	stack *compose.DockerCompose
	restEndpoint string
	s3Endpoint string
}

func (s *SparkIntegrationTestSuite) SetupSuite() {
	ctx := s.T().Context()
	stack, err := compose.NewDockerComposeWith(compose.WithStackReaders(strings.NewReader(transactionTestComposeContent)))
	s.Require().NoError(err)
	s.stack = stack
	s.Require().NoError(stack.Up(ctx))

	restSvc, err := stack.ServiceContainer(ctx, "rest")
	s.Require().NoError(err)
	s.Require().NotNil(restSvc)

	endpoint, err := restSvc.PortEndpoint(ctx, "8181", "http")
	s.Require().NoError(err)
	s.Require().NotNil(endpoint)
	s.restEndpoint = endpoint

	minioSvc, err := stack.ServiceContainer(ctx, "minio")
	s.Require().NoError(err)
	s.Require().NotNil(minioSvc)

	endpoint, err = minioSvc.PortEndpoint(ctx, "9000", "http")
	s.Require().NoError(err)
	s.Require().NotNil(endpoint)
	s.s3Endpoint = endpoint

	sparkSvc, err := stack.ServiceContainer(ctx, "spark-iceberg")
	s.Require().NoError(err)
	s.Require().NotNil(sparkSvc)

	provisionPath, err := filepath.Abs(filepath.Join("..", "internal", "recipe", "provision.py"))
	s.Require().NoError(err)

	err = sparkSvc.CopyFileToContainer(ctx, provisionPath, "/opt/spark/provision.py", 0644)
	s.Require().NoError(err)

	_, stdout, err := sparkSvc.Exec(ctx, []string{"ipython", "./provision.py"})
	s.Require().NoError(err)

	data, err := io.ReadAll(stdout)
	s.Require().NoError(err)
	s.T().Log(string(data))
}

func (s *SparkIntegrationTestSuite) SetupTest() {
	s.ctx = context.Background()
	s.props = iceberg.Properties{
		iceio.S3Region:          "us-east-1",
		iceio.S3EndpointURL:     s.s3Endpoint,
		iceio.S3AccessKeyID:     "admin",
		iceio.S3SecretAccessKey: "password",
	}

	cat, err := rest.NewCatalog(s.ctx, "rest", s.restEndpoint, rest.WithAdditionalProps(s.props))
	s.Require().NoError(err)
	s.cat = cat
}

func (s *SparkIntegrationTestSuite) TestAddFile() {
	const filename = "s3://warehouse/default/test_partitioned_by_days/data/ts_day=2023-03-13/supertest.parquet"

	tbl, err := s.cat.LoadTable(s.ctx, catalog.ToIdentifier("default", "test_partitioned_by_days"), nil)
	s.Require().NoError(err)

	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	if err != nil {
		panic(err)
	}

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer bldr.Release()

	tm := time.Date(2023, 03, 13, 13, 22, 0, 0, time.UTC)
	ts, _ := arrow.TimestampFromTime(tm, arrow.Microsecond)
	bldr.Field(0).(*array.Date32Builder).Append(arrow.Date32FromTime(tm))
	bldr.Field(1).(*array.TimestampBuilder).Append(ts)
	bldr.Field(2).(*array.Int32Builder).Append(13)
	bldr.Field(3).(*array.StringBuilder).Append("m")

	rec := bldr.NewRecord()
	defer rec.Release()

	fw, err := mustFS(s.T(), tbl).(iceio.WriteFileIO).Create(filename)
	if err != nil {
		panic(err)
	}
	defer fw.Close()

	if err := pqarrow.WriteTable(array.NewTableFromRecords(sc, []arrow.Record{rec}), fw, rec.NumRows(), parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		panic(err)
	}

	tx := tbl.NewTransaction()
	err = tx.AddFiles(s.ctx, []string{filename}, nil, false)
	s.Require().NoError(err)

	tbl, err = tx.Commit(s.ctx)
	s.Require().NoError(err)

	spark, err := s.stack.ServiceContainer(s.T().Context(), "spark-iceberg")
	s.Require().NoError(err)

	runSparkCountSqlPath, err := filepath.Abs(filepath.Join("..", "internal", "recipe", "run_spark_count_sql.py"))
	s.Require().NoError(err)

	err = spark.CopyFileToContainer(s.ctx, runSparkCountSqlPath, "/opt/spark/run_spark_count_sql.py", 0644)
	s.Require().NoError(err)

	_, stdout, err := spark.Exec(s.ctx, []string{"ipython", "./run_spark_count_sql.py"})
	s.Require().NoError(err)

	output, err := io.ReadAll(stdout)
	s.Require().NoError(err)
	strings.HasSuffix(string(output), `
+--------+
|count(1)|
+--------+
|      13|
+--------+
`)
}

func TestSparkIntegration(t *testing.T) {
	suite.Run(t, new(SparkIntegrationTestSuite))
}
