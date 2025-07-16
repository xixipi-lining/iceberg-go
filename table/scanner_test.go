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
	"iter"
	"math"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

const scannerTestComposeContent = `
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
      echo 'BUCKET_READY';
      tail -f /dev/null
      "
networks:
  iceberg_net:
`

type ScannerSuite struct {
	suite.Suite

	ctx   context.Context
	cat   catalog.Catalog
	props iceberg.Properties

	stack        compose.ComposeStack
	restEndpoint string
	s3Endpoint string
}

func (s *ScannerSuite) SetupSuite() {
	ctx := s.T().Context()
	stack, err := compose.NewDockerComposeWith(compose.WithStackReaders(strings.NewReader(scannerTestComposeContent)))
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

	s.waitForMinIOReady(ctx)

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

func (s *ScannerSuite) waitForMinIOReady(ctx context.Context) {
	mcSvc, err := s.stack.ServiceContainer(ctx, "mc")
	s.Require().NoError(err)
	s.Require().NotNil(mcSvc)
	
	// Wait for the "BUCKET_READY" log message from mc service
	waitStrategy := wait.ForLog("BUCKET_READY").WithStartupTimeout(60 * time.Second)
	s.Require().NoError(waitStrategy.WaitUntilReady(ctx, mcSvc))
}

func (s *ScannerSuite) SetupTest() {
	s.ctx = context.Background()

	cat, err := rest.NewCatalog(s.ctx, "rest", s.restEndpoint)
	s.Require().NoError(err)

	s.cat = cat
	s.props = iceberg.Properties{
		iceio.S3Region:      "us-east-1",
		iceio.S3AccessKeyID: "admin",
		iceio.S3SecretAccessKey: "password",
		iceio.S3EndpointURL: s.s3Endpoint,
	}
}

func (s *ScannerSuite) TestScanner() {
	tests := []struct {
		table            string
		expr             iceberg.BooleanExpression
		expectedNumTasks int
	}{
		{"test_all_types", iceberg.AlwaysTrue{}, 5},
		{"test_all_types", iceberg.LessThan(iceberg.Reference("intCol"), int32(3)), 3},
		{"test_all_types", iceberg.GreaterThanEqual(iceberg.Reference("intCol"), int32(3)), 2},
		{"test_partitioned_by_identity",
			iceberg.GreaterThanEqual(iceberg.Reference("ts"), "2023-03-05T00:00:00+00:00"), 8},
		{"test_partitioned_by_identity",
			iceberg.LessThan(iceberg.Reference("ts"), "2023-03-05T00:00:00+00:00"), 4},
		{"test_partitioned_by_years", iceberg.AlwaysTrue{}, 2},
		{"test_partitioned_by_years", iceberg.LessThan(iceberg.Reference("dt"), "2023-03-05"), 1},
		{"test_partitioned_by_years", iceberg.GreaterThanEqual(iceberg.Reference("dt"), "2023-03-05"), 1},
		{"test_partitioned_by_months", iceberg.GreaterThanEqual(iceberg.Reference("dt"), "2023-03-05"), 1},
		{"test_partitioned_by_days", iceberg.GreaterThanEqual(iceberg.Reference("ts"), "2023-03-05T00:00:00+00:00"), 8},
		{"test_partitioned_by_hours", iceberg.GreaterThanEqual(iceberg.Reference("ts"), "2023-03-05T00:00:00+00:00"), 8},
		{"test_partitioned_by_truncate", iceberg.GreaterThanEqual(iceberg.Reference("letter"), "e"), 8},
		{"test_partitioned_by_bucket", iceberg.GreaterThanEqual(iceberg.Reference("number"), int32(5)), 6},
		{"test_uuid_and_fixed_unpartitioned", iceberg.EqualTo(iceberg.Reference("uuid_col"), "102cb62f-e6f8-4eb0-9973-d9b012ff0967"), 1},
	}

	for _, tt := range tests {
		s.Run(tt.table+" "+tt.expr.String(), func() {
			ident := catalog.ToIdentifier("default", tt.table)

			tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
			s.Require().NoError(err)

			scan := tbl.Scan(table.WithRowFilter(tt.expr))
			tasks, err := scan.PlanFiles(s.ctx)
			s.Require().NoError(err)

			s.Len(tasks, tt.expectedNumTasks)
		})
	}
}

func (s *ScannerSuite) TestScannerWithDeletes() {
	ident := catalog.ToIdentifier("default", "test_positional_mor_deletes")

	tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
	s.Require().NoError(err)

	scan := tbl.Scan()
	tasks, err := scan.PlanFiles(s.ctx)
	s.Require().NoError(err)

	s.Len(tasks, 1)
	s.Len(tasks[0].DeleteFiles, 1)

	tagScan, err := scan.UseRef("tag_12")
	s.Require().NoError(err)

	tasks, err = tagScan.PlanFiles(s.ctx)
	s.Require().NoError(err)

	s.Len(tasks, 1)
	s.Len(tasks[0].DeleteFiles, 0)

	_, err = tagScan.UseRef("without_5")
	s.ErrorIs(err, iceberg.ErrInvalidArgument)

	tagScan, err = scan.UseRef("without_5")
	s.Require().NoError(err)

	tasks, err = tagScan.PlanFiles(s.ctx)
	s.Require().NoError(err)

	s.Len(tasks, 1)
	s.Len(tasks[0].DeleteFiles, 1)
}

func (s *ScannerSuite) TestArrowNan() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	for _, name := range []string{"test_null_nan", "test_null_nan_rewritten"} {
		s.Run(name, func() {
			ident := catalog.ToIdentifier("default", name)
			tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
			s.Require().NoError(err)

			ctx := compute.WithAllocator(s.ctx, mem)
			results, err := tbl.Scan(table.WithRowFilter(iceberg.IsNaN(iceberg.Reference("col_numeric"))),
				table.WithSelectedFields("idx", "col_numeric")).ToArrowTable(ctx)
			s.Require().NoError(err)
			defer results.Release()

			s.EqualValues(2, results.NumCols())
			s.EqualValues(1, results.NumRows())

			s.Equal(int32(1), results.Column(0).Data().Chunk(0).(*array.Int32).Value(0))
			s.True(math.IsNaN(float64(results.Column(1).Data().Chunk(0).(*array.Float32).Value(0))))
		})
	}
}

func (s *ScannerSuite) TestArrowNotNanCount() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	ident := catalog.ToIdentifier("default", "test_null_nan")
	tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
	s.Require().NoError(err)

	ctx := compute.WithAllocator(s.ctx, mem)
	results, err := tbl.Scan(table.WithRowFilter(iceberg.NotNaN(iceberg.Reference("col_numeric"))),
		table.WithSelectedFields("idx")).ToArrowTable(ctx)
	s.Require().NoError(err)
	defer results.Release()

	s.EqualValues(1, results.NumCols())
	s.EqualValues(2, results.NumRows())
}

func (s *ScannerSuite) TestScanWithLimit() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	ident := catalog.ToIdentifier("default", "test_limit")
	tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
	s.Require().NoError(err)

	tests := []struct {
		limit        int64
		expectedRows int64
	}{
		{1, 1},
		{0, 0},
		{999, 10},
	}

	for _, tt := range tests {
		s.Run(strconv.Itoa(int(tt.limit)), func() {
			scopedMem := memory.NewCheckedAllocatorScope(mem)
			defer scopedMem.CheckSize(s.T())

			ctx := compute.WithAllocator(s.ctx, mem)
			result, err := tbl.Scan(table.WithSelectedFields("idx"),
				table.WithLimit(tt.limit)).ToArrowTable(ctx)
			s.Require().NoError(err)
			defer result.Release()

			s.EqualValues(tt.expectedRows, result.NumRows())
		})
	}
}

func (s *ScannerSuite) TestScannerRecordsDeletes() {
	// number, letter
	//  (1, 'a'),
	//  (2, 'b'),
	//  (3, 'c'),
	//  (4, 'd'),
	//  (5, 'e'),
	//  (6, 'f'),
	//  (7, 'g'),
	//  (8, 'h'),
	//  (9, 'i'), <- deleted
	//  (10, 'j'),
	//  (11, 'k'),
	//  (12, 'l')
	ident := catalog.ToIdentifier("default", "test_positional_mor_deletes")

	tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
	s.Require().NoError(err)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	ref := iceberg.Reference("letter")

	tests := []struct {
		name     string
		filter   iceberg.BooleanExpression
		rowLimit int64
		expected string
	}{
		{"all", iceberg.AlwaysTrue{}, table.ScanNoLimit,
			`[1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12]`},
		{"filter", iceberg.NewAnd(iceberg.GreaterThanEqual(ref, "e"),
			iceberg.LessThan(ref, "k")), table.ScanNoLimit, `[5, 6, 7, 8, 10]`},
		{"filter and limit", iceberg.NewAnd(iceberg.GreaterThanEqual(ref, "e"),
			iceberg.LessThan(ref, "k")), 1, `[5]`},
		{"limit", nil, 3, `[1, 2, 3]`},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			scopedMem := memory.NewCheckedAllocatorScope(mem)
			defer scopedMem.CheckSize(s.T())

			ctx := compute.WithAllocator(s.ctx, mem)

			scan := tbl.Scan(table.WithRowFilter(tt.filter),
				table.WithSelectedFields("number"))
			tasks, err := scan.PlanFiles(ctx)
			s.Require().NoError(err)

			s.Len(tasks, 1)
			s.Len(tasks[0].DeleteFiles, 1)

			_, itr, err := scan.UseRowLimit(tt.rowLimit).ToArrowRecords(ctx)
			s.Require().NoError(err)

			next, stop := iter.Pull2(itr)
			defer stop()

			rec, err, valid := next()
			s.Require().True(valid)
			s.Require().NoError(err)
			defer rec.Release()

			s.True(expectedSchema.Equal(rec.Schema()), "expected: %s\ngot: %s\n",
				expectedSchema, rec.Schema())

			arr, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32,
				strings.NewReader(tt.expected))
			s.Require().NoError(err)
			defer arr.Release()

			expectedResult := array.NewRecord(expectedSchema, []arrow.Array{arr}, int64(arr.Len()))
			defer expectedResult.Release()

			s.True(array.RecordEqual(expectedResult, rec), "expected: %s\ngot: %s\n", expectedResult, rec)

			_, err, valid = next()
			s.Require().NoError(err)
			s.Require().False(valid)
		})
	}
}

func (s *ScannerSuite) TestScannerRecordsDoubleDeletes() {
	// number, letter
	//  (1, 'a'),
	//  (2, 'b'),
	//  (3, 'c'),
	//  (4, 'd'),
	//  (5, 'e'),
	//  (6, 'f'), <- second delete
	//  (7, 'g'),
	//  (8, 'h'),
	//  (9, 'i'), <- first delete
	//  (10, 'j'),
	//  (11, 'k'),
	//  (12, 'l')
	ident := catalog.ToIdentifier("default", "test_positional_mor_double_deletes")

	tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
	s.Require().NoError(err)

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	ref := iceberg.Reference("letter")

	tests := []struct {
		name     string
		filter   iceberg.BooleanExpression
		rowLimit int64
		expected string
	}{
		{"all", iceberg.AlwaysTrue{}, table.ScanNoLimit,
			`[1, 2, 3, 4, 5, 7, 8, 10, 11, 12]`},
		{"filter", iceberg.NewAnd(iceberg.GreaterThanEqual(ref, "e"),
			iceberg.LessThan(ref, "k")), table.ScanNoLimit, `[5, 7, 8, 10]`},
		{"filter and limit", iceberg.NewAnd(iceberg.GreaterThanEqual(ref, "e"),
			iceberg.LessThan(ref, "k")), 1, `[5]`},
		{"limit", nil, 8, `[1, 2, 3, 4, 5, 7, 8, 10]`},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			scopedMem := memory.NewCheckedAllocatorScope(mem)
			defer scopedMem.CheckSize(s.T())

			ctx := compute.WithAllocator(s.ctx, mem)

			scan := tbl.Scan(table.WithRowFilter(tt.filter),
				table.WithSelectedFields("number"))
			tasks, err := scan.PlanFiles(ctx)
			s.Require().NoError(err)

			s.Len(tasks, 1)
			s.GreaterOrEqual(len(tasks[0].DeleteFiles), 1)

			_, itr, err := scan.UseRowLimit(tt.rowLimit).ToArrowRecords(ctx)
			s.Require().NoError(err)

			next, stop := iter.Pull2(itr)
			defer stop()

			rec, err, valid := next()
			s.Require().True(valid)
			s.Require().NoError(err)
			defer rec.Release()

			s.True(expectedSchema.Equal(rec.Schema()), "expected: %s\ngot: %s\n",
				expectedSchema, rec.Schema())

			arr, _, err := array.FromJSON(mem, arrow.PrimitiveTypes.Int32,
				strings.NewReader(tt.expected))
			s.Require().NoError(err)
			defer arr.Release()

			expectedResult := array.NewRecord(expectedSchema, []arrow.Array{arr}, int64(arr.Len()))
			defer expectedResult.Release()

			s.True(array.RecordEqual(expectedResult, rec), "expected: %s\ngot: %s\n", expectedResult, rec)

			_, err, valid = next()
			s.Require().NoError(err)
			s.Require().False(valid)
		})
	}
}

func getSortedValues(col *arrow.Column) []int32 {
	result := make([]int32, 0, col.Len())
	for _, c := range col.Data().Chunks() {
		arr := c.(*array.Int32)
		result = append(result, arr.Int32Values()...)
	}
	slices.Sort(result)
	return result
}

func getStrValues(col *arrow.Column) []string {
	result := make([]string, 0, col.Len())
	for _, c := range col.Data().Chunks() {
		for i := 0; i < c.Len(); i++ {
			result = append(result, c.ValueStr(i))
		}
	}
	return result
}

func (s *ScannerSuite) TestPartitionedTables() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	tests := []struct {
		table     string
		predicate iceberg.BooleanExpression
	}{
		{"test_partitioned_by_identity",
			iceberg.GreaterThanEqual(iceberg.Reference("ts"), "2023-03-05T00:00:00+00:00")},
		{"test_partitioned_by_years", iceberg.GreaterThanEqual(iceberg.Reference("dt"), "2023-03-05")},
		{"test_partitioned_by_months", iceberg.GreaterThanEqual(iceberg.Reference("dt"), "2023-03-05")},
		{"test_partitioned_by_days", iceberg.GreaterThanEqual(iceberg.Reference("ts"), "2023-03-05T00:00:00+00:00")},
		{"test_partitioned_by_hours", iceberg.GreaterThanEqual(iceberg.Reference("ts"), "2023-03-05T00:00:00+00:00")},
		{"test_partitioned_by_truncate", iceberg.GreaterThanEqual(iceberg.Reference("letter"), "e")},
		{"test_partitioned_by_bucket", iceberg.GreaterThanEqual(iceberg.Reference("number"), int32(5))},
	}

	for _, tt := range tests {
		s.Run(tt.table+" "+tt.predicate.String(), func() {
			scopedMem := memory.NewCheckedAllocatorScope(mem)
			defer scopedMem.CheckSize(s.T())
			ctx := compute.WithAllocator(s.ctx, mem)

			ident := catalog.ToIdentifier("default", tt.table)

			tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
			s.Require().NoError(err)

			scan := tbl.Scan(table.WithRowFilter(tt.predicate),
				table.WithSelectedFields("number"))
			resultTable, err := scan.ToArrowTable(ctx)
			s.Require().NoError(err)
			defer resultTable.Release()

			s.True(expectedSchema.Equal(resultTable.Schema()), "expected: %s\ngot: %s\n",
				expectedSchema, resultTable.Schema())

			s.Equal([]int32{5, 6, 7, 8, 9, 10, 11, 12},
				getSortedValues(resultTable.Column(0)))
		})
	}
}

func (s *ScannerSuite) TestNestedColumns() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	ident := catalog.ToIdentifier("default", "test_all_types")

	tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
	s.Require().NoError(err)

	ctx := compute.WithAllocator(s.ctx, mem)
	results, err := tbl.Scan().ToArrowTable(ctx)
	s.Require().NoError(err)
	defer results.Release()

	s.EqualValues(5, results.NumRows())
}

func (s *ScannerSuite) TestIsInFilterTable() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	ident := catalog.ToIdentifier("default", "test_uuid_and_fixed_unpartitioned")

	tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
	s.Require().NoError(err)

	ctx := compute.WithAllocator(s.ctx, mem)
	results, err := tbl.Scan(table.WithRowFilter(
		iceberg.NewNot(iceberg.IsIn(iceberg.Reference("uuid_col"),
			"102cb62f-e6f8-4eb0-9973-d9b012ff0967",
			"639cccce-c9d2-494a-a78c-278ab234f024"))),
		table.WithSelectedFields("uuid_col")).ToArrowTable(ctx)
	s.Require().NoError(err)
	defer results.Release()

	s.EqualValues(3, results.NumRows())
	s.Equal([]string{"ec33e4b2-a834-4cc3-8c4a-a1d3bfc2f226",
		"c1b0d8e0-0b0e-4b1e-9b0a-0e0b0d0c0a0b",
		"923dae77-83d6-47cd-b4b0-d383e64ee57e"}, getStrValues(results.Column(0)))
}

func (s *ScannerSuite) TestUnpartitionedUUIDTable() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "uuid_col", Type: extensions.NewUUIDType(), Nullable: true},
	}, nil)

	ident := catalog.ToIdentifier("default", "test_uuid_and_fixed_unpartitioned")

	tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
	s.Require().NoError(err)

	ctx := compute.WithAllocator(s.ctx, mem)
	results, err := tbl.Scan(table.WithRowFilter(
		iceberg.EqualTo(iceberg.Reference("uuid_col"),
			"102cb62f-e6f8-4eb0-9973-d9b012ff0967")),
		table.WithSelectedFields("uuid_col")).ToArrowTable(ctx)
	s.Require().NoError(err)
	defer results.Release()

	s.True(expectedSchema.Equal(results.Schema()), "expected: %s\ngot: %s\n",
		expectedSchema, results.Schema())

	s.EqualValues(1, results.NumRows())
	resultCol := results.Column(0).Data().Chunk(0).(*extensions.UUIDArray)
	s.Equal("102cb62f-e6f8-4eb0-9973-d9b012ff0967", resultCol.ValueStr(0))

	neqResults, err := tbl.Scan(table.WithRowFilter(
		iceberg.NewAnd(
			iceberg.NotEqualTo(iceberg.Reference("uuid_col"),
				"102cb62f-e6f8-4eb0-9973-d9b012ff0967"),
			iceberg.NotEqualTo(iceberg.Reference("uuid_col"),
				"639cccce-c9d2-494a-a78c-278ab234f024"))),
		table.WithSelectedFields("uuid_col")).ToArrowTable(ctx)
	s.Require().NoError(err)
	defer neqResults.Release()

	s.EqualValues(3, neqResults.NumRows())
	s.Equal([]string{"ec33e4b2-a834-4cc3-8c4a-a1d3bfc2f226",
		"c1b0d8e0-0b0e-4b1e-9b0a-0e0b0d0c0a0b",
		"923dae77-83d6-47cd-b4b0-d383e64ee57e"}, getStrValues(neqResults.Column(0)))
}

func (s *ScannerSuite) TestUnpartitionedFixedTable() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	ident := catalog.ToIdentifier("default", "test_uuid_and_fixed_unpartitioned")

	tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
	s.Require().NoError(err)

	ctx := compute.WithAllocator(s.ctx, mem)
	results, err := tbl.Scan(table.WithRowFilter(
		iceberg.EqualTo(iceberg.Reference("fixed_col"),
			"1234567890123456789012345")),
		table.WithCaseSensitive(false),
		table.WithSelectedFields("fixed_col")).ToArrowTable(ctx)
	s.Require().NoError(err)
	defer results.Release()

	s.EqualValues(1, results.NumRows())
	resultCol := results.Column(0).Data().Chunk(0).(*array.FixedSizeBinary)
	s.Equal([]byte("1234567890123456789012345"), resultCol.Value(0))

	results, err = tbl.Scan(table.WithRowFilter(
		iceberg.NewAnd(
			iceberg.NotEqualTo(iceberg.Reference("fixed_col"), "1234567890123456789012345"),
			iceberg.NotEqualTo(iceberg.Reference("uuid_col"), "c1b0d8e0-0b0e-4b1e-9b0a-0e0b0d0c0a0b"))),
		table.WithCaseSensitive(false), table.WithSelectedFields("fixed_col")).ToArrowTable(ctx)
	s.Require().NoError(err)
	defer results.Release()

	s.EqualValues(3, results.NumRows())
	resultCol = results.Column(0).Data().Chunk(0).(*array.FixedSizeBinary)
	s.Equal([]byte("1231231231231231231231231"), resultCol.Value(0))
	resultCol = results.Column(0).Data().Chunk(1).(*array.FixedSizeBinary)
	s.Equal([]byte("12345678901234567ass12345"), resultCol.Value(0))
	resultCol = results.Column(0).Data().Chunk(2).(*array.FixedSizeBinary)
	s.Equal([]byte("qweeqwwqq1231231231231111"), resultCol.Value(0))
}

func (s *ScannerSuite) TestScanTag() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	ident := catalog.ToIdentifier("default", "test_positional_mor_deletes")

	tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
	s.Require().NoError(err)

	ctx := compute.WithAllocator(s.ctx, mem)
	scan, err := tbl.Scan().UseRef("tag_12")
	s.Require().NoError(err)

	results, err := scan.ToArrowTable(ctx)
	defer results.Release()

	s.EqualValues(3, results.NumCols())
	s.Equal([]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		results.Column(1).Data().Chunk(0).(*array.Int32).Int32Values())
}

func (s *ScannerSuite) TestScanBranch() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	ident := catalog.ToIdentifier("default", "test_positional_mor_deletes")

	tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
	s.Require().NoError(err)

	ctx := compute.WithAllocator(s.ctx, mem)
	scan, err := tbl.Scan().UseRef("without_5")
	s.Require().NoError(err)

	results, err := scan.ToArrowTable(ctx)
	defer results.Release()

	s.EqualValues(3, results.NumCols())
	s.Equal([]int32{1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12},
		results.Column(1).Data().Chunk(0).(*array.Int32).Int32Values())
}

func (s *ScannerSuite) TestFilterOnNewColumn() {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(s.T(), 0)

	ident := catalog.ToIdentifier("default", "test_table_add_column")

	tbl, err := s.cat.LoadTable(s.ctx, ident, s.props)
	s.Require().NoError(err)

	ctx := compute.WithAllocator(s.ctx, mem)
	results, err := tbl.Scan(table.WithRowFilter(
		iceberg.EqualTo(iceberg.Reference("b"), "2"))).ToArrowTable(ctx)
	s.Require().NoError(err)
	defer results.Release()

	s.EqualValues(2, results.NumCols())
	s.EqualValues(1, results.NumRows())
	s.Equal("2", results.Column(1).Data().Chunk(0).(*array.String).Value(0))

	results, err = tbl.Scan(table.WithRowFilter(
		iceberg.NotNull(iceberg.Reference("b")))).ToArrowTable(ctx)
	s.Require().NoError(err)
	defer results.Release()

	s.EqualValues(2, results.NumCols())
	s.EqualValues(1, results.NumRows())
	s.Equal("2", results.Column(1).Data().Chunk(0).(*array.String).Value(0))

	results, err = tbl.Scan(table.WithRowFilter(
		iceberg.IsNull(iceberg.Reference("b")))).ToArrowTable(ctx)
	s.Require().NoError(err)
	defer results.Release()

	s.EqualValues(2, results.NumCols())
	s.EqualValues(1, results.NumRows())
	s.False(results.Column(1).Data().Chunk(0).(*array.String).IsValid(0))
}

func TestScanner(t *testing.T) {
	suite.Run(t, new(ScannerSuite))
}
