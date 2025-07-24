# xixipi-lining/iceberg-go

> ⚠️ This repository is a fork of [apache/iceberg-go](https://github.com/apache/iceberg-go), retaining the Apache 2.0 License.
>
> This fork **restores and enhances the DynamoDB Catalog implementation**, which has been marked as deprecated in the upstream repository but is actively maintained and available here.
>
> In addition, this repository contains custom features and changes that are not intended to be merged upstream and are for personal or project-specific use only.
>
> The repository complies with the Apache 2.0 License, preserving the original LICENSE and NOTICE files. For details on changes, see this README and the NOTICE file.

# Iceberg Golang

[![Go Reference](https://pkg.go.dev/badge/github.com/apache/iceberg-go.svg)](https://pkg.go.dev/github.com/apache/iceberg-go)

`iceberg` is a Golang implementation of the [Iceberg table spec](https://iceberg.apache.org/spec/).

## Build From Source

### Prerequisites

- Go 1.23 or later

### Build

```shell
$ git clone https://github.com/apache/iceberg-go.git
$ cd iceberg-go/cmd/iceberg && go build .
```

## Feature Support / Roadmap

### FileSystem Support

|   Filesystem Type    | Supported |
| :------------------: | :-------: |
|          S3          |     X     |
| Google Cloud Storage |     X     |
|  Azure Blob Storage  |     X     |
|   Local Filesystem   |     X     |

### Metadata

| Operation              | Supported |
| :--------------------- | :-------: |
| Get Schema             |     X     |
| Get Snapshots          |     X     |
| Get Sort Orders        |     X     |
| Get Partition Specs    |     X     |
| Get Manifests          |     X     |
| Create New Manifests   |     X     |
| Plan Scan              |     x     |
| Plan Scan for Snapshot |     x     |

### Catalog Support

| Operation                   | REST | Hive | Glue | SQL | Dynamodb |
| :-------------------------- | :--: | :--: | :--: | :-: | :------: |
| Load Table                  |  X   |      |  X   |  X  |    X     |
| List Tables                 |  X   |      |  X   |  X  |    X     |
| Create Table                |  X   |      |  X   |  X  |    X     |
| Register Table              |  X   |      |  X   |     |    X     |
| Update Current Snapshot     |  X   |      |  X   |  X  |    X     |
| Create New Snapshot         |  X   |      |  X   |  X  |    X     |
| Rename Table                |  X   |      |  X   |  X  |    X     |
| Drop Table                  |  X   |      |  X   |  X  |    X     |
| Alter Table                 |  X   |      |  X   |  X  |    X     |
| Check Table Exists          |  X   |      |  X   |  X  |    X     |
| Set Table Properties        |  X   |      |  X   |  X  |    X     |
| List Namespaces             |  X   |      |  X   |  X  |    X     |
| Create Namespace            |  X   |      |  X   |  X  |    X     |
| Check Namespace Exists      |  X   |      |  X   |  X  |    X     |
| Drop Namespace              |  X   |      |  X   |  X  |    X     |
| Update Namespace Properties |  X   |      |  X   |  X  |    X     |
| Create View                 |  X   |      |      |  X  |          |
| Load View                   |      |      |      |  X  |          |
| List View                   |  X   |      |      |  X  |          |
| Drop View                   |  X   |      |      |  X  |          |
| Check View Exists           |  X   |      |      |  X  |          |

> **Note:**  
> In the upstream [apache/iceberg-go](https://github.com/apache/iceberg-go) repository, the DynamoDB Catalog has been marked as deprecated and is no longer maintained.  
> This fork restores and enhances the DynamoDB Catalog, making it available and maintained for users who need this feature.

### Read/Write Data Support

- Data can currently be read as an Arrow Table or as a stream of Arrow record batches.

#### Supported Write Operations

As long as the FileSystem is supported and the Catalog supports altering
the table, the following tracks the current write support:

|     Operation     | Supported |
| :---------------: | :-------: |
|   Append Stream   |     X     |
| Append Data Files |     X     |
|   Rewrite Files   |           |
| Rewrite manifests |           |
|  Overwrite Files  |           |
| Write Pos Delete  |           |
|  Write Eq Delete  |           |
|     Row Delta     |           |

### CLI Usage

Run `go build ./cmd/iceberg` from the root of this repository to build the CLI executable, alternately you can run `go install github.com/apache/iceberg-go/cmd/iceberg` to install it to the `bin` directory of your `GOPATH`.

The `iceberg` CLI usage is very similar to [pyiceberg CLI](https://py.iceberg.apache.org/cli/) \
You can pass the catalog URI with `--uri` argument.

Example:
You can start the Iceberg REST API docker image which runs on default in port `8181`

```
docker pull apache/iceberg-rest-fixture:latest
docker run -p 8181:8181 apache/iceberg-rest-fixture:latest
```

and run the `iceberg` CLI pointing to the REST API server.

```
 ./iceberg --uri http://0.0.0.0:8181 list
┌─────┐
| IDs |
| --- |
└─────┘
```

**Create Namespace**

```
./iceberg --uri http://0.0.0.0:8181 create namespace taxitrips
```

**List Namespace**

```
 ./iceberg --uri http://0.0.0.0:8181 list
┌───────────┐
| IDs       |
| --------- |
| taxitrips |
└───────────┘


```

# Get in Touch

- [Iceberg community](https://iceberg.apache.org/community/)
