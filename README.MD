# SQLpipe

SQLpipe is a program that moves data between different database systems.

The free and open source version of SQLpipe can move the result of a query, or an entire table, with no usage restrictions. SQLpipe Pro adds the ability to perform incremental data loads.

SQLpipe also offers data engineering services! We specialize in database migration and data integration projects. Please check out the [SQLpipe website](https://sqlpipe.com) to learn more about us.

Last, please consider starring this repository, it really helps us!

## Supported DB types
SQLpipe supports moving data interchangably between the following DB types:
- PostgreSQL `postgresql`
- SQL Server `mssql`
- MySQL `mysql`
- Oracle `oracle`
- Snowflake `snowflake`

## Installation
SQLpipe runs on Linux, and there are three ways to install it. In order of ease, they are:
- Run official Docker image.
- Download official SQLpipe executable.
- Build SQLpipe from source.

### Docker image

Assuming you already have Docker installed, you can run SQLpipe like so:

```
docker run --publish 9000:9000 --name sqlpipe sqlpipe/sqlpipe
```

That's it! SQLpipe is now ready to start transferring data.

### Download executable
If you prefer to use SQLpipe without Docker, you can download the program itself on the [SQLpipe downloads page](https://sqlpipe.com/downloads).

Please note that SQLpipe runs on Linux and requires a few dependencies - these are discussed in the "Install dependencies" section below.

Once you have downloaded it, just grant the downloaded file executable permission:

```
chmod +x sqlpipe
```

And run it!

### Build from source
Building from source isn't hard, you just need Go and a few dependencies.

#### Install Go
Search the web for "Go installation instructions" - the first result should be the right one. It is very easy to install Go.

Be sure to follow all instructions and add Go to your path.

#### Install dependencies
SQLpipe relies on DB specific clients to insert into some databases. You will need to make sure they are installed and on your path if you plan on inserting into those databases.

Those dependencies are:

- psql to insert into PostgreSQL
- bcp to insert into SQL Server
- SQL*Loader  to insert into Oracle.
- MySQL inserts do not require any dependencies
- Snowflake inserts do not require any dependencies

Each of these clients should have readily accesible installation instructions, just search on the web or ask an LLM for help.

You do not need to install dependencies that you won't use - for example, if your only insertion target is Snowflake, you do not need to install any dependencies.

#### Build SQLpipe
Assuming you have Go installed (you can check by running `go version`), you just need to clone this repository and run:

```
go build -ldflags="-w -s" -o=./bin/sqlpipe ./cmd/sqlpipe
```

SQLpipe will be built and put in the `/bin` directory of this repo. You can then run SQLpipe by navigating to the directory containing the file and executing it.

```
cd bin
./sqlpipe

OR

sudo ./sqlpipe
```

## Using SQLpipe
SQLpipe exposes its functionality through a JSON API. It has the following routes:

-  `POST /transfers/create` - Creates a transfer
-  `GET /transfers/show/:id` - Shows an individual transfer
-  `GET /transfers/list` - Lists transfers
-  `PATCH /transfers/cancel/:id` - Cancels a transfer
-  `GET /healthcheck` - A healtcheck
-  `GET /debug/vars` - Shows system statistics

### Creating a transfer

To transfer data, you must submit a POST request with a JSON payload. We will discuss the required fields and their meanings further on in the docs, but here is a rough outline:

```
{
  "source-name": "<any name you want>",
  "source-type": "<one of SQLpipe's supported types>",
  "source-connection-string": "<connection string>",
  "source-schema": "<schema name>",
  "source-table": "<table name>",
  "target-name": "<any name you want>",
  "target-type": "<one of SQLpipe's supported types>",
  "target-connection-string": "<connection string>",
  "target-schema": "<schema name>",
  "target-table": "<table name>",
  "drop-target-table-if-exists": <true or false>,
  "create-target-table-if-not-exists": <true or false>,
  ... additional required fields, as needed by specific system types
}

```

Here are a few example curl commands to create SQLpipe transfers from various systems:

```

**********************************
** FULL TABLE TRANSFER EXAMPLES **
**********************************

-- mssql to snowflake full table transfer. drop and recreate target table in snowflake.
-- note on mssql sources: to supply additional arguments to a target connection string, you must put the in the target-hostname field
curl -d '{"source-name": "<any name you want>", "source-type": "mssql", "source-connection-string": "Server=<hostname>,<port>;Database=<db name>;User Id=<username>;Password=<password>;", "source-schema": "<schema name... such as dbo>", "source-table": "<table name>", "target-name": "<any name you want>", "target-type": "snowflake", "target-connection-string": "<snowflake username>:<snowflake password>@<account identifier>.snowflakecomputing.com/<db name>", "target-schema": "<schema name... such as public>", "target-table": "<table name>", "drop-target-table-if-exists": true, "create-target-table-if-not-exists": true, "create-target-schema-if-not-exists": true, "target-database": "<db name>"}' localhost:9000/transfers/create

-- oracle to postgresql full table transfer. drop and recreate.
curl -d '{"source-type": "oracle", "source-connection-string": "oracle://<username>:<password>@<hostname>:<port>/<db_name>","target-name":"<any name you like>", "target-type": "postgresql", "target-connection-string": "postgresql://<username>:<password>@<hostname>:<port>/<db_name>", "source-schema": "<schema name>", "source-table": "<table to move>", "drop-target-table-if-exists": true, "create-target-schema-if-not-exists": true, "create-target-table-if-not-exists": true, "target-schema": "<schema_name>", "target-table": "<new_table_name>"}' localhost:9000/transfers/create

*********************
** QUERY TRANSFERS **
*********************

-- postgresql to mssql query transfer
curl -d '{"source-name": "<any name you want>", "source-type": "postgresql", "source-connection-string": "postgresql://<username>:<password>@<hostname>:<port>/<db name>", "query": "<query to move>", "target-name": "<any name you want>", "target-type": "mssql", "target-connection-string": "Server=<hostname>,<port>;Database=<db name>;User Id=<username>;Password=<password>;", "target-schema": "<schema name... such as dbo>", "target-table": "<table name>", "drop-target-table-if-exists": true, "create-target-schema-if-not-exists": true, "create-target-table-if-not-exists": true, "target-hostname": "<hostname>;<additional flags, such as TrustServerCertificate=yes>", "target-database": "<db name>", "target-username": "<username>", "target-password": "<password>"}' localhost:9000/transfers/create

-- mysql to oracle query transfer. do not drop target table, and do not attempt to create it.
-- note on mysql sources: you must supply a "parseTime" and url-encoded "loc" query parameter in the source connection string
curl -d '{"source-name": "<any name you want>", "source-type": "mysql", "source-connection-string": "<username>:<password@tcp(<hostname>:<port>)/<db name>?parseTime=true&loc=US%2FPacific", "query": "<query to move>", "target-name": "<any name you want>", "target-type": "oracle", "target-connection-string": "oracle://<username>:<password>@<hostname>:<port>/<db name>?dba privilege=sysdba", "target-schema": "<schema name>", "target-table": "<table name>", "drop-target-table-if-exists": true, "create-target-table-if-not-exists": true, "target-hostname": "<hostname>", "target-port": <port number>, "target-database": "<db name>", "target-username": "<username>", "target-password": "<password>"}' localhost:9000/transfers/create

-- snowflake to mysql query transfer. do not drop, but do create target table if it doens't exist.
curl -d '{"source-name": "<any name you want>", "source-type": "snowflake", "source-connection-string": "<snowflake username>:<snowflake password>@<account identifier>.snowflakecomputing.com/<db name>", "query": "<query to move>", "target-name": "<any name you want>", "target-type": "mysql", "target-connection-string": "<username>:<password>@tcp(<hostname>:<port number>)/<db name>?parseTime=true&loc=US%2FPacific", "target-table": "<table name>", "drop-target-table-if-exists": true, "create-target-table-if-not-exists": true}' localhost:9000/transfers/create
```

As you can see from the examples, transfers are triggered by sending a POST request to the `/transfers/create` route with a specific JSON payload. The fields of that payload are listed and discussed below.

#### Fields required for all transfers
```
source-name
source-type
source-connection-string
target-name
target-type
target-connection-string
query OR source-table
target-table
```

#### Schemas
When your target is of type `postgresql`, `mssql`, `oracle`, or `snowflake`, you must provide the following field:
```
target-schema
```

#### Extra required fields for target type SQL Server
```
target-hostname
target-username
target-password
target-database
```

#### Extra required fields for target type Oracle
```
target-hostname
target-port
target-username
target-password
target-database
```

#### A note on using MySQL as a source:
You must supply `parseTime=true` and a url-encoded IANA time zone name to the `loc`` query parameter in the source connection string when using MySQL as a source. If your database was in the US/Pacific IANA time zone, for example, your source connection string might look like this:
```
my_username:my_password@tcp(my_hostname.com:3306)/my_db_name?parseTime=true&loc=US%2FPacific
```
You can see [IANA time zone names on Wikipedia](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) and can learn about [URL encoding on W3 Schools](https://www.w3schools.com/html/html_urlencode.asp).

#### Optional fields
The following are optional on all transfers:
```
source-name
target-name
drop-target-table-if-exists
create-target-table-if-not-exists
create-target-schema-if-not-exists
delimiter
newline
null
keep-files
```

#### Field definitions
- `source-name`: A name for your source system, this will show up in the logs.
- `source-type`: Must be one of SQLpipe's supported source types.
  - `postgresql`
  - `mssql`
  - `mysql`
  - `oracle`
  - `snowflake`
- `source-connection-string`: A connection string to connect to the DB. Some systems require you to URL encode special characters.
- `target-name`
- `target-type`
- `target-connection-string`
- `target-schema`: For systems that support schemas, you must specify which schema to put the data in.
- `target-table`: The table that SQLpipe will move the data to.
- `target-hostname`: For some systems, in addition to specifying a connection string, you must also specify the hostname.
- `target-port`: For some systems, in addition to specifying a connection string, you must also specify the port.
- `target-username`: For some systems, in addition to specifying a connection string, you must also specify the username.
- `target-password`: For some systems, in addition to specifying a connection string, you must also specify the password.
- `target-database`:  For some systems, in addition to specifying a connection string, you must also specify the database name.
- `query`: The query which you want to move. Do not enter a query if you provide a `source-schema` and `source-table`.
- `source-schema`: For systems that support schemas, you must specify which schema to put the data in.
- `source-table`: The table that SQLpipe will move the data to.
- `drop-target-table-if-exists`: Drops the target table (if it exists) before moving the data.
- `create-target-table-if-not-exists`: Creates the target table before moving the data. SQLpipe intelligently maps from one DB's types to another.
- `create-target-schema-if-not-exists`: Creates the target schema before moving the data. Only works in databases where schemas are supported. You will need sysdba privs to do this in Oracle.
- `delimiter`: Some DB clients do not support [RFC 4180 CSVs](https://datatracker.ietf.org/doc/html/rfc4180) (shame on them!). This optional flag lets you set a custom multi-character delimiter - you should pick one that will not appear on your data. The default is `{dlm}`.
- `newline`: Some DB clients do not support [RFC 4180 CSVs](https://datatracker.ietf.org/doc/html/rfc4180). This optional flag lets you set a custom multi-character newline. The default is `{nwln}`.
- `null`: Some DB clients do not support [RFC 4180 CSVs](https://datatracker.ietf.org/doc/html/rfc4180). This optional flag lets you set a custom multi-character null value. The default is `{nll}`
- `keep-files`: SQLpipe uses your OS's default temp directory to create working directories for each transfer. It deletes these files after the transfer is done unless you mark this flag as `true`. This can be helpful for troubleshooting or therapeutically watching your data move in real time.

#### Create transfer response
If you send a valid transfer creation request, you'll get a response that looks like this:

```
{
        "transfer": {
                "id": "3e2b2e20-d35e-4ce2-ba06-6ba55aadcf5e",
                "created-at": "2023-11-08T09:34:24.750550334Z",
                "status": "queued",
                "keep-files": false,
                "tmp-dir": "/tmp/sqlpipe/3e2b2e20-d35e-4ce2-ba06-6ba55aadcf5e",
                "pipe-file-dir": "/tmp/sqlpipe/3e2b2e20-d35e-4ce2-ba06-6ba55aadcf5e/pipe-files",
                "final-csv-dir": "/tmp/sqlpipe/3e2b2e20-d35e-4ce2-ba06-6ba55aadcf5e/final-csv",
                "source-connection-info": {
                        "instance-name": "postgresql",
                        "type": "postgresql",
                        "hostname": "",
                        "database": "",
                        "username": ""
                },
                "target-connection-info": {
                        "instance-name": "mssql",
                        "type": "mssql",
                        "hostname": "mssql;TrustServerCertificate=yes",
                        "database": "mydb",
                        "username": "sa"
                },
                "drop-target-table-if-exists": false,
                "create-target-table-if-not-exists": true,
                "target-schema": "dbo",
                "target-name": "postgresql_my_table",
                "query": "select * from my_table",
                "delimiter": "{dlm}",
                "newline": "{nwln}",
                "null": "{nll}"
        }
}
```

If you send an invalid request, SQLpipe will do its best to tell you what's wrong. For example, if you were trying to transfer from PostgreSQL to Oracle and forgot to specify a target table and Oracle's target port, the response would look like this:

```
{
        "error": {
                "target-port": "must be provided for target type oracle",
                "target-table": "must be provided"
        }
}
```

### Viewing a transfer's status
You can check the status of a transfer by sending a request to the `/transfers/show/:id` route. Here is an example curl command:

```
curl localhost:9000/transfers/show/0510f644-4970-4815-87fc-1cf9c680d491
```

That command will return a similar response to the transfer creation command, but it will tell you the current status of the transfer and report any errors.

For example, let's try to run a transfer from PostgreSQL to SQL Server with a query of `select * from my_non_existant_table`. As you might guess, that table doesn't exist in PostgreSQL, so the transfer will fail.

I created the transfer using the `/transfer/create` route, and then used the `/transfers/show/:id` to view the transfer:

```
{
        "transfer": {
                "id": "8a81fa19-c7e1-424b-93e2-3ebeeb6ecdbd",
                "created-at": "2023-11-08T09:36:11.551315925Z",
                "status": "error",
                "error": "error running transfer 8a81fa19-c7e1-424b-93e2-3ebeeb6ecdbd :: error querying source :: error running dql on postgresql :: select * from my_nonexistant_table :: ERROR: relation \"my_nonexistant_table\" does not exist (SQLSTATE 42P01)",
                "keep-files": false,
                "tmp-dir": "/tmp/sqlpipe/8a81fa19-c7e1-424b-93e2-3ebeeb6ecdbd",
                "pipe-file-dir": "/tmp/sqlpipe/8a81fa19-c7e1-424b-93e2-3ebeeb6ecdbd/pipe-files",
                "final-csv-dir": "/tmp/sqlpipe/8a81fa19-c7e1-424b-93e2-3ebeeb6ecdbd/final-csv",
                "source-connection-info": {
                        "instance-name": "postgresql",
                        "type": "postgresql",
                        "hostname": "",
                        "database": "",
                        "username": ""
                },
                "target-connection-info": {
                        "instance-name": "mssql",
                        "type": "mssql",
                        "hostname": "mssql;TrustServerCertificate=yes",
                        "database": "mydb",
                        "username": "sa"
                },
                "drop-target-table-if-exists": false,
                "create-target-table-if-not-exists": true,
                "target-schema": "dbo",
                "target-name": "postgresql_my_table",
                "query": "select * from my_nonexistant_table",
                "delimiter": "{dlm}",
                "newline": "{nwln}",
                "null": "{nll}"
        }
}
```

Great! We have all the information we would need to troubleshoot.

### Viewing multiple transfers
You can view multiple transfers using the `/transfers/list` route. It will return responses similar to the ones shown above.

You can also filter for transfers of a certain status like so:

```
curl localhost:9000/transfers/list?status=queued
curl localhost:9000/transfers/list?status=running
curl localhost:9000/transfers/list?status=complete
curl localhost:9000/transfers/list?status=cancelled
curl localhost:9000/transfers/list?status=error
```

### Cancelling a transfer
You can cancel a running transfer by sending a PATCH request to the `/transfers/cancel/:id` route - here is an example:

```
curl -X PATCH localhost:9000/transfers/cancel/0a896d4c-edfc-4f60-bff5-2d03581707c3
```

It may take a few seconds for the cancel command to propogate through the system - there are multiple concurrent processes that need to be stopped. Also, it may print an error saying that the context was cancelled - don't worry about that! That's just the program cancelling a query.
