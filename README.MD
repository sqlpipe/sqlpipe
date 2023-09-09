# SQLpipe

SQLpipe is a free, open-source program that can move the result of SQL queries between different RDBMS systems.

SQLpipe is also a company! We offer database migration and data integration services as well as paid data engineering products. Please check out the [SQLpipe website](https://sqlpipe.com) if you'd like to learn more about us.

## Supported DB types
SQLpipe supports moving data interchangably between the following DB types:
- PostgreSQL `postgresql`
- SQL Server `mssql`
- MySQL `mysql`
- Oracle `oracle`
- Snowflake `snowflake`

## Installation
There are three ways to install SQLpipe. In order of ease, they are:
- Run official Docker image.
- Download official SQLpipe executable.
- Build SQLpipe from source.

### Docker image
Our official Docker image comes pre-installed with the SQLpipe executable and all needed drivers.

Github stars really help us - we require one to download the image. Visit the [SQLpipe downloads page](https://downloads.sqlpipe.com) to verify your star, then run the following command:

```
docker run --publish 9000:9000 --name sqlpipe sqlpipe/sqlpipe:latest
```

That's it! SQLpipe is now ready to start transferring data.

### Download executable
If you prefer to use SQLpipe without Docker, you can download the program itself on the [SQLpipe downloads page](https://downloads.sqlpipe.com).

Please note that SQLpipe requires a few dependencies - these are discussed in the "Install dependencies" section below.

Once you have downloaded it, just grant it executable permissions and run it!

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

You do not need to install dependencies that you won't use - for example, if your only insertion target is Snowflake, you do not need to install anything.

#### Build SQLpipe
Assuming you have Go installed (you can check by running `go version`), you just need to clone this repository and run:

```
go build -ldflags="-w -s" -o=./bin/sqlpipe ./cmd/sqlpipe
```

SQLpipe will be built and put in the `/bin` directory of this repo. You can then run SQLpipe by navigating to the directory containing the file and executing it.

```
cd bin
./sqlpipe
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
Before we get into the details, here are a few example curl commands to create SQLpipe transfers:

```
-- example transfer from mssql to snowflake
curl -d '{"source-type": "mssql", "source-connection-string": "Server=<hostname>,<port>;Database=<db_name>;User Id=<username>;Password=<password>;", "target-type": "snowflake", "target-connection-string": "<username>:<password>@<snowflake_account_identifier>/<db_name>", "query": "select * from my_table", "target-table": "<new_table_name>", "drop-target-table-if-exists": true, "create-target-table": true, "target-schema": "<schema_name>"}' localhost:9000/transfers/create

-- example transfer from postgresql to mssql
curl -d '{"source-type": "postgresql", "source-connection-string": "postgresql://<username>:<password>@<hostname>:<port>/<db_name>", "target-type": "mssql", "target-connection-string": "Server=<hostname>,<port>;Database=<db_name>;User Id=<username>;Password=<password>;", "query": "select * from my_table", "target-table": "<new_table_name>", "drop-target-table-if-exists": true, "create-target-table": true, "target-schema": "<schema_name>", "target-database": "<db_name>", "target-password": "<password>", "target-hostname": "<hostname>;<additional args, such as TrustServerCertificate=yes>", "target-username": "<username>"}' localhost:9000/transfers/create

-- example transfer from mysql to oracle
-- note on mysql sources: you must supply a "parseTime" and url-encoded "loc" query parameter in the source connection string
curl -d '{"source-type": "mysql", "source-connection-string": "<username>:<password@tcp(<hostname>:<port>)/<db_name>?parseTime=true&loc=<url_encoded_iana_time_zone>", "target-type": "oracle", "target-connection-string": "oracle://<username>:<password>@<hostname>:<port>/<db_name>", "query": "select * from my_table", "target-table": "<new_table_name>", "drop-target-table-if-exists": true, "create-target-table": true, "target-schema": "<schema_name>", "target-hostname": "<hostname>", "target-username": "<username>", "target-password": "<password>", "target-database": "<db_name>", "target-port": <port>}' localhost:9000/transfers/create

-- example transfer from oracle to postgresql
-- note on mssql sources: to supply additional arguments to a target connection string, you must put the in the target-hostname field
curl -d '{"source-type": "oracle", "source-connection-string": "oracle://<username>:<password>@<hostname>:<port>/<db_name>", "target-type": "postgresql", "target-connection-string": "postgresql://<username>:<password>@<hostname>:<port>/<db_name>", "query": "select * from my_table", "target-table": "<new_table_name", "drop-target-table-if-exists": true, "create-target-table": true, "target-schema": "<schema_name>"}' localhost:9000/transfers/create

-- example transfer from snowflake to mysql
curl -d '{"source-type": "snowflake", "source-connection-string": "<username:<password>@<snowflake_account_identifier>/<db_name>", "target-type": "mysql", "target-connection-string": "<username>:<password@tcp(<hostname>:<port>)/<db_name>", "query": "select * from my_table", "target-table": "<new_table_name>", "drop-target-table-if-exists": true, "create-target-table": true}' localhost:9000/transfers/create
```

As you can see from the examples, transfers are triggered by sending a POST request to the `/transfers/create` route with a specific JSON payload. The fields of that payload are listed and discussed below.

#### Fields required for all transfers
```
source-type
source-connection-string
target-type
target-connection-string
query
target-table
```

#### Extra required fields for target type PostgreSQL
```
target-schema
```

#### Extra required fields for target type SQL Server
```
target-schema
target-hostname
target-username
target-password
target-database
```

#### Extra required fields for target type Oracle
```
target-schema
target-hostname
target-port
target-username
target-password
target-database
```

#### Extra required fields for target type Snowflake
```
target-schema
```

#### A note on using MySQL as a source:
You must supply "parseTime=true" and a url-encoded IANA time zone name to the "loc" query parameter in the source connection string when using MySQL as a source. If your database was in the US/Pacific IANA time zone, for example:
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
create-target-table
delimiter
new-line
null
keep-files
```

#### Field definitions
- `source-name`: An optional name for your source system, this will show up in the logs.
- `source-type`: Must be one of SQLpipe's supported source types (listed at top of readme).
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
- `query`: The query which you want to move.
- `drop-target-table-if-exists`: Drops the target table (if it exists) before moving the data.
- `create-target-table`: Creates the target table before moving the data. SQLpipe intelligently maps from one DB's types to another.
- `delimiter`: Some DB clients do not support [RFC 4180 CSVs](https://datatracker.ietf.org/doc/html/rfc4180) (shame on them!). This optional flag lets you set a custom multi-character delimiter - you should pick one that will not appear on your data. The default is `{dlm}`.
- `new-line`: Some DB clients do not support [RFC 4180 CSVs](https://datatracker.ietf.org/doc/html/rfc4180). This optional flag lets you set a custom multi-character newline. The default is `{nwln}`.
- `null`: Some DB clients do not support [RFC 4180 CSVs](https://datatracker.ietf.org/doc/html/rfc4180). This optional flag lets you set a custom multi-character null value. The default is `{nll}`
- `keep-files`: SQLpipe uses your OS's default temp directory to create working directories for each transfer. It deletes these files after the transfer is done unless you mark this flag as `true`. This can be helpful for troubleshooting or therapeutically watching your data move in real time.

#### Create transfer response
If you send a valid transfer creation request, you'll get a response that looks like this:

```
{
        "transfer": {
                "id": "a984a2d7-0d88-406e-98f6-8f93b157ae86",
                "created-at": "2023-09-09T10:15:37.066911129Z",
                "status": "queued",
                "tmp-dir": "/tmp/sqlpipe/a984a2d7-0d88-406e-98f6-8f93b157ae86",
                "pipe-file-dir": "/tmp/sqlpipe/a984a2d7-0d88-406e-98f6-8f93b157ae86/pipe-files",
                "final-csv-dir": "/tmp/sqlpipe/a984a2d7-0d88-406e-98f6-8f93b157ae86/final-csvs",
                "keep-files": false,
                "delimiter": "{dlm}",
                "new-line": "{nwln}",
                "null": "{nll}",
                "source-name": "postgresql",
                "source-type": "postgresql",
                "target-name": "mssql",
                "target-type": "mssql",
                "target-hostname": "mssql;TrustServerCertificate=yes",
                "target-username": "sa",
                "target-database": "mydb",
                "query": "select * from my_table",
                "target-schema": "dbo",
                "target-table": "postgresql_my_table",
                "drop-target-table-if-exists": true,
                "create-target-table": true
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
                "id": "51591431-d414-4dac-9cdc-e1c078883666",
                "created-at": "2023-09-09T10:16:31.160125855Z",
                "stopped-at": "2023-09-09 10:16:31.200742341 +0000 UTC m=+121.977763641",
                "status": "error",
                "error": "error querying source :: error running dql on postgresql :: select * from my_non_existant_table :: ERROR: relation \"my_non_existant_table\" does not exist (SQLSTATE 42P01)",
                "tmp-dir": "/tmp/sqlpipe/51591431-d414-4dac-9cdc-e1c078883666",
                "pipe-file-dir": "/tmp/sqlpipe/51591431-d414-4dac-9cdc-e1c078883666/pipe-files",
                "final-csv-dir": "/tmp/sqlpipe/51591431-d414-4dac-9cdc-e1c078883666/final-csvs",
                "keep-files": false,
                "delimiter": "{dlm}",
                "new-line": "{nwln}",
                "null": "{nll}",
                "source-name": "postgresql",
                "source-type": "postgresql",
                "target-name": "mssql",
                "target-type": "mssql",
                "target-hostname": "my_hostname",
                "target-username": "my_username",
                "target-database": "my_db",
                "query": "select * from my_non_existant_table",
                "target-schema": "my_schema",
                "target-table": "my_new_table",
                "drop-target-table-if-exists": true,
                "create-target-table": true
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

It may take a few seconds for the cancel command to propogate through the system - there are multiple concurrent processes that need to be stopped.

