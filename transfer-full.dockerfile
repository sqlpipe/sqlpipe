FROM debian:12.1

# install basic dependencies and psql
RUN apt-get update
RUN apt-get install -y curl postgresql-client libaio1 unixodbc unixodbc-dev unzip

# install bcp
RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
RUN curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools18
ENV PATH="${PATH}:/opt/mssql-tools18/bin"

# install sql*loader
RUN mkdir /opt/oracle
WORKDIR /home
RUN curl -O https://download.oracle.com/otn_software/linux/instantclient/2111000/instantclient-basic-linux.x64-21.11.0.0.0dbru.zip
RUN curl -O https://download.oracle.com/otn_software/linux/instantclient/2111000/instantclient-tools-linux.x64-21.11.0.0.0dbru.zip
RUN unzip instantclient-basic-linux.x64-21.11.0.0.0dbru.zip -d /opt/oracle
RUN unzip instantclient-tools-linux.x64-21.11.0.0.0dbru.zip -d /opt/oracle
ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:/opt/oracle/instantclient_21_11"
ENV PATH="${PATH}:/opt/oracle/instantclient_21_11"


WORKDIR /

COPY ./bin/transfer /usr/local/bin/transfer
COPY ./LICENSE.MD /SQLPIPE-LICENSE.MD
ENTRYPOINT /usr/local/bin/transfer \
    --source-name "${SOURCE_NAME}" \
    --source-type "${SOURCE_TYPE}" \
    --source-connection-string "${SOURCE_CONNECTION_STRING}" \
    --target-name "${TARGET_NAME}" \
    --target-type "${TARGET_TYPE}" \
    --target-connection-string "${TARGET_CONNECTION_STRING}" \
    --drop-target-table-if-exists "${DROP_TARGET_TABLE_IF_EXISTS}" \
    --create-target-schema-if-not-exists "${CREATE_TARGET_SCHEMA_IF_NOT_EXISTS}" \
    --create-target-table-if-not-exists "${CREATE_TARGET_TABLE_IF_NOT_EXISTS}" \
    --target-schema "${TARGET_SCHEMA}" \
    --target-table "${TARGET_TABLE}" \
    --source-schema "${SOURCE_SCHEMA}" \
    --source-table "${SOURCE_TABLE}"