FROM debian:12.1

RUN apt-get update

RUN apt-get install -y ca-certificates

RUN update-ca-certificates

WORKDIR /

COPY ./bin/transfer /usr/local/bin/transfer
COPY ./LICENSE.MD /SQLPIPE-LICENSE.MD
ENTRYPOINT /usr/local/bin/transfer \
    -target-schema="${TARGET_SCHEMA}" \
    -target-table="${TARGET_TABLE}" \
    -source-schema="${SOURCE_SCHEMA}" \
    -source-table="${SOURCE_TABLE}" \
    -source-name="${SOURCE_NAME}" \
    -source-type="${SOURCE_TYPE}" \
    -source-connection-string="${SOURCE_CONNECTION_STRING}" \
    -target-type="${TARGET_TYPE}" \
    -target-connection-string="${TARGET_CONNECTION_STRING}" \
    -create-target-schema-if-not-exists="${CREATE_TARGET_SCHEMA_IF_NOT_EXISTS}" \
    -create-target-table-if-not-exists="${CREATE_TARGET_TABLE_IF_NOT_EXISTS}" \
    -drop-target-table-if-exists="${DROP_TARGET_TABLE_IF_EXISTS}" \
    -target-name="${TARGET_NAME}"