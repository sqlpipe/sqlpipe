FROM debian:12.1

RUN apt-get update

RUN apt-get install -y ca-certificates

ENV DELIMITER="{dlm}"
ENV NEWLINE="{nwln}"
ENV NULL="{nll}"

WORKDIR /

COPY ./bin/transferInstance /usr/local/bin/transferInstance
COPY ./LICENSE.MD /SQLPIPE-LICENSE.MD

ENTRYPOINT update-ca-certificates && /usr/local/bin/transferInstance \
    -instance-transfer-id="${INSTANCE_TRANSFER_ID}" \
    -naming-template="${NAMING_TEMPLATE}" \
    -source-instance-id="${SOURCE_INSTANCE_ID}" \
    -source-instance-type="${SOURCE_INSTANCE_TYPE}" \
    -source-instance-region="${SOURCE_INSTANCE_REGION}" \
    -source-instance-host="${SOURCE_INSTANCE_HOST}" \
    -source-instance-port="${SOURCE_INSTANCE_PORT}" \
    -source-instance-username="${SOURCE_INSTANCE_USERNAME}" \
    -restored-instance-id="${RESTORED_INSTANCE_ID}" \
    -target-type="${TARGET_TYPE}" \
    -target-host="${TARGET_HOST}" \
    -target-username="${TARGET_USERNAME}" \
    -target-password="${TARGET_PASSWORD}" \
    -cloud-username="${CLOUD_USERNAME}" \
    -cloud-password="${CLOUD_PASSWORD}" \
    -delimiter="${DELIMITER}" \
    -newline="${NEWLINE}" \
    -null="${NULL}"

