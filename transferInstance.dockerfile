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
    -source-name="${SOURCE_NAME}" \
    -source-type="${SOURCE_TYPE}" \
    -source-hostname="${SOURCE_HOSTNAME}" \
    -source-port="${SOURCE_PORT}" \
    -source-username="${SOURCE_USERNAME}" \
    -source-password="${SOURCE_PASSWORD}" \
    -target-name="${TARGET_NAME}" \
    -target-type="${TARGET_TYPE}" \
    -target-hostname="${TARGET_HOSTNAME}" \
    -target-username="${TARGET_USERNAME}" \
    -target-password="${TARGET_PASSWORD}" \
    -delimiter="${DELIMITER}" \
    -newline="${NEWLINE}" \
    -null="${NULL}" \
    -account-id="${ACCOUNT_ID}" \
    -region="${REGION}" \
    -account-username="${ACCOUNT_USERNAME}" \
    -account-password="${ACCOUNT_PASSWORD}" \
    -backup-id="${BACKUP_ID}"
