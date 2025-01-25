FROM debian:12-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt-get install --no-install-recommends -y ca-certificates python3 pipx git build-essential python3-dev
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/sqlpipe/presidio

WORKDIR /presidio/presidio-structured
RUN pipx install poetry
RUN /root/.local/bin/poetry install --all-extras

COPY ./bin/transferInstance /usr/local/bin/transferInstance
COPY ./LICENSE.MD /SQLPIPE-LICENSE.MD

ENV DELIMITER="{dlm}"
ENV NEWLINE="{nwln}"
ENV NULL="{nll}"

CMD ["tail", "-f", "/dev/null"]

# ENTRYPOINT update-ca-certificates && /usr/local/bin/transferInstance \
#     -instance-transfer-id="${INSTANCE_TRANSFER_ID}" \
#     -database-naming-convention="${DATABASE_NAMING_CONVENTION}" \
#     -schema-naming-convention="${SCHEMA_NAMING_CONVENTION}" \
#     -schema-fallback="${SCHEMA_FALLBACK}" \
#     -table-naming-convention="${TABLE_NAMING_CONVENTION}" \
#     -source-instance-cloud-provider="${SOURCE_INSTANCE_CLOUD_PROVIDER}" \
#     -source-instance-cloud-account-id="${SOURCE_INSTANCE_CLOUD_ACCOUNT_ID}" \
#     -source-instance-id="${SOURCE_INSTANCE_ID}" \
#     -source-instance-type="${SOURCE_INSTANCE_TYPE}" \
#     -source-instance-region="${SOURCE_INSTANCE_REGION}" \
#     -source-instance-host="${SOURCE_INSTANCE_HOST}" \
#     -source-instance-port="${SOURCE_INSTANCE_PORT}" \
#     -source-instance-username="${SOURCE_INSTANCE_USERNAME}" \
#     -restored-instance-id="${RESTORED_INSTANCE_ID}" \
#     -target-type="${TARGET_TYPE}" \
#     -target-host="${TARGET_HOST}" \
#     -target-username="${TARGET_USERNAME}" \
#     -target-password="${TARGET_PASSWORD}" \
#     -cloud-username="${CLOUD_USERNAME}" \
#     -cloud-password="${CLOUD_PASSWORD}" \
#     -delimiter="${DELIMITER}" \
#     -newline="${NEWLINE}" \
#     -null="${NULL}"

