FROM debian:12-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update
RUN apt-get install --no-install-recommends -y ca-certificates python3.11 pipx curl tar
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
RUN pipx install poetry

RUN curl -L -O https://github.com/sqlpipe/presidio/archive/refs/heads/main.tar.gz
RUN tar -xvf main.tar.gz
RUN mv presidio-main presidio
WORKDIR /presidio/presidio-structured
RUN /root/.local/bin/poetry install --all-extras
RUN /root/.local/bin/poetry run python -m spacy download en_core_web_lg
RUN echo "export PATH=$(/root/.local/bin/poetry env info --path)/bin:$PATH" >> /root/.bashrc
RUN echo -n "$(/root/.local/bin/poetry env info --path)/bin/python" >> /python_location.txt

WORKDIR /
COPY ./LICENSE.MD /SQLPIPE-LICENSE.MD
COPY ./pii_scan.py /pii_scan.py
COPY ./bin/transferInstance /usr/local/bin/transferInstance
ENV DELIMITER="{dlm}"
ENV NEWLINE="{nwln}"
ENV NULL="{nll}"


# CMD ["/bin/bash"]

ENTRYPOINT update-ca-certificates && /usr/local/bin/transferInstance \
    -instance-transfer-id="${INSTANCE_TRANSFER_ID}" \
    -database-naming-convention="${DATABASE_NAMING_CONVENTION}" \
    -schema-naming-convention="${SCHEMA_NAMING_CONVENTION}" \
    -schema-fallback="${SCHEMA_FALLBACK}" \
    -table-naming-convention="${TABLE_NAMING_CONVENTION}" \
    -source-instance-cloud-provider="${SOURCE_INSTANCE_CLOUD_PROVIDER}" \
    -source-instance-cloud-account-id="${SOURCE_INSTANCE_CLOUD_ACCOUNT_ID}" \
    -source-instance-id="${SOURCE_INSTANCE_ID}" \
    -source-instance-type="${SOURCE_INSTANCE_TYPE}" \
    -source-instance-region="${SOURCE_INSTANCE_REGION}" \
    -source-instance-host="${SOURCE_INSTANCE_HOST}" \
    -source-instance-port="${SOURCE_INSTANCE_PORT}" \
    -source-instance-username="${SOURCE_INSTANCE_USERNAME}" \
    -source-instance-password="${SOURCE_INSTANCE_PASSWORD}" \
    -restored-instance-id="${RESTORED_INSTANCE_ID}" \
    -target-type="${TARGET_TYPE}" \
    -target-host="${TARGET_HOST}" \
    -target-username="${TARGET_USERNAME}" \
    -target-password="${TARGET_PASSWORD}" \
    -cloud-username="${CLOUD_USERNAME}" \
    -cloud-password="${CLOUD_PASSWORD}" \
    -delimiter="${DELIMITER}" \
    -newline="${NEWLINE}" \
    -null="${NULL}" \
    -scan-for-pii="${SCAN_FOR_PII}" \
    -delete-restored-instance-after-transfer="${DELETE_RESTORED_INSTANCE_AFTER_TRANSFER}" \
    -custom-strategy-threshold="${CUSTOM_STRATEGY_THRESHOLD}" \
    -custom-strategy-percentile="${CUSTOM_STRATEGY_PERCENTILE}" \
    -num-rows-to-scan-for-pii="${NUM_ROWS_TO_SCAN_FOR_PII}"

