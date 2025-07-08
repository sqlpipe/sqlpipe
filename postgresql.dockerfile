FROM postgres:15

# Install wal2json (uses apt for Debian-based images)
RUN apt-get update \
    && apt-get install -y postgresql-15-wal2json \
    && rm -rf /var/lib/apt/lists/*
