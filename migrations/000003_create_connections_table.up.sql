CREATE TABLE IF NOT EXISTS connections (
    id bigserial PRIMARY KEY,
    created_at timestamp(0) with time zone NOT NULL DEFAULT NOW(),
    name text NOT NULL,
    username TEXT NOT NULL,
    password TEXT NOT NULL,
    account_id TEXT NOT NULL DEFAULT '',
    hostname TEXT NOT NULL DEFAULT '',
    port INT NOT NULL DEFAULT 0,
    db_name TEXT NOT NULL,
    version INT NOT NULL DEFAULT 1
);