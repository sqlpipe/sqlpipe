CREATE TABLE IF NOT EXISTS users (
    id bigserial PRIMARY KEY,
    created_at timestamp(0) with time zone NOT NULL DEFAULT NOW(),
    username text UNIQUE NOT NULL,
    email citext NOT NULL,
    password_hash bytea NOT NULL,
    admin bool NOT NULL DEFAULT false,
    activated bool NOT NULL,
    version integer NOT NULL DEFAULT 1
);