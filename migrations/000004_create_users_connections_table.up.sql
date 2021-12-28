CREATE TABLE IF NOT EXISTS users_connections (
    user_id bigint NOT NULL REFERENCES users ON DELETE CASCADE,
    connection_id bigint NOT NULL REFERENCES connections ON DELETE CASCADE,
    PRIMARY KEY (user_id, connection_id)
);