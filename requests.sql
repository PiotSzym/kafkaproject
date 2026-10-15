DROP TABLE IF EXISTS accepted_requests;

CREATE TABLE accepted_requests (
    request_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    request_type TEXT NOT NULL,
    request_data INT NOT NULL,
    status TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL
);