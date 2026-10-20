CREATE TABLE IF NOT EXISTS request_rollups (
    window_start TIMESTAMPTZ NOT NULL,
    request_type TEXT NOT NULL,
    accepted_count INT NOT NULL,
    PRIMARY KEY (window_start, request_type)
);