CREATE TABLE IF NOT EXISTS cdcTable (
    id int primary key,
    name VARCHAR NOT NULL,
    date date not null
);

ALTER TABLE cdcTable REPLICA IDENTITY FULL;

