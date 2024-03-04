CREATE TABLE IF NOT EXISTS cdcTable (
    id int primary key,
    name VARCHAR NOT NULL,
    date date not null
);

ALTER TABLE cdcTable REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS table1 (
    col1 integer,
    col2 date,
    col3 character varying(50) COLLATE pg_catalog."default"
)