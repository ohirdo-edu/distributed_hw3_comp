DROP TABLE IF EXISTS entries;

CREATE TABLE IF NOT EXISTS entries (
    receipt_id int,
    shop varchar,
    product varchar,
    total_price int,
    quantity float,
    timestamp timestamp,
    CONSTRAINT pkey PRIMARY KEY (receipt_id, shop, product)
);
