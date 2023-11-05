create table if not exists "entries"
(
    receipt_id  int,
    shop        text,
    product     text,
    quantity    float,
    total_price int,
    timestamp   timestamp,
    PRIMARY KEY (shop, receipt_id, product)
);
