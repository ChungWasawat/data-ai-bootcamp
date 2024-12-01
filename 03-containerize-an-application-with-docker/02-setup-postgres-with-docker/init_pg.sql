-- Active: 1732941727013@@localhost@5432@postgres@public
CREATE TABLE customers (
    customer_id UUID PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    segment VARCHAR(50),
    join_date TIMESTAMP,
    loyalty_points INTEGER
);

INSERT INTO customers (
    customer_id, name, email, phone, 
    address, segment, join_date, loyalty_points
) VALUES (
    gen_random_uuid(), 
    'John Doe', 
    'john.doe@example.com', 
    '123-456-7890',
    '123 Main St, Anytown, USA',
    'Retail',
    CURRENT_TIMESTAMP,
    500
);

SELECT * FROM customers;