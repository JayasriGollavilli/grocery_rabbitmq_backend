CREATE TABLE IF NOT EXISTS grocery_products (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR(150) NOT NULL CHECK (length(trim(name)) > 0),
    category VARCHAR(100) NOT NULL CHECK (length(trim(category)) > 0),
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    quantity INTEGER NOT NULL CHECK (quantity >= 0)
);
