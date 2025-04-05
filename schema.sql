-- schema.sql

-- Create the 'users' table
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create the 'products' table with more complex structure
CREATE TABLE IF NOT EXISTS products (
  id SERIAL PRIMARY KEY,
  sku VARCHAR(50) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  description TEXT NULL,
  price DECIMAL(10, 2) DEFAULT 0.00,
  stock_count INTEGER DEFAULT 0,
  is_active BOOLEAN DEFAULT TRUE,
  created_by VARCHAR(100), -- Could store user ID/sub
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a trigger function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for products table
CREATE TRIGGER update_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Create a function to validate product updates
CREATE OR REPLACE FUNCTION validate_product_update()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if the user making the request (current_setting('request.claims'))
    -- is the one who created the product.
    -- Allow update only if they match OR if the user is an admin
    IF OLD.created_by != (current_setting('request.claims', TRUE)::json->>'sub')
       AND (current_setting('request.claims', TRUE)::json->>'role') != 'admin' THEN
        -- If trying to update restricted fields
        IF NEW.name != OLD.name OR NEW.description != OLD.description THEN
            RAISE EXCEPTION 'Authorization failed: You can only modify products you created.';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for product validation
CREATE TRIGGER before_product_update
    BEFORE UPDATE ON products
    FOR EACH ROW
    EXECUTE FUNCTION validate_product_update();

-- Create a function that returns products for current user
CREATE OR REPLACE FUNCTION get_my_products()
RETURNS TEXT AS $$
DECLARE
    result TEXT;
BEGIN
    SELECT json_agg(row_to_json(p))
    INTO result
    FROM (
        SELECT id, sku, name, price, stock_count, is_active, created_at
        FROM products
        WHERE created_by = (current_setting('request.claims', TRUE)::json->>'sub')
    ) p;
    
    RETURN COALESCE(result, '[]');
END;
$$ LANGUAGE plpgsql;

-- Create a simple function for testing
CREATE OR REPLACE FUNCTION do_something(json_param TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN 'Processed: ' || json_param;
END;
$$ LANGUAGE plpgsql;
