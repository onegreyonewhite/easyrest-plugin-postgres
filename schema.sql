-- schema.sql

-- Create the 'users' table
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a function 'doSomething'
CREATE OR REPLACE FUNCTION doSomething(jsonParam TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN 'Processed: ' || jsonParam;
END;
$$ LANGUAGE plpgsql;
