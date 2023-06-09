-- Named placeholders for checking if a database exists
SELECT 1 FROM pg_database WHERE datname = {dbname};