SELECT 'CREATE DATABASE volok1'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'volok1')\gexec