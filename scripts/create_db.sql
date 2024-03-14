SELECT 'CREATE DATABASE cdcdb'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'cdcdb')\gexec