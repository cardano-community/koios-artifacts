-- Recreate grest schema
DROP SCHEMA IF EXISTS grest CASCADE;

CREATE SCHEMA grest;

GRANT USAGE ON SCHEMA grest TO authenticator, web_anon;

GRANT SELECT ON ALL TABLES IN SCHEMA grest TO authenticator, web_anon;

ALTER DEFAULT PRIVILEGES IN SCHEMA grest GRANT
SELECT ON TABLES TO authenticator, web_anon;
