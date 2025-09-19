# PostgreSQL Permissions Guide

This document outlines the necessary PostgreSQL permissions for the Redis-PostgreSQL Synchronization Tool.

## Required Permissions

To function properly, the tool requires the following permissions for the database user specified in your `env.conf` file:

1. **Connect to Database**: Basic connection access to the specified database
2. **Usage on Schema**: Permission to access and use the schema
3. **Insert/Update/Select**: Permission to insert, update, and select data in tables

**If the schema and tables don't already exist**, the user also needs:
1. **Create Schema**: Permission to create the schema if it doesn't exist
2. **Create Table**: Permission to create tables within the schema 
3. **Create Index**: Permission to create indexes on tables

> **Note**: If your database administrator has already created the schema and tables, you only need the first three permissions. The application will detect existing objects and use them without trying to create anything.

## Different Permission Scenarios

### Scenario 1: Using Pre-Created Schema and Tables (Minimal Permissions)

If your database administrator has already created the schema and tables according to the required structure, your database user only needs:

```sql
-- Grant connect permission on the database
GRANT CONNECT ON DATABASE postgres TO redis_relay_user;

-- Grant usage on the existing schema
GRANT USAGE ON SCHEMA topology TO redis_relay_user;

-- Grant permissions on existing tables
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA topology TO redis_relay_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA topology TO redis_relay_user;
```

### Scenario 2: User Needs to Create Everything (Full Permissions)

If your user needs to create the schema and tables:

```sql
-- Grant connect and create permission on the database
GRANT CONNECT, CREATE ON DATABASE postgres TO redis_relay_user;

-- Or have an administrator create the schema
CREATE SCHEMA IF NOT EXISTS topology;

-- Then grant all needed permissions
GRANT USAGE, CREATE ON SCHEMA topology TO redis_relay_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA topology TO redis_relay_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA topology TO redis_relay_user;
```

## Checking Existing Permissions

To check the current permissions for a user, connect to your PostgreSQL instance as a superuser and run:

```sql
-- Replace 'redis_relay_user' with your configured user name
SELECT grantee, table_schema, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'redis_relay_user'
ORDER BY table_schema, privilege_type;

-- Check schema permissions
SELECT nspname AS schema, 
       privilege_type
FROM information_schema.role_usage_grants 
WHERE grantee = 'redis_relay_user'
ORDER BY nspname;
```

## Checking If Schema and Tables Already Exist

To check if the schema and tables already exist:

```sql
-- Check if schema exists
SELECT EXISTS(
  SELECT 1 FROM information_schema.schemata 
  WHERE schema_name = 'topology'
);

-- Check if tables exist
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'topology' 
  AND table_name IN ('redis_streams', 'redis_key_values');
```

## Granting Required Permissions

If your user lacks necessary permissions, here's how to grant them:

### Basic Approach (Minimal Permissions)

```sql
-- Replace 'redis_relay_user', 'postgres', and 'topology' with your values
-- Connect as a superuser to run these commands

-- Grant connect permission on the database
GRANT CONNECT ON DATABASE postgres TO redis_relay_user;

-- Create the schema if it doesn't exist (do this as a superuser)
CREATE SCHEMA IF NOT EXISTS topology;

-- Grant usage and create permissions on the schema
GRANT USAGE, CREATE ON SCHEMA topology TO redis_relay_user;

-- Grant table and index creation permissions in the schema
GRANT CREATE ON SCHEMA topology TO redis_relay_user;

-- Additional permissions for existing objects
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA topology TO redis_relay_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA topology TO redis_relay_user;
```

### Alternative: Schema Ownership (More Permissive)

If you want to give the user full control over the schema:

```sql
-- Create the schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS topology;

-- Make the user the owner of the schema
ALTER SCHEMA topology OWNER TO redis_relay_user;
```

### Setting Default Privileges (Recommended)

This ensures the user will have appropriate permissions on future objects:

```sql
-- Set default privileges on future tables and sequences
ALTER DEFAULT PRIVILEGES IN SCHEMA topology
GRANT SELECT, INSERT, UPDATE ON TABLES TO redis_relay_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA topology
GRANT USAGE ON SEQUENCES TO redis_relay_user;
```

## Troubleshooting

### Common Permission Errors

1. **"permission denied for database postgres"**
   - The user lacks CONNECT privilege on the database
   - Solution: `GRANT CONNECT ON DATABASE postgres TO redis_relay_user;`

2. **"permission denied for schema topology"**
   - The user lacks USAGE privilege on the schema
   - Solution: `GRANT USAGE ON SCHEMA topology TO redis_relay_user;`

3. **"permission denied to create schema"**
   - The user lacks CREATE privilege on the database
   - Solution: `GRANT CREATE ON DATABASE postgres TO redis_relay_user;`
   - Alternatively: Have an administrator create the schema for you

4. **"permission denied to create table"**
   - The user lacks CREATE privilege on the schema
   - Solution: `GRANT CREATE ON SCHEMA topology TO redis_relay_user;`
   - Alternatively: Have an administrator create the tables for you

### Verifying Setup

After granting permissions, you can verify them with:

```sql
-- Test if the user can access objects
SET ROLE redis_relay_user;
-- Try to select from or insert into the tables
SELECT count(*) FROM topology.redis_streams;
SELECT count(*) FROM topology.redis_key_values;
RESET ROLE;
```

## Using Existing Tables

If you cannot grant the necessary permissions to create schemas and tables, you can:

1. Have a database administrator create the required schemas and tables
2. Grant only the INSERT, UPDATE, and SELECT permissions to your user
3. Configure the tool to use these existing tables

The tables should match the expected schema as described in the main documentation. 