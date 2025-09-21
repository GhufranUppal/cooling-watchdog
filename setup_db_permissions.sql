-- Run this script as a superuser (postgres) to set up permissions for ignition_user

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO ignition_user;

-- Grant necessary permissions on schema public
GRANT CREATE ON SCHEMA public TO ignition_user;

-- Grant table permissions for existing tables and make ignition_user the owner
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ignition_user;
ALTER TABLE IF EXISTS risk_now OWNER TO ignition_user;
ALTER TABLE IF EXISTS risk_hourly OWNER TO ignition_user;
ALTER TABLE IF EXISTS risk_windows OWNER TO ignition_user;

-- Grant sequence permissions for existing sequences and make ignition_user the owner
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ignition_user;
ALTER SEQUENCE IF EXISTS risk_now_id_seq OWNER TO ignition_user;
ALTER SEQUENCE IF EXISTS risk_hourly_id_seq OWNER TO ignition_user;
ALTER SEQUENCE IF EXISTS risk_windows_id_seq OWNER TO ignition_user;

-- Grant table permissions (for future tables)
ALTER DEFAULT PRIVILEGES FOR USER postgres IN SCHEMA public
GRANT ALL PRIVILEGES ON TABLES TO ignition_user;

-- Grant sequence permissions (for future serial/identity columns)
ALTER DEFAULT PRIVILEGES FOR USER postgres IN SCHEMA public
GRANT ALL PRIVILEGES ON SEQUENCES TO ignition_user;