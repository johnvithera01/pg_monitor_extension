#!/bin/bash
# Script to consolidate pg_monitor v0.10.0 files
# This creates the complete pg_monitor--0.10.0.sql file

echo "Creating pg_monitor--0.10.0.sql..."

# Start with base from 0.9.0
cat pg_monitor--0.9.0.sql > pg_monitor--0.10.0.sql

# Add upgrade modifications (schema changes and CVEs)
echo "" >> pg_monitor--0.10.0.sql
echo "-- ============================================================" >> pg_monitor--0.10.0.sql
echo "-- VERSION 0.10.0 ADDITIONS - CVE DETECTION SYSTEM" >> pg_monitor--0.10.0.sql
echo "-- ============================================================" >> pg_monitor--0.10.0.sql
echo "" >> pg_monitor--0.10.0.sql

# Add schema modifications from upgrade script (skip first 5 lines of comments)
tail -n +6 pg_monitor--0.9.0--0.10.0.sql | head -n 198 >> pg_monitor--0.10.0.sql

# Add CVE detection functions
cat pg_monitor_cve_functions.sql >> pg_monitor--0.10.0.sql

echo "✅ Created pg_monitor--0.10.0.sql"
echo ""
echo "Files created:"
echo "  - pg_monitor--0.9.0--0.10.0.sql (upgrade script)"
echo "  - pg_monitor--0.10.0.sql (complete version)"
echo "  - pg_monitor_cve_functions.sql (functions library)"
echo ""
echo "To install:"
echo "  CREATE EXTENSION pg_monitor VERSION '0.10.0';"
echo ""
echo "To upgrade from 0.9.0:"
echo "  ALTER EXTENSION pg_monitor UPDATE TO '0.10.0';"
