-- pg_monitor Extension Upgrade Script
-- Version: 0.9.0 → 0.10.0
-- Description: Adds comprehensive CVE detection system

-- ============================================================
-- SCHEMA MODIFICATIONS
-- ============================================================

-- Expand known_bugs table with CVE metadata
ALTER TABLE pgmon.known_bugs 
    ADD COLUMN IF NOT EXISTS cve_id TEXT,
    ADD COLUMN IF NOT EXISTS cvss_score NUMERIC(3,1),
    ADD COLUMN IF NOT EXISTS cvss_vector TEXT,
    ADD COLUMN IF NOT EXISTS patch_version TEXT,
    ADD COLUMN IF NOT EXISTS exploit_available BOOLEAN DEFAULT FALSE;

-- Create security scan history table
CREATE TABLE IF NOT EXISTS pgmon.security_scan_history (
    id SERIAL PRIMARY KEY,
    scan_timestamp TIMESTAMPTZ DEFAULT NOW(),
    pg_version TEXT,
    total_cves_applicable INT,
    critical_count INT,
    high_count INT,
    medium_count INT,
    low_count INT,
    scan_results JSONB
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_known_bugs_cve ON pgmon.known_bugs(cve_id);
CREATE INDEX IF NOT EXISTS idx_known_bugs_severity ON pgmon.known_bugs(severity);
CREATE INDEX IF NOT EXISTS idx_security_scan_timestamp ON pgmon.security_scan_history(scan_timestamp);

-- ============================================================
-- POPULATE CVE DATA
-- ============================================================

-- PostgreSQL 7.x CVEs
INSERT INTO pgmon.known_bugs (bug_id, cve_id, title, affected_versions, severity, category, description, workaround, cvss_score, patch_version, wiki_url) VALUES
('CVE-2010-3433', 'CVE-2010-3433', 'PL/Perl and PL/Tcl code execution vulnerability', ARRAY[7,8,9], 'CRITICAL', 'Procedural Languages',
 'Authenticated users with permission to create functions in PL/Perl or PL/Tcl could subvert the "safe" environment and execute arbitrary code with elevated privileges (e.g., in SECURITY DEFINER functions).',
 'Upgrade to patched version. Restrict CREATE FUNCTION privileges. Audit existing PL/Perl/PL/Tcl functions.',
 7.5, '7.4.30, 8.0.22, 8.1.18, 8.2.14, 8.3.8, 8.4.1, 9.0.0',
 'https://www.postgresql.org/support/security/CVE-2010-3433/'),

('CVE-2007-6601', 'CVE-2007-6601', 'dblink privilege escalation', ARRAY[7,8], 'CRITICAL', 'Extensions',
 'Functions in the dblink module combined with "trust" or "ident" authentication methods allowed malicious users to gain superuser privileges on the local database.',
 'Upgrade to 7.3.21, 7.4.19, 8.0.15, 8.1.11, 8.2.6. Restrict dblink usage. Use secure authentication methods.',
 9.0, '7.3.21, 7.4.19, 8.0.15, 8.1.11, 8.2.6',
 'https://www.postgresql.org/support/security/CVE-2007-6601/');

-- PostgreSQL 8.x CVEs  
INSERT INTO pgmon.known_bugs (bug_id, cve_id, title, affected_versions, severity, category, description, workaround, cvss_score, patch_version, wiki_url) VALUES
('CVE-2010-1975', 'CVE-2010-1975', 'Removal of restricted parameters by unprivileged users', ARRAY[7,8], 'HIGH', 'Authentication',
 'Unprivileged users could remove secure settings applied to their account by a superuser (ALTER USER ... SET), bypassing superuser-only parameter restrictions.',
 'Upgrade to 7.4.29, 8.0.22, 8.1.21, 8.2.17, 8.3.11, 8.4.4. Review user-specific parameter settings.',
 6.5, '7.4.29, 8.0.22, 8.1.21, 8.2.17, 8.3.11, 8.4.4',
 'https://www.postgresql.org/support/security/CVE-2010-1975/'),

('CVE-2007-2138', 'CVE-2007-2138', 'SECURITY DEFINER search_path vulnerability', ARRAY[7,8], 'CRITICAL', 'Query',
 'Insecure search_path settings allowed unprivileged users to execute functions defined with SECURITY DEFINER with the privileges of the function owner by creating malicious objects in public schemas.',
 'Upgrade to 7.3.19, 7.4.17, 8.0.13, 8.1.9, 8.2.4. Set secure search_path in SECURITY DEFINER functions.',
 8.5, '7.3.19, 7.4.17, 8.0.13, 8.1.9, 8.2.4',
 'https://www.postgresql.org/support/security/CVE-2007-2138/'),

('CVE-2007-4769', 'CVE-2007-4769', 'Regular expression vulnerabilities', ARRAY[7,8], 'MEDIUM', 'Query',
 'Three flaws in the regex engine allowed specially crafted regular expressions to cause backend crashes, infinite loops, or excessive memory consumption.',
 'Upgrade to 7.4.19, 8.0.15, 8.1.11, 8.2.6. Validate regex inputs from untrusted sources.',
 5.0, '7.4.19, 8.0.15, 8.1.11, 8.2.6',
 'https://www.postgresql.org/support/security/CVE-2007-4769/');

-- PostgreSQL 9.x CVEs
INSERT INTO pgmon.known_bugs (bug_id, cve_id, title, affected_versions, severity, category, description, workaround, cvss_score, patch_version, wiki_url) VALUES
('CVE-2013-1899', 'CVE-2013-1899', 'Database name command injection', ARRAY[9], 'CRITICAL', 'Injection',
 'Connection requests with database names starting with "-" could be manipulated to delete or corrupt files in the server data directory via command-line argument injection.',
 'Upgrade to 9.0.13, 9.1.9, 9.2.4. Do not expose PostgreSQL to untrusted networks.',
 8.5, '9.0.13, 9.1.9, 9.2.4',
 'https://www.postgresql.org/support/security/CVE-2013-1899/'),

('CVE-2014-0067', 'CVE-2014-0067', 'Unix socket privilege escalation during tests', ARRAY[9], 'HIGH', 'Authentication',
 'During "make check", the postmaster opened Unix sockets in /tmp with incorrect permissions, allowing local users to connect as superuser during tests.',
 'Upgrade to 8.4.20, 9.0.16, 9.1.12, 9.2.7, 9.3.3. Use dedicated test environments.',
 7.0, '8.4.20, 9.0.16, 9.1.12, 9.2.7, 9.3.3',
 'https://www.postgresql.org/support/security/CVE-2014-0067/'),

('CVE-2018-1058', 'CVE-2018-1058', 'search_path privilege escalation (Trojan functions)', ARRAY[9,10], 'HIGH', 'Query',
 'Users could create malicious objects in accessible schemas (like public) to modify execution of other users queries or SECURITY DEFINER functions, potentially gaining privileges.',
 'Upgrade to 9.3.23, 9.4.18, 9.5.13, 9.6.9, 10.4. Remove untrusted schemas from search_path.',
 8.8, '9.3.23, 9.4.18, 9.5.13, 9.6.9, 10.4',
 'https://www.postgresql.org/support/security/CVE-2018-1058/');

-- PostgreSQL 10+ CVEs
INSERT INTO pgmon.known_bugs (bug_id, cve_id, title, affected_versions, severity, category, description, workaround, cvss_score, patch_version, wiki_url) VALUES
('CVE-2020-25695', 'CVE-2020-25695', 'Autovacuum restricted operations sandbox escape', ARRAY[9,10,11,12,13], 'CRITICAL', 'Maintenance',
 'Commands used by autovacuum (ANALYZE, CLUSTER, REINDEX, CREATE INDEX, VACUUM FULL, REFRESH VIEW) could escape execution restrictions, allowing attackers to execute arbitrary SQL functions as superuser.',
 'Upgrade to 9.5.24, 9.6.20, 10.15, 11.10, 12.5, 13.1. Disable autovacuum as temporary mitigation.',
 8.8, '9.5.24, 9.6.20, 10.15, 11.10, 12.5, 13.1',
 'https://www.postgresql.org/support/security/CVE-2020-25695/'),

('CVE-2020-25696', 'CVE-2020-25696', 'psql gset command execution vulnerability', ARRAY[9,10,11,12,13], 'HIGH', 'Client Tools',
 'When using psql, if the server was compromised, an attacker could exploit the \gset command to execute arbitrary code on the client machine.',
 'Upgrade psql to 9.5.24, 9.6.20, 10.15, 11.10, 12.5, 13.1. Do not connect to untrusted servers.',
 7.5, '9.5.24, 9.6.20, 10.15, 11.10, 12.5, 13.1',
 'https://www.postgresql.org/support/security/CVE-2020-25696/'),

('CVE-2021-23214', 'CVE-2021-23214', 'SSL/TLS MITM data injection (server)', ARRAY[11,12,13,14], 'HIGH', 'Authentication',
 'Server did not discard unencrypted data sent immediately after SSL/TLS handshake. MITM attacker could inject SQL commands at session start if server did not require immediate authentication.',
 'Upgrade to 11.14, 12.9, 13.5, 14.1. Require SSL with certificate authentication.',
 8.1, '11.14, 12.9, 13.5, 14.1',
 'https://www.postgresql.org/support/security/CVE-2021-23214/'),

('CVE-2021-23222', 'CVE-2021-23222', 'SSL/TLS MITM data injection (client)', ARRAY[11,12,13,14], 'HIGH', 'Client Tools',
 'Client libpq did not discard unencrypted data received after SSL/GSS handshake. MITM attacker could inject false responses or exfiltrate sensitive data like passwords.',
 'Upgrade libpq to 11.14, 12.9, 13.5, 14.1. Use certificate pinning.',
 8.1, '11.14, 12.9, 13.5, 14.1',
 'https://www.postgresql.org/support/security/CVE-2021-23222/'),

('CVE-2022-1552', 'CVE-2022-1552', 'Extension scripts replace unrelated objects', ARRAY[13,14], 'HIGH', 'Extensions',
 'When installing extensions, the SQL script could inadvertently replace objects not belonging to the extension, allowing attackers with CREATE EXTENSION permission to overwrite critical objects.',
 'Upgrade to 13.7, 14.3. Restrict CREATE EXTENSION privileges.',
 8.8, '13.7, 14.3',
 'https://www.postgresql.org/support/security/CVE-2022-1552/'),

('CVE-2023-2455', 'CVE-2023-2455', 'Row-Level Security bypass after inlining', ARRAY[14,15], 'HIGH', 'Row-Level Security',
 'After certain optimizations (function inlining), Row-Level Security policies could be bypassed, allowing users to read or modify rows that should be hidden.',
 'Upgrade to 14.8, 15.3. Review RLS policies on critical tables.',
 8.0, '14.8, 15.3',
 'https://www.postgresql.org/support/security/CVE-2023-2455/'),

('CVE-2023-2454', 'CVE-2023-2454', 'CREATE SCHEMA bypasses search_path protection', ARRAY[14,15], 'HIGH', 'Query',
 'CREATE SCHEMA with initial elements allowed users to execute commands in unintended schemas, bypassing search_path protections.',
 'Upgrade to 14.8, 15.3. Restrict CREATE SCHEMA privileges.',
 7.5, '14.8, 15.3',
 'https://www.postgresql.org/support/security/CVE-2023-2454/'),

('CVE-2023-39418', 'CVE-2023-39418', 'MERGE ignores Row-Level Security policies', ARRAY[15], 'HIGH', 'Row-Level Security',
 'The MERGE command did not properly apply RLS policies for UPDATE or SELECT operations, allowing users to read or update data that should be hidden.',
 'Upgrade to 15.4. Avoid MERGE on RLS-protected tables until patched.',
 8.0, '15.4',
 'https://www.postgresql.org/support/security/CVE-2023-39418/'),

('CVE-2023-5868', 'CVE-2023-5868', 'Array modification buffer overflow', ARRAY[14,15,16], 'CRITICAL', 'Memory Safety',
 'Integer overflow in array modification functions could cause out-of-bounds writes, potentially leading to arbitrary code execution.',
 'Upgrade to 14.10, 15.5, 16.1. Validate array operations from untrusted sources.',
 8.8, '14.10, 15.5, 16.1',
 'https://www.postgresql.org/support/security/CVE-2023-5868/'),

('CVE-2023-5869', 'CVE-2023-5869', 'Array overflow variant', ARRAY[14,15,16], 'CRITICAL', 'Memory Safety',
 'Similar to CVE-2023-5868, another integer overflow in array handling could lead to memory corruption and code execution.',
 'Upgrade to 14.10, 15.5, 16.1. Validate array operations.',
 8.8, '14.10, 15.5, 16.1',
 'https://www.postgresql.org/support/security/CVE-2023-5869/'),

('CVE-2023-5870', 'CVE-2023-5870', 'pg_signal_backend can signal superuser processes', ARRAY[14,15,16], 'MEDIUM', 'Privilege Escalation',
 'The pg_signal_backend role could send signals to superuser processes including postmaster, enabling DoS attacks.',
 'Upgrade to 14.10, 15.5, 16.1. Review pg_signal_backend role grants.',
 6.5, '14.10, 15.5, 16.1',
 'https://www.postgresql.org/support/security/CVE-2023-5870/'),

('CVE-2023-39417', 'CVE-2023-39417', 'SQL injection via extension @ substitutions', ARRAY[14,15,16], 'HIGH', 'Injection',
 'Extension installation mechanism allowed SQL injection through @...@ parameter substitutions in scripts.',
 'Upgrade to 14.10, 15.5, 16.1. Review extension sources before installation.',
 7.5, '14.10, 15.5, 16.1',
 'https://www.postgresql.org/support/security/CVE-2023-39417/'),

('CVE-2024-10979', 'CVE-2024-10979', 'PL/Perl environment variable code execution', ARRAY[14,15,16,17], 'CRITICAL', 'Procedural Languages',
 'Environment variable changes in PL/Perl could persist and execute arbitrary code in superuser context when calling PL/Perl functions.',
 'Upgrade to 14.14, 15.9, 16.5, 17.1. Audit PL/Perl function usage.',
 8.8, '14.14, 15.9, 16.5, 17.1',
 'https://www.postgresql.org/support/security/CVE-2024-10979/'),

('CVE-2025-8714', 'CVE-2025-8714', 'pg_dump allows RCE on client/restore server', ARRAY[13,14,15,16,17], 'CRITICAL', 'Client Tools',
 'pg_dump did not properly escape newlines in object names. Malicious superuser on source server could create objects with names containing psql commands, executing arbitrary code during restore.',
 'Upgrade to 14.19, 15.14, 16.10, 17.6. Do not dump from untrusted servers.',
 9.0, '14.19, 15.14, 16.10, 17.6',
 'https://www.postgresql.org/support/security/CVE-2025-8714/'),

('CVE-2025-8713', 'CVE-2025-8713', 'Statistics expose sampled data from inaccessible columns', ARRAY[13,14,15,16,17], 'MEDIUM', 'Information Disclosure',
 'Privacy gap allowed users to obtain sampled data from columns in views or partitions they should not have access to via optimizer statistics.',
 'Upgrade to 14.19, 15.14, 16.10, 17.6. Review column-level permissions.',
 5.5, '14.19, 15.14, 16.10, 17.6',
 'https://www.postgresql.org/support/security/CVE-2025-8713/'),

-- ============================================================
-- OPERATIONAL BUGS (without CVE identifiers)
-- ============================================================

-- PostgreSQL 7.x Operational Bugs
('BUG-7X-SUBSELECT', NULL, 'Cannot handle unplanned sub-select error', ARRAY[7], 'MEDIUM', 'Query Planning',
 'Certain queries with complex subselects (especially involving join aliases expanding to another subselect) failed with error "cannot handle unplanned sub-select".',
 'Upgrade to PostgreSQL 7.4.30 or later. Rewrite complex subselects as CTEs or separate queries.',
 NULL, '7.4.30',
 'https://www.postgresql.org/docs/release/7.4.30/'),

('BUG-7X-LOCKFILE-CORRUPT', NULL, 'Lockfile corruption prevents restart after crash', ARRAY[7], 'HIGH', 'Server Stability',
 'The server lock file (postmaster.pid and socket lock) was not fsynced correctly on creation. A system crash shortly after starting postmaster could leave a corrupted lockfile on disk, preventing PostgreSQL from restarting until manual removal.',
 'Upgrade to PostgreSQL 7.4.30. If affected, manually remove postmaster.pid and socket lock files after verifying no postgres processes are running.',
 NULL, '7.4.30',
 'https://www.postgresql.org/docs/release/7.4.30/'),

-- PostgreSQL 8.x Operational Bugs
('BUG-8X-GIST-BIT-CORRUPT', NULL, 'GiST index corruption on bit/bit varying columns', ARRAY[8], 'HIGH', 'Index Corruption',
 'In GiST indexes on bit/bit varying columns, padding bytes were not initialized correctly. This could cause values that should be considered equal to be treated as different, causing incorrect query results.',
 'Upgrade to PostgreSQL 8.4.22. Run REINDEX on all GiST indexes on bit/bit varying columns after applying the patch.',
 NULL, '8.4.22',
 'https://www.postgresql.org/docs/release/8.4.22/'),

-- PostgreSQL 9.x Operational Bugs
('BUG-9X-CONCURRENT-INDEX-TUPLES', NULL, 'CREATE INDEX CONCURRENTLY may lose tuples', ARRAY[9,10,11,12,13], 'HIGH', 'Index Corruption',
 'A rare race condition where indexes created with CREATE INDEX CONCURRENTLY could miss some rows from prepared transactions pending at index creation time. This led to incomplete indexes and queries potentially missing data.',
 'Upgrade to 9.6.24, 10.19, 11.14, 12.9, 13.5. Reindex indexes created concurrently on systems with max_prepared_transactions > 0.',
 NULL, '9.6.24, 10.19, 11.14, 12.9, 13.5',
 'https://www.postgresql.org/docs/release/9.6.24/'),

-- PostgreSQL 10 Operational Bugs
('BUG-10-WINDOWS-INSTALLER', NULL, 'Windows installer binary hijacking vulnerability', ARRAY[9,10,11,12], 'HIGH', 'Installation Security',
 'The PostgreSQL installer for Windows executed system utilities without using qualified paths. A local attacker could place a malicious binary in PATH to gain privileged execution during installation.',
 'Upgrade to PostgreSQL 10.13 or later. Use installers from official sources only.',
 NULL, '10.13',
 'https://www.postgresql.org/about/news/postgresql-123-118-1013-9618-9522-and-1114-released-2011/'),

-- PostgreSQL 11 Operational Bugs
('BUG-11-WAL-SLOT-REMOVAL', NULL, 'Premature WAL removal breaks replication slots', ARRAY[11,12], 'HIGH', 'Replication',
 'Under certain checkpoint conditions, the system could remove WAL segments still needed by active replication slots, causing standby recovery to fail with WAL sequence errors.',
 'Upgrade to PostgreSQL 11.7 or later. Monitor replication lag and WAL retention.',
 NULL, '11.7',
 'https://www.postgresql.org/docs/release/11.7/'),

-- PostgreSQL 13 Operational Bugs
('BUG-13-XML-10MB-LIMIT', NULL, 'XML documents larger than 10MB rejected', ARRAY[13], 'MEDIUM', 'Data Types',
 'A change in the XML parser inadvertently rejected valid XML documents larger than approximately 10MB, returning an error.',
 'Upgrade to a patched PostgreSQL 13.x version. Split large XML documents if upgrade is not immediately possible.',
 NULL, '13.x',
 'https://www.postgresql.org/about/news/postgresql-176-1610-1514-1419-and-1322-released-3018/'),

-- PostgreSQL 15 Operational Bugs
('BUG-15-MERGE-CONCURRENT', NULL, 'MERGE command issues under concurrent workloads', ARRAY[15], 'MEDIUM', 'Query Execution',
 'Under concurrent loads, the MERGE command could produce incorrect results or unexpected conflicts, especially when the target table participates in inheritance or partitioning.',
 'Upgrade to PostgreSQL 15.4 or later. Avoid MERGE on heavily concurrent tables until patched.',
 NULL, '15.4',
 'https://www.postgresql.org/about/news/postgresql-176-1610-1514-1419-and-1322-released-3018/'),

-- PostgreSQL 17 Operational Bugs
('BUG-17-BRIN-BLOAT', NULL, 'BRIN index bloat with numeric_minmax_multi_ops', ARRAY[17], 'MEDIUM', 'Index Maintenance',
 'BRIN indexes using numeric_minmax_multi_ops operator class could experience excessive bloat under certain data patterns.',
 'Upgrade to PostgreSQL 17.1 or later. Consider REINDEX on affected BRIN indexes.',
 NULL, '17.1',
 'https://www.postgresql.org/about/news/postgresql-176-1610-1514-1419-and-1322-released-3018/'),

('BUG-17-LOGICAL-REP-MEMLEAK', NULL, 'Logical replication memory leak and replay duplication', ARRAY[17], 'MEDIUM', 'Replication',
 'Logical replication could experience memory leaks and duplicate replay of changes under certain conditions.',
 'Upgrade to PostgreSQL 17.1 or later. Monitor memory usage of logical replication workers.',
 NULL, '17.1',
 'https://www.postgresql.org/about/news/postgresql-176-1610-1514-1419-and-1322-released-3018/'),

('BUG-17-CHECKPOINT-LOOP', NULL, 'Infinite loop in checkpoints with many buffers', ARRAY[17], 'HIGH', 'Server Stability',
 'Checkpoints could enter an infinite loop when dealing with a very large number of dirty buffers.',
 'Upgrade to PostgreSQL 17.1 or later. Monitor checkpoint duration.',
 NULL, '17.1',
 'https://www.postgresql.org/about/news/postgresql-176-1610-1514-1419-and-1322-released-3018/'),

('BUG-17-LZ4-DECOMPRESS', NULL, 'LZ4 decompression failure on low-compressibility data', ARRAY[17], 'MEDIUM', 'Compression',
 'LZ4 decompression could fail on data with low compressibility ratios.',
 'Upgrade to PostgreSQL 17.1 or later. Consider using pglz compression for affected tables.',
 NULL, '17.1',
 'https://www.postgresql.org/about/news/postgresql-176-1610-1514-1419-and-1322-released-3018/')

ON CONFLICT (bug_id) DO UPDATE SET
    cve_id = EXCLUDED.cve_id,
    title = EXCLUDED.title,
    affected_versions = EXCLUDED.affected_versions,
    severity = EXCLUDED.severity,
    category = EXCLUDED.category,
    description = EXCLUDED.description,
    workaround = EXCLUDED.workaround,
    cvss_score = EXCLUDED.cvss_score,
    patch_version = EXCLUDED.patch_version,
    wiki_url = EXCLUDED.wiki_url;

-- Update hello function to reflect new version
CREATE OR REPLACE FUNCTION pgmon.hello()
RETURNS TEXT AS $$
BEGIN
    RETURN 'pg_monitor extension v0.10.0 is working! Now with CVE detection.';
END;
$$ LANGUAGE plpgsql;
