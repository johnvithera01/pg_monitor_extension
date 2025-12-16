-- pg_monitor extension upgrade from 0.8.0 to 0.9.0
-- ============================================================================
-- VERSION 0.9.0: Current PostgreSQL Open Bugs Detection
-- Detects known open bugs in PostgreSQL 13, 14, 15, and 16
-- ============================================================================

\echo Use "ALTER EXTENSION pg_monitor UPDATE TO '0.9.0'" to load this file. \quit

-- ============================================================================
-- TABLE: Known PostgreSQL Bugs Registry
-- ============================================================================

CREATE TABLE IF NOT EXISTS pgmon.known_bugs (
    id SERIAL PRIMARY KEY,
    bug_id TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    affected_versions INT[] NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('CRITICAL', 'HIGH', 'MEDIUM', 'LOW')),
    category TEXT NOT NULL,
    description TEXT,
    workaround TEXT,
    wiki_url TEXT,
    detection_function TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Populate known bugs
INSERT INTO pgmon.known_bugs (bug_id, title, affected_versions, severity, category, description, workaround, wiki_url, detection_function) VALUES
-- CRITICAL BUGS
('BUG-RECOVERY-SIGNAL', 
 'RecoveryConflictInterrupt() unsafe in signal handler', 
 ARRAY[13,14,15,16], 
 'CRITICAL', 
 'Replication',
 'RecoveryConflictInterrupt() can be called during signal handling unsafely, leading to crashes or FATAL recursive terminations during recovery conflicts on replicated servers.',
 'Monitor for unexpected replica crashes. Keep replicas in sync to minimize conflicts.',
 'https://wiki.postgresql.org/wiki/PostgreSQL_13_Open_Items',
 'pgmon.check_recovery_conflict_risk'),

('BUG-GROUPING-SETS',
 'Incorrect results with GROUPING SETS',
 ARRAY[13,14,15,16],
 'CRITICAL',
 'Query',
 'Queries using GROUP BY GROUPING SETS can return incorrect results (wrong or duplicated rows) due to a logical bug in grouping processing.',
 'Avoid complex GROUPING SETS queries or verify results manually. Consider using UNION ALL of simple GROUP BY instead.',
 'https://wiki.postgresql.org/wiki/PostgreSQL_13_Open_Items',
 'pgmon.check_grouping_sets_usage'),

('BUG-TOAST-ACCESS',
 'Possible access to already-removed TOAST data',
 ARRAY[13,14,15,16],
 'CRITICAL',
 'Storage',
 'A TOAST fetch may occur after the required data has been removed from storage, leading to errors or incorrect behavior.',
 'Ensure proper VACUUM maintenance. Monitor for TOAST-related errors in logs.',
 'https://wiki.postgresql.org/wiki/PostgreSQL_13_Open_Items',
 'pgmon.check_toast_issues'),

-- HIGH/FUNCTIONAL BUGS
('BUG-PARTITION-TRIGGERS',
 'Triggers on partitions lose tgenabled flag',
 ARRAY[13,14,15,16],
 'HIGH',
 'Partitioning',
 'When creating partitioned tables with triggers, the internal trigger enabled flag (tgenabled) may be lost on child tables. Triggers may be inadvertently disabled.',
 'Verify trigger status after partition creation. Re-enable triggers if needed.',
 'https://wiki.postgresql.org/wiki/PostgreSQL_13_Open_Items',
 'pgmon.check_partition_trigger_status'),

('BUG-2PC-SNAPSHOT',
 'Incorrect snapshot calculation with 2PC',
 ARRAY[13,14,15,16],
 'HIGH',
 'Transaction',
 'Bug in transaction snapshot calculation when using two-phase commit (PREPARE/COMMIT). May lead to visibility anomalies.',
 'Minimize use of two-phase commit if possible. Monitor for transaction isolation issues.',
 'https://wiki.postgresql.org/wiki/PostgreSQL_13_Open_Items',
 'pgmon.check_2pc_usage'),

('BUG-REINDEX-CATALOG',
 'REINDEX on system catalog creates duplicate index entries',
 ARRAY[13,14,15,16],
 'HIGH',
 'Index',
 'Rare case where REINDEX on a system catalog can leave the resulting index with two index tuples pointing to the same heap tuple, violating HOT invariants.',
 'Avoid REINDEX SYSTEM unless necessary. Use REINDEX CONCURRENTLY when possible.',
 'https://wiki.postgresql.org/wiki/PostgreSQL_13_Open_Items',
 'pgmon.check_catalog_index_health'),

('BUG-WINDOWS-STATS',
 'Failure to rename temporary statistics file on Windows',
 ARRAY[13,14],
 'MEDIUM',
 'Statistics',
 'On Windows, the statistics process may log errors like "could not rename temporary statistics file". Does not cause data loss but pollutes logs.',
 'Upgrade to PostgreSQL 15+ where statistics collector was redesigned.',
 'https://wiki.postgresql.org/wiki/PostgreSQL_13_Open_Items',
 'pgmon.check_stats_collector_errors'),

('BUG-BACKUP-SIGNAL',
 'Need recovery.signal/standby.signal with backup_label',
 ARRAY[13,14,15,16],
 'MEDIUM',
 'Backup',
 'When restoring a base backup, PostgreSQL requires a signal file (recovery.signal or standby.signal) in addition to backup_label. Without it, the server ignores backup_label.',
 'Always create recovery.signal or standby.signal when restoring from backup.',
 'https://wiki.postgresql.org/wiki/PostgreSQL_13_Open_Items',
 NULL),

('BUG-PG-VISIBILITY',
 'pg_check_visible() false positives with autovacuum',
 ARRAY[13,14,15,16],
 'LOW',
 'Maintenance',
 'The pg_visibility check function may report pages as "invisible" (corrupted) incorrectly when running concurrently with autovacuum. False positive only.',
 'Run pg_check_visible when autovacuum is not active on the table.',
 'https://wiki.postgresql.org/wiki/PostgreSQL_13_Open_Items',
 'pgmon.check_visibility_autovacuum_conflict'),

('BUG-ICU-EMPTY-RULES',
 'Strange behavior when ICU rules are empty string',
 ARRAY[16],
 'MEDIUM',
 'Collation',
 'Creating ICU collation definitions with empty rules string causes unexpected behavior. The ICU library does not behave as expected.',
 'Avoid using empty string for ICU collation rules.',
 'https://wiki.postgresql.org/wiki/PostgreSQL_16_Open_Items',
 'pgmon.check_icu_collation_issues')
ON CONFLICT (bug_id) DO UPDATE SET
    title = EXCLUDED.title,
    affected_versions = EXCLUDED.affected_versions,
    severity = EXCLUDED.severity,
    description = EXCLUDED.description,
    workaround = EXCLUDED.workaround;

-- ============================================================================
-- BUG DETECTION FUNCTIONS
-- ============================================================================

-- Get PostgreSQL major version as integer
CREATE OR REPLACE FUNCTION pgmon.get_pg_major_version()
RETURNS INT
LANGUAGE sql
STABLE
AS $$
    SELECT (current_setting('server_version_num')::INT / 10000)::INT;
$$;

-- Check if current version is affected by a specific bug
CREATE OR REPLACE FUNCTION pgmon.is_version_affected(p_bug_id TEXT)
RETURNS BOOLEAN
LANGUAGE sql
AS $$
    SELECT pgmon.get_pg_major_version() = ANY(affected_versions)
    FROM pgmon.known_bugs
    WHERE bug_id = p_bug_id;
$$;

-- ============================================================================
-- CRITICAL BUG DETECTION
-- ============================================================================

-- Check Recovery Conflict Risk (standby servers)
CREATE OR REPLACE FUNCTION pgmon.check_recovery_conflict_risk()
RETURNS TABLE(
    is_standby BOOLEAN,
    recovery_conflicts_total BIGINT,
    conflicts_snapshot BIGINT,
    conflicts_lock BIGINT,
    conflicts_bufferpin BIGINT,
    risk_level TEXT,
    recommendation TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pg_is_in_recovery() AS is_standby,
        COALESCE(SUM(confl_tablespace + confl_lock + confl_snapshot + confl_bufferpin + confl_deadlock), 0) AS recovery_conflicts_total,
        COALESCE(SUM(confl_snapshot), 0) AS conflicts_snapshot,
        COALESCE(SUM(confl_lock), 0) AS conflicts_lock,
        COALESCE(SUM(confl_bufferpin), 0) AS conflicts_bufferpin,
        CASE 
            WHEN NOT pg_is_in_recovery() THEN 'N/A (Primary)'
            WHEN COALESCE(SUM(confl_snapshot), 0) > 100 THEN 'HIGH'
            WHEN COALESCE(SUM(confl_snapshot + confl_lock), 0) > 50 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level,
        CASE 
            WHEN NOT pg_is_in_recovery() THEN 'Not a standby server - bug does not apply'
            WHEN COALESCE(SUM(confl_snapshot), 0) > 100 THEN 'HIGH RISK: Many recovery conflicts detected. BUG-RECOVERY-SIGNAL may cause crashes. Consider increasing max_standby_streaming_delay.'
            WHEN COALESCE(SUM(confl_snapshot + confl_lock), 0) > 50 THEN 'MEDIUM RISK: Recovery conflicts present. Monitor for unexpected crashes.'
            ELSE 'LOW RISK: Few recovery conflicts, but monitor regularly.'
        END AS recommendation
    FROM pg_stat_database_conflicts;
END;
$$;

-- Check GROUPING SETS Usage in Active Queries
CREATE OR REPLACE FUNCTION pgmon.check_grouping_sets_usage()
RETURNS TABLE(
    has_active_grouping_sets BOOLEAN,
    affected_queries INT,
    sample_query TEXT,
    risk_level TEXT,
    recommendation TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_count INT;
    v_sample TEXT;
BEGIN
    -- Check active queries
    SELECT COUNT(*), MAX(LEFT(query, 500))
    INTO v_count, v_sample
    FROM pg_stat_activity
    WHERE state = 'active'
      AND (query ILIKE '%GROUPING SETS%' 
           OR query ILIKE '%ROLLUP%' 
           OR query ILIKE '%CUBE%');
    
    RETURN QUERY
    SELECT 
        v_count > 0 AS has_active_grouping_sets,
        v_count AS affected_queries,
        v_sample AS sample_query,
        CASE 
            WHEN v_count > 5 THEN 'HIGH'
            WHEN v_count > 0 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level,
        CASE 
            WHEN v_count > 0 THEN 'WARNING: BUG-GROUPING-SETS may cause incorrect results. Verify query outputs manually or consider alternatives.'
            ELSE 'No active GROUPING SETS queries detected.'
        END AS recommendation;
END;
$$;

-- Check for GROUPING SETS in pg_stat_statements
CREATE OR REPLACE FUNCTION pgmon.check_grouping_sets_in_history()
RETURNS TABLE(
    query_text TEXT,
    calls BIGINT,
    total_time_ms NUMERIC,
    risk_level TEXT,
    recommendation TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN
        RETURN QUERY
        SELECT 
            'pg_stat_statements not installed'::TEXT,
            0::BIGINT,
            0::NUMERIC,
            'UNKNOWN'::TEXT,
            'Install pg_stat_statements to detect GROUPING SETS usage in historical queries'::TEXT;
        RETURN;
    END IF;
    
    RETURN QUERY EXECUTE '
        SELECT 
            LEFT(query, 300) AS query_text,
            calls,
            ROUND(total_exec_time::NUMERIC, 2) AS total_time_ms,
            CASE 
                WHEN calls > 1000 THEN ''HIGH''
                WHEN calls > 100 THEN ''MEDIUM''
                ELSE ''LOW''
            END AS risk_level,
            ''BUG-GROUPING-SETS: This query uses GROUPING SETS/ROLLUP/CUBE and may return incorrect results. Verify output.''::TEXT AS recommendation
        FROM pg_stat_statements
        WHERE query ILIKE ''%GROUPING SETS%'' 
           OR query ILIKE ''%ROLLUP%'' 
           OR query ILIKE ''%CUBE%''
        ORDER BY calls DESC
        LIMIT 20';
END;
$$;

-- Check TOAST-related issues
CREATE OR REPLACE FUNCTION pgmon.check_toast_issues()
RETURNS TABLE(
    table_name TEXT,
    toast_table TEXT,
    toast_size_bytes BIGINT,
    toast_size_pretty TEXT,
    main_table_dead_tuples BIGINT,
    risk_level TEXT,
    recommendation TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        n.nspname || '.' || c.relname AS table_name,
        t.relname::TEXT AS toast_table,
        pg_relation_size(t.oid) AS toast_size_bytes,
        pg_size_pretty(pg_relation_size(t.oid)) AS toast_size_pretty,
        COALESCE(s.n_dead_tup, 0) AS main_table_dead_tuples,
        CASE 
            WHEN pg_relation_size(t.oid) > 1073741824 AND COALESCE(s.n_dead_tup, 0) > 100000 THEN 'HIGH'
            WHEN pg_relation_size(t.oid) > 104857600 AND COALESCE(s.n_dead_tup, 0) > 10000 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level,
        CASE 
            WHEN COALESCE(s.n_dead_tup, 0) > 100000 THEN 'BUG-TOAST-ACCESS: High dead tuples may increase risk of TOAST access issues. Run VACUUM.'
            WHEN COALESCE(s.n_dead_tup, 0) > 10000 THEN 'BUG-TOAST-ACCESS: Monitor for TOAST-related errors. Consider VACUUM.'
            ELSE 'TOAST health appears normal.'
        END AS recommendation
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_class t ON t.oid = c.reltoastrelid
    LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
    WHERE c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND c.reltoastrelid != 0
    ORDER BY pg_relation_size(t.oid) DESC
    LIMIT 50;
END;
$$;

-- ============================================================================
-- HIGH PRIORITY BUG DETECTION
-- ============================================================================

-- Check Partition Trigger Status (BUG-PARTITION-TRIGGERS)
CREATE OR REPLACE FUNCTION pgmon.check_partition_trigger_status()
RETURNS TABLE(
    partition_schema TEXT,
    partition_name TEXT,
    trigger_name TEXT,
    trigger_enabled TEXT,
    parent_trigger_enabled TEXT,
    status_mismatch BOOLEAN,
    risk_level TEXT,
    recommendation TEXT
)
LANGUAGE sql
AS $$
    WITH parent_triggers AS (
        SELECT 
            tgrelid,
            tgname,
            CASE tgenabled
                WHEN 'O' THEN 'ORIGIN'
                WHEN 'D' THEN 'DISABLED'
                WHEN 'R' THEN 'REPLICA'
                WHEN 'A' THEN 'ALWAYS'
                ELSE 'UNKNOWN'
            END AS enabled_status
        FROM pg_trigger
        WHERE tgparentid = 0
    ),
    child_triggers AS (
        SELECT 
            c.oid AS child_oid,
            n.nspname AS child_schema,
            c.relname AS child_name,
            t.tgname,
            CASE t.tgenabled
                WHEN 'O' THEN 'ORIGIN'
                WHEN 'D' THEN 'DISABLED'
                WHEN 'R' THEN 'REPLICA'
                WHEN 'A' THEN 'ALWAYS'
                ELSE 'UNKNOWN'
            END AS enabled_status,
            t.tgparentid
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_inherits i ON i.inhrelid = c.oid
        WHERE t.tgparentid != 0
    )
    SELECT 
        ct.child_schema AS partition_schema,
        ct.child_name AS partition_name,
        ct.tgname AS trigger_name,
        ct.enabled_status AS trigger_enabled,
        pt.enabled_status AS parent_trigger_enabled,
        ct.enabled_status != pt.enabled_status AS status_mismatch,
        CASE 
            WHEN ct.enabled_status = 'DISABLED' AND pt.enabled_status != 'DISABLED' THEN 'HIGH'
            WHEN ct.enabled_status != pt.enabled_status THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level,
        CASE 
            WHEN ct.enabled_status = 'DISABLED' AND pt.enabled_status != 'DISABLED' 
                THEN 'BUG-PARTITION-TRIGGERS: Trigger is DISABLED on partition but ENABLED on parent. Re-enable with ALTER TABLE ... ENABLE TRIGGER.'
            WHEN ct.enabled_status != pt.enabled_status 
                THEN 'BUG-PARTITION-TRIGGERS: Trigger status differs from parent. Verify intended behavior.'
            ELSE 'Trigger status matches parent.'
        END AS recommendation
    FROM child_triggers ct
    JOIN parent_triggers pt ON pt.tgrelid = (
        SELECT inhparent FROM pg_inherits WHERE inhrelid = ct.child_oid LIMIT 1
    ) AND pt.tgname = ct.tgname
    WHERE ct.enabled_status != pt.enabled_status
    ORDER BY risk_level DESC, ct.child_schema, ct.child_name;
$$;

-- Check Two-Phase Commit Usage (BUG-2PC-SNAPSHOT)
CREATE OR REPLACE FUNCTION pgmon.check_2pc_usage()
RETURNS TABLE(
    prepared_transactions_count INT,
    oldest_prepared_xact_age INTERVAL,
    gid TEXT,
    owner TEXT,
    database TEXT,
    risk_level TEXT,
    recommendation TEXT
)
LANGUAGE sql
AS $$
    SELECT 
        (SELECT COUNT(*)::INT FROM pg_prepared_xacts) AS prepared_transactions_count,
        NOW() - prepared AS oldest_prepared_xact_age,
        gid,
        owner::TEXT,
        database::TEXT,
        CASE 
            WHEN NOW() - prepared > INTERVAL '1 hour' THEN 'HIGH'
            WHEN NOW() - prepared > INTERVAL '10 minutes' THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level,
        CASE 
            WHEN NOW() - prepared > INTERVAL '1 hour' 
                THEN 'BUG-2PC-SNAPSHOT: Long-lived prepared transaction detected. May cause snapshot calculation issues. COMMIT or ROLLBACK PREPARED.'
            WHEN NOW() - prepared > INTERVAL '10 minutes' 
                THEN 'BUG-2PC-SNAPSHOT: Prepared transaction open for extended period. Monitor closely.'
            ELSE 'Prepared transaction detected. Complete it promptly to avoid snapshot issues.'
        END AS recommendation
    FROM pg_prepared_xacts
    ORDER BY prepared ASC;
$$;

-- Check Catalog Index Health (BUG-REINDEX-CATALOG)
CREATE OR REPLACE FUNCTION pgmon.check_catalog_index_health()
RETURNS TABLE(
    index_name TEXT,
    table_name TEXT,
    index_size_bytes BIGINT,
    index_size_pretty TEXT,
    is_valid BOOLEAN,
    index_scans BIGINT,
    risk_level TEXT,
    recommendation TEXT
)
LANGUAGE sql
AS $$
    SELECT 
        i.relname::TEXT AS index_name,
        c.relname::TEXT AS table_name,
        pg_relation_size(i.oid) AS index_size_bytes,
        pg_size_pretty(pg_relation_size(i.oid)) AS index_size_pretty,
        ix.indisvalid AS is_valid,
        COALESCE(s.idx_scan, 0) AS index_scans,
        CASE 
            WHEN NOT ix.indisvalid THEN 'CRITICAL'
            WHEN pg_relation_size(i.oid) > pg_relation_size(c.oid) * 3 THEN 'HIGH'
            ELSE 'LOW'
        END AS risk_level,
        CASE 
            WHEN NOT ix.indisvalid 
                THEN 'BUG-REINDEX-CATALOG: Invalid system index detected! Run REINDEX SYSTEM to fix.'
            WHEN pg_relation_size(i.oid) > pg_relation_size(c.oid) * 3 
                THEN 'BUG-REINDEX-CATALOG: System index is abnormally large. May indicate duplicate entries. Consider REINDEX.'
            ELSE 'System index appears healthy.'
        END AS recommendation
    FROM pg_class i
    JOIN pg_index ix ON i.oid = ix.indexrelid
    JOIN pg_class c ON c.oid = ix.indrelid
    JOIN pg_namespace n ON n.oid = i.relnamespace
    LEFT JOIN pg_stat_sys_indexes s ON s.indexrelid = i.oid
    WHERE n.nspname = 'pg_catalog'
      AND i.relkind = 'i'
    ORDER BY 
        CASE WHEN NOT ix.indisvalid THEN 0 ELSE 1 END,
        pg_relation_size(i.oid) DESC
    LIMIT 50;
$$;

-- ============================================================================
-- MEDIUM/LOW PRIORITY BUG DETECTION
-- ============================================================================

-- Check Stats Collector Errors (Windows - BUG-WINDOWS-STATS)
CREATE OR REPLACE FUNCTION pgmon.check_stats_collector_errors()
RETURNS TABLE(
    platform TEXT,
    pg_version INT,
    is_affected BOOLEAN,
    risk_level TEXT,
    recommendation TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_version INT;
    v_platform TEXT;
BEGIN
    v_version := pgmon.get_pg_major_version();
    
    -- Detect platform (simplified - checks for common Windows indicators)
    IF current_setting('data_directory') LIKE '%:\\%' OR 
       current_setting('data_directory') LIKE '%/%' = FALSE THEN
        v_platform := 'Windows';
    ELSE
        v_platform := 'Unix/Linux';
    END IF;
    
    RETURN QUERY
    SELECT 
        v_platform AS platform,
        v_version AS pg_version,
        (v_platform = 'Windows' AND v_version IN (13, 14)) AS is_affected,
        CASE 
            WHEN v_platform = 'Windows' AND v_version IN (13, 14) THEN 'MEDIUM'
            ELSE 'N/A'
        END AS risk_level,
        CASE 
            WHEN v_platform = 'Windows' AND v_version IN (13, 14) 
                THEN 'BUG-WINDOWS-STATS: Windows PG 13/14 may log "could not rename temporary statistics file" errors. Upgrade to PG 15+ to resolve.'
            WHEN v_platform = 'Windows' 
                THEN 'Running on Windows with PG 15+ - stats collector redesigned, bug not applicable.'
            ELSE 'Not running on Windows - bug not applicable.'
        END AS recommendation;
END;
$$;

-- Check pg_visibility vs Autovacuum Conflict (BUG-PG-VISIBILITY)
CREATE OR REPLACE FUNCTION pgmon.check_visibility_autovacuum_conflict()
RETURNS TABLE(
    autovacuum_running BOOLEAN,
    tables_being_vacuumed TEXT[],
    can_run_pg_check_visible BOOLEAN,
    risk_level TEXT,
    recommendation TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_vacuuming_tables TEXT[];
    v_autovac_running BOOLEAN;
BEGIN
    -- Check for active autovacuum
    SELECT 
        COUNT(*) > 0,
        ARRAY_AGG(DISTINCT relname)
    INTO v_autovac_running, v_vacuuming_tables
    FROM pg_stat_progress_vacuum v
    JOIN pg_class c ON c.oid = v.relid;
    
    RETURN QUERY
    SELECT 
        v_autovac_running AS autovacuum_running,
        v_vacuuming_tables AS tables_being_vacuumed,
        NOT v_autovac_running AS can_run_pg_check_visible,
        CASE 
            WHEN v_autovac_running THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level,
        CASE 
            WHEN v_autovac_running 
                THEN 'BUG-PG-VISIBILITY: Autovacuum is running. Running pg_check_visible() now may produce false positives. Wait for autovacuum to complete.'
            ELSE 'No autovacuum running. Safe to run pg_check_visible() without false positive risk.'
        END AS recommendation;
END;
$$;

-- Check ICU Collation Issues (PG16+ BUG-ICU-EMPTY-RULES)
CREATE OR REPLACE FUNCTION pgmon.check_icu_collation_issues()
RETURNS TABLE(
    collation_schema TEXT,
    collation_name TEXT,
    provider TEXT,
    deterministic BOOLEAN,
    has_empty_rules BOOLEAN,
    risk_level TEXT,
    recommendation TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    IF pgmon.get_pg_major_version() < 16 THEN
        RETURN QUERY
        SELECT 
            NULL::TEXT, NULL::TEXT, NULL::TEXT, NULL::BOOLEAN, NULL::BOOLEAN,
            'N/A'::TEXT,
            'BUG-ICU-EMPTY-RULES only affects PostgreSQL 16+. Current version not affected.'::TEXT;
        RETURN;
    END IF;
    
    RETURN QUERY
    SELECT 
        n.nspname::TEXT AS collation_schema,
        c.collname::TEXT AS collation_name,
        CASE c.collprovider
            WHEN 'i' THEN 'ICU'
            WHEN 'c' THEN 'libc'
            WHEN 'd' THEN 'default'
            ELSE 'unknown'
        END AS provider,
        c.collisdeterministic AS deterministic,
        (c.collicurules = '' OR c.collicurules IS NULL) AS has_empty_rules,
        CASE 
            WHEN c.collprovider = 'i' AND (c.collicurules = '' OR c.collicurules IS NULL) THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level,
        CASE 
            WHEN c.collprovider = 'i' AND (c.collicurules = '' OR c.collicurules IS NULL) 
                THEN 'BUG-ICU-EMPTY-RULES: ICU collation with empty rules detected. May cause unexpected sorting behavior.'
            ELSE 'Collation configuration appears normal.'
        END AS recommendation
    FROM pg_collation c
    JOIN pg_namespace n ON n.oid = c.collnamespace
    WHERE c.collprovider = 'i'
    ORDER BY has_empty_rules DESC, c.collname;
END;
$$;

-- ============================================================================
-- COMPREHENSIVE BUG REPORT
-- ============================================================================

-- Get all applicable bugs for current version
CREATE OR REPLACE FUNCTION pgmon.get_applicable_bugs()
RETURNS TABLE(
    bug_id TEXT,
    title TEXT,
    severity TEXT,
    category TEXT,
    description TEXT,
    workaround TEXT,
    wiki_url TEXT
)
LANGUAGE sql
AS $$
    SELECT 
        bug_id,
        title,
        severity,
        category,
        description,
        workaround,
        wiki_url
    FROM pgmon.known_bugs
    WHERE pgmon.get_pg_major_version() = ANY(affected_versions)
    ORDER BY 
        CASE severity
            WHEN 'CRITICAL' THEN 1
            WHEN 'HIGH' THEN 2
            WHEN 'MEDIUM' THEN 3
            ELSE 4
        END,
        category;
$$;

-- Comprehensive Open Bugs Report
CREATE OR REPLACE FUNCTION pgmon.open_bugs_report()
RETURNS TABLE(
    bug_id TEXT,
    title TEXT,
    severity TEXT,
    category TEXT,
    current_status TEXT,
    details TEXT,
    recommendation TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_version INT;
    v_is_standby BOOLEAN;
    v_prepared_xacts INT;
    v_grouping_sets INT;
BEGIN
    v_version := pgmon.get_pg_major_version();
    v_is_standby := pg_is_in_recovery();
    
    SELECT COUNT(*) INTO v_prepared_xacts FROM pg_prepared_xacts;
    
    SELECT COUNT(*) INTO v_grouping_sets
    FROM pg_stat_activity
    WHERE state = 'active'
      AND (query ILIKE '%GROUPING SETS%' OR query ILIKE '%ROLLUP%' OR query ILIKE '%CUBE%');
    
    -- Recovery Conflict Bug
    IF v_is_standby THEN
        RETURN QUERY
        SELECT 
            'BUG-RECOVERY-SIGNAL'::TEXT,
            'RecoveryConflictInterrupt() unsafe in signal handler'::TEXT,
            'CRITICAL'::TEXT,
            'Replication'::TEXT,
            'AT RISK - Running as standby'::TEXT,
            'Server is in recovery mode. Recovery conflicts may trigger unsafe signal handling.'::TEXT,
            'Monitor for unexpected crashes. Keep replication lag low. Consider hot_standby_feedback = on.'::TEXT;
    ELSE
        RETURN QUERY
        SELECT 
            'BUG-RECOVERY-SIGNAL'::TEXT,
            'RecoveryConflictInterrupt() unsafe in signal handler'::TEXT,
            'CRITICAL'::TEXT,
            'Replication'::TEXT,
            'NOT AFFECTED - Primary server'::TEXT,
            'Server is primary. Bug only affects standbys.'::TEXT,
            'No action needed for this server.'::TEXT;
    END IF;
    
    -- GROUPING SETS Bug
    IF v_grouping_sets > 0 THEN
        RETURN QUERY
        SELECT 
            'BUG-GROUPING-SETS'::TEXT,
            'Incorrect results with GROUPING SETS'::TEXT,
            'CRITICAL'::TEXT,
            'Query'::TEXT,
            'AT RISK - Active queries using GROUPING SETS'::TEXT,
            v_grouping_sets || ' active queries using GROUPING SETS/ROLLUP/CUBE detected.'::TEXT,
            'Verify query results manually. Consider alternatives like UNION ALL of simple GROUP BY.'::TEXT;
    ELSE
        RETURN QUERY
        SELECT 
            'BUG-GROUPING-SETS'::TEXT,
            'Incorrect results with GROUPING SETS'::TEXT,
            'CRITICAL'::TEXT,
            'Query'::TEXT,
            'LOW RISK - No active GROUPING SETS queries'::TEXT,
            'No current queries using GROUPING SETS detected.'::TEXT,
            'Be aware when using GROUPING SETS/ROLLUP/CUBE in future queries.'::TEXT;
    END IF;
    
    -- TOAST Bug
    RETURN QUERY
    SELECT 
        'BUG-TOAST-ACCESS'::TEXT,
        'Possible access to already-removed TOAST data'::TEXT,
        'CRITICAL'::TEXT,
        'Storage'::TEXT,
        CASE 
            WHEN EXISTS (SELECT 1 FROM pg_stat_user_tables WHERE n_dead_tup > 100000) 
            THEN 'ELEVATED RISK - High dead tuples'
            ELSE 'LOW RISK - Normal operation'
        END::TEXT,
        'Tables with high dead tuples increase risk of TOAST access issues.'::TEXT,
        'Maintain regular VACUUM schedule. Monitor for TOAST-related errors in logs.'::TEXT;
    
    -- 2PC Bug
    IF v_prepared_xacts > 0 THEN
        RETURN QUERY
        SELECT 
            'BUG-2PC-SNAPSHOT'::TEXT,
            'Incorrect snapshot calculation with 2PC'::TEXT,
            'HIGH'::TEXT,
            'Transaction'::TEXT,
            'AT RISK - ' || v_prepared_xacts || ' prepared transactions'::TEXT,
            'Two-phase commit is in use. Snapshot calculation may be affected.'::TEXT,
            'Complete prepared transactions promptly. Avoid long-lived prepared transactions.'::TEXT;
    ELSE
        RETURN QUERY
        SELECT 
            'BUG-2PC-SNAPSHOT'::TEXT,
            'Incorrect snapshot calculation with 2PC'::TEXT,
            'HIGH'::TEXT,
            'Transaction'::TEXT,
            'NOT AFFECTED - No prepared transactions'::TEXT,
            'No two-phase commit transactions detected.'::TEXT,
            'Bug only applies when using PREPARE TRANSACTION.'::TEXT;
    END IF;
    
    -- Partition Triggers Bug
    RETURN QUERY
    SELECT 
        'BUG-PARTITION-TRIGGERS'::TEXT,
        'Triggers on partitions lose tgenabled flag'::TEXT,
        'HIGH'::TEXT,
        'Partitioning'::TEXT,
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM pg_trigger t1
                JOIN pg_trigger t2 ON t1.tgparentid = t2.oid
                WHERE t1.tgenabled != t2.tgenabled
            ) THEN 'DETECTED - Trigger status mismatch found'
            ELSE 'OK - No trigger mismatches detected'
        END::TEXT,
        'Checking for trigger enable status mismatches between parent and partitions.'::TEXT,
        'Run pgmon.check_partition_trigger_status() for details.'::TEXT;
    
    -- REINDEX Catalog Bug
    RETURN QUERY
    SELECT 
        'BUG-REINDEX-CATALOG'::TEXT,
        'REINDEX on system catalog creates duplicate index entries'::TEXT,
        'HIGH'::TEXT,
        'Index'::TEXT,
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM pg_index ix
                JOIN pg_class c ON c.oid = ix.indexrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'pg_catalog' AND NOT ix.indisvalid
            ) THEN 'CRITICAL - Invalid system index detected!'
            ELSE 'OK - System indexes appear valid'
        END::TEXT,
        'Checking system catalog index validity.'::TEXT,
        'Avoid REINDEX SYSTEM unless necessary. Use pg_amcheck for validation.'::TEXT;
    
    -- Windows Stats Bug (PG 13/14 only)
    IF v_version IN (13, 14) THEN
        RETURN QUERY
        SELECT 
            'BUG-WINDOWS-STATS'::TEXT,
            'Failure to rename temporary statistics file on Windows'::TEXT,
            'MEDIUM'::TEXT,
            'Statistics'::TEXT,
            'POTENTIALLY AFFECTED - Running PG ' || v_version::TEXT,
            'PostgreSQL 13/14 on Windows may experience stats file rename errors.'::TEXT,
            'Check logs for "could not rename temporary statistics file". Upgrade to PG 15+ to resolve.'::TEXT;
    END IF;
    
    -- pg_visibility Bug
    RETURN QUERY
    SELECT 
        'BUG-PG-VISIBILITY'::TEXT,
        'pg_check_visible() false positives with autovacuum'::TEXT,
        'LOW'::TEXT,
        'Maintenance'::TEXT,
        CASE 
            WHEN EXISTS (SELECT 1 FROM pg_stat_progress_vacuum)
            THEN 'CAUTION - Autovacuum running'
            ELSE 'OK - Safe to run pg_check_visible'
        END::TEXT,
        'pg_visibility checks may report false positives during autovacuum.'::TEXT,
        'Run pg_check_visible() when autovacuum is not active on target tables.'::TEXT;
    
    -- ICU Bug (PG 16 only)
    IF v_version >= 16 THEN
        RETURN QUERY
        SELECT 
            'BUG-ICU-EMPTY-RULES'::TEXT,
            'Strange behavior when ICU rules are empty string'::TEXT,
            'MEDIUM'::TEXT,
            'Collation'::TEXT,
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM pg_collation 
                    WHERE collprovider = 'i' AND (collicurules = '' OR collicurules IS NULL)
                ) THEN 'AT RISK - ICU collations with empty rules found'
                ELSE 'OK - No problematic ICU collations'
            END::TEXT,
            'ICU collations with empty rules may have unexpected sorting behavior.'::TEXT,
            'Avoid empty string for ICU collation rules.'::TEXT;
    END IF;
    
    RETURN;
END;
$$;

-- Quick Bug Status Summary
CREATE OR REPLACE FUNCTION pgmon.bug_status_summary()
RETURNS TABLE(
    total_known_bugs INT,
    critical_bugs INT,
    high_bugs INT,
    bugs_at_risk INT,
    pg_version INT,
    recommendation TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_total INT;
    v_critical INT;
    v_high INT;
    v_at_risk INT;
BEGIN
    v_total := (SELECT COUNT(*) FROM pgmon.known_bugs WHERE pgmon.get_pg_major_version() = ANY(affected_versions));
    v_critical := (SELECT COUNT(*) FROM pgmon.known_bugs WHERE pgmon.get_pg_major_version() = ANY(affected_versions) AND severity = 'CRITICAL');
    v_high := (SELECT COUNT(*) FROM pgmon.known_bugs WHERE pgmon.get_pg_major_version() = ANY(affected_versions) AND severity = 'HIGH');
    
    -- Count bugs we're actually at risk for
    v_at_risk := 0;
    
    IF pg_is_in_recovery() THEN v_at_risk := v_at_risk + 1; END IF; -- Recovery conflict
    IF EXISTS (SELECT 1 FROM pg_stat_activity WHERE query ILIKE '%GROUPING SETS%') THEN v_at_risk := v_at_risk + 1; END IF;
    IF EXISTS (SELECT 1 FROM pg_prepared_xacts) THEN v_at_risk := v_at_risk + 1; END IF;
    IF EXISTS (SELECT 1 FROM pg_stat_user_tables WHERE n_dead_tup > 100000) THEN v_at_risk := v_at_risk + 1; END IF;
    
    RETURN QUERY
    SELECT 
        v_total,
        v_critical,
        v_high,
        v_at_risk,
        pgmon.get_pg_major_version(),
        CASE 
            WHEN v_at_risk > 2 THEN 'HIGH EXPOSURE: Multiple open bugs may affect your workload. Review pgmon.open_bugs_report() immediately.'
            WHEN v_at_risk > 0 THEN 'MODERATE EXPOSURE: Some open bugs may affect your workload. Review pgmon.open_bugs_report().'
            ELSE 'LOW EXPOSURE: Known bugs are unlikely to affect current workload, but stay vigilant.'
        END AS recommendation;
END;
$$;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE pgmon.known_bugs IS 'Registry of known PostgreSQL open bugs affecting supported versions';
COMMENT ON FUNCTION pgmon.check_recovery_conflict_risk() IS 'Detects risk from BUG-RECOVERY-SIGNAL (signal handler crash on standbys)';
COMMENT ON FUNCTION pgmon.check_grouping_sets_usage() IS 'Detects usage of GROUPING SETS affected by BUG-GROUPING-SETS';
COMMENT ON FUNCTION pgmon.check_grouping_sets_in_history() IS 'Scans pg_stat_statements for historical GROUPING SETS usage';
COMMENT ON FUNCTION pgmon.check_toast_issues() IS 'Checks for conditions that increase BUG-TOAST-ACCESS risk';
COMMENT ON FUNCTION pgmon.check_partition_trigger_status() IS 'Detects BUG-PARTITION-TRIGGERS trigger status mismatches';
COMMENT ON FUNCTION pgmon.check_2pc_usage() IS 'Detects two-phase commit usage affected by BUG-2PC-SNAPSHOT';
COMMENT ON FUNCTION pgmon.check_catalog_index_health() IS 'Checks system catalog indexes for BUG-REINDEX-CATALOG issues';
COMMENT ON FUNCTION pgmon.check_stats_collector_errors() IS 'Checks for BUG-WINDOWS-STATS (Windows PG 13/14)';
COMMENT ON FUNCTION pgmon.check_visibility_autovacuum_conflict() IS 'Detects when BUG-PG-VISIBILITY may cause false positives';
COMMENT ON FUNCTION pgmon.check_icu_collation_issues() IS 'Detects BUG-ICU-EMPTY-RULES in PostgreSQL 16+';
COMMENT ON FUNCTION pgmon.get_applicable_bugs() IS 'Lists all known bugs affecting current PostgreSQL version';
COMMENT ON FUNCTION pgmon.open_bugs_report() IS 'Comprehensive report of open bugs and current exposure level';
COMMENT ON FUNCTION pgmon.bug_status_summary() IS 'Quick summary of bug exposure status';

-- Grant permissions
GRANT SELECT ON pgmon.known_bugs TO PUBLIC;
