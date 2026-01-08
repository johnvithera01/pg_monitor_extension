-- pg_monitor Extension Lite for Docker
-- Core functions for monitoring and AI integration
-- Version: 0.9.0-lite

-- ============================================================
-- CREATE SCHEMA
-- ============================================================

CREATE SCHEMA IF NOT EXISTS pgmon;

-- ============================================================
-- EXTENSION TABLES (additional to basic schema)
-- ============================================================

-- Alert history table
CREATE TABLE IF NOT EXISTS pgmon.alert_history (
    id SERIAL PRIMARY KEY,
    alert_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Query metrics table (for AI analysis)
CREATE TABLE IF NOT EXISTS pgmon.query_metrics (
    id SERIAL PRIMARY KEY,
    query_hash VARCHAR(64) NOT NULL,
    query_normalized TEXT NOT NULL,
    total_calls BIGINT DEFAULT 1,
    total_time_ms NUMERIC(15,3) DEFAULT 0,
    min_time_ms NUMERIC(15,3),
    max_time_ms NUMERIC(15,3),
    avg_time_ms NUMERIC(15,3),
    total_rows BIGINT DEFAULT 0,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(query_hash)
);

-- AI suggestions storage
CREATE TABLE IF NOT EXISTS pgmon.ai_suggestions (
    id SERIAL PRIMARY KEY,
    query_hash VARCHAR(64) NOT NULL,
    suggestions JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    applied_at TIMESTAMP,
    UNIQUE(query_hash)
);

-- Metrics history for trends
CREATE TABLE IF NOT EXISTS pgmon.metrics_history (
    id SERIAL PRIMARY KEY,
    captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    database_name TEXT,
    database_size_bytes BIGINT,
    active_connections INTEGER,
    idle_connections INTEGER,
    idle_in_transaction INTEGER,
    waiting_connections INTEGER,
    max_transaction_age BIGINT,
    cache_hit_ratio NUMERIC(5,2),
    commits_per_sec NUMERIC(10,2),
    rollbacks_per_sec NUMERIC(10,2),
    blocks_read_per_sec NUMERIC(10,2),
    blocks_hit_per_sec NUMERIC(10,2),
    temp_files INTEGER,
    temp_bytes BIGINT,
    deadlocks INTEGER,
    checksum_failures INTEGER
);

-- Table metrics history
CREATE TABLE IF NOT EXISTS pgmon.table_metrics_history (
    id SERIAL PRIMARY KEY,
    captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_name TEXT,
    table_name TEXT,
    table_size_bytes BIGINT,
    index_size_bytes BIGINT,
    toast_size_bytes BIGINT,
    live_tuples BIGINT,
    dead_tuples BIGINT,
    n_tup_ins BIGINT,
    n_tup_upd BIGINT,
    n_tup_del BIGINT,
    n_tup_hot_upd BIGINT,
    seq_scan BIGINT,
    idx_scan BIGINT,
    last_vacuum TIMESTAMP,
    last_autovacuum TIMESTAMP,
    last_analyze TIMESTAMP,
    last_autoanalyze TIMESTAMP
);

-- Update activity tracking (for REPACK analysis)
CREATE TABLE IF NOT EXISTS pgmon.update_activity (
    id SERIAL PRIMARY KEY,
    captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    n_tup_upd BIGINT,
    n_tup_del BIGINT,
    n_tup_hot_upd BIGINT,
    n_live_tup BIGINT,
    n_dead_tup BIGINT,
    table_size_bytes BIGINT,
    bloat_estimate_bytes BIGINT
);

-- REPACK history
CREATE TABLE IF NOT EXISTS pgmon.repack_history (
    id SERIAL PRIMARY KEY,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    method TEXT DEFAULT 'pg_repack',
    size_before_bytes BIGINT,
    size_after_bytes BIGINT,
    duration_seconds NUMERIC(10,2),
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT
);

-- Known PostgreSQL Bugs Registry (Version 0.9.0)
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

-- Indexes
CREATE INDEX IF NOT EXISTS idx_query_metrics_hash ON pgmon.query_metrics(query_hash);
CREATE INDEX IF NOT EXISTS idx_query_metrics_last_seen ON pgmon.query_metrics(last_seen);
CREATE INDEX IF NOT EXISTS idx_metrics_history_captured ON pgmon.metrics_history(captured_at);
CREATE INDEX IF NOT EXISTS idx_table_metrics_history_captured ON pgmon.table_metrics_history(captured_at);
CREATE INDEX IF NOT EXISTS idx_table_metrics_history_table ON pgmon.table_metrics_history(schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_update_activity_captured ON pgmon.update_activity(captured_at);
CREATE INDEX IF NOT EXISTS idx_update_activity_table ON pgmon.update_activity(schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_repack_history_table ON pgmon.repack_history(schema_name, table_name);

-- ============================================================
-- CORE MONITORING FUNCTIONS
-- ============================================================

-- Test function
CREATE OR REPLACE FUNCTION pgmon.hello()
RETURNS TEXT AS $$
BEGIN
    RETURN 'pg_monitor extension v0.9.0-lite is working!';
END;
$$ LANGUAGE plpgsql;

-- Check slow queries
CREATE OR REPLACE FUNCTION pgmon.check_slow_queries(threshold_seconds INTEGER DEFAULT 5)
RETURNS TABLE (
    pid INTEGER,
    duration INTERVAL,
    query TEXT,
    state TEXT,
    wait_event_type TEXT,
    wait_event TEXT,
    username TEXT,
    database TEXT,
    client_addr INET
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        a.pid,
        now() - a.query_start as duration,
        a.query,
        a.state,
        a.wait_event_type,
        a.wait_event,
        a.usename::TEXT,
        a.datname::TEXT,
        a.client_addr
    FROM pg_stat_activity a
    WHERE a.state = 'active'
      AND a.query NOT LIKE '%pg_stat_activity%'
      AND now() - a.query_start > (threshold_seconds || ' seconds')::interval
    ORDER BY duration DESC;
END;
$$ LANGUAGE plpgsql;

-- Get active connections
CREATE OR REPLACE FUNCTION pgmon.get_active_connections()
RETURNS TABLE (
    pid INTEGER,
    username TEXT,
    database TEXT,
    client_addr INET,
    state TEXT,
    query TEXT,
    backend_start TIMESTAMP WITH TIME ZONE,
    query_start TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        a.pid,
        a.usename::TEXT,
        a.datname::TEXT,
        a.client_addr,
        a.state,
        a.query,
        a.backend_start,
        a.query_start
    FROM pg_stat_activity a
    WHERE a.pid <> pg_backend_pid()
    ORDER BY a.backend_start;
END;
$$ LANGUAGE plpgsql;

-- Cache hit ratio
CREATE OR REPLACE FUNCTION pgmon.cache_hit_ratio()
RETURNS TABLE (
    database_name TEXT,
    heap_hit_ratio NUMERIC,
    index_hit_ratio NUMERIC,
    status TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        d.datname::TEXT,
        ROUND(COALESCE(100.0 * d.blks_hit / NULLIF(d.blks_hit + d.blks_read, 0), 0), 2) as heap_hit_ratio,
        ROUND(COALESCE(
            (SELECT 100.0 * sum(idx_blks_hit) / NULLIF(sum(idx_blks_hit) + sum(idx_blks_read), 0) 
             FROM pg_statio_user_indexes), 0), 2) as index_hit_ratio,
        CASE 
            WHEN COALESCE(100.0 * d.blks_hit / NULLIF(d.blks_hit + d.blks_read, 0), 0) >= 95 THEN 'GOOD'
            WHEN COALESCE(100.0 * d.blks_hit / NULLIF(d.blks_hit + d.blks_read, 0), 0) >= 90 THEN 'WARNING'
            ELSE 'CRITICAL'
        END as status
    FROM pg_stat_database d
    WHERE d.datname NOT LIKE 'template%'
    ORDER BY d.datname;
END;
$$ LANGUAGE plpgsql;

-- Table bloat estimate
CREATE OR REPLACE FUNCTION pgmon.table_bloat_estimate()
RETURNS TABLE (
    schemaname TEXT,
    tablename TEXT,
    real_size TEXT,
    bloat_size TEXT,
    bloat_ratio NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        n.nspname::TEXT,
        c.relname::TEXT,
        pg_size_pretty(pg_relation_size(c.oid)) as real_size,
        pg_size_pretty(GREATEST(pg_relation_size(c.oid) - 
            (c.reltuples * 
                (SELECT avg(ps.avg_width) FROM pg_stats ps WHERE ps.schemaname = n.nspname AND ps.tablename = c.relname)
            )::BIGINT, 0)) as bloat_size,
        ROUND(CASE 
            WHEN pg_relation_size(c.oid) = 0 THEN 0
            ELSE GREATEST(
                (pg_relation_size(c.oid) - 
                    (c.reltuples * 
                        COALESCE((SELECT avg(ps.avg_width) FROM pg_stats ps WHERE ps.schemaname = n.nspname AND ps.tablename = c.relname), 100)
                    )::BIGINT
                )::NUMERIC / pg_relation_size(c.oid) * 100, 0)
        END, 2) as bloat_ratio
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
      AND c.reltuples > 0
    ORDER BY pg_relation_size(c.oid) DESC
    LIMIT 20;
END;
$$ LANGUAGE plpgsql;

-- Health check
CREATE OR REPLACE FUNCTION pgmon.health_check()
RETURNS TABLE (
    check_name TEXT,
    status TEXT,
    value TEXT,
    details TEXT
) AS $$
DECLARE
    v_cache_ratio NUMERIC;
    v_active_conn INTEGER;
    v_idle_tx INTEGER;
    v_db_size BIGINT;
    v_oldest_xact BIGINT;
BEGIN
    -- Cache hit ratio
    SELECT ROUND(COALESCE(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 100), 2)
    INTO v_cache_ratio
    FROM pg_stat_database WHERE datname = current_database();
    
    RETURN QUERY SELECT 
        'Cache Hit Ratio'::TEXT,
        CASE WHEN v_cache_ratio >= 95 THEN 'OK' WHEN v_cache_ratio >= 90 THEN 'WARNING' ELSE 'CRITICAL' END,
        v_cache_ratio::TEXT || '%',
        'Should be > 95%'::TEXT;
    
    -- Active connections
    SELECT count(*) INTO v_active_conn
    FROM pg_stat_activity WHERE state = 'active';
    
    RETURN QUERY SELECT 
        'Active Connections'::TEXT,
        CASE WHEN v_active_conn < 50 THEN 'OK' WHEN v_active_conn < 100 THEN 'WARNING' ELSE 'CRITICAL' END,
        v_active_conn::TEXT,
        'Active queries running'::TEXT;
    
    -- Idle in transaction
    SELECT count(*) INTO v_idle_tx
    FROM pg_stat_activity WHERE state = 'idle in transaction';
    
    RETURN QUERY SELECT 
        'Idle in Transaction'::TEXT,
        CASE WHEN v_idle_tx = 0 THEN 'OK' WHEN v_idle_tx < 5 THEN 'WARNING' ELSE 'CRITICAL' END,
        v_idle_tx::TEXT,
        'Should be 0'::TEXT;
    
    -- Database size
    SELECT pg_database_size(current_database()) INTO v_db_size;
    
    RETURN QUERY SELECT 
        'Database Size'::TEXT,
        'INFO'::TEXT,
        pg_size_pretty(v_db_size),
        current_database()::TEXT;
    
    -- Oldest transaction
    SELECT COALESCE(max(age(backend_xid)), 0) INTO v_oldest_xact
    FROM pg_stat_activity WHERE backend_xid IS NOT NULL;
    
    RETURN QUERY SELECT 
        'Oldest Transaction Age'::TEXT,
        CASE WHEN v_oldest_xact < 1000000 THEN 'OK' WHEN v_oldest_xact < 10000000 THEN 'WARNING' ELSE 'CRITICAL' END,
        v_oldest_xact::TEXT,
        'XID age (wraparound at 2B)'::TEXT;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- INDEX ANALYSIS FUNCTIONS
-- ============================================================

-- Unused indexes
CREATE OR REPLACE FUNCTION pgmon.unused_indexes(min_size_mb INTEGER DEFAULT 1)
RETURNS TABLE (
    schemaname TEXT,
    tablename TEXT,
    indexname TEXT,
    index_size TEXT,
    index_scans BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.schemaname::TEXT,
        s.relname::TEXT,
        s.indexrelname::TEXT,
        pg_size_pretty(pg_relation_size(i.indexrelid)) as index_size,
        s.idx_scan
    FROM pg_stat_user_indexes s
    JOIN pg_index i ON s.indexrelid = i.indexrelid
    WHERE s.idx_scan = 0
      AND NOT i.indisprimary
      AND NOT i.indisunique
      AND pg_relation_size(s.indexrelid) > (min_size_mb * 1024 * 1024)
    ORDER BY pg_relation_size(s.indexrelid) DESC;
END;
$$ LANGUAGE plpgsql;

-- Tables needing vacuum
CREATE OR REPLACE FUNCTION pgmon.tables_needing_vacuum(dead_tuple_threshold INTEGER DEFAULT 10000)
RETURNS TABLE (
    schemaname TEXT,
    tablename TEXT,
    n_live_tup BIGINT,
    n_dead_tup BIGINT,
    dead_tuple_percent NUMERIC,
    last_vacuum TIMESTAMP WITH TIME ZONE,
    last_autovacuum TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.schemaname::TEXT,
        s.relname::TEXT,
        s.n_live_tup,
        s.n_dead_tup,
        ROUND(100.0 * s.n_dead_tup / NULLIF(s.n_live_tup + s.n_dead_tup, 0), 2) as dead_tuple_percent,
        s.last_vacuum,
        s.last_autovacuum
    FROM pg_stat_user_tables s
    WHERE s.n_dead_tup > dead_tuple_threshold
    ORDER BY s.n_dead_tup DESC;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- HIGH UPDATE/DELETE WORKLOAD FUNCTIONS (REPACK ANALYSIS)
-- ============================================================

-- High update tables
CREATE OR REPLACE FUNCTION pgmon.high_update_tables(min_updates BIGINT DEFAULT 10000)
RETURNS TABLE (
    schemaname TEXT,
    tablename TEXT,
    n_tup_upd BIGINT,
    n_tup_del BIGINT,
    n_tup_hot_upd BIGINT,
    hot_update_ratio NUMERIC,
    n_live_tup BIGINT,
    n_dead_tup BIGINT,
    table_size TEXT,
    update_intensity TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.schemaname::TEXT,
        s.relname::TEXT,
        s.n_tup_upd,
        s.n_tup_del,
        s.n_tup_hot_upd,
        ROUND(100.0 * s.n_tup_hot_upd / NULLIF(s.n_tup_upd, 0), 2) as hot_update_ratio,
        s.n_live_tup,
        s.n_dead_tup,
        pg_size_pretty(pg_relation_size(s.relid)) as table_size,
        CASE 
            WHEN s.n_tup_upd + s.n_tup_del > s.n_live_tup * 10 THEN 'EXTREME'
            WHEN s.n_tup_upd + s.n_tup_del > s.n_live_tup * 5 THEN 'VERY_HIGH'
            WHEN s.n_tup_upd + s.n_tup_del > s.n_live_tup THEN 'HIGH'
            ELSE 'MODERATE'
        END as update_intensity
    FROM pg_stat_user_tables s
    WHERE s.n_tup_upd > min_updates
    ORDER BY s.n_tup_upd DESC;
END;
$$ LANGUAGE plpgsql;

-- HOT update efficiency
CREATE OR REPLACE FUNCTION pgmon.hot_update_efficiency()
RETURNS TABLE (
    schemaname TEXT,
    tablename TEXT,
    total_updates BIGINT,
    hot_updates BIGINT,
    hot_update_ratio NUMERIC,
    efficiency_status TEXT,
    recommendation TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        s.schemaname::TEXT,
        s.relname::TEXT,
        s.n_tup_upd,
        s.n_tup_hot_upd,
        ROUND(100.0 * s.n_tup_hot_upd / NULLIF(s.n_tup_upd, 0), 2) as hot_update_ratio,
        CASE 
            WHEN 100.0 * s.n_tup_hot_upd / NULLIF(s.n_tup_upd, 0) >= 90 THEN 'EXCELLENT'
            WHEN 100.0 * s.n_tup_hot_upd / NULLIF(s.n_tup_upd, 0) >= 70 THEN 'GOOD'
            WHEN 100.0 * s.n_tup_hot_upd / NULLIF(s.n_tup_upd, 0) >= 50 THEN 'MODERATE'
            ELSE 'POOR'
        END as efficiency_status,
        CASE 
            WHEN 100.0 * s.n_tup_hot_upd / NULLIF(s.n_tup_upd, 0) < 50 
            THEN 'Consider reducing fillfactor or reviewing indexes on frequently updated columns'
            WHEN 100.0 * s.n_tup_hot_upd / NULLIF(s.n_tup_upd, 0) < 70 
            THEN 'May benefit from fillfactor adjustment'
            ELSE 'HOT updates working efficiently'
        END as recommendation
    FROM pg_stat_user_tables s
    WHERE s.n_tup_upd > 1000
    ORDER BY s.n_tup_upd DESC;
END;
$$ LANGUAGE plpgsql;

-- REPACK recommendations
CREATE OR REPLACE FUNCTION pgmon.repack_recommendations()
RETURNS TABLE (
    priority INTEGER,
    schemaname TEXT,
    tablename TEXT,
    table_size TEXT,
    bloat_estimate TEXT,
    dead_tuple_percent NUMERIC,
    update_rate BIGINT,
    hot_update_ratio NUMERIC,
    recommendation TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH table_stats AS (
        SELECT 
            s.schemaname,
            s.relname,
            pg_relation_size(s.relid) as table_size_bytes,
            s.n_dead_tup,
            s.n_live_tup,
            s.n_tup_upd,
            s.n_tup_del,
            s.n_tup_hot_upd,
            ROUND(100.0 * s.n_dead_tup / NULLIF(s.n_live_tup + s.n_dead_tup, 0), 2) as dead_pct,
            ROUND(100.0 * s.n_tup_hot_upd / NULLIF(s.n_tup_upd, 0), 2) as hot_ratio
        FROM pg_stat_user_tables s
        WHERE s.n_live_tup > 0
    )
    SELECT 
        ROW_NUMBER() OVER (ORDER BY 
            CASE 
                WHEN ts.dead_pct > 30 AND ts.table_size_bytes > 100*1024*1024 THEN 1
                WHEN ts.dead_pct > 20 AND ts.table_size_bytes > 50*1024*1024 THEN 2
                WHEN ts.dead_pct > 10 AND ts.n_tup_upd > 100000 THEN 3
                WHEN ts.hot_ratio < 50 AND ts.n_tup_upd > 50000 THEN 4
                ELSE 5
            END,
            ts.table_size_bytes DESC
        )::INTEGER as priority,
        ts.schemaname::TEXT,
        ts.relname::TEXT,
        pg_size_pretty(ts.table_size_bytes),
        pg_size_pretty((ts.table_size_bytes * COALESCE(ts.dead_pct, 0) / 100)::BIGINT) as bloat_estimate,
        ts.dead_pct,
        ts.n_tup_upd + ts.n_tup_del,
        ts.hot_ratio,
        CASE 
            WHEN ts.dead_pct > 30 THEN 'URGENT: High bloat - REPACK immediately'
            WHEN ts.dead_pct > 20 THEN 'HIGH: Schedule REPACK soon'
            WHEN ts.dead_pct > 10 AND ts.n_tup_upd > 100000 THEN 'MEDIUM: Consider REPACK'
            WHEN ts.hot_ratio < 50 AND ts.n_tup_upd > 50000 THEN 'MEDIUM: Low HOT ratio - consider fillfactor'
            ELSE 'LOW: Monitor'
        END as recommendation
    FROM table_stats ts
    WHERE ts.dead_pct > 5 OR (ts.n_tup_upd > 50000 AND ts.hot_ratio < 70)
    ORDER BY priority
    LIMIT 20;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- SNAPSHOT CAPTURE FUNCTIONS
-- ============================================================

-- Capture metrics snapshot
CREATE OR REPLACE FUNCTION pgmon.capture_metrics_snapshot()
RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    INSERT INTO pgmon.metrics_history (
        database_name,
        database_size_bytes,
        active_connections,
        idle_connections,
        idle_in_transaction,
        waiting_connections,
        max_transaction_age,
        cache_hit_ratio,
        deadlocks
    )
    SELECT 
        current_database(),
        pg_database_size(current_database()),
        (SELECT count(*) FROM pg_stat_activity WHERE state = 'active'),
        (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle'),
        (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction'),
        (SELECT count(*) FROM pg_stat_activity WHERE wait_event IS NOT NULL),
        (SELECT COALESCE(max(age(backend_xid)), 0) FROM pg_stat_activity),
        (SELECT ROUND(COALESCE(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 100), 2) FROM pg_stat_database WHERE datname = current_database()),
        (SELECT deadlocks FROM pg_stat_database WHERE datname = current_database())
    RETURNING id INTO v_id;
    
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

-- Capture update activity snapshot
CREATE OR REPLACE FUNCTION pgmon.capture_update_activity_snapshot()
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER := 0;
BEGIN
    INSERT INTO pgmon.update_activity (
        schema_name,
        table_name,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        n_live_tup,
        n_dead_tup,
        table_size_bytes
    )
    SELECT 
        schemaname,
        relname,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        n_live_tup,
        n_dead_tup,
        pg_relation_size(relid)
    FROM pg_stat_user_tables
    WHERE n_tup_upd > 0 OR n_tup_del > 0;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- AI COMPREHENSIVE ANALYSIS FUNCTION
-- ============================================================

CREATE OR REPLACE FUNCTION pgmon.ai_comprehensive_analysis()
RETURNS JSONB AS $$
DECLARE
    v_result JSONB;
BEGIN
    SELECT jsonb_build_object(
        'timestamp', now(),
        'database_info', jsonb_build_object(
            'name', current_database(),
            'version', version(),
            'size', pg_size_pretty(pg_database_size(current_database())),
            'size_bytes', pg_database_size(current_database())
        ),
        'health_check', (
            SELECT jsonb_agg(jsonb_build_object(
                'check', check_name,
                'status', status,
                'value', value,
                'details', details
            ))
            FROM pgmon.health_check()
        ),
        'connections', jsonb_build_object(
            'total', (SELECT count(*) FROM pg_stat_activity),
            'active', (SELECT count(*) FROM pg_stat_activity WHERE state = 'active'),
            'idle', (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle'),
            'idle_in_transaction', (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction')
        ),
        'slow_queries', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'pid', pid,
                'duration', duration::TEXT,
                'query', LEFT(query, 200),
                'user', username
            )), '[]'::jsonb)
            FROM pgmon.check_slow_queries(5)
            LIMIT 10
        ),
        'tables_needing_vacuum', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'schema', schemaname,
                'table', tablename,
                'dead_tuples', n_dead_tup,
                'dead_percent', dead_tuple_percent
            )), '[]'::jsonb)
            FROM pgmon.tables_needing_vacuum(1000)
            LIMIT 10
        ),
        'high_update_tables', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'schema', schemaname,
                'table', tablename,
                'updates', n_tup_upd,
                'deletes', n_tup_del,
                'hot_ratio', hot_update_ratio,
                'intensity', update_intensity
            )), '[]'::jsonb)
            FROM pgmon.high_update_tables(1000)
            LIMIT 10
        ),
        'repack_recommendations', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'priority', priority,
                'schema', schemaname,
                'table', tablename,
                'size', table_size,
                'dead_percent', dead_tuple_percent,
                'recommendation', recommendation
            )), '[]'::jsonb)
            FROM pgmon.repack_recommendations()
            LIMIT 10
        ),
        'unused_indexes', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'schema', schemaname,
                'table', tablename,
                'index', indexname,
                'size', index_size
            )), '[]'::jsonb)
            FROM pgmon.unused_indexes(1)
            LIMIT 10
        )
    ) INTO v_result;
    
    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- AI REPACK Analysis
CREATE OR REPLACE FUNCTION pgmon.ai_repack_analysis()
RETURNS JSONB AS $$
DECLARE
    v_result JSONB;
BEGIN
    SELECT jsonb_build_object(
        'timestamp', now(),
        'analysis_type', 'repack_recommendation',
        'high_update_tables', (
            SELECT COALESCE(jsonb_agg(row_to_json(t)), '[]'::jsonb)
            FROM pgmon.high_update_tables(1000) t
        ),
        'hot_update_efficiency', (
            SELECT COALESCE(jsonb_agg(row_to_json(t)), '[]'::jsonb)
            FROM pgmon.hot_update_efficiency() t
        ),
        'repack_recommendations', (
            SELECT COALESCE(jsonb_agg(row_to_json(t)), '[]'::jsonb)
            FROM pgmon.repack_recommendations() t
        ),
        'recent_repack_history', (
            SELECT COALESCE(jsonb_agg(rh.data), '[]'::jsonb)
            FROM (
                SELECT jsonb_build_object(
                    'table', schema_name || '.' || table_name,
                    'executed_at', executed_at,
                    'size_before', pg_size_pretty(size_before_bytes),
                    'size_after', pg_size_pretty(size_after_bytes),
                    'reduction_percent', ROUND(100.0 * (size_before_bytes - size_after_bytes) / NULLIF(size_before_bytes, 0), 2),
                    'success', success
                ) as data
                FROM pgmon.repack_history
                WHERE executed_at > now() - interval '30 days'
                ORDER BY executed_at DESC
                LIMIT 10
            ) rh
        ),
        'summary', jsonb_build_object(
            'tables_need_repack', (SELECT count(*) FROM pgmon.repack_recommendations() WHERE priority <= 3),
            'tables_high_update', (SELECT count(*) FROM pgmon.high_update_tables(10000)),
            'tables_poor_hot_ratio', (SELECT count(*) FROM pgmon.hot_update_efficiency() WHERE efficiency_status IN ('POOR', 'MODERATE'))
        )
    ) INTO v_result;
    
    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- Record REPACK execution
CREATE OR REPLACE FUNCTION pgmon.record_repack_execution(
    p_schema TEXT,
    p_table TEXT,
    p_method TEXT DEFAULT 'pg_repack',
    p_size_before BIGINT DEFAULT NULL,
    p_size_after BIGINT DEFAULT NULL,
    p_duration NUMERIC DEFAULT NULL,
    p_success BOOLEAN DEFAULT TRUE,
    p_error TEXT DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    INSERT INTO pgmon.repack_history (
        schema_name, table_name, method,
        size_before_bytes, size_after_bytes,
        duration_seconds, success, error_message
    ) VALUES (
        p_schema, p_table, p_method,
        p_size_before, p_size_after,
        p_duration, p_success, p_error
    )
    RETURNING id INTO v_id;
    
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- BUG DETECTION FUNCTIONS (Version 0.9.0)
-- ============================================================

-- Populate known bugs
INSERT INTO pgmon.known_bugs (bug_id, title, affected_versions, severity, category, description, workaround, wiki_url, detection_function) VALUES
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
 'pgmon.check_visibility_autovacuum_conflict')
ON CONFLICT (bug_id) DO UPDATE SET
    title = EXCLUDED.title,
    affected_versions = EXCLUDED.affected_versions,
    severity = EXCLUDED.severity,
    description = EXCLUDED.description,
    workaround = EXCLUDED.workaround;

-- Get PostgreSQL major version
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

-- Get all applicable bugs for current PostgreSQL version
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
            WHEN 'LOW' THEN 4
        END,
        bug_id;
$$;

-- Bug status summary
CREATE OR REPLACE FUNCTION pgmon.bug_status_summary()
RETURNS TABLE(
    total_known_bugs INTEGER,
    critical_bugs INTEGER,
    high_bugs INTEGER,
    bugs_at_risk INTEGER,
    pg_version INTEGER,
    recommendation TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_total INTEGER;
    v_critical INTEGER;
    v_high INTEGER;
    v_at_risk INTEGER;
    v_version INTEGER;
BEGIN
    v_version := pgmon.get_pg_major_version();
    
    SELECT 
        COUNT(*)::INTEGER,
        COUNT(*) FILTER (WHERE severity = 'CRITICAL')::INTEGER,
        COUNT(*) FILTER (WHERE severity = 'HIGH')::INTEGER
    INTO v_total, v_critical, v_high
    FROM pgmon.known_bugs
    WHERE v_version = ANY(affected_versions);
    
    -- Simplified risk assessment (always 0 for lite version without complex detection)
    v_at_risk := 0;
    
    RETURN QUERY SELECT
        v_total,
        v_critical,
        v_high,
        v_at_risk,
        v_version,
        CASE 
            WHEN v_at_risk > 0 THEN 'HIGH EXPOSURE: Active bugs detected. Review recommendations immediately.'
            WHEN v_critical > 0 THEN 'MODERATE EXPOSURE: Critical bugs exist in your PG version. Monitor workload.'
            ELSE 'LOW EXPOSURE: Known bugs are unlikely to affect current workload, but stay vigilant.'
        END;
END;
$$;

-- Open bugs report
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
BEGIN
    RETURN QUERY
    SELECT 
        kb.bug_id,
        kb.title,
        kb.severity,
        kb.category,
        CASE 
            WHEN kb.bug_id = 'BUG-RECOVERY-SIGNAL' THEN
                CASE WHEN (SELECT pg_is_in_recovery()) THEN 'POTENTIALLY AFFECTED - Standby server' ELSE 'NOT AFFECTED - Primary server' END
            WHEN kb.bug_id = 'BUG-2PC-SNAPSHOT' THEN
                CASE WHEN (SELECT count(*) FROM pg_prepared_xacts) > 0 THEN 'POTENTIALLY AFFECTED - Has prepared transactions' ELSE 'NOT AFFECTED - No prepared transactions' END
            WHEN kb.bug_id = 'BUG-WINDOWS-STATS' THEN
                CASE WHEN pgmon.get_pg_major_version() IN (13, 14) THEN 'POTENTIALLY AFFECTED - Running PG 13/14' ELSE 'NOT AFFECTED - PG 15+' END
            ELSE 'POTENTIALLY AFFECTED'
        END as current_status,
        COALESCE(kb.description, 'No details available') as details,
        COALESCE(kb.workaround, 'Monitor for related issues') as recommendation
    FROM pgmon.known_bugs kb
    WHERE pgmon.get_pg_major_version() = ANY(kb.affected_versions)
    ORDER BY 
        CASE kb.severity
            WHEN 'CRITICAL' THEN 1
            WHEN 'HIGH' THEN 2
            WHEN 'MEDIUM' THEN 3
            WHEN 'LOW' THEN 4
        END,
        kb.bug_id;
END;
$$;

-- ============================================================
-- CLEANUP FUNCTIONS
-- ============================================================

CREATE OR REPLACE FUNCTION pgmon.cleanup_history(days_to_keep INTEGER DEFAULT 30)
RETURNS TABLE (
    table_name TEXT,
    rows_deleted BIGINT
) AS $$
DECLARE
    v_cutoff TIMESTAMP := now() - (days_to_keep || ' days')::interval;
    v_deleted BIGINT;
BEGIN
    -- Clean metrics_history
    DELETE FROM pgmon.metrics_history WHERE captured_at < v_cutoff;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    table_name := 'metrics_history'; rows_deleted := v_deleted;
    RETURN NEXT;
    
    -- Clean table_metrics_history
    DELETE FROM pgmon.table_metrics_history WHERE captured_at < v_cutoff;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    table_name := 'table_metrics_history'; rows_deleted := v_deleted;
    RETURN NEXT;
    
    -- Clean update_activity
    DELETE FROM pgmon.update_activity WHERE captured_at < v_cutoff;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    table_name := 'update_activity'; rows_deleted := v_deleted;
    RETURN NEXT;
    
    -- Clean alert_history
    DELETE FROM pgmon.alert_history WHERE created_at < v_cutoff;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    table_name := 'alert_history'; rows_deleted := v_deleted;
    RETURN NEXT;
    
    RETURN;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- SUCCESS MESSAGE
-- ============================================================

DO $$
BEGIN
    RAISE NOTICE '✅ pg_monitor extension lite v0.9.0 installed successfully';
    RAISE NOTICE '   Available functions:';
    RAISE NOTICE '   - pgmon.health_check()';
    RAISE NOTICE '   - pgmon.check_slow_queries()';
    RAISE NOTICE '   - pgmon.cache_hit_ratio()';
    RAISE NOTICE '   - pgmon.table_bloat_estimate()';
    RAISE NOTICE '   - pgmon.unused_indexes()';
    RAISE NOTICE '   - pgmon.tables_needing_vacuum()';
    RAISE NOTICE '   - pgmon.high_update_tables()';
    RAISE NOTICE '   - pgmon.hot_update_efficiency()';
    RAISE NOTICE '   - pgmon.repack_recommendations()';
    RAISE NOTICE '   - pgmon.ai_comprehensive_analysis()';
    RAISE NOTICE '   - pgmon.ai_repack_analysis()';
    RAISE NOTICE '   - pgmon.capture_metrics_snapshot()';
    RAISE NOTICE '   - pgmon.capture_update_activity_snapshot()';
    RAISE NOTICE '   Bug Detection (v0.9.0):';
    RAISE NOTICE '   - pgmon.get_applicable_bugs()';
    RAISE NOTICE '   - pgmon.bug_status_summary()';
    RAISE NOTICE '   - pgmon.open_bugs_report()';
END $$;
