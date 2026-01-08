-- pg_monitor Extension
-- Core functions for monitoring and AI integration
-- Version: 0.9.0

-- ============================================================
-- CREATE SCHEMA
-- ============================================================


-- (Schema creation handled by .control file)


-- ============================================================
-- EXTENSION TABLES
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
    RETURN 'pg_monitor extension v0.9.0 is working!';
END;
$$ LANGUAGE plpgsql;

-- Check slow queries (OPTIMIZED: Uses make_interval)
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
      AND now() - a.query_start > make_interval(secs => threshold_seconds)
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
    status TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        d.datname::TEXT,
        ROUND(COALESCE(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 0), 2) as heap_hit_ratio,
        CASE 
            WHEN COALESCE(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 0) >= 95 THEN 'GOOD'
            WHEN COALESCE(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 0) >= 90 THEN 'WARNING'
            ELSE 'CRITICAL'
        END as status
    FROM pg_stat_database d
    WHERE d.datname NOT LIKE 'template%'
    ORDER BY d.datname;
END;
$$ LANGUAGE plpgsql;



-- Table bloat estimate (OPTIMIZED: Uses CTE for performance)
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
    WITH top_tables AS (
        SELECT c.oid, c.relname, n.nspname, c.reltuples, pg_relation_size(c.oid) as raw_size
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND c.reltuples > 0
        ORDER BY pg_relation_size(c.oid) DESC
        LIMIT 20
    )
    SELECT
        t.nspname::TEXT,
        t.relname::TEXT,
        pg_size_pretty(t.raw_size) as real_size,
        pg_size_pretty(GREATEST(t.raw_size - 
            (t.reltuples * 
                COALESCE((SELECT avg(avg_width) FROM pg_stats s WHERE s.schemaname = t.nspname AND s.tablename = t.relname), 100)
            )::BIGINT, 0)) as bloat_size,
        ROUND(CASE 
            WHEN t.raw_size = 0 THEN 0
            ELSE GREATEST(
                (t.raw_size - 
                    (t.reltuples * 
                         COALESCE((SELECT avg(avg_width) FROM pg_stats s WHERE s.schemaname = t.nspname AND s.tablename = t.relname), 100)
                    )::BIGINT
                )::NUMERIC / t.raw_size * 100, 0)
        END, 2) as bloat_ratio
    FROM top_tables t;
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
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'table', schema_name || '.' || table_name,
                'executed_at', executed_at,
                'size_before', pg_size_pretty(size_before_bytes),
                'size_after', pg_size_pretty(size_after_bytes),
                'reduction_percent', ROUND(100.0 * (size_before_bytes - size_after_bytes) / NULLIF(size_before_bytes, 0), 2),
                'success', success
            )), '[]'::jsonb)
            FROM pgmon.repack_history
            WHERE executed_at > now() - interval '30 days'
            ORDER BY executed_at DESC
            LIMIT 10
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
    -- Using data directory format as a heuristic
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
        'UNKNOWN'::TEXT,
        'Cannot automatically detect safely without exclusive locks.'::TEXT,
        'Avoid REINDEX SYSTEM. Use standard maintenance.'::TEXT;

END;
$$;



-- ============================================================
-- VERSION 0.10.0 ADDITIONS - CVE DETECTION SYSTEM
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
-- ============================================================
-- CVE DETECTION FUNCTIONS (Part 2 of upgrade script)
-- ============================================================
-- This file contains the detection and reporting functions for v0.10.0
-- Append this to pg_monitor--0.9.0--0.10.0.sql or include in pg_monitor--0.10.0.sql

-- Simple CVE check function - returns all applicable CVEs for current version
CREATE OR REPLACE FUNCTION pgmon.check_security_cves()
RETURNS TABLE(
    cve_id TEXT,
    title TEXT,
    severity TEXT,
    category TEXT,
    cvss_score NUMERIC,
    is_applicable BOOLEAN,
    recommendation TEXT
)
LANGUAGE sql
AS $$
    SELECT 
        kb.cve_id,
        kb.title,
        kb.severity,
        kb.category,
        kb.cvss_score,
        pgmon.get_pg_major_version() = ANY(kb.affected_versions) AS is_applicable,
        kb.workaround AS recommendation
    FROM pgmon.known_bugs kb
    WHERE kb.cve_id IS NOT NULL
      AND pgmon.get_pg_major_version() = ANY(kb.affected_versions)
    ORDER BY 
        CASE kb.severity
            WHEN 'CRITICAL' THEN 1
            WHEN 'HIGH' THEN 2
            WHEN 'MEDIUM' THEN 3
            ELSE 4
        END,
        kb.cvss_score DESC NULLS LAST;
$$;

-- Comprehensive security audit report
CREATE OR REPLACE FUNCTION pgmon.security_audit_report()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
    v_version INT;
BEGIN
    v_version := pgmon.get_pg_major_version();
    
    SELECT jsonb_build_object(
        'scan_timestamp', NOW(),
        'postgresql_version', version(),
        'major_version', v_version,
        'total_cves_in_database', (SELECT COUNT(*) FROM pgmon.known_bugs WHERE cve_id IS NOT NULL),
        'applicable_cves', (
            SELECT COUNT(*) 
            FROM pgmon.known_bugs 
            WHERE cve_id IS NOT NULL 
              AND v_version = ANY(affected_versions)
        ),
        'severity_breakdown', (
            SELECT jsonb_build_object(
                'CRITICAL', COUNT(*) FILTER (WHERE severity = 'CRITICAL'),
                'HIGH', COUNT(*) FILTER (WHERE severity = 'HIGH'),
                'MEDIUM', COUNT(*) FILTER (WHERE severity = 'MEDIUM'),
                'LOW', COUNT(*) FILTER (WHERE severity = 'LOW')
            )
            FROM pgmon.known_bugs
            WHERE cve_id IS NOT NULL 
              AND v_version = ANY(affected_versions)
        ),
        'critical_cves', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'cve_id', cve_id,
                'title', title,
                'cvss_score', cvss_score,
                'category', category,
                'recommendation', workaround
            )), '[]'::jsonb)
            FROM pgmon.known_bugs
            WHERE cve_id IS NOT NULL 
              AND severity = 'CRITICAL'
              AND v_version = ANY(affected_versions)
        ),
        'high_priority_cves', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'cve_id', cve_id,
                'title', title,
                'cvss_score', cvss_score,
                'category', category
            )), '[]'::jsonb)
            FROM pgmon.known_bugs
            WHERE cve_id IS NOT NULL 
              AND severity = 'HIGH'
              AND v_version = ANY(affected_versions)
            LIMIT 10
        ),
        'recommendations', (
            SELECT jsonb_agg(DISTINCT category)
            FROM pgmon.known_bugs
            WHERE cve_id IS NOT NULL 
              AND v_version = ANY(affected_versions)
              AND severity IN ('CRITICAL', 'HIGH')
        )
    ) INTO v_result;
    
    RETURN v_result;
END;
$$;

-- CVE summary by severity
CREATE OR REPLACE FUNCTION pgmon.cve_summary_by_severity()
RETURNS TABLE(
    severity TEXT,
    count BIGINT,
    cve_list TEXT[],
    highest_cvss NUMERIC,
    categories TEXT[]
)
LANGUAGE sql
AS $$
    SELECT 
        kb.severity,
        COUNT(*) AS count,
        ARRAY_AGG(kb.cve_id ORDER BY kb.cvss_score DESC NULLS LAST) AS cve_list,
        MAX(kb.cvss_score) AS highest_cvss,
        ARRAY_AGG(DISTINCT kb.category) AS categories
    FROM pgmon.known_bugs kb
    WHERE kb.cve_id IS NOT NULL
      AND pgmon.get_pg_major_version() = ANY(kb.affected_versions)
    GROUP BY kb.severity
    ORDER BY 
        CASE kb.severity
            WHEN 'CRITICAL' THEN 1
            WHEN 'HIGH' THEN 2
            WHEN 'MEDIUM' THEN 3
            ELSE 4
        END;
$$;

-- Get mitigation recommendations
CREATE OR REPLACE FUNCTION pgmon.get_mitigation_recommendations(p_cve_id TEXT DEFAULT NULL)
RETURNS TABLE(
    cve_id TEXT,
    priority INT,
    severity TEXT,
    title TEXT,
    mitigation TEXT,
    patch_version TEXT,
    wiki_url TEXT
)
LANGUAGE sql
AS $$
    SELECT 
        kb.cve_id,
        CASE kb.severity
            WHEN 'CRITICAL' THEN 1
            WHEN 'HIGH' THEN 2
            WHEN 'MEDIUM' THEN 3
            ELSE 4
        END AS priority,
        kb.severity,
        kb.title,
        kb.workaround AS mitigation,
        kb.patch_version,
        kb.wiki_url
    FROM pgmon.known_bugs kb
    WHERE kb.cve_id IS NOT NULL
      AND pgmon.get_pg_major_version() = ANY(kb.affected_versions)
      AND (p_cve_id IS NULL OR kb.cve_id = p_cve_id)
    ORDER BY priority, kb.cvss_score DESC NULLS LAST;
$$;

-- Record security scan
CREATE OR REPLACE FUNCTION pgmon.record_security_scan()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_id INTEGER;
    v_version INT;
    v_scan_results JSONB;
BEGIN
    v_version := pgmon.get_pg_major_version();
    v_scan_results := pgmon.security_audit_report();
    
    INSERT INTO pgmon.security_scan_history (
        pg_version,
        total_cves_applicable,
        critical_count,
        high_count,
        medium_count,
        low_count,
        scan_results
    )
    SELECT 
        v_version::TEXT,
        (v_scan_results->>'applicable_cves')::INT,
        (v_scan_results->'severity_breakdown'->>'CRITICAL')::INT,
        (v_scan_results->'severity_breakdown'->>'HIGH')::INT,
        (v_scan_results->'severity_breakdown'->>'MEDIUM')::INT,
        (v_scan_results->'severity_breakdown'->>'LOW')::INT,
        v_scan_results
    RETURNING id INTO v_id;
    
    RETURN v_id;
END;
$$;

-- Enhanced open_bugs_report to include CVEs
CREATE OR REPLACE FUNCTION pgmon.open_bugs_report_with_cves()
RETURNS TABLE(
    bug_id TEXT,
    bug_type TEXT,
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
BEGIN
    v_version := pgmon.get_pg_major_version();
    
    -- Return both operational bugs and CVEs
    RETURN QUERY
    SELECT 
        kb.bug_id,
        CASE WHEN kb.cve_id IS NOT NULL THEN 'CVE' ELSE 'OPERATIONAL' END AS bug_type,
        kb.title,
        kb.severity,
        kb.category,
        CASE 
            WHEN v_version = ANY(kb.affected_versions) THEN 'APPLICABLE'
            ELSE 'NOT AFFECTED'
        END AS current_status,
        kb.description AS details,
        kb.workaround AS recommendation
    FROM pgmon.known_bugs kb
    WHERE v_version = ANY(kb.affected_versions)
    ORDER BY 
        CASE kb.severity
            WHEN 'CRITICAL' THEN 1
            WHEN 'HIGH' THEN 2
            WHEN 'MEDIUM' THEN 3
            ELSE 4
        END,
        kb.cve_id IS NOT NULL DESC,
        kb.cvss_score DESC NULLS LAST;
END;
$$;

-- Quick security status check
CREATE OR REPLACE FUNCTION pgmon.security_status()
RETURNS TABLE(
    status TEXT,
    critical_cves INT,
    high_cves INT,
    total_applicable_cves INT,
    recommendation TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_critical INT;
    v_high INT;
    v_total INT;
    v_version INT;
BEGIN
    v_version := pgmon.get_pg_major_version();
    
    SELECT 
        COUNT(*) FILTER (WHERE severity = 'CRITICAL'),
        COUNT(*) FILTER (WHERE severity = 'HIGH'),
        COUNT(*)
    INTO v_critical, v_high, v_total
    FROM pgmon.known_bugs
    WHERE cve_id IS NOT NULL
      AND v_version = ANY(affected_versions);
    
    RETURN QUERY SELECT 
        CASE 
            WHEN v_critical > 0 THEN 'CRITICAL'
            WHEN v_high > 0 THEN 'HIGH'
            WHEN v_total > 0 THEN 'MEDIUM'
            ELSE 'GOOD'
        END AS status,
        v_critical AS critical_cves,
        v_high AS high_cves,
        v_total AS total_applicable_cves,
        CASE 
            WHEN v_critical > 0 THEN 'URGENT: ' || v_critical || ' critical CVEs found. Upgrade PostgreSQL immediately!'
            WHEN v_high > 0 THEN 'WARNING: ' || v_high || ' high-severity CVEs found. Plan upgrade soon.'
            WHEN v_total > 0 THEN 'INFO: ' || v_total || ' CVEs applicable. Review and plan mitigation.'
            ELSE 'Your PostgreSQL version has no known applicable CVEs in our database.'
        END AS recommendation;
END;
$$;

COMMENT ON FUNCTION pgmon.check_security_cves() IS 'Returns all CVEs applicable to current PostgreSQL version';
COMMENT ON FUNCTION pgmon.security_audit_report() IS 'Comprehensive security audit report in JSON format';
COMMENT ON FUNCTION pgmon.cve_summary_by_severity() IS 'Summary of CVEs grouped by severity level';
COMMENT ON FUNCTION pgmon.get_mitigation_recommendations(TEXT) IS 'Get mitigation recommendations for CVEs';
COMMENT ON FUNCTION pgmon.record_security_scan() IS 'Record a security scan to history table';
COMMENT ON FUNCTION pgmon.security_status() IS 'Quick security status check';
