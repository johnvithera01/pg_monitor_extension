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


