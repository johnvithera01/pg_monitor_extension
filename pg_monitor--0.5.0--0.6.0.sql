-- Upgrade script from version 0.5.0 to 0.6.0
-- Focus: Complete AI Analysis Data & Historical Metrics

-- ============================================================================
-- PHASE 7 - HISTORICAL METRICS (TIME SERIES)
-- ============================================================================

-- Table to store historical snapshots of key metrics
CREATE TABLE IF NOT EXISTS pgmon.metrics_history (
    id BIGSERIAL PRIMARY KEY,
    snapshot_time TIMESTAMPTZ DEFAULT NOW(),
    
    -- Database-level metrics
    database_name TEXT NOT NULL,
    database_size_bytes BIGINT,
    
    -- Connection metrics
    active_connections INT,
    idle_connections INT,
    idle_in_transaction INT,
    total_connections INT,
    max_connections INT,
    connection_usage_pct NUMERIC(5,2),
    
    -- Performance metrics
    cache_hit_ratio NUMERIC(5,2),
    index_hit_ratio NUMERIC(5,2),
    
    -- Transaction metrics
    xact_commit BIGINT,
    xact_rollback BIGINT,
    tup_inserted BIGINT,
    tup_updated BIGINT,
    tup_deleted BIGINT,
    tup_returned BIGINT,
    
    -- Maintenance metrics
    total_dead_tuples BIGINT,
    total_live_tuples BIGINT,
    
    -- Lock metrics
    blocking_queries_count INT,
    locks_waiting INT,
    
    -- Replication metrics (nullable)
    replication_lag_bytes BIGINT,
    replication_lag_seconds NUMERIC,
    standby_count INT,
    
    -- Checkpoint metrics
    checkpoints_timed BIGINT,
    checkpoints_req BIGINT,
    checkpoint_write_time DOUBLE PRECISION,
    checkpoint_sync_time DOUBLE PRECISION,
    
    -- BGWriter metrics
    buffers_checkpoint BIGINT,
    buffers_clean BIGINT,
    buffers_backend BIGINT,
    
    -- Temp file usage
    temp_files BIGINT,
    temp_bytes BIGINT,
    
    -- Deadlocks
    deadlocks BIGINT
);

-- Index for efficient time-range queries
CREATE INDEX IF NOT EXISTS idx_metrics_history_time 
    ON pgmon.metrics_history(snapshot_time DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_history_db_time 
    ON pgmon.metrics_history(database_name, snapshot_time DESC);

-- Table for table-level historical metrics
CREATE TABLE IF NOT EXISTS pgmon.table_metrics_history (
    id BIGSERIAL PRIMARY KEY,
    snapshot_time TIMESTAMPTZ DEFAULT NOW(),
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    
    -- Size metrics
    total_size_bytes BIGINT,
    table_size_bytes BIGINT,
    index_size_bytes BIGINT,
    
    -- Row counts
    n_live_tup BIGINT,
    n_dead_tup BIGINT,
    
    -- Access patterns
    seq_scan BIGINT,
    seq_tup_read BIGINT,
    idx_scan BIGINT,
    idx_tup_fetch BIGINT,
    
    -- Modifications
    n_tup_ins BIGINT,
    n_tup_upd BIGINT,
    n_tup_del BIGINT,
    n_tup_hot_upd BIGINT,
    
    -- Vacuum info
    last_vacuum TIMESTAMPTZ,
    last_autovacuum TIMESTAMPTZ,
    vacuum_count BIGINT,
    autovacuum_count BIGINT
);

CREATE INDEX IF NOT EXISTS idx_table_metrics_history_time 
    ON pgmon.table_metrics_history(snapshot_time DESC);
CREATE INDEX IF NOT EXISTS idx_table_metrics_history_table 
    ON pgmon.table_metrics_history(schema_name, table_name, snapshot_time DESC);

-- ============================================================================
-- SNAPSHOT CAPTURE FUNCTIONS
-- ============================================================================

-- Capture a snapshot of database-level metrics
CREATE OR REPLACE FUNCTION pgmon.capture_metrics_snapshot()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_db_row RECORD;
    v_cache_ratio NUMERIC;
    v_index_ratio NUMERIC;
    v_blocking_count INT;
    v_locks_waiting INT;
    v_rep_lag_bytes BIGINT;
    v_rep_lag_seconds NUMERIC;
    v_standby_count INT;
    v_bgwriter RECORD;
BEGIN
    -- Get cache hit ratios
    SELECT heap_hit_ratio, index_hit_ratio 
    INTO v_cache_ratio, v_index_ratio
    FROM pgmon.cache_hit_ratio() LIMIT 1;
    
    -- Get blocking queries count
    SELECT COUNT(*) INTO v_blocking_count FROM pgmon.blocking_queries();
    
    -- Get waiting locks
    SELECT COUNT(*) INTO v_locks_waiting 
    FROM pg_locks WHERE NOT granted;
    
    -- Get replication info
    SELECT 
        COUNT(*),
        COALESCE(MAX((sent_lsn - replay_lsn)::BIGINT), 0),
        COALESCE(MAX(EXTRACT(EPOCH FROM replay_lag)), 0)
    INTO v_standby_count, v_rep_lag_bytes, v_rep_lag_seconds
    FROM pg_stat_replication;
    
    -- Get bgwriter stats
    SELECT * INTO v_bgwriter FROM pg_stat_bgwriter LIMIT 1;
    
    -- Insert snapshot for each database
    FOR v_db_row IN 
        SELECT 
            d.datname,
            pg_database_size(d.datname) AS size_bytes,
            d.numbackends,
            d.xact_commit,
            d.xact_rollback,
            d.tup_inserted,
            d.tup_updated,
            d.tup_deleted,
            d.tup_returned,
            d.temp_files,
            d.temp_bytes,
            d.deadlocks
        FROM pg_stat_database d
        WHERE d.datname NOT IN ('template0', 'template1')
    LOOP
        INSERT INTO pgmon.metrics_history (
            database_name, database_size_bytes,
            active_connections, idle_connections, idle_in_transaction, total_connections,
            max_connections, connection_usage_pct,
            cache_hit_ratio, index_hit_ratio,
            xact_commit, xact_rollback,
            tup_inserted, tup_updated, tup_deleted, tup_returned,
            total_dead_tuples, total_live_tuples,
            blocking_queries_count, locks_waiting,
            replication_lag_bytes, replication_lag_seconds, standby_count,
            checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time,
            buffers_checkpoint, buffers_clean, buffers_backend,
            temp_files, temp_bytes, deadlocks
        )
        SELECT
            v_db_row.datname,
            v_db_row.size_bytes,
            COUNT(*) FILTER (WHERE state = 'active'),
            COUNT(*) FILTER (WHERE state = 'idle'),
            COUNT(*) FILTER (WHERE state LIKE 'idle in transaction%'),
            COUNT(*),
            current_setting('max_connections')::INT,
            ROUND(100.0 * COUNT(*) / current_setting('max_connections')::INT, 2),
            v_cache_ratio,
            v_index_ratio,
            v_db_row.xact_commit,
            v_db_row.xact_rollback,
            v_db_row.tup_inserted,
            v_db_row.tup_updated,
            v_db_row.tup_deleted,
            v_db_row.tup_returned,
            (SELECT COALESCE(SUM(n_dead_tup), 0) FROM pg_stat_user_tables),
            (SELECT COALESCE(SUM(n_live_tup), 0) FROM pg_stat_user_tables),
            v_blocking_count,
            v_locks_waiting,
            v_rep_lag_bytes,
            v_rep_lag_seconds,
            v_standby_count,
            v_bgwriter.checkpoints_timed,
            v_bgwriter.checkpoints_req,
            v_bgwriter.checkpoint_write_time,
            v_bgwriter.checkpoint_sync_time,
            v_bgwriter.buffers_checkpoint,
            v_bgwriter.buffers_clean,
            v_bgwriter.buffers_backend,
            v_db_row.temp_files,
            v_db_row.temp_bytes,
            v_db_row.deadlocks
        FROM pg_stat_activity
        WHERE datname = v_db_row.datname;
    END LOOP;
END;
$$;

-- Capture table-level metrics snapshot
CREATE OR REPLACE FUNCTION pgmon.capture_table_metrics_snapshot()
RETURNS VOID
LANGUAGE sql
AS $$
    INSERT INTO pgmon.table_metrics_history (
        schema_name, table_name,
        total_size_bytes, table_size_bytes, index_size_bytes,
        n_live_tup, n_dead_tup,
        seq_scan, seq_tup_read, idx_scan, idx_tup_fetch,
        n_tup_ins, n_tup_upd, n_tup_del, n_tup_hot_upd,
        last_vacuum, last_autovacuum, vacuum_count, autovacuum_count
    )
    SELECT 
        schemaname,
        relname,
        pg_total_relation_size(schemaname||'.'||relname),
        pg_relation_size(schemaname||'.'||relname),
        pg_indexes_size(schemaname||'.'||relname),
        n_live_tup,
        n_dead_tup,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        last_vacuum,
        last_autovacuum,
        vacuum_count,
        autovacuum_count
    FROM pg_stat_user_tables;
$$;

-- ============================================================================
-- PHASE 7 - CHECKPOINT & BGWRITER METRICS
-- ============================================================================

-- Checkpoint statistics
CREATE OR REPLACE FUNCTION pgmon.checkpoint_stats()
RETURNS TABLE(
    checkpoints_timed BIGINT,
    checkpoints_req BIGINT,
    checkpoint_write_time_seconds NUMERIC,
    checkpoint_sync_time_seconds NUMERIC,
    buffers_checkpoint BIGINT,
    buffers_clean BIGINT,
    maxwritten_clean BIGINT,
    buffers_backend BIGINT,
    buffers_backend_fsync BIGINT,
    buffers_alloc BIGINT,
    stats_reset TIMESTAMPTZ,
    -- Calculated metrics
    checkpoint_ratio NUMERIC,
    avg_checkpoint_write_time_ms NUMERIC,
    avg_checkpoint_sync_time_ms NUMERIC
)
LANGUAGE sql
AS $$
    SELECT 
        checkpoints_timed,
        checkpoints_req,
        ROUND((checkpoint_write_time / 1000.0)::numeric, 2) AS checkpoint_write_time_seconds,
        ROUND((checkpoint_sync_time / 1000.0)::numeric, 2) AS checkpoint_sync_time_seconds,
        buffers_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc,
        stats_reset,
        -- Timed vs requested ratio (higher is better)
        ROUND((100.0 * checkpoints_timed / NULLIF(checkpoints_timed + checkpoints_req, 0))::numeric, 2),
        -- Average times
        ROUND((checkpoint_write_time / NULLIF(checkpoints_timed + checkpoints_req, 0))::numeric, 2),
        ROUND((checkpoint_sync_time / NULLIF(checkpoints_timed + checkpoints_req, 0))::numeric, 2)
    FROM pg_stat_bgwriter;
$$;

-- ============================================================================
-- PHASE 7 - EXPLAIN/PLAN ANALYSIS
-- ============================================================================

-- Store query plans for AI analysis
CREATE TABLE IF NOT EXISTS pgmon.query_plans (
    id BIGSERIAL PRIMARY KEY,
    query_hash TEXT NOT NULL,
    query_text TEXT NOT NULL,
    explain_plan JSONB NOT NULL,
    plan_cost_estimate NUMERIC,
    plan_rows_estimate NUMERIC,
    actual_time_ms NUMERIC,
    actual_rows BIGINT,
    planning_time_ms NUMERIC,
    execution_time_ms NUMERIC,
    -- Plan characteristics
    has_seq_scan BOOLEAN DEFAULT false,
    has_index_scan BOOLEAN DEFAULT false,
    has_bitmap_scan BOOLEAN DEFAULT false,
    has_nested_loop BOOLEAN DEFAULT false,
    has_hash_join BOOLEAN DEFAULT false,
    has_merge_join BOOLEAN DEFAULT false,
    has_sort BOOLEAN DEFAULT false,
    has_aggregate BOOLEAN DEFAULT false,
    tables_scanned TEXT[],
    indexes_used TEXT[],
    -- Timestamps
    captured_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_query_plans_hash ON pgmon.query_plans(query_hash);
CREATE INDEX IF NOT EXISTS idx_query_plans_time ON pgmon.query_plans(captured_at DESC);

-- Function to capture and store a query plan
CREATE OR REPLACE FUNCTION pgmon.capture_query_plan(
    p_query TEXT,
    p_params TEXT[] DEFAULT NULL
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_plan JSONB;
    v_query_hash TEXT;
    v_has_seq BOOLEAN := false;
    v_has_idx BOOLEAN := false;
    v_has_bitmap BOOLEAN := false;
    v_has_nested BOOLEAN := false;
    v_has_hash BOOLEAN := false;
    v_has_merge BOOLEAN := false;
    v_has_sort BOOLEAN := false;
    v_has_agg BOOLEAN := false;
    v_plan_text TEXT;
BEGIN
    -- Generate query hash
    v_query_hash := MD5(regexp_replace(p_query, '\s+', ' ', 'g'));
    
    -- Get EXPLAIN output as JSON
    EXECUTE 'EXPLAIN (FORMAT JSON, COSTS, VERBOSE, BUFFERS) ' || p_query
    INTO v_plan;
    
    -- Convert to text for pattern matching
    v_plan_text := v_plan::TEXT;
    
    -- Detect plan characteristics
    v_has_seq := v_plan_text ILIKE '%Seq Scan%';
    v_has_idx := v_plan_text ILIKE '%Index Scan%' OR v_plan_text ILIKE '%Index Only Scan%';
    v_has_bitmap := v_plan_text ILIKE '%Bitmap%';
    v_has_nested := v_plan_text ILIKE '%Nested Loop%';
    v_has_hash := v_plan_text ILIKE '%Hash Join%';
    v_has_merge := v_plan_text ILIKE '%Merge Join%';
    v_has_sort := v_plan_text ILIKE '%Sort%';
    v_has_agg := v_plan_text ILIKE '%Aggregate%';
    
    -- Store the plan
    INSERT INTO pgmon.query_plans (
        query_hash, query_text, explain_plan,
        plan_cost_estimate, plan_rows_estimate,
        has_seq_scan, has_index_scan, has_bitmap_scan,
        has_nested_loop, has_hash_join, has_merge_join,
        has_sort, has_aggregate
    )
    VALUES (
        v_query_hash, p_query, v_plan,
        (v_plan->0->'Plan'->>'Total Cost')::NUMERIC,
        (v_plan->0->'Plan'->>'Plan Rows')::NUMERIC,
        v_has_seq, v_has_idx, v_has_bitmap,
        v_has_nested, v_has_hash, v_has_merge,
        v_has_sort, v_has_agg
    );
    
    RETURN v_plan;
EXCEPTION
    WHEN OTHERS THEN
        RETURN jsonb_build_object('error', SQLERRM);
END;
$$;

-- ============================================================================
-- PHASE 7 - TREND ANALYSIS FUNCTIONS
-- ============================================================================

-- Get database growth trend
CREATE OR REPLACE FUNCTION pgmon.database_growth_trend(
    p_database TEXT DEFAULT current_database(),
    p_days INT DEFAULT 7
)
RETURNS TABLE(
    snapshot_date DATE,
    database_size_bytes BIGINT,
    size_change_bytes BIGINT,
    growth_pct NUMERIC,
    avg_connections NUMERIC,
    peak_connections INT,
    total_transactions BIGINT,
    avg_cache_hit_ratio NUMERIC
)
LANGUAGE sql
AS $$
    WITH daily_stats AS (
        SELECT 
            DATE(snapshot_time) AS day,
            MAX(database_size_bytes) AS max_size,
            LAG(MAX(database_size_bytes)) OVER (ORDER BY DATE(snapshot_time)) AS prev_size,
            ROUND(AVG(total_connections), 1) AS avg_conn,
            MAX(total_connections) AS peak_conn,
            MAX(xact_commit) + MAX(xact_rollback) AS total_xact,
            ROUND(AVG(cache_hit_ratio), 2) AS avg_cache
        FROM pgmon.metrics_history
        WHERE database_name = p_database
          AND snapshot_time >= NOW() - (p_days || ' days')::INTERVAL
        GROUP BY DATE(snapshot_time)
    )
    SELECT 
        day,
        max_size,
        COALESCE(max_size - prev_size, 0),
        COALESCE(ROUND(100.0 * (max_size - prev_size) / NULLIF(prev_size, 0), 2), 0),
        avg_conn,
        peak_conn,
        total_xact,
        avg_cache
    FROM daily_stats
    ORDER BY day DESC;
$$;

-- Get table growth trend
CREATE OR REPLACE FUNCTION pgmon.table_growth_trend(
    p_schema TEXT DEFAULT 'public',
    p_table TEXT DEFAULT NULL,
    p_days INT DEFAULT 7
)
RETURNS TABLE(
    schema_name TEXT,
    table_name TEXT,
    snapshot_date DATE,
    total_size_bytes BIGINT,
    row_count BIGINT,
    size_change_bytes BIGINT,
    row_change BIGINT,
    growth_pct NUMERIC
)
LANGUAGE sql
AS $$
    WITH daily_stats AS (
        SELECT 
            t.schema_name,
            t.table_name,
            DATE(t.snapshot_time) AS day,
            MAX(t.total_size_bytes) AS max_size,
            MAX(t.n_live_tup) AS max_rows,
            LAG(MAX(t.total_size_bytes)) OVER (
                PARTITION BY t.schema_name, t.table_name 
                ORDER BY DATE(t.snapshot_time)
            ) AS prev_size,
            LAG(MAX(t.n_live_tup)) OVER (
                PARTITION BY t.schema_name, t.table_name 
                ORDER BY DATE(t.snapshot_time)
            ) AS prev_rows
        FROM pgmon.table_metrics_history t
        WHERE t.snapshot_time >= NOW() - (p_days || ' days')::INTERVAL
          AND t.schema_name = p_schema
          AND (p_table IS NULL OR t.table_name = p_table)
        GROUP BY t.schema_name, t.table_name, DATE(t.snapshot_time)
    )
    SELECT 
        schema_name,
        table_name,
        day,
        max_size,
        max_rows,
        COALESCE(max_size - prev_size, 0),
        COALESCE(max_rows - prev_rows, 0),
        COALESCE(ROUND(100.0 * (max_size - prev_size) / NULLIF(prev_size, 0), 2), 0)
    FROM daily_stats
    ORDER BY schema_name, table_name, day DESC;
$$;

-- Performance trend over time
CREATE OR REPLACE FUNCTION pgmon.performance_trend(
    p_hours INT DEFAULT 24
)
RETURNS TABLE(
    time_bucket TIMESTAMPTZ,
    avg_cache_hit_ratio NUMERIC,
    avg_connections NUMERIC,
    peak_connections INT,
    blocking_queries_detected INT,
    total_deadlocks BIGINT,
    avg_temp_bytes NUMERIC
)
LANGUAGE sql
AS $$
    SELECT 
        DATE_TRUNC('hour', snapshot_time) AS time_bucket,
        ROUND(AVG(cache_hit_ratio), 2),
        ROUND(AVG(total_connections), 1),
        MAX(total_connections),
        MAX(blocking_queries_count),
        MAX(deadlocks),
        ROUND(AVG(temp_bytes), 0)
    FROM pgmon.metrics_history
    WHERE snapshot_time >= NOW() - (p_hours || ' hours')::INTERVAL
    GROUP BY DATE_TRUNC('hour', snapshot_time)
    ORDER BY time_bucket DESC;
$$;

-- ============================================================================
-- PHASE 7 - COMPREHENSIVE AI ANALYSIS DATA
-- ============================================================================

-- Single function that returns ALL data needed for AI analysis
CREATE OR REPLACE FUNCTION pgmon.ai_comprehensive_analysis()
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_result JSONB;
BEGIN
    SELECT jsonb_build_object(
        'snapshot_time', NOW(),
        'database_info', (
            SELECT jsonb_build_object(
                'name', current_database(),
                'version', version(),
                'size_bytes', pg_database_size(current_database()),
                'size_pretty', pg_size_pretty(pg_database_size(current_database())),
                'uptime', NOW() - pg_postmaster_start_time()
            )
        ),
        'health_check', (
            SELECT jsonb_agg(row_to_json(h))
            FROM pgmon.health_check() h
        ),
        'connections', (
            SELECT jsonb_build_object(
                'current', COUNT(*),
                'max', current_setting('max_connections')::INT,
                'usage_pct', ROUND(100.0 * COUNT(*) / current_setting('max_connections')::INT, 2),
                'by_state', (
                    SELECT jsonb_object_agg(state, cnt)
                    FROM (
                        SELECT state, COUNT(*) as cnt 
                        FROM pg_stat_activity 
                        GROUP BY state
                    ) s
                ),
                'by_database', (
                    SELECT jsonb_agg(row_to_json(c))
                    FROM pgmon.connections_by_database() c
                )
            )
            FROM pg_stat_activity
        ),
        'performance', (
            SELECT jsonb_build_object(
                'cache_hit_ratio', (SELECT heap_hit_ratio FROM pgmon.cache_hit_ratio()),
                'index_hit_ratio', (SELECT index_hit_ratio FROM pgmon.cache_hit_ratio()),
                'checkpoint_stats', (SELECT row_to_json(c) FROM pgmon.checkpoint_stats() c),
                'slow_queries_count', (
                    SELECT COUNT(*) FROM pg_stat_activity 
                    WHERE state = 'active' 
                    AND NOW() - query_start > INTERVAL '5 seconds'
                ),
                'temp_files', (
                    SELECT jsonb_build_object(
                        'count', temp_files,
                        'bytes', temp_bytes
                    )
                    FROM pg_stat_database 
                    WHERE datname = current_database()
                )
            )
        ),
        'locks', (
            SELECT jsonb_build_object(
                'blocking_queries', (
                    SELECT jsonb_agg(row_to_json(b))
                    FROM pgmon.blocking_queries() b
                ),
                'waiting_locks', (
                    SELECT COUNT(*) FROM pg_locks WHERE NOT granted
                ),
                'deadlocks', (
                    SELECT deadlocks FROM pg_stat_database 
                    WHERE datname = current_database()
                )
            )
        ),
        'vacuum_status', (
            SELECT jsonb_build_object(
                'tables_needing_vacuum', (
                    SELECT jsonb_agg(row_to_json(v))
                    FROM pgmon.tables_needing_vacuum() v
                    LIMIT 20
                ),
                'total_dead_tuples', (
                    SELECT SUM(n_dead_tup) FROM pg_stat_user_tables
                ),
                'autovacuum_running', (
                    SELECT COUNT(*) > 0 FROM pg_stat_progress_vacuum
                )
            )
        ),
        'indexes', (
            SELECT jsonb_build_object(
                'unused', (
                    SELECT jsonb_agg(row_to_json(u))
                    FROM pgmon.unused_indexes() u
                    LIMIT 20
                ),
                'duplicate', (
                    SELECT jsonb_agg(row_to_json(d))
                    FROM pgmon.duplicate_indexes() d
                    LIMIT 10
                ),
                'missing_candidates', (
                    SELECT jsonb_agg(row_to_json(m))
                    FROM pgmon.missing_indexes_analysis() m
                    LIMIT 20
                ),
                'bloated', (
                    SELECT jsonb_agg(row_to_json(b))
                    FROM pgmon.index_bloat_estimate() b
                    WHERE bloat_ratio > 30
                    LIMIT 10
                )
            )
        ),
        'tables', (
            SELECT jsonb_build_object(
                'top_by_size', (
                    SELECT jsonb_agg(row_to_json(t))
                    FROM pgmon.top_tables_by_size(10) t
                ),
                'bloated', (
                    SELECT jsonb_agg(row_to_json(b))
                    FROM pgmon.table_bloat_detailed() b
                    WHERE priority IN ('CRITICAL', 'HIGH')
                    LIMIT 10
                )
            )
        ),
        'replication', (
            SELECT jsonb_build_object(
                'status', (
                    SELECT jsonb_agg(row_to_json(r))
                    FROM pgmon.replication_status() r
                ),
                'slots', (
                    SELECT jsonb_agg(row_to_json(s))
                    FROM pgmon.replication_slots_info() s
                ),
                'lag_analysis', (
                    SELECT jsonb_agg(row_to_json(l))
                    FROM pgmon.standby_lag_analysis() l
                )
            )
        ),
        'security', (
            SELECT jsonb_build_object(
                'superusers', (
                    SELECT jsonb_agg(row_to_json(s))
                    FROM pgmon.superuser_accounts() s
                ),
                'ssl_connections', (
                    SELECT jsonb_build_object(
                        'ssl_enabled', (SELECT COUNT(*) FROM pg_stat_ssl WHERE ssl = true),
                        'non_ssl', (SELECT COUNT(*) FROM pg_stat_ssl WHERE ssl = false)
                    )
                ),
                'config_check', (
                    SELECT jsonb_agg(row_to_json(c))
                    FROM pgmon.configuration_security_check() c
                    WHERE severity != 'OK'
                )
            )
        ),
        'query_metrics', (
            SELECT jsonb_build_object(
                'summary', (SELECT row_to_json(s) FROM pgmon.query_metrics_summary s),
                'slow_queries', (
                    SELECT jsonb_agg(row_to_json(q))
                    FROM pgmon.get_slow_queries_from_metrics() q
                    LIMIT 20
                ),
                'needing_indexes', (
                    SELECT jsonb_agg(row_to_json(i))
                    FROM pgmon.queries_needing_indexes() i
                    LIMIT 20
                )
            )
        ),
        'recent_trends', (
            SELECT jsonb_build_object(
                'growth_7d', (
                    SELECT jsonb_agg(row_to_json(g))
                    FROM pgmon.database_growth_trend(current_database(), 7) g
                ),
                'performance_24h', (
                    SELECT jsonb_agg(row_to_json(p))
                    FROM pgmon.performance_trend(24) p
                )
            )
        ),
        'configuration', (
            SELECT jsonb_object_agg(name, setting)
            FROM pg_settings
            WHERE name IN (
                'shared_buffers', 'effective_cache_size', 'work_mem',
                'maintenance_work_mem', 'max_connections', 'max_wal_size',
                'checkpoint_completion_target', 'random_page_cost',
                'effective_io_concurrency', 'max_worker_processes',
                'max_parallel_workers', 'max_parallel_workers_per_gather',
                'autovacuum', 'autovacuum_max_workers',
                'log_min_duration_statement', 'statement_timeout'
            )
        ),
        'recommendations', (
            SELECT jsonb_build_object(
                'immediate_actions', (
                    SELECT jsonb_agg(action) FROM (
                        -- Blocking queries
                        SELECT 'Resolve blocking queries immediately' AS action
                        FROM pgmon.blocking_queries() LIMIT 1
                        UNION ALL
                        -- High connection usage
                        SELECT 'Connection usage above 80% - consider pooling' AS action
                        FROM (
                            SELECT COUNT(*)::FLOAT / current_setting('max_connections')::INT AS usage
                            FROM pg_stat_activity
                        ) u
                        WHERE u.usage > 0.8
                        UNION ALL
                        -- Critical vacuum needed
                        SELECT 'Critical vacuum needed on tables with high dead tuples' AS action
                        FROM pgmon.tables_needing_vacuum() WHERE priority = 'CRITICAL' LIMIT 1
                        UNION ALL
                        -- Low cache hit ratio
                        SELECT 'Cache hit ratio below 90% - review shared_buffers' AS action
                        FROM pgmon.cache_hit_ratio()
                        WHERE heap_hit_ratio < 90
                    ) actions
                ),
                'optimization_opportunities', (
                    SELECT jsonb_agg(opportunity) FROM (
                        SELECT 'Consider removing unused indexes (' || COUNT(*) || ' found)' AS opportunity
                        FROM pgmon.unused_indexes()
                        HAVING COUNT(*) > 0
                        UNION ALL
                        SELECT 'Tables with high sequential scans may need indexes (' || cnt || ' found)' AS opportunity
                        FROM (
                            SELECT COUNT(*) AS cnt
                            FROM pgmon.missing_indexes_analysis() 
                            WHERE priority IN ('CRITICAL', 'HIGH')
                        ) m
                        WHERE cnt > 0
                        UNION ALL
                        SELECT 'Duplicate indexes found - wasting space' AS opportunity
                        FROM pgmon.duplicate_indexes() LIMIT 1
                    ) opportunities
                )
            )
        )
    ) INTO v_result;
    
    RETURN v_result;
END;
$$;

-- ============================================================================
-- CLEANUP FUNCTIONS
-- ============================================================================

-- Cleanup old historical data
CREATE OR REPLACE FUNCTION pgmon.cleanup_history(
    p_days INT DEFAULT 30
)
RETURNS TABLE(
    metrics_deleted BIGINT,
    table_metrics_deleted BIGINT,
    query_plans_deleted BIGINT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_metrics BIGINT;
    v_table_metrics BIGINT;
    v_plans BIGINT;
BEGIN
    DELETE FROM pgmon.metrics_history
    WHERE snapshot_time < NOW() - (p_days || ' days')::INTERVAL;
    GET DIAGNOSTICS v_metrics = ROW_COUNT;
    
    DELETE FROM pgmon.table_metrics_history
    WHERE snapshot_time < NOW() - (p_days || ' days')::INTERVAL;
    GET DIAGNOSTICS v_table_metrics = ROW_COUNT;
    
    DELETE FROM pgmon.query_plans
    WHERE captured_at < NOW() - (p_days || ' days')::INTERVAL;
    GET DIAGNOSTICS v_plans = ROW_COUNT;
    
    RETURN QUERY SELECT v_metrics, v_table_metrics, v_plans;
END;
$$;

-- ============================================================================
-- PERMISSIONS
-- ============================================================================

GRANT SELECT, INSERT ON pgmon.metrics_history TO PUBLIC;
GRANT SELECT, INSERT ON pgmon.table_metrics_history TO PUBLIC;
GRANT SELECT, INSERT ON pgmon.query_plans TO PUBLIC;
GRANT USAGE ON SEQUENCE pgmon.metrics_history_id_seq TO PUBLIC;
GRANT USAGE ON SEQUENCE pgmon.table_metrics_history_id_seq TO PUBLIC;
GRANT USAGE ON SEQUENCE pgmon.query_plans_id_seq TO PUBLIC;
