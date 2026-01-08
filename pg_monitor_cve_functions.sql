-- ============================================================
-- CVE DETECTION FUNCTIONS (Part 2 of upgrade script)
-- ============================================================
-- This file contains the detection and reporting functions for v0.10.0
-- Append this to pg_monitor--0.9.0--0.10.0.sql or include in pg_monitor--0.10.0.sql

-- Simple CVE check function - returns all applicable bugs/CVEs for current version
CREATE OR REPLACE FUNCTION pgmon.check_security_cves()
RETURNS TABLE(
    bug_id TEXT,
    title TEXT,
    severity TEXT,
    category TEXT,
    is_applicable BOOLEAN,
    recommendation TEXT
)
LANGUAGE sql
AS $$
    SELECT 
        kb.bug_id,
        kb.title,
        kb.severity,
        kb.category,
        pgmon.get_pg_major_version() = ANY(kb.affected_versions) AS is_applicable,
        kb.workaround AS recommendation
    FROM pgmon.known_bugs kb
    WHERE pgmon.get_pg_major_version() = ANY(kb.affected_versions)
    ORDER BY 
        CASE kb.severity
            WHEN 'CRITICAL' THEN 1
            WHEN 'HIGH' THEN 2
            WHEN 'MEDIUM' THEN 3
            ELSE 4
        END;
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
        'total_bugs_in_database', (SELECT COUNT(*) FROM pgmon.known_bugs),
        'applicable_bugs', (
            SELECT COUNT(*) 
            FROM pgmon.known_bugs 
            WHERE v_version = ANY(affected_versions)
        ),
        'severity_breakdown', (
            SELECT jsonb_build_object(
                'CRITICAL', COUNT(*) FILTER (WHERE severity = 'CRITICAL'),
                'HIGH', COUNT(*) FILTER (WHERE severity = 'HIGH'),
                'MEDIUM', COUNT(*) FILTER (WHERE severity = 'MEDIUM'),
                'LOW', COUNT(*) FILTER (WHERE severity = 'LOW')
            )
            FROM pgmon.known_bugs
            WHERE v_version = ANY(affected_versions)
        ),
        'critical_cves', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'bug_id', bug_id,
                'title', title,
                'category', category,
                'recommendation', workaround
            )), '[]'::jsonb)
            FROM pgmon.known_bugs
            WHERE severity = 'CRITICAL'
              AND v_version = ANY(affected_versions)
        ),
        'high_priority_cves', (
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'bug_id', bug_id,
                'title', title,
                'category', category
            )), '[]'::jsonb)
            FROM pgmon.known_bugs
            WHERE severity = 'HIGH'
              AND v_version = ANY(affected_versions)
            LIMIT 10
        ),
        'recommendations', (
            SELECT jsonb_agg(DISTINCT category)
            FROM pgmon.known_bugs
            WHERE v_version = ANY(affected_versions)
              AND severity IN ('CRITICAL', 'HIGH')
        )
    ) INTO v_result;
    
    RETURN v_result;
END;
$$;

-- Bug summary by severity
CREATE OR REPLACE FUNCTION pgmon.cve_summary_by_severity()
RETURNS TABLE(
    severity TEXT,
    count BIGINT,
    bug_list TEXT[],
    categories TEXT[]
)
LANGUAGE sql
AS $$
    SELECT 
        kb.severity,
        COUNT(*) AS count,
        ARRAY_AGG(kb.bug_id) AS bug_list,
        ARRAY_AGG(DISTINCT kb.category) AS categories
    FROM pgmon.known_bugs kb
    WHERE pgmon.get_pg_major_version() = ANY(kb.affected_versions)
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
CREATE OR REPLACE FUNCTION pgmon.get_mitigation_recommendations(p_bug_id TEXT DEFAULT NULL)
RETURNS TABLE(
    bug_id TEXT,
    priority INT,
    severity TEXT,
    title TEXT,
    mitigation TEXT,
    wiki_url TEXT
)
LANGUAGE sql
AS $$
    SELECT 
        kb.bug_id,
        CASE kb.severity
            WHEN 'CRITICAL' THEN 1
            WHEN 'HIGH' THEN 2
            WHEN 'MEDIUM' THEN 3
            ELSE 4
        END AS priority,
        kb.severity,
        kb.title,
        kb.workaround AS mitigation,
        kb.wiki_url
    FROM pgmon.known_bugs kb
    WHERE pgmon.get_pg_major_version() = ANY(kb.affected_versions)
      AND (p_bug_id IS NULL OR kb.bug_id = p_bug_id)
    ORDER BY priority;
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
        'OPERATIONAL'::TEXT AS bug_type,
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
        END;
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
    WHERE v_version = ANY(affected_versions);
    
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
