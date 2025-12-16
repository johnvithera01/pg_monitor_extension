<div align="center">

# 🐘 pg_monitor Extension

### Native PostgreSQL Extension for Database Monitoring & Bug Detection

`pg_monitor` is a native PostgreSQL extension that provides advanced monitoring, alerting, and **AI-ready metrics** capabilities directly within your database.

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-316192.svg)](https://www.postgresql.org/)
[![Version](https://img.shields.io/badge/Version-0.9.0-green.svg)](https://github.com/johnvithera01/pg_monitor_extension)

[Installation](#-installation) • [Functions](#-functions) • [Bug Detection](#-bug-detection) • [Examples](#-examples)

</div>

---

## ✨ Features

**pg_monitor** is a native PostgreSQL extension providing **105+ specialized SQL functions** for comprehensive database monitoring, performance analysis, and bug detection.

- **105+ SQL Functions** for zero-dependency monitoring
- **13 Bug Detection Functions** for currently open PostgreSQL bugs (v13-v16)
- **22 Historical Issue Functions** for known problems (v6.x-v15+)
- **AI-Ready** data aggregation for LLM-based analysis
- **Pure SQL/C** - No external dependencies required
- **Lightweight** - Minimal performance overhead

### Phase 0 - Basic Monitoring
- ✅ **Slow Query Detection** - Identify queries running longer than a threshold
- ✅ **Active Connection Monitoring** - Track all active database connections
- ✅ **Cache Hit Ratio Analysis** - Monitor heap and index cache efficiency
- ✅ **Database Size Tracking** - View size information for all databases
- ✅ **Table Bloat Detection** - Estimate table bloat and wasted space
- ✅ **Idle Transaction Detection** - Find long-running idle transactions

### Phase 1 - Performance & Queries
- ✅ **Top Queries Analysis** - By total time, calls, temp files
- ✅ **Wait Events Monitoring** - Active wait events analysis
- ✅ **Lock Monitoring** - Active locks and blocking queries detection
- ✅ **Vacuum Status** - Autovacuum status and tables needing vacuum
- ✅ **Connection Pool Stats** - Detailed connection statistics
- ✅ **Health Check** - Comprehensive database health check

### Phase 2 - Index & Table Analysis
- ✅ **Unused Indexes** - Identify indexes that can be dropped
- ✅ **Duplicate Indexes** - Find redundant indexes
- ✅ **Index Bloat** - Estimate index bloat and recommend REINDEX
- ✅ **Missing Indexes** - Tables with high sequential scans
- ✅ **Table Bloat Detailed** - Comprehensive bloat analysis

### Phase 3 - Replication & HA
- ✅ **Replication Status** - Monitor streaming replication
- ✅ **Replication Slots** - Track slot status
- ✅ **Standby Lag Analysis** - Monitor replica lag
- ✅ **WAL Statistics** - WAL generation metrics
- ✅ **Archive Status** - WAL archiving monitoring

### Phase 4 - Security & Audit
- ✅ **Superuser Accounts** - List privileged accounts
- ✅ **SSL Connections** - Monitor encrypted connections
- ✅ **Role Memberships** - Track role hierarchies
- ✅ **Object Permissions** - Permission auditing
- ✅ **Configuration Security** - Security parameter check

### Phase 5 - Capacity & Growth
- ✅ **Table Growth Estimate** - Size and row projections
- ✅ **Disk I/O Stats** - Per-table I/O metrics
- ✅ **Tablespace Usage** - Space utilization
- ✅ **Dead Tuples Trend** - Bloat trend analysis
- ✅ **Long Running Transactions** - Transaction monitoring
- ✅ **Database Stats Summary** - Comprehensive statistics

### Phase 6 - Query Metrics System (AI-Ready)
- ✅ **Query Metrics Table** - Persistent query performance tracking
- ✅ **Query Patterns** - Pattern analysis and trends
- ✅ **AI Analysis Data** - Formatted data for AI consumption
- ✅ **AI Suggestions Storage** - Store AI-generated recommendations

### Phase 7 - Historical Metrics & Trends (NEW!)
- ✅ **Metrics History** - Time-series database metrics
- ✅ **Table Metrics History** - Historical table statistics
- ✅ **Query Plans Storage** - EXPLAIN plan capture and analysis
- ✅ **Checkpoint Stats** - BGWriter and checkpoint monitoring
- ✅ **Growth Trends** - Database and table growth over time
- ✅ **Performance Trends** - Historical performance analysis
- ✅ **AI Comprehensive Analysis** - Single function returning ALL metrics for AI

### Phase 8 - High UPDATE/DELETE Workload Analysis (v0.7.0 NEW!)
- ✅ **Update Activity Tracking** - Track UPDATE/DELETE rates per table
- ✅ **HOT Update Efficiency** - Analyze HOT vs non-HOT updates
- ✅ **Table Churn Rate** - Combined UPDATE+DELETE activity metrics
- ✅ **Bloat Growth Rate** - Track bloat accumulation speed over time
- ✅ **REPACK Recommendations** - Automatic priority-based REPACK suggestions
- ✅ **REPACK Schedule Estimation** - Predict optimal REPACK timing
- ✅ **Fillfactor Analysis** - Fillfactor optimization recommendations
- ✅ **REPACK History** - Track REPACK executions and effectiveness
- ✅ **AI REPACK Analysis** - Complete JSON for AI analysis of REPACK needs

---

## 📦 Installation

### From Source (Recommended)

```bash
# Clone the repository
git clone https://github.com/johnvithera01/pg_monitor_extension.git
cd pg_monitor_extension

# Install (requires PostgreSQL dev headers)
make install

# Enable in your database
psql -d your_database -c "CREATE EXTENSION pg_monitor;"
```

### Requirements
- PostgreSQL 13, 14, 15, or 16
- PostgreSQL development headers (`postgresql-server-dev-XX`)
- GCC compiler

### Lite Version (Docker/Cloud/Managed Databases)
If you cannot install C extensions (RDS, Aurora, Cloud SQL, Docker without superuser):

**Version 0.9.0-lite** includes all core monitoring functions plus bug detection:

```bash
# Install lite extension (no CREATE EXTENSION needed)
psql -h your-db-host -U postgres -d your_db -f pg_monitor_extension_lite.sql

# Verify installation
psql -d your_db -c "SELECT pgmon.hello();"
```

**Lite Features (v0.9.0):**
- ✅ All core monitoring functions
- ✅ Health checks and performance analysis
- ✅ Table bloat and REPACK recommendations
- ✅ HOT update efficiency monitoring
- ✅ Bug detection (9 known PG bugs v13-16)
- ✅ Historical metrics tracking
- ✅ AI-ready comprehensive analysis
- ❌ No C functions (checkpoint_stats limited)
- ❌ No extension upgrade mechanism (reinstall to upgrade)

**Upgrade Lite Version:**
```bash
# Drop old version and reinstall
psql -d your_db -c "DROP SCHEMA IF EXISTS pgmon CASCADE;"
psql -d your_db -f pg_monitor_extension_lite.sql
```

---

## 📋 Functions

All functions are available in the `pgmon` schema.

### 🏥 Health & Status

| Function | Description |
|----------|-------------|
| `pgmon.health_check()` | Comprehensive health check with recommendations |
| `pgmon.hello()` | Simple connectivity test |
| `pgmon.cache_hit_ratio()` | Buffer cache hit ratio (target > 99%) |
| `pgmon.database_size_info()` | Database sizes in bytes and pretty format |
| `pgmon.checkpoint_stats()` | Checkpoint statistics |

### 🔌 Connections

| Function | Description |
|----------|-------------|
| `pgmon.get_active_connections()` | All non-idle connections with duration |
| `pgmon.connection_pool_stats()` | Aggregated connection counts by state |
| `pgmon.connections_by_database()` | Connection counts per database |
| `pgmon.check_idle_in_transaction(min)` | Sessions idle in transaction > N minutes |

### 🐢 Queries & Performance

| Function | Description |
|----------|-------------|
| `pgmon.check_slow_queries(sec)` | Active queries running > N seconds |
| `pgmon.top_queries_by_total_time(limit)` | Top queries by execution time |
| `pgmon.queries_using_temp_files()` | Queries spilling to disk |
| `pgmon.active_wait_events()` | Current wait events |

### 🔒 Locks & Blocking

| Function | Description |
|----------|-------------|
| `pgmon.active_locks()` | Currently held locks |
| `pgmon.blocking_queries()` | Who is blocking whom |

### 🧹 VACUUM & Bloat

| Function | Description |
|----------|-------------|
| `pgmon.autovacuum_status()` | Autovacuum worker status |
| `pgmon.vacuum_progress()` | VACUUM operation progress |
| `pgmon.tables_needing_vacuum(threshold)` | Tables with dead tuples > threshold |
| `pgmon.table_bloat_estimate()` | Table bloat estimation |

### 📇 Indexes

| Function | Description |
|----------|-------------|
| `pgmon.index_usage_stats()` | Index hit rates and scan counts |
| `pgmon.unused_indexes(min_mb)` | Unused indexes > N MB |
| `pgmon.duplicate_indexes()` | Redundant indexes |
| `pgmon.missing_indexes_analysis()` | Missing index suggestions |

### 🔐 Security

| Function | Description |
|----------|-------------|
| `pgmon.configuration_security_check()` | Security settings audit |
| `pgmon.superuser_accounts()` | Users with superuser privileges |
| `pgmon.ssl_connections()` | SSL connection status |

### 🔁 Replication

| Function | Description |
|----------|-------------|
| `pgmon.replication_status()` | Standby status and lag |
| `pgmon.replication_slots_info()` | Replication slot status |
| `pgmon.standby_lag_analysis()` | Detailed lag analysis |

---

## 🐛 Bug Detection (v0.9.0)

Detection for **currently open PostgreSQL bugs** affecting versions 13-16.

### CRITICAL Bugs

| Bug ID | Versions | Description |
|--------|----------|-------------|
| `RECOVERY_CONFLICT` | 13-16 | RecoveryConflictInterrupt crashes on standby |
| `GROUPING_SETS` | 13-16 | Incorrect results with ROLLUP/CUBE |
| `TOAST_ACCESS` | 13-16 | Access to removed TOAST data |

### Detection Functions

| Function | Description |
|----------|-------------|
| `pgmon.open_bugs_report()` | **Main function** - All bugs affecting your version |
| `pgmon.bug_status_summary()` | Quick exposure summary |
| `pgmon.check_recovery_conflict_risk()` | Standby crash risk |
| `pgmon.check_grouping_sets_usage()` | GROUPING SETS query detection |
| `pgmon.check_toast_issues()` | TOAST table health |
| `pgmon.check_partition_trigger_status()` | Partition trigger issues |
| `pgmon.check_2pc_usage()` | Two-phase commit problems |
| `pgmon.get_applicable_bugs()` | Bugs for your PG version |

### Usage

```sql
-- Am I at risk?
SELECT * FROM pgmon.bug_status_summary();

-- Detailed bug report
SELECT * FROM pgmon.open_bugs_report();

-- Check standby crash risk
SELECT * FROM pgmon.check_recovery_conflict_risk();

-- Find GROUPING SETS queries (may return wrong results)
SELECT * FROM pgmon.check_grouping_sets_usage();
```

---

## 🕰️ Historical Issues (v0.8.0)

Detection for well-known PostgreSQL problems from v6.x to v15+.

### Key Functions

| Function | Description |
|----------|-------------|
| `pgmon.historical_issues_report()` | All historical issues report |
| `pgmon.xid_age_check()` | XID wraparound risk |
| `pgmon.multixact_age_check()` | MultiXact age (v9.3 bug) |
| `pgmon.fsync_configuration_check()` | fsync/data integrity settings |
| `pgmon.inactive_replication_slots()` | Slots filling disk |
| `pgmon.partition_statistics()` | Partition count analysis |
| `pgmon.jit_statistics()` | JIT compilation overhead |
| `pgmon.index_health_check()` | Index corruption detection |

### Usage

```sql
-- Full historical issues report
SELECT * FROM pgmon.historical_issues_report();

-- Check XID wraparound risk
SELECT * FROM pgmon.xid_age_check() WHERE severity IN ('CRITICAL', 'HIGH');

-- Check fsync configuration
SELECT * FROM pgmon.fsync_configuration_check();
```

---

## 📖 Examples

### Health Check

```sql
SELECT * FROM pgmon.health_check();
```

```text
 category    |      check_name      | status  | severity |          recommendation
-------------+----------------------+---------+----------+----------------------------------
 Performance | Cache Hit Ratio      | OK      | LOW      | Cache performance is good
 Locks       | Blocking Queries     | WARNING | HIGH     | Investigate blocking queries
 Maintenance | Dead Tuples          | OK      | LOW      | Dead tuple count is acceptable
```

### Find Blocking Queries

```sql
SELECT * FROM pgmon.blocking_queries();
```

```text
 blocked_pid | blocked_user | blocked_query         | blocking_pid | blocking_query
-------------+--------------+-----------------------+--------------+------------------------
 10234       | app_user     | UPDATE orders SET ... | 9876         | BEGIN; UPDATE orders...
```

### Bug Detection

```sql
SELECT * FROM pgmon.open_bugs_report();
```

```text
 bug_id          | severity | status   | pg_version | affected_feature      | recommendation
-----------------+----------+----------+------------+-----------------------+------------------
 GROUPING_SETS   | CRITICAL | AT RISK  | 15.4       | Aggregate Functions   | Avoid ROLLUP/CUBE
 RECOVERY_CONFLICT| CRITICAL | DETECTED | 15.4       | Hot Standby           | Monitor standby
```

---

## 🔧 Upgrade

To upgrade from a previous version:

```sql
ALTER EXTENSION pg_monitor UPDATE TO '0.9.0';
```

Available upgrade paths:
- 0.1.0 → 0.2.0 → 0.3.0 → 0.4.0 → 0.5.0 → 0.6.0 → 0.7.0 → 0.8.0 → 0.9.0

---

## 📁 Files

```text
pg_monitor_extension/
├── Makefile                         # Build configuration
├── pg_monitor.control               # Extension metadata
├── pg_monitor--0.9.0.sql            # Current version (full)
├── pg_monitor--0.8.0.sql            # Previous version
├── pg_monitor--0.8.0--0.9.0.sql     # Migration script
├── pg_monitor_extension_lite.sql    # Lite version v0.9.0 (Docker/Cloud)
├── src/
│   └── pg_monitor.c                 # C functions (optional)
└── README.md                        # This file
```

## AI Integration

### Get All Data for AI Analysis

The `ai_comprehensive_analysis()` function returns a complete JSON document with all metrics needed for AI-powered analysis:

```sql
SELECT pgmon.ai_comprehensive_analysis();
```

Returns a JSON object with:
- `database_info` - Database name, version, size, uptime
- `health_check` - Complete health check results
- `connections` - Connection statistics and breakdown
- `performance` - Cache hit, checkpoints, slow queries
- `locks` - Blocking queries, deadlocks
- `vacuum_status` - Tables needing vacuum
- `indexes` - Unused, duplicate, missing, bloated indexes
- `tables` - Top tables by size, bloated tables
- `replication` - Replication status and lag
- `security` - Superusers, SSL, config issues
- `query_metrics` - Slow queries, patterns
- `recent_trends` - Growth and performance trends
- `configuration` - Key PostgreSQL settings
- `recommendations` - Immediate actions and optimizations

### High UPDATE/DELETE Workload Analysis (v0.7.0 NEW!)

For databases with high UPDATE/DELETE activity requiring periodic REPACK:

```sql
-- Get complete REPACK analysis for AI
SELECT pgmon.ai_repack_analysis();
```

Returns a JSON object with:
- `high_update_tables` - Tables with highest UPDATE/DELETE activity
- `hot_update_efficiency` - HOT update ratio (goal: > 90%)
- `bloat_growth_rate` - How fast bloat accumulates
- `churn_rate` - Combined UPDATE+DELETE activity
- `repack_recommendations` - Priority-sorted REPACK suggestions
- `repack_history` - Past REPACK executions and effectiveness
- `fillfactor_recommendations` - Optimal fillfactor settings
- `schedule_estimates` - When to schedule next REPACK

### Monitoring High UPDATE Tables

```sql
-- Find tables with more than 10,000 updates since stats reset
SELECT * FROM pgmon.high_update_tables(10000);

-- Check HOT update efficiency (low efficiency = more bloat)
SELECT * FROM pgmon.hot_update_efficiency();

-- View bloat growth rate over time
SELECT * FROM pgmon.bloat_growth_rate();

-- Get prioritized REPACK recommendations
SELECT * FROM pgmon.repack_recommendations();
```

### REPACK Scheduling

```sql
-- Estimate when a table needs REPACK
SELECT * FROM pgmon.repack_schedule_estimate('public', 'my_table');

-- Get fillfactor recommendations for UPDATE-heavy tables
SELECT * FROM pgmon.fillfactor_analysis();
```

### Recording REPACK Executions

```sql
-- Record before REPACK
SELECT size_mb, dead_tuple_percent FROM pgmon.table_bloat_detailed()
WHERE schemaname = 'public' AND tablename = 'my_table';

-- After REPACK, record the execution
SELECT pgmon.record_repack_execution('public', 'my_table', 'pg_repack', 1024, 256);

-- Compare pre/post REPACK effectiveness
SELECT * FROM pgmon.compare_pre_post_repack('public', 'my_table');

-- View REPACK effectiveness history
SELECT * FROM pgmon.repack_effectiveness_history(30);
```

### Schedule Metric Snapshots

For trend analysis, schedule periodic snapshots:

```sql
-- Run every 5 minutes via cron or pg_cron
SELECT pgmon.capture_metrics_snapshot();
SELECT pgmon.capture_table_metrics_snapshot();
```

### Cleanup Old Data

```sql
-- Keep 30 days of history
SELECT * FROM pgmon.cleanup_history(30);
```

## Tables

### pgmon.alert_history
Stores historical alerts for tracking purposes.

```sql
SELECT * FROM pgmon.alert_history 
ORDER BY created_at DESC 
LIMIT 10;
```

### pgmon.metrics_history (NEW!)
Time-series database metrics for trend analysis.

### pgmon.table_metrics_history (NEW!)
Time-series table-level metrics.

### pgmon.query_metrics
Persistent query performance metrics.

### pgmon.query_plans (NEW!)
Stored EXPLAIN plans for analysis.

### pgmon.update_activity (v0.7.0 NEW!)
Historical UPDATE/DELETE activity tracking per table.

```sql
SELECT * FROM pgmon.update_activity 
WHERE table_name = 'my_table'
ORDER BY captured_at DESC 
LIMIT 10;
```

### pgmon.repack_history (v0.7.0 NEW!)
REPACK execution history and effectiveness tracking.

```sql
SELECT * FROM pgmon.repack_history
ORDER BY executed_at DESC
LIMIT 10;
```

## Integration with Ruby Application

The extension can be used alongside the existing Ruby application:

```ruby
# lib/pg_monitor/extension_client.rb
class ExtensionClient
  def initialize(connection)
    @conn = connection
  end
  
  def slow_queries(threshold = 5)
    @conn.exec("SELECT * FROM pgmon.check_slow_queries(#{threshold})")
  end
  
  def cache_hit_ratio
    @conn.exec("SELECT * FROM pgmon.cache_hit_ratio()")
  end
  
  def active_connections
    @conn.exec("SELECT * FROM pgmon.get_active_connections()")
  end
end
```

## Uninstall

```sql
DROP EXTENSION pg_monitor CASCADE;
```

## Development

### Rebuild After Changes

```bash
cd extension
make clean
make
sudo make install
```

### Reinstall Extension

```sql
DROP EXTENSION pg_monitor CASCADE;
CREATE EXTENSION pg_monitor;
```

## Roadmap

- [ ] Background worker for continuous monitoring
- [ ] CPU usage monitoring (C function)
- [ ] I/O statistics (C function)
- [ ] Automatic alert generation
- [ ] Integration with pg_stat_statements
- [ ] Query plan analysis
- [ ] Partition monitoring and maintenance recommendations
- [ ] Auto-REPACK scheduler integration

## Changelog

### Version 0.9.0 (December 2024)
- Added bug detection system for 9 known PostgreSQL bugs (v13-16)
- Added `known_bugs` registry table
- Added bug detection functions: `bug_status_summary()`, `get_applicable_bugs()`, `open_bugs_report()`
- Critical bugs: Recovery conflict crashes, GROUPING SETS issues, TOAST access problems
- Lite extension upgraded to v0.9.0 with full bug detection support
- All versions tested: 0.1.0 through 0.9.0

### Version 0.8.0
- Added 22 historical issue detection functions (v6.x-v15+)
- XID wraparound monitoring
- MultiXact age checking
- fsync configuration validation
- Inactive replication slot detection

### Version 0.7.0
- Added high UPDATE/DELETE workload analysis
- Added REPACK recommendations and scheduling
- Added HOT update efficiency monitoring
- Added bloat growth rate tracking
- Added fillfactor analysis
- Added REPACK history tracking

### Version 0.6.0
- Added metrics history tables
- Added checkpoint statistics
- Added ai_comprehensive_analysis() function
- Added query plan capture

### Version 0.5.0
- Initial release with 50+ monitoring functions
- Six phases of monitoring coverage

---

<div align="center">

**Made with ❤️ for PostgreSQL DBAs**

</div>
