MODULE_big = pg_monitor
OBJS = src/pg_monitor.o

EXTENSION = pg_monitor
DATA = pg_monitor--0.9.0.sql pg_monitor--0.8.0--0.9.0.sql
PGFILEDESC = "pg_monitor - Advanced PostgreSQL monitoring"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
