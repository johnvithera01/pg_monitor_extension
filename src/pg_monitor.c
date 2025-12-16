#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_init(void)
{
    /* No-op for now, just to allow the extension to load */
}
