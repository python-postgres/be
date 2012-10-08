/*
 * FDW Support
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <compile.h>
#include <structmember.h>

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq/libpq-be.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "access/transam.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/indexing.h"
#include "storage/block.h"
#include "storage/off.h"
#include "storage/ipc.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "nodes/memnodes.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/syscache.h"
#include "utils/relcache.h"
#include "utils/typcache.h"
#include "mb/pg_wchar.h"

#include "foreign/foreign.h"
#include "foreign/fdwapi.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/extension.h"
#include "pypg/pl.h"
#include "pypg/fdw.h"
#include "pypg/errordata.h"
#include "pypg/errcodes.h"
#include "pypg/error.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/record.h"
#include "pypg/type/array.h"
#include "pypg/type/bitwise.h"
#include "pypg/type/numeric.h"
#include "pypg/type/string.h"
#include "pypg/type/system.h"
#include "pypg/type/timewise.h"
#include "pypg/tupledesc.h"
#include "pypg/statement.h"
#include "pypg/cursor.h"
#include "pypg/module.h"

static void
size(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{

}

static void
paths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{

}

static ForeignScan *
init(
	PlannerInfo *root,
	RelOptInfo *baserel,
	Oid foreigntableid,
	ForeignPath *best_path,
	List *tlist, List *scan_clauses)
{
	return(NULL);
}

static void
explain(ForeignScanState *node, ExplainState *es)
{
	;
}

static void
begin(ForeignScanState *node, int eflags)
{
	;
}

static TupleTableSlot *
iterate(ForeignScanState *node)
{
	return(NULL);
}

static void
restart(ForeignScanState *node)
{

}

static void
end(ForeignScanState *node)
{
	;
}

#if PG_VERSION_NUM >= 902000
static int
sample(
	Relation relation, int elevel,
	HeapTuple *rows, int targrows,
	double *totalrows, double *totaldeadrows)
{
	*totaldeadrows = 0;

	return(0);
}

static bool
analyze(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
	*func = sample;
	return(true);
}
#endif

PG_FUNCTION_INFO_V1(fdw_validator);
Datum
fdw_validator(PG_FUNCTION_ARGS)
{
	Datum options;
	Oid catalog = PG_GETARG_OID(1);

	if (ext_state == init_pending)
		ext_entry();

	/*
	 * XXX: pass onto the implementation's validator.
	 */

	options = PG_GETARG_DATUM(0);

	switch (catalog)
	{
		case ForeignDataWrapperRelationId:
		break;
		case ForeignServerRelationId:
		break;
		case UserMappingRelationId:
		break;
		case ForeignTableRelationId:
		break;
	}

	PG_RETURN_BOOL(1);
}

/*
 * fdw_handler - initialize the fdw structure
 */
PG_FUNCTION_INFO_V1(fdw_handler);
Datum
fdw_handler(PG_FUNCTION_ARGS)
{
	struct FDWR *fdwr;

	if (ext_state == init_pending)
		ext_entry();

	fdwr = palloc0(sizeof(struct FDWR));
	/*
	 * Set to ext_xact_count when the pointer here is actually valid.
	 *
	 * Ultimately identifies whether or not the Python object
	 * can be referenced.
	 */
	fdwr->ext_xact = 0;
	fdwr->implementation = NULL;

	/*
	 * Would use makeNode, but Python is going to store some
	 * handler data here.
	 *
	 * Notably, a reference to the implementation.
	 * However, there's no access to the actual FDW object here;
	 * so leave it empty for the time being.
	 */
	/*nodeTag(fdwr) = T_FdwRoutine;*/

	/*fdwr->node.PlanForeignScan = init;*/
	fdwr->node.ExplainForeignScan = explain;
	fdwr->node.BeginForeignScan = begin;
	fdwr->node.IterateForeignScan = iterate;
	fdwr->node.ReScanForeignScan = restart;
	fdwr->node.EndForeignScan = end;
#if PG_VERSION_NUM >= 902000
	fdwr->node.AnalyzeForeignTable = analyze;
	fdwr->node.GetForeignRelSize = size;
	fdwr->node.GetForeignPaths = paths;
#endif

	PG_RETURN_POINTER(fdwr);
}
