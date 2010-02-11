/*
 * exit.c - on_proc_exit handler implementation
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "postgres.h"
#include "fmgr.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "access/transam.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "storage/itemptr.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/strings.h"
#include "pypg/externs.h"
#include "pypg/pl.h"
#include "pypg/error.h"

#include "pypg/exit.h"

/*
 * pl_exit - on_proc_exit callback
 */
void
pl_exit(int code, Datum arg)
{
	MemoryContext former = CurrentMemoryContext;

	if (pl_state == pl_terminated)
	{
		/*
		 * Already terminated...
		 */
		return;
	}

	if (pl_state == pl_not_initialized)
	{
		elog(WARNING, "exit callback for Python called, but the language was not initialized");
		/*
		 * Never intialized, so no hooks...
		 */
		return;
	}

	/*
	 * No database access.
	 */
	pl_state = pl_terminated;

	PyObject_CallMethod(Py_Postgres_module, "_pl_on_proc_exit", "");
	CurrentMemoryContext = former;

	if (PyErr_Occurred())
	{
		PyErr_ThrowPostgresError("could not initialize on_proc_exit function");
	}

	return;
}
