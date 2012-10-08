/*
 * PostgreSQL extension for Python 3
 *
 * Services two entry point sets:
 *  Foreign Data Wrapper (fdw.c)
 *  Procedural Language Extension (pl.c)
 *
 * Common aspects across those services:
 *  Interrupt Management
 *  Postgres Type Interfaces
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
#include "access/tupdesc.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_language.h"
#include "catalog/indexing.h"
#include "storage/block.h"
#include "storage/off.h"
#include "storage/ipc.h"
#include "commands/trigger.h"
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
#include "utils/timestamp.h"
#include "mb/pg_wchar.h"
#include "storage/itemptr.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/extension.h"
#include "pypg/pl.h"
#include "pypg/errordata.h"
#include "pypg/triggerdata.h"
#include "pypg/errcodes.h"
#include "pypg/error.h"
#include "pypg/ist.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/record.h"
#include "pypg/type/array.h"
#include "pypg/type/bitwise.h"
#include "pypg/type/numeric.h"
#include "pypg/type/string.h"
#include "pypg/type/system.h"
#include "pypg/type/timewise.h"
#include "pypg/function.h"
#include "pypg/tupledesc.h"
#include "pypg/statement.h"
#include "pypg/cursor.h"
#include "pypg/module.h"
#include "pypg/xact.h"

PG_MODULE_MAGIC;

/*
 * Number of transactions that the PL participated in.
 *
 * Used to identify whether or not Portals or fn_extra should be ignored.
 *
 * It's started at 2 so that we can use 1 for some special cases.
 */
unsigned long ext_xact_count = 2;

PyMODINIT_FUNC init_Postgres(void);

const char *python_server_encoding = NULL;

/*
 * This is used to determine whether ext_entry needs to
 * be called and whether the pl is inside a failed transaction.
 */
extension_state_t ext_state = init_pending;

/*
 * Primarily used to hold Postgres.Object !typbyval Datums.
 */
MemoryContext PythonMemoryContext = NULL, PythonWorkMemoryContext = NULL;

PyObj TransactionScope = NULL;
PyObj Py_Postgres_module = NULL;

static void
increment_xact_count(void)
{
	/*
	 * increment xact_count
	 */
	++ext_xact_count;
	if (ext_xact_count == 0)
	{
		ext_xact_count = 2;

		HOLD_INTERRUPTS();
		ereport(WARNING, (errmsg("internal transaction counter wrapped")));
		RESUME_INTERRUPTS();
	}
}

static void
xact_hook(XactEvent xev, void *arg)
{
	/*
	 * Expecting _PG_init to have already been called.
	 */
	Assert(Py_IsInitialized());

	switch (xev)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_ABORT:
		{
			/*
			 * Restore the count after GC has run.
			 * During GC, it *is* possible for ereport(WARNING) to be emitted.
			 */
			uint32 stored_InterruptHoldoffCount = InterruptHoldoffCount;

			/*
			 * A residual KeyboardInterrupt may exist.
			 */
			if (ext_state == xact_failed)
			{
				/*
				 * It won't set an interrupt exception if handler_count is zero.
				 */
				if (Py_MakePendingCalls())
				{
					/*
					 * Some other pending call raised an exception.
					 */
					PyErr_EmitPgErrorAsWarning(
						"unexpected Python exception between transactions");
				}

				PyErr_Clear();
				ext_state = ext_ready;
			}

			/*
			 * Any Python error should have been converted to a Postgres error and
			 * cleared before getting here.
			 */
			Assert(!PyErr_Occurred());

			increment_xact_count();

			/*
			 * Reset extension state.
			 */
			ist_count = 0;
			if (ext_state == ext_ready)
				ext_state = xact_between;

			PG_TRY();
			{
				PyObj rob;
				/*
				 * These are not expected elog() out, but protect against
				 * it anyways as arbitrary code is being ran.
				 * [think ob.__del__()]
				 */
				rob = PyObject_CallMethod(Py_Postgres_module, "_xact_exit", "");
				if (rob != NULL)
					Py_DECREF(rob);
				else
				{
					PyErr_RelayException();
				}
			}
			PG_CATCH();
			{
				PyErr_EmitPgErrorAsWarning(
					"unexpected Python exception between transactions");
			}
			PG_END_TRY();

			PG_TRY();
			{
				/*
				 * *Very* unlikely to error out, but protect anyways as this
				 * can execute arbitrary code.
				 */
				PySet_Clear(TransactionScope);
				PyGC_Collect();
			}
			PG_CATCH();
			{
				PyErr_EmitPgErrorAsWarning("unexpected error between transactions");
			}
			PG_END_TRY();

			if (ext_state == xact_between)
				ext_state = ext_ready;

			InterruptHoldoffCount = stored_InterruptHoldoffCount;
		}
		break;
	}
}

/*
 * Used to track if `set_interrupt` is already pending.
 */
static bool interrupt_set = false;

static int
set_interrupt(void *ignored)
{
	if (PyEval_GetFrame() != NULL)
	{
		PG_TRY();
		{
			CHECK_FOR_INTERRUPTS();
		}
		PG_CATCH();
		{
			/*
			 * Inhibit the warning, the interrupt may be overriding
			 * an existing exception.
			 */
			PyErr_SetPgError(true);
		}
		PG_END_TRY();
	}

	/*
	 * It can set the interrupt again.
	 */
	interrupt_set = false;

	/*
	 * It is inside the Python interpreter, so the cleared interrupt
	 * is merely converted like any other PG error.
	 */
	return(-1);
}

static pqsigfunc SIGINT_original = NULL;
static void
ext_sigint(SIGNAL_ARGS)
{
	SIGINT_original(postgres_signal_arg);
	/*
	 * If cancelling and there has been PL activity.
	 */
	if (QueryCancelPending && PyEval_GetFrame() != NULL && interrupt_set == false)
	{
		ext_state = xact_failed;
		interrupt_set = true;
	    Py_AddPendingCall(set_interrupt, NULL);
	}
}

static pqsigfunc SIGTERM_original = NULL;
static void
ext_sigterm(SIGNAL_ARGS)
{
	SIGTERM_original(postgres_signal_arg);
	/*
	 * If dying and there has been PL activity.
	 */
	if (ProcDiePending && PyEval_GetFrame() != NULL && interrupt_set == false)
	{
		ext_state = xact_failed;
		interrupt_set = true;
	    Py_AddPendingCall(set_interrupt, NULL);
	}
}

/*
 * Called prior to Postgres server process exit.
 */
static void
exit_hook(int code, Datum arg)
{
	MemoryContext former = CurrentMemoryContext;

	if (ext_state == ext_term)
	{
		/*
		 * Already terminated...?
		 */
		return;
	}

	if (ext_state == init_pending)
	{
		elog(WARNING,
			"exit callback for Python called, but the language was not initialized");
		/*
		 * Never intialized, so no hooks...
		 */
		return;
	}

	/*
	 * No database access.
	 */
	ext_state = ext_term;

	PyObject_CallMethod(Py_Postgres_module, "_exit", "");
	CurrentMemoryContext = former;

	if (PyErr_Occurred())
	{
		PyErr_ThrowPostgresError("exception occurred during on_proc_exit");
	}

	return;
}

/*
 * Portions of _PG_init have been written with the thought that it may, one day,
 * be useful to be able to completely re-initialize the extension. Additionally, if
 * init were to fail due to a transient error, it is able to fully recover with
 * a subsequent, successful run.
 */
void
_PG_init(void)
{
	PyObj ob;
	static wchar_t progname[] = {
		'p','o','s','t','g','r','e','s',0
	};

	/*
	 * Identify the proper Python encoding name to use to encode/decode strings.
	 */
#define IDSTR(PGID, PYID) \
	case PGID: python_server_encoding = PYID; break;

	switch (GetDatabaseEncoding())
	{
		PG_SERVER_ENCODINGS()

		default:
			/*
			 * XXX: *Most* PG encodings are supported by Python.
			 */
			ereport(ERROR,(
				errmsg("cannot use server encoding with Python"),
				errhint("The database cluster must be created using a different encoding.")
			));
		break;
	}
#undef IDSTR

	/*
	 * Memory Context for Python objects that have
	 * pointers referring to PostgreSQL objects/data.
	 *
	 * Intended to be permanent.
	 */
	if (PythonMemoryContext != NULL)
	{
		MemoryContext tmp = PythonMemoryContext;
		PythonMemoryContext = NULL;
		MemoryContextDelete(tmp);
	}
	PythonMemoryContext = AllocSetContextCreate(TopMemoryContext,
		"PythonMemoryContext",
		ALLOCSET_DEFAULT_MINSIZE,
		ALLOCSET_DEFAULT_INITSIZE,
		ALLOCSET_DEFAULT_MAXSIZE);

	PythonWorkMemoryContext = AllocSetContextCreate(PythonMemoryContext,
		"PythonWorkMemoryContext",
		ALLOCSET_DEFAULT_MINSIZE,
		ALLOCSET_DEFAULT_INITSIZE,
		ALLOCSET_DEFAULT_MAXSIZE);

	Py_SetProgramName(progname);
	Py_Initialize();
	if (!Py_IsInitialized())
		elog(ERROR, "could not initialize Python");

	Py_XDECREF(TransactionScope);
	TransactionScope = PySet_New(NULL);
	if (TransactionScope == NULL)
		PyErr_ThrowPostgresError("could not create transaction set object");

	error_init_tracebacks();

#define IDSTR(NAME) \
	Py_XDECREF(NAME##_str_ob); \
	NAME##_str_ob = PyUnicode_FromString(#NAME); \
	if (NAME##_str_ob == NULL) \
		PyErr_ThrowPostgresError("could not create requisite string object \"" #NAME "\"");
	PL_INTERNAL();
	PL_ENTRY_POINTS();
	PL_MANIPULATIONS()
	PL_TRIGGER_ORIENTATIONS();
	PL_TRIGGER_TIMINGS();
#undef IDSTR

	/*
	 * We now have traceback functionality, so PyErr_ThrowPostgresError is the
	 * appropriate reporting mechanism from here out.
	 */
	Py_XDECREF(Py_Postgres_module);
	Py_Postgres_module = init_Postgres();
	if (Py_Postgres_module == NULL)
		PyErr_ThrowPostgresError("could not initialize Postgres module");

	/*
	 * The _init function is used to pass back some module objects.
	 */
	ob = PyObject_CallMethod(Py_Postgres_module, "_init", "O", Py_Postgres_module);
	if (ob == NULL)
		PyErr_ThrowPostgresError("could not perform Postgres._init");
	else if (!PyTuple_Check(ob))
	{
		Py_DECREF(ob);
		elog(ERROR, "Postgres._init did not return a tuple");
	}
	else if (PyTuple_GET_SIZE(ob) != 9)
	{
		Py_DECREF(ob);
		elog(ERROR, "expected Postgres._init to return a tuple of nine items");
	}

	PyExc_PostgresException = PyTuple_GET_ITEM(ob, 0);
	Py_INCREF(PyExc_PostgresException);

	PyExc_PostgresStopEvent = PyTuple_GET_ITEM(ob, 1);
	Py_INCREF(PyExc_PostgresStopEvent);

	Py_compile_ob = PyTuple_GET_ITEM(ob, 2);
	Py_INCREF(Py_compile_ob);

	Py_ReturnArgs = PyTuple_GET_ITEM(ob, 3);
	Py_INCREF(Py_ReturnArgs);

	PYSTR(pg_inhibit_pl_context) = PyTuple_GET_ITEM(ob, 4);
	Py_INCREF(PYSTR(pg_inhibit_pl_context));

	PYSTR(exec) = PyTuple_GET_ITEM(ob, 5);
	Py_INCREF(PYSTR(exec));

	Py_builtins_module = PyTuple_GET_ITEM(ob, 6);
	Py_INCREF(Py_builtins_module);

	PYSTR(pg_errordata) = PyTuple_GET_ITEM(ob, 7);
	Py_INCREF(PYSTR(pg_errordata));

	Py_linecache_updatecache_ob = PyTuple_GET_ITEM(ob, 8);
	Py_INCREF(Py_linecache_updatecache_ob);

	Py_DECREF(ob);

	RegisterXactCallback(xact_hook, NULL);
	on_proc_exit(exit_hook, (Datum) 0); /* Calls Postgres._exit */

	ereport(DEBUG3,(
			errmsg("initialized Python %s", Py_GetVersion())));
}

/*
 * ext_entry
 *
 * Completes the initialization of the extension.
 * This runs the sys cache hits that could not be done in _PG_init.
 */
void
ext_entry(void)
{
	HeapTuple db;
	PyObj ob;

	/*
	 * Expects _PG_init() has been called.
	 */
	Assert(Py_IsInitialized());

	if (SIGINT_original == NULL)
		SIGINT_original = pqsignal(SIGINT, ext_sigint);
	if (SIGTERM_original == NULL)
		SIGTERM_original = pqsignal(SIGTERM, ext_sigterm);

	Py_XDECREF(py_my_datname_str_ob);
	py_my_datname_str_ob = NULL;

	/*
	 * _PG_init does not have cache/database access, so initialize that here.
	 */
	db = SearchSysCache(DATABASEOID, MyDatabaseId, 0, 0, 0);
	if (!HeapTupleIsValid(db))
		elog(ERROR, "could not get pg_database row for %u", MyDatabaseId);

	py_my_datname_str_ob = PyUnicode_FromCString(
		NameStr(((Form_pg_database) GETSTRUCT(db))->datname)
	);
	ReleaseSysCache(db);

	if (py_my_datname_str_ob == NULL)
		PyErr_ThrowPostgresError("create not create database name string");

	if (PyModule_AddObject(Py_Postgres_module, "current_database", py_my_datname_str_ob) == -1)
	{
		PyErr_ThrowPostgresError(
			"could not set \"current_database\" constant");
	}
	Py_INCREF(py_my_datname_str_ob);

	/* client_addr */
	if (MyProcPort != NULL && MyProcPort->remote_host != NULL)
	{
		if (PyModule_AddStringConstant(
				Py_Postgres_module, "client_addr", MyProcPort->remote_host))
		{
			PyErr_ThrowPostgresError("could not set \"client_addr\" constant");
		}
	}
	else
	{
		/*
		 * otherwise None
		 */
		if (PyModule_AddObject(
				Py_Postgres_module, "client_addr", Py_None))
		{
			PyErr_ThrowPostgresError("could not set \"client_addr\" constant");
		}
		Py_INCREF(Py_None);
	}

	/* client_port */
	if (MyProcPort != NULL && MyProcPort->remote_port != NULL)
	{
		if (PyModule_AddStringConstant(
				Py_Postgres_module, "client_port", MyProcPort->remote_port))
		{
			PyErr_ThrowPostgresError("could not set \"client_port\" constant");
		}
	}
	else
	{
		/*
		 * otherwise None
		 */
		if (PyModule_AddObject(
				Py_Postgres_module, "client_port", Py_None))
		{
			PyErr_ThrowPostgresError("could not set \"client_port\" constant");
		}
		Py_INCREF(Py_None);
	}

	if (PyModule_AddStringConstant(
			Py_Postgres_module, "encoding", python_server_encoding))
	{
		PyErr_ThrowPostgresError("could not set \"encoding\" constant");
	}

	/*
	 * State needs to be ready from here out.
	 * Reset to ext_not_initialized on failure.
	 */
	ext_state = ext_ready;

	if (PyPgType_Init() == -1)
	{
		ext_state = init_pending;
		PyErr_ThrowPostgresError("could not initialize built-in types");
	}

	/* backend_start (needs types to be initialized) */
	if (MyProcPort != NULL)
	{
		PyObj ob;
		ob = PyPgObject_New(&PyPg_timestamptz_Type,
								TimestampTzGetDatum(MyProcPort->SessionStartTime));
		if (ob == NULL)
		{
			ext_state = init_pending;
			PyErr_ThrowPostgresError("could not create \"backend_start\" object");
		}
		else
		{
			if (PyModule_AddObject(
					Py_Postgres_module, "backend_start", ob))
			{
				Py_DECREF(ob);
				ext_state = init_pending;
				PyErr_ThrowPostgresError("could not set \"backend_start\" constant");
			}
		}
	}
	else
	{
		/*
		 * otherwise None
		 */
		if (PyModule_AddObject(
				Py_Postgres_module, "backend_start", Py_None))
		{
			ext_state = init_pending;
			PyErr_ThrowPostgresError("could not set \"backend_start\" constant");
		}
		Py_INCREF(Py_None);
	}

	ob = PyObject_CallMethod(Py_Postgres_module, "_entry", "");
	if (ob == NULL)
	{
		/*
		 * Initialization failed.
		 */
		ext_state = init_pending;
		PyErr_ThrowPostgresError("module \"Postgres\" failed to initialize");
	}
	Py_DECREF(ob);
}

/*
 * check_state
 *
 * Validate that all ISTs have been closed, and that the
 * PL has not recognized an error condition without a Python
 * exception.
 *
 * The elevel is used to allow a WARNING to be issued instead of
 * an error in cases where an ERROR condition is already present.
 * That is, a Python exception occurred *and* there were open
 * subtransactions.
 */
void
ext_check_state(int elevel, unsigned long previous_ist_count)
{
	/*
	 * Subtransaction and Error state checks.
	 *
	 * First, check the IST count. If there are open ISTs, abort them and
	 * report the inappropriate exit state using an error or a warning
	 * if the PL is failing out.
	 */
	Assert(elevel == ERROR || elevel == WARNING);

	if (ist_count > previous_ist_count)
	{
		unsigned long dif = ist_count - previous_ist_count;

		/*
		 * Abort the lingering subtransactions.
		 */
		ist_reset(dif);

		/*
		 * Restore the count to where it was before entering Python code.
		 */
		ist_count = previous_ist_count;

		if (ext_state > 0)
		{
			/* reset the indicator; error is being indicated */
			/* However, do so before warning in case of interrupt. */
			ext_state = ext_ready;

			/*
			 * Only warn about the failure as the savepoint exception is
			 * the appropriate error to thrown in this situation.
			 */

			HOLD_INTERRUPTS();
			ereport(WARNING,(
				errcode(ERRCODE_PYTHON_PROTOCOL_VIOLATION),
				errmsg("function failed to propagate error state")
			));
			RESUME_INTERRUPTS();
		}

		if (elevel == WARNING)
			HOLD_INTERRUPTS();
		ereport(elevel,(
			errcode(ERRCODE_SAVEPOINT_EXCEPTION),
			errmsg("function failed to exit all subtransactions"),
			errdetail("The %lu remaining subtransactions have been aborted.", dif)
		));
		if (elevel == WARNING)
			RESUME_INTERRUPTS();
	}

	/*
	 * The Python code caused a database error, but did not raise it or
	 * any other error. It protests that things are fine, but it was noted
	 * earlier to not be the case.
	 * [Only an IST-Abort can "correct" the error conditon without ultimately
	 * emitting an error.]
	 */
	if (ext_state > 0)
	{
		ext_state = ext_ready; /* reset the indicator; error will be raised */

		ereport(ERROR,(
			errcode(ERRCODE_PYTHON_PROTOCOL_VIOLATION),
			errmsg("function failed to propagate error state"),
			errhint("A subtransaction can be used to recover from database errors.")
		));
	}
}
