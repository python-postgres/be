/*
 * Python 3.x procedural language extension
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
#include "mb/pg_wchar.h"

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

struct pl_exec_state *pl_execution_context = NULL;

/*
 * SXD() - Used to provide *some* information about what the PL is doing.
 *
 * Often, a Python error can occur outside of the function module.
 * When this happens, there is no traceback. The execution context description
 * attempts to fill some of that void with what the PL was trying to do at the
 * time.
 */
#define SXD(DESCR) pl_execution_context->description = DESCR

/*
 * Used by SRFs
 */
PyObj Py_ReturnArgs = NULL;
/*
 * Postgres.StopEvent (triggers)
 */
PyObj PyExc_PostgresStopEvent = NULL;

/*
 * common, persistent, global strings(PyUnicode).
 *
 * main_str_ob = "main"
 * before_insert_str_ob = "before_insert"
 * INSERT_str_ob = "INSERT"
 * BEFORE_str_ob = "BEFORE"
 * STATEMENT_str_ob = "STATEMENT"
 * etc..
 */
#define IDSTR(NAME) PyObj NAME##_str_ob = NULL;
PL_INTERNAL()
PL_ENTRY_POINTS()
PL_MANIPULATIONS()
PL_TRIGGER_ORIENTATIONS()
PL_TRIGGER_TIMINGS()
#undef IDSTR

/*
 * Call the function's load_module() method.
 *
 * This does *not* execute main.
 * It only runs the module code in preparation for main.
 */
static PyObj
run_PyPgFunction_module(PyObj func)
{
	unsigned long stored_ist_count = ist_count;
	PyObj rob;

	Assert(func != NULL);
	Assert(PyPgFunction_CheckExact(func));

	SXD("loading function module");
	rob = PyPgFunction_load_module(func);

	if (rob == NULL)
	{
		/*
		 * Error is being indicated via a thrown Python exception,
		 * only correct and warn about the issue, if necessary.
		 */
		if (ext_state > 0)
			ext_state = ext_ready;

		ext_check_state(WARNING, stored_ist_count);

		PyErr_ThrowPostgresError(
			"could not load Python function's module object");
	}
	else
	{
		/*
		 * Add the function module to the transaction scope.
		 * This is kept in the transaction scope for now until
		 * it actually gets into sys.modules.
		 */
		if (Py_XACTREF(rob) == -1)
		{
			Py_DECREF(rob);
			PyErr_ThrowPostgresError(
				"failed to add function module to transaction scope");
		}
		Py_DECREF(rob);

		/*
		 * The module body can start ISTs, so the count and state needs to be
		 * validated.
		 */
		ext_check_state(ERROR, stored_ist_count);
	}

	return(rob);
}

/*
 * Get the currently cached module object from sys.modules or create one.
 *
 * >>> getattr(sys.modules.get(str(fn_oid)), '__func__', None) or Postgres.Function(fn_oid)
 */
static PyObj
get_PyPgFunction_from_oid(Oid fn_oid, PyObj *module)
{
	PyObj modules, func = NULL;

	Assert(OidIsValid(fn_oid));
	Assert(module != NULL);

	modules = PyImport_GetModuleDict(); /* borrowed */
	if (PyErr_Occurred())
		return(NULL);

	Py_ALLOCATE_OWNER();
	{
		PyObj so;

		so = PyUnicode_FromFormat("%lu", fn_oid);
		if (so == NULL)
		{
			/* Nothing has been acquired so it's safe to return here. */
			return(NULL);
		}
		Py_ACQUIRE(so);

		/*
		 * Check if requested function exists in sys.modules.
		 */
		if (PyMapping_HasKey(modules, so) == 1)
		{
			/*
			 * Function has already been loaded. Check for protocol consistency.
			 */
			*module = PyObject_GetItem(modules, so);
			if (*module != NULL)
			{
				Py_ACQUIRE(*module);

				func = PyObject_GetAttr(*module, __func___str_ob);
				if (func != NULL)
				{
					Py_ACQUIRE(func);

					if (!PyPgFunction_CheckExact(func))
					{
						PyErr_SetString(PyExc_TypeError,
							"module's '__func__' attribute is not a Postgres.Function object");
						func = NULL;
					}
					else if (PyPgFunction_GetOid(func) != fn_oid)
					{
						PyErr_SetString(PyExc_ValueError,
							"module's '__func__' attribute does not have the expected object identifier");
						func = NULL;
					}
					else
					{
						/*
						 * It's good, INCREF for *return*.
						 */
						Py_INCREF(func);
						Py_INCREF(*module);
					}
				}
			}
		}
		else
		{
			*module = NULL;
			func = PyPgFunction_FromOid(fn_oid);
		}
	}
	Py_DEALLOCATE_OWNER();

	return(func);
}

static PyObj
build_args(PyObj input, int nargs, Datum *arg, bool *argnull)
{
	PyObj rob;

	SXD("building arguments");

	/*
	 * elog as this expects proper argument counts.
	 */
	if (PyObject_Length(input) != nargs)
		elog(ERROR, "invalid number of argument for Python function");

	rob = PyTuple_New(nargs);
	if (rob == NULL)
		PyErr_ThrowPostgresError(
			"failed to create arguments tuple for function invocation");

	if (nargs > 0)
	{
		int i;

		for (i = 0; i < nargs; ++i)
		{
			PyObj typ = NULL, ob = NULL;

			if (argnull[i])
			{
				ob = Py_None;
				Py_INCREF(ob);
			}
			else
			{
				typ = PyPgTupleDesc_GetAttributeType(input, i);
				if (typ == NULL)
				{
					Py_DECREF(rob);
					PyErr_ThrowPostgresError("failed to lookup argument type");
				}
				ob = PyPgObject_New(typ, arg[i]);
			}
			if (ob == NULL)
			{
				Py_DECREF(rob);
				PyErr_ThrowPostgresError(
					"failed to build arguments for function invocation");
			}

			PyTuple_SET_ITEM(rob, i, ob);
		}
	}

	return(rob);
}

/*
 * invoke the "main" object in the given module object using the given args
 */
static PyObj
invoke_main(PyObj module, PyObj args)
{
	PyObj main_ob, rob;

	/*
	 * Yes, get the attribute everytime.
	 */
	main_ob = PyObject_GetAttr(module, main_str_ob);
	if (main_ob == NULL)
	{
		Py_DECREF(args);
		PyErr_ThrowPostgresErrorWithCode(
			ERRCODE_PYTHON_PROTOCOL_VIOLATION,
			"function module has no \"main\" object");
	}

	SXD("executing main");

	rob = PyObject_CallObject(main_ob, args);
	Py_DECREF(main_ob);
	Py_DECREF(args);

	SXD(NULL);

	if (rob == NULL)
		PyErr_ThrowPostgresErrorWithCode(
			ERRCODE_PYTHON_EXCEPTION,
			"function's \"main\" raised a Python exception");

	return(rob);
}

/*
 * SRF ExprContext CallBack function to clean up after VPC-SRFs
 */
static void
srf_eccb(Datum arg)
{
	struct pl_fn_info *fn_info = (struct pl_fn_info *) DatumGetPointer(arg);
	Assert(fn_info != NULL);
	Assert(fn_info->fi_internal_state != NULL);

	/*
	 * XXX: Perhaps too optimistic about this not failing.
	 */
	Py_DEXTREF(fn_info->fi_internal_state);
	fn_info->fi_internal_state = NULL;
}

/*
 * pl_validator - compile the function to validate its syntax.
 */
PG_FUNCTION_INFO_V1(pl_validator);
Datum
pl_validator(PG_FUNCTION_ARGS)
{
	Oid fn_oid = PG_GETARG_OID(0);
	PyObj func, code;
	PyGILState_STATE gs;
	struct pl_exec_state pl_ctx = {NULL, NULL, NULL,};

	Assert(fn_oid != InvalidOid);
	Assert(Py_IsInitialized());

	if (ext_state == init_pending)
		ext_entry();

	gs = PyGILState_Ensure();
	PG_TRY();
	{
		func = PyPgFunction_FromOid(fn_oid);
		if (func == NULL)
		{
			PyErr_ThrowPostgresErrorWithContext(
				ERRCODE_PYTHON_ERROR,
				"could not create Postgres.Function from Oid",
				&pl_ctx);
		}

		code = PyPgFunction_get_code(func);
		if (code == NULL)
		{
			Py_DECREF(func);
			PyErr_ThrowPostgresErrorWithContext(
				ERRCODE_PYTHON_ERROR,
				"cannot compile Python function",
				&pl_ctx);
		}
		Py_DECREF(code);

		/*
		 * All functions that are looked up are cached. This is mere validation,
		 * so there's no need to actually keep it around.
		 */
		if (PyPgFunction_RemoveModule(func))
		{
			Py_DECREF(func);
			PyErr_ThrowPostgresErrorWithContext(
				ERRCODE_PYTHON_ERROR,
				"could not remove of function module from sys.modules",
				&pl_ctx);
		}
		Py_DECREF(func);
	}
	PG_CATCH();
	{
		PyGILState_Release(gs);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/*
	 * If a Python error occurred, it should have been raised by now.
	 */
	Assert(!PyErr_Occurred());
	PyGILState_Release(gs);

	return(0);
}

/*
 * Given a TriggerEvent, return the PyUnicode object
 * that is used to represent the handler.
 */
static PyObj
select_trigger_handler(TriggerEvent tev)
{
#define Py_ROW_TRIGGER_TEMPLATE(ev,name) \
	case TRIGGER_EVENT_##ev|TRIGGER_EVENT_ROW|TRIGGER_EVENT_BEFORE: \
		return(before_##name##_str_ob); \
	break; \
	case TRIGGER_EVENT_##ev|TRIGGER_EVENT_ROW: \
		return(after_##name##_str_ob); \
	break;
#define Py_STATEMENT_TRIGGER_TEMPLATE(ev,name) \
	case TRIGGER_EVENT_##ev|TRIGGER_EVENT_BEFORE: \
		return(before_##name##_statement_str_ob); \
	break; \
	case TRIGGER_EVENT_##ev: \
		return(after_##name##_statement_str_ob); \
	break;

	switch (tev & (TRIGGER_EVENT_OPMASK|TRIGGER_EVENT_ROW|TRIGGER_EVENT_BEFORE))
	{
		Py_ROW_TRIGGER_TEMPLATE(INSERT,insert)
		Py_STATEMENT_TRIGGER_TEMPLATE(INSERT,insert)
		Py_ROW_TRIGGER_TEMPLATE(UPDATE,update)
		Py_STATEMENT_TRIGGER_TEMPLATE(UPDATE,update)
		Py_ROW_TRIGGER_TEMPLATE(DELETE,delete)
		Py_STATEMENT_TRIGGER_TEMPLATE(DELETE,delete)
#if TRIGGER_EVENT_TRUNCATE != 0xDEADBEEF
		Py_STATEMENT_TRIGGER_TEMPLATE(TRUNCATE,truncate)
#endif

		default:
			return(NULL);
		break;
	}
#undef Py_ROW_TRIGGER_TEMPLATE
#undef Py_STATEMENT_TRIGGER_TEMPLATE
}

/*
 * row_trigger - execute the row handler for the event
 */
static Datum
row_trigger(PyObj handler, PyObj trigger_data,
	HeapTuple ht_old, HeapTuple ht_new)
{
	Datum rd = 0;
	MemoryContext former;
	PyObj rob, args, old = NULL, new = NULL;
	PyObj reltype;
	PyObj timing;

	/*
	 * If both are not NULL, it's an update.
	 */
	if (ht_new != NULL && ht_old != NULL)
		args = PyTuple_New(3);
	else
		args = PyTuple_New(2);
	if (args == NULL)
		PyErr_ThrowPostgresError("failed to build arguments tuple for trigger");

	/*
	 * Using the pl_handler's reference owner.
	 */
	Py_ACQUIRE(args);
	Py_INCREF(trigger_data);
	PyTuple_SET_ITEM(args, 0, trigger_data);

	reltype = PyPgTriggerData_GetRelationType(trigger_data);
	timing = PyPgTriggerData_GetTiming(trigger_data);

	/*
	 * Need to be in PythonMemoryContext for "PyPgObject_FromPyPgTypeAndHeapTuple".
	 *
	 * These *can* elog-out.
	 */
	former = MemoryContextSwitchTo(PythonMemoryContext);
	if (ht_old == NULL)
	{
		/* INSERT */
		new = PyPgObject_FromPyPgTypeAndHeapTuple(reltype, ht_new);
		PyTuple_SET_ITEM(args, 1, new);
	}
	else if (ht_new == NULL)
	{
		/* DELETE */
		old = PyPgObject_FromPyPgTypeAndHeapTuple(reltype, ht_old);
		PyTuple_SET_ITEM(args, 1, old);
	}
	else
	{
		/* UPDATE */
		old = PyPgObject_FromPyPgTypeAndHeapTuple(reltype, ht_old);
		PyTuple_SET_ITEM(args, 1, old);

		new = PyPgObject_FromPyPgTypeAndHeapTuple(reltype, ht_new);
		PyTuple_SET_ITEM(args, 2, new);
	}
	MemoryContextSwitchTo(former);

	rob = PyObject_CallObject(handler, args);
	/*
	 * ReferenceOwner will handle args DECREF, and subseqently new &| old.
	 */

	if (rob == NULL)
	{
		/*
		 * Python exception raised.
		 */
		if (PyErr_ExceptionMatches(PyExc_PostgresStopEvent))
		{
			/*
			 * Indicate that they should not throw StopEvent in AFTER triggers.
			 *
			 * Note that ERRCODE_PYTHON_PROTOCOL_VIOLATION is used.
			 */
			if (timing == AFTER_str_ob)
				PyErr_ThrowPostgresErrorWithCode(
					ERRCODE_PYTHON_PROTOCOL_VIOLATION,
					"cannot stop events that have already occurred");

			/*
			 * Return a zero-Datum to stop the event.
			 */
			PyErr_Clear();
			return(0);
		}

		PyErr_ThrowPostgresError("trigger function raised Python exception");
	}
	else
	{
		if (rob == Py_None || rob == new)
		{
			/*
			 * Returned `None`; do default action.
			 */
			Py_DECREF(rob);
			rd = PointerGetDatum(ht_new ? ht_new : ht_old);
		}
		else if (rob == old)
		{
			Py_DECREF(rob);
			rd = PointerGetDatum(ht_old);
		}
		else if (timing == AFTER_str_ob)
		{
			/*
			 * Be picky about the return value. This should help identify
			 * potential problems early.
			 */
			Py_DECREF(rob);
			ereport(ERROR,
				(errcode(ERRCODE_PYTHON_PROTOCOL_VIOLATION),
				errmsg("non-None value returned by trigger fired after"))
			);
		}
		else
		{
			MemoryContext former;
			Datum hthd;
			bool isnull = false;
			HeapTupleData ht;
			PyObj row;

			row = Py_NormalizeRow(
						PyPgTupleDesc_GetNatts(PyPgType_GetPyPgTupleDesc(reltype)),
						PyPgType_GetTupleDesc(reltype),
						PyPgTupleDesc_GetNameMap(PyPgType_GetPyPgTupleDesc(reltype)),
						rob);
			Py_DECREF(rob);
			if (row == NULL)
				PyErr_ThrowPostgresError("could not normalize replacement row");
			rob = row;

			PyPgType_DatumNew(reltype, rob, -1, &hthd, &isnull);

			ht.t_data = (HeapTupleHeader) DatumGetPointer(hthd);
			ht.t_len = HeapTupleHeaderGetDatumLength(ht.t_data);
			ht.t_tableOid = PyPgType_GetTableOid(reltype);

			former = MemoryContextSwitchTo(pl_execution_context->return_memory_context);
			rd = PointerGetDatum(heap_copytuple(&ht));
			MemoryContextSwitchTo(former);

			pfree(DatumGetPointer(hthd));
		}
	}

	return(rd);
}

/*
 * statement_trigger - execute the statement handler for the event
 */
static Datum
statement_trigger(PyObj handler, PyObj trigger_data)
{
	PyObj rob, args;

	if (PyPgTriggerData_GetTiming(trigger_data) == AFTER_str_ob)
	{
		/*
		 * If Postgres ever gets transition tables, this will look much different.
		 */
		if (PyPgTriggerData_GetManipulation(trigger_data) == UPDATE_str_ob)
		{
			args = PyTuple_New(3);
			Py_INCREF(Py_None);
			Py_INCREF(Py_None);
			PyTuple_SET_ITEM(args, 1, Py_None);
			PyTuple_SET_ITEM(args, 2, Py_None);
		}
		else
		{
			args = PyTuple_New(2);
			Py_INCREF(Py_None);
			PyTuple_SET_ITEM(args, 1, Py_None);
		}
	}
	else
	{
		/*
		 * BEFORE the statements run, there are no transition tables.
		 */
		args = PyTuple_New(1);
	}

	Py_INCREF(trigger_data);
	PyTuple_SET_ITEM(args, 0, trigger_data);

	rob = PyObject_CallObject(handler, args);
	Py_DECREF(args);

	if (rob == NULL)
	{
		PyErr_ThrowPostgresError("statement trigger raised Python exception");
	}
	else
	{
		Py_DECREF(rob); /* whatcha gonna do with it? */

		if (rob != Py_None)
		{
			ereport(ERROR,(
				errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				errmsg("statement trigger did not return None")
			));
		}
	}

	return(0);
}

/*
 * Execute a trigger function.
 */
static Datum
pull_trigger(PG_FUNCTION_ARGS)
{
	struct pl_fn_info *fn_info = fcinfo->flinfo->fn_extra;
	Datum rd;
	PyObj handler_str_ob, handler;
	TriggerData *td = (TriggerData *) (fcinfo->context);
	TriggerEvent ev = td->tg_event;

	Assert(PyPgFunction_IsTrigger(fn_info->fi_func));
	Assert(PyPgTriggerData_Check(fn_info->fi_input));

	SXD("pulling trigger");

	/*
	 * Select the handler string: "after_insert", "before_insert",
	 * "after_delete_statement", etc.
	 */
	handler_str_ob = select_trigger_handler(ev);
	if (handler_str_ob == NULL)
		elog(ERROR, "unknown trigger event");

	/*
	 * Get the module object that will handle the specific event.
	 */
	handler = PyObject_GetAttr(fn_info->fi_module, handler_str_ob);
	if (handler == NULL)
	{
		PyErr_ThrowPostgresErrorWithCode(
			ERRCODE_TRIGGERED_ACTION_EXCEPTION,
			"trigger function does not support event");
	}

	/* borrow reference from module */
	Py_DECREF(handler);

	/*
	 * At this point, the code is no longer common between statement and row
	 * triggers, so choose the appropriate path.
	 */
	if (TRIGGER_FIRED_FOR_ROW(ev))
		rd = row_trigger(handler, fn_info->fi_input,
							td->tg_trigtuple, td->tg_newtuple);
	else if (TRIGGER_FIRED_FOR_STATEMENT(td->tg_event))
		rd = statement_trigger(handler, fn_info->fi_input);
	else
	{
		elog(ERROR, "unknown trigger event");
		/*
		 * Keep compiler quiet.
		 */
		Assert(false);
		return(0);
	}

	return(rd);
}

static Datum
create_result_datum(PyObj output, PyObj rob, bool *isnull)
{
	Datum rd;
	MemoryContext former = CurrentMemoryContext;
	bool dont_free = false;

	Assert(output != NULL);
	Assert(rob != NULL);
	Assert(isnull != NULL);

	SXD("creating result");

	Py_ACQUIRE(rob);

	if (output != (PyObj) Py_TYPE(rob))
	{
		/*
		 * elog()'s on failure; Also handles the Py_None case.
		 */
		PyPgType_DatumNew(output, rob, (int32) -1, &rd, isnull);
	}
	else
	{
		/*
		 * Exact type.
		 */
		rd = PyPgObject_GetDatum(rob);
		dont_free = true;
	}

	/*
	 * Not NULL and !typbyval, so copy the Datum out.
	 */
	if (!(*isnull) && !PyPgType_Get_typbyval(output))
	{
		Datum tmpd;
		/*
		 * Iff !typbyval, the datum is currently in the CurrentMemoryContext,
		 * so be sure to copy it to the handler context
		 */
		MemoryContextSwitchTo(pl_execution_context->return_memory_context);
		tmpd = datumCopy(rd, false, PyPgType_Get_typlen(output));
		MemoryContextSwitchTo(former);

		/* bein' kind */
		if (!dont_free)
			pfree(DatumGetPointer(rd));
		rd = tmpd;
	}

	return(rd);
}

/*
 * srf_materialize_cursor - materialize a Postgres.Cursor
 */
static Datum
srf_materialize_cursor(FunctionCallInfo fcinfo, PyObj cursor)
{
	struct pl_fn_info *fn_info = fcinfo->flinfo->fn_extra;
	ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext former = NULL;
	Tuplestorestate *tss;
	TupleDesc srcdesc;
	PyObj buf, row;
	Portal p;
	bool forward;
	int i;

	/*
	 * Fast path for PyPgCursor's
	 */
	Assert(PyPgCursor_Check(cursor));

	SXD("materializing cursor");

	srcdesc = PyPgType_GetTupleDesc(PyPgCursor_GetOutput(cursor));

	p = PyPgCursor_GetPortal(cursor);
	buf = PyPgCursor_GetBuffer(cursor);

	/*
	 * Note the direction. (Backward specifies a scrollable cursor)
	 */
	if (PyPgCursor_GetChunksize(cursor) == CUR_SCROLL_BACKWARD)
		forward = false;
	else
		forward = true;

	former = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);

	rsi->returnMode = SFRM_Materialize;
	rsi->isDone = ExprSingleResult;
	rsi->setDesc = CreateTupleDescCopy(srcdesc);

	rsi->setResult = tss = tuplestore_begin_heap(
		rsi->allowedModes, false, work_mem
	);

	/*
	 * If the cursor had a buffer(rows()), write those to the store
	 * first.
	 */
	if (buf != NULL)
	{
		Py_ACQUIRE_SPACE();
		{
			while ((row = PyIter_Next(buf)) != NULL)
			{
				HeapTupleData ht;

				Py_XREPLACE(row);

				ht.t_data = (HeapTupleHeader) DatumGetPointer(PyPgObject_GetDatum(row));
				ht.t_len = HeapTupleHeaderGetDatumLength(ht.t_data);
				ht.t_tableOid = PyPgType_GetTableOid(fn_info->fi_input);

				tuplestore_puttuple(tss, &ht);
			}
		}
		Py_RELEASE_SPACE();

		/*
		 * If failure was caused by a Postgres error, flow should never
		 * get here. Otherwise, watch for a Python exception.
		 */
		if (PyErr_Occurred())
		{
			PyErr_ThrowPostgresError(
				"could not materialize cursor buffer");
		}
	}

	/*
	 * Write the remaining tuples.
	 */
	do
	{
		/*
		 * We assume that the tuples are of reasonable size.
		 */
		MemoryContextSwitchTo(former);
		SPI_cursor_fetch(p, forward, 30);
		former = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);
		for (i = 0; i < SPI_processed; ++i)
			tuplestore_puttuple(tss, SPI_tuptable->vals[i]);
		SPI_freetuptable(SPI_tuptable);
	}
	while (SPI_processed == 30);

	MemoryContextSwitchTo(former);

	return(0);
}

/*
 * srf_materialize_iter - materialize an arbitrary iterator
 */
static Datum
srf_materialize_iter(FunctionCallInfo fcinfo, PyObj iter)
{
	struct pl_fn_info *fn_info = fcinfo->flinfo->fn_extra;
	ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext former = NULL;
	Tuplestorestate *tss;
	PyObj row, tdo;
	Datum *datums;
	bool *nulls;
	PyObj namemap, typs;
	int rnatts, *freemap;

	SXD("materializing");

	if (PyPgType_IsComposite(fn_info->fi_output))
		tdo = PyPgType_GetPyPgTupleDesc(fn_info->fi_output);
	else
	{
		tdo = PyPgTupleDesc_FromCopy(rsi->expectedDesc);
		Py_ACQUIRE(tdo); /* owned by the _handler call */
	}

	rnatts = PyPgTupleDesc_GetNatts(tdo);
	namemap = PyPgTupleDesc_GetNameMap(tdo);
	typs = PyPgTupleDesc_GetTypesTuple(tdo);
	freemap = PyPgTupleDesc_GetFreeMap(tdo);

	rsi->returnMode = SFRM_Materialize;
	rsi->isDone = ExprSingleResult;
	former = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);
	rsi->setDesc = CreateTupleDescCopy(PyPgTupleDesc_GetTupleDesc(tdo));
	rsi->setResult = tss = tuplestore_begin_heap(
		rsi->allowedModes & SFRM_Materialize_Random, false, work_mem
	);
	MemoryContextSwitchTo(former);

	/*
	 * Allocate memory for building tuples.
	 */
	datums = palloc(sizeof(Datum) * rsi->setDesc->natts);
	nulls = palloc(sizeof(bool) * rsi->setDesc->natts);

	Py_ACQUIRE_SPACE();
	{
		while ((row = PyIter_Next(iter)) != NULL)
		{
			HeapTuple ht;

			Py_XREPLACE(row); /** managed reference **/

			row = Py_NormalizeRow(rnatts, rsi->setDesc, namemap, row);
			if (row == NULL)
				break;
			Py_XREPLACE(row); /** replace managed reference **/

			Py_BuildDatumsAndNulls(rsi->setDesc, typs, row, datums, nulls);

			ht = heap_form_tuple(rsi->setDesc, datums, nulls);
			/*
			 * Any memory allocated for datums & nulls needs to be freed.
			 * Likely, it would be wise to run Py_BuildDatumsAndNulls in a memory
			 * context that gets reset every N iterations, but for now, explicitly
			 * pfree the memory.
			 */
			FreeReferences(freemap, datums, nulls);

			former = MemoryContextSwitchTo(rsi->econtext->ecxt_per_query_memory);
			tuplestore_puttuple(tss, ht);
			MemoryContextSwitchTo(former);

			heap_freetuple(ht);
		}
	}
	Py_RELEASE_SPACE();

	pfree(datums);
	datums = NULL;
	pfree(nulls);
	nulls = NULL;

	MemoryContextSwitchTo(former);

	if (PyErr_Occurred())
	{
		PyErr_ThrowPostgresError(
			"could not materialize result from returned iterable");
	}

	return(0);
}

/*
 * srf_materialize - call the function and materialize the result
 */
static Datum
srf_materialize(PG_FUNCTION_ARGS)
{
	struct pl_fn_info *fn_info = fcinfo->flinfo->fn_extra;
	PyObj args, returned, iter;

	Assert(PyPgFunction_CheckExact(fn_info->fi_func));
	Assert(!PyErr_Occurred());

	args = build_args(fn_info->fi_input,
		fcinfo->nargs, fcinfo->arg, fcinfo->argnull);

	returned = invoke_main(fn_info->fi_module, args); /* DECREFs args */

	SXD("preparing for materialization");

	/*
	 * Anything but column cursors can be materialized in the fast path.
	 */
	if (PyPgCursor_Materializable(returned) ||
		PyPgStatement_CheckExact(returned))
	{
		iter = PyObject_GetIter(returned);
		Py_DECREF(returned);
		if (iter == NULL)
		{
			/*
			 * This should *never* happen when returned is a cursor.
			 */
			PyErr_ThrowPostgresError(
				"could not to get cursor from statement");
		}
		/*
		 * Give the cursor reference to the handler.
		 * In or out of error, it will DECREF the cursor.
		 */
		Py_ACQUIRE(iter);
		return(srf_materialize_cursor(fcinfo, iter));
	}

	/*
	 * Not an SRF? Okay, cope by wrapping the 'returned' with a tuple.
	 * This will allow the code below to run unchanged.
	 */
	if (!PyPgFunction_IsSRF(fn_info->fi_func))
	{
		PyObj rtup;

		/*
		 * If it's stateful, grab the first result and dump
		 * the generator.
		 */
		if (PyPgFunction_IsStateful(fn_info->fi_func))
		{
			PyObj first;

			first = PyIter_Next(returned);
			Py_DECREF(returned);
			returned = first;
			if (returned == NULL)
				PyErr_ThrowPostgresError(
					"could not get first item from stateful function");
		}

		rtup = PyTuple_New(1);
		if (rtup == NULL)
		{
			Py_DECREF(returned);
			PyErr_ThrowPostgresError("could not create wrapper tuple");
		}
		PyTuple_SET_ITEM(rtup, 0, returned);

		returned = rtup;
	}

	/*
	 * Everything produced via materialization is treated as DatumTuple,
	 * so if the fi_output is not a composite, cope by creating a mapping with
	 * the PyReturnArgsTuple function object.
	 */
	if (!PyPgType_IsComposite(fn_info->fi_output))
	{
		PyObj mapping;
		mapping = Py_Call((PyObj) &PyMap_Type, Py_ReturnArgs, returned);
		Py_DECREF(returned);
		if (mapping == NULL)
		{
			PyErr_ThrowPostgresError("could not create tuple mapping");
		}
		returned = mapping;
	}

	iter = PyObject_GetIter(returned);
	Py_DECREF(returned);
	if (iter == NULL)
		PyErr_ThrowPostgresError("could not get iterator");

	/*
	 * Make sure a DB error wasn't caused by any of the above entries
	 * into the interpreter.
	 */
	if (DB_IS_NOT_READY())
	{
		Py_DECREF(iter);
		PyErr_ThrowPostgresErrorWithCode(
			ERRCODE_PYTHON_PROTOCOL_VIOLATION,
			"function failed to propagate database error"
		);
	}

	Py_ACQUIRE(iter);
	return(srf_materialize_iter(fcinfo, iter));
}

/*
 * Initialize a VPC-SRF's state.
 */
static void
srf_vpcinit(PG_FUNCTION_ARGS)
{
	struct pl_fn_info *fn_info = fcinfo->flinfo->fn_extra;
	unsigned long stored_ist_count;
	PyObj args, rob;
	ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;

	args = build_args(fn_info->fi_input,
		fcinfo->nargs, fcinfo->arg, fcinfo->argnull);

	rob = invoke_main(fn_info->fi_module, args); /* DECREFs args */

	/*
	 * It's arbitrary code, so validate the IST count.
	 * (The object returned by main could be anything)
	 */
	stored_ist_count = ist_count;
	fn_info->fi_internal_state = PyObject_GetIter(rob);
	Py_DECREF(rob);
	if (fn_info->fi_internal_state == NULL)
	{
		PyErr_ThrowPostgresError("could not get iterator for set-returning function");
	}
	else
	{
		int r;

		r = Py_XACTREF(fn_info->fi_internal_state);
		Py_DECREF(fn_info->fi_internal_state);
		if (r == -1)
		{
			PyErr_ThrowPostgresError(
				"could not add iterator to the transaction scope");
		}
	}

	ext_check_state(ERROR, stored_ist_count);

	/*
	 * Remove the iterator from the TransactionScope when the
	 * caller is done with the expression context.
	 */
	RegisterExprContextCallback(
		rsi->econtext, srf_eccb, PointerGetDatum(fn_info)
	);
	rsi->returnMode = SFRM_ValuePerCall;
	rsi->isDone = ExprMultipleResult;
}

/*
 * Get the next value for VPC-SRFs.
 */
static Datum
srf_vpcnext(PG_FUNCTION_ARGS)
{
	struct pl_fn_info *fn_info = fcinfo->flinfo->fn_extra;
	ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;
	PyObj rob;

	Assert(PyPgFunction_Check(fn_info->fi_func));
	Assert(fn_info->fi_internal_state != NULL);

	SXD("getting the next value");

	rob = PyIter_Next(fn_info->fi_internal_state);

	if (rob == NULL)
	{
		rsi->isDone = ExprEndResult;
		UnregisterExprContextCallback(
			rsi->econtext, srf_eccb, PointerGetDatum(fn_info)
		);

		/*
		 * This can cause a success to fail.
		 * If it fails on top of an existing exception, fine.
		 */
		Py_DEXTREF(fn_info->fi_internal_state);
		fn_info->fi_internal_state = NULL;
		fcinfo->isnull = true;

		if (PyErr_Occurred())
		{
			PyErr_ThrowPostgresErrorWithCode(
				ERRCODE_PYTHON_EXCEPTION,
				"iteration for VPC-SRF raised a Python exception");
		}
		else
		{
			/*
			 * It's really done.
			 */
			return(0);
		}
	}
	else
	{
		/*
		 * It continues. yay.
		 */
		rsi->isDone = ExprMultipleResult;
	}

	return(create_result_datum(fn_info->fi_output, rob, &(fcinfo->isnull)));
}

/*
 * Execute a regular function.
 */
static Datum
call_function(PG_FUNCTION_ARGS)
{
	struct pl_fn_info *fn_info = fcinfo->flinfo->fn_extra;
	PyObj args, rob = NULL;

	args = build_args(fn_info->fi_input,
		fcinfo->nargs, fcinfo->arg, fcinfo->argnull);

	rob = invoke_main(fn_info->fi_module, args); /* DECREFs args */

	return(create_result_datum(fn_info->fi_output, rob, &(fcinfo->isnull)));
}

/*
 * initialize - Initialize fn_extra with the pl_fn_info structure and return the
 * PyPgFunction object.
 *
 * If fn_extra is NULL or out-of-date, load a new Postgres.Function.
 *
 * In the worst case scenario, pl_handler will acquire two [python]
 * references to the same function.
 */
static PyObj
initialize(PG_FUNCTION_ARGS)
{
	Oid fn_oid;
	struct pl_fn_info *fn_info;
	PyObj module = NULL, func = NULL;
	int r;

	SXD("loading Python function");
	fn_oid = fcinfo->flinfo->fn_oid;
	fn_info = (struct pl_fn_info *) fcinfo->flinfo->fn_extra;

	/*
	 * If it's NULL, fn_extra has not been initialized.
	 *
	 * If the xid is not the current xid, then chances are the data in
	 * fn_info is obsolete[ as TransactionScope has dumped its references].
	 */
	if (FN_INFO_NEEDS_REFRESH(fn_info))
	{
		if (ext_state == init_pending)
			ext_entry();

		if (fn_info == NULL)
		{
			/*
			 * If it's NULL, allocate space for it.
			 * However, if it's not NULL, assume that fn_extra has
			 * been preserved--we'll re-initialize it.
			 */
			fn_info = MemoryContextAlloc(
				fcinfo->flinfo->fn_mcxt, sizeof(struct pl_fn_info));
			fcinfo->flinfo->fn_extra = fn_info;
		}

		/*
		 * Reset a few fields. These will or may be initialized later.
		 */
		fn_info->fi_xid = 0;
		fn_info->fi_func = NULL;
		fn_info->fi_state = NULL;
		fn_info->fi_state_owner = NULL;
		fn_info->fi_internal_state = NULL;

		/*
		 * Point the execution context at the function info.
		 */
		pl_execution_context->fn_info = fn_info;

		func = get_PyPgFunction_from_oid(fn_oid, &module);
		if (func == NULL)
		{
			if (module != NULL)
			{
				/* module reference is borrowed when func == NULL */
				PyErr_ThrowPostgresError("could not find Python function");
			}
			else
				PyErr_ThrowPostgresErrorWithCode(
					ERRCODE_PYTHON_PROTOCOL_VIOLATION,
					"could not get the \"__func__\" attribute from the function module");
		}

		/*
		 * Give the reference to the handler; if a failure occurs during the
		 * remaining initialization, it'll be appropriately short lived.
		 */
		Py_ACQUIRE(func);

		/*
		 * Initialize the fi_func ASAP, this gives CONTEXT if it errors out.
		 * Even if the function is not current, the regprocedure representation
		 * should be accurate.
		 */
		fn_info->fi_func = func;

		/*
		 * If it needs to be updated, reload it.
		 * When module is NULL, the function was just loaded, so
		 * only bother with the current check if module is not NULL.
		 */
		if (module != NULL && !PyPgFunction_IsCurrent(func))
		{
			/*
			 * The function is not current and needs to be updated.
			 * Not current meaning that CREATE OR REPLACE has been ran
			 * since it was last invoked.
			 */
			r = PyPgFunction_RemoveModule(func);
			func = NULL; /* let the handler take care of the reference */
			Py_DECREF(module); /* it's being reloaded, dump the reference */
			module = NULL;

			if (r)
				PyErr_ThrowPostgresError("could not remove function module from sys.modules");

			/*
			 * The function object should *not* be removed from the
			 * TransactionScope set as other fn_extra's may still be using it.
			 */

			/*
			 * Function module no longer exists in sys.modules,
			 * module should be NULL this time around.
			 */
			func = get_PyPgFunction_from_oid(fn_oid, &module);
			if (func == NULL)
				PyErr_ThrowPostgresError("could not find Python function");

			/*
			 * pl_handler now owns the reference.
			 */
			Py_ACQUIRE(func);
			fn_info->fi_func = func;

			if (module != NULL)
			{
				/*
				 * "Can't happen" condition.
				 *
				 * The module was *just* removed form sys.modules without an
				 * error being noted.
				 */
				Py_DECREF(module);
				elog(ERROR,
					"unexpected Python function resolution using sys.modules");
			}
		}

		/*
		 * If the module exists in sys.modules, it will return the existing
		 * module object. Otherwise, it will run the module code and
		 * install the new object into sys.modules.
		 *
		 * In this context, it should always run the module code.
		 */
		if (module == NULL)
			module = run_PyPgFunction_module(func); /* ereport's on failure */
		else
		{
			r = Py_XACTREF(module);
			Py_DECREF(module);
			if (r == -1)
			{
				PyErr_ThrowPostgresError(
					"could not add module to transaction scope");
			}
		}

		/*
		 * Add func to the transaction scope now.
		 * func has been ACQUIRE'd by the handler, so don't DECREF.
		 */
		r = Py_XACTREF(func);
		if (r == -1)
		{
			PyErr_ThrowPostgresError(
				"could not add function to transaction scope");
		}

		SXD("initializing Python function");

		fn_info->fi_module = module;
		fn_info->fi_output = PyPgFunction_GetOutput(func);
		fn_info->fi_input = PyPgFunction_GetInput(func);

		/*
		 * function module loader protocol, module is loaded, so return it.
		 */
		if (fcinfo->nargs == -1)
		{
			return(func);
		}

		if (!PyPgFunction_IsPolymorphic(func))
		{
			if (CALLED_AS_TRIGGER(fcinfo))
			{
				TriggerData *tdsrc = (TriggerData *) (fcinfo->context);
				PyObj td;

				td = PyPgTriggerData_New(tdsrc);
				if (td == NULL)
					PyErr_ThrowPostgresError("could not create Postgres.TriggerData object");

				r = Py_XACTREF(td);
				Py_DECREF(td);
				if (r == -1)
					PyErr_ThrowPostgresError("could not add trigger data to transaction scope");

				fn_info->fi_input = td;
			}

			/*
			 * Nothing else to do for a non-TRIGGER, non-polymorphic function.
			 */
		}
		else
		{
			/*
			 * It's polymorphic. Extract the types from fn_expr.
			 */
			int polytype = PyPgTupleDesc_GetPolymorphic(fn_info->fi_input);
			Oid target_oid;
			PyObj pinput, poutput, target;

			Assert(!CALLED_AS_TRIGGER(fcinfo));

			/*
			 * It's polymorphic, so get a reference target type.
			 */
			if (polytype == -1)
			{
				/*
				 * Probably dead code, polymorphic functions normally
				 * have polymorphic arguments.
				 */
				target_oid = get_fn_expr_rettype(fcinfo->flinfo);
			}
			else
				target_oid = get_fn_expr_argtype(fcinfo->flinfo, polytype);

			/*
			 * Get the actual PyPgType and resolve it to the basetype.
			 */
			target = PyPgType_FromOid(target_oid);
			if (target == NULL)
			{
				PyErr_ThrowPostgresError("could not load polymorphic target");
			}
			Py_DECREF(target); /* borrow reference from the type cache */

			/*
			 * The element type is used.
			 */
			if (PyPgType_IsArray(target))
			{
				PyObj t = PyPgType_GetElementType(target);
				Assert(t != NULL);
				target = t;
			}

			/*
			 * Polymorphin' time!
			 */
			pinput = PyPgTupleDesc_Polymorph(fn_info->fi_input, target);
			if (pinput == NULL)
				PyErr_ThrowPostgresError("could not polymorph function input");
			/*
			 * New input? Add it to the transaction scope.
			 */
			if (pinput != fn_info->fi_input)
			{
				r = Py_XACTREF(pinput);
				Py_DECREF(pinput);

				if (r == -1)
				{
					PyErr_ThrowPostgresError(
						"could not add polymorphed input to transaction scope");
				}
			}

			poutput = PyPgType_Polymorph(fn_info->fi_output, target);
			if (poutput == NULL)
				PyErr_ThrowPostgresError("could not polymorph function output");
			/*
			 * New output? Add it to the transaction scope.
			 */
			if (poutput != fn_info->fi_output)
			{
				r = Py_XACTREF(poutput);
				Py_DECREF(poutput);

				if (r == -1)
				{
					PyErr_ThrowPostgresError(
						"could not add polymorphed output to transaction scope");
				}
				/*
				 * So it's a polymorphic, record returning function?
				 * Then be sure to bless the output for VPC-SRFs.
				 */
				if (PyPgType_IsComposite(poutput))
					BlessTupleDesc(PyPgType_GetTupleDesc(poutput));
			}

			fn_info->fi_input = pinput;
			fn_info->fi_output = poutput;
		}

		/*
		 * If it's an SRF *still* returning a RECORD, override the fi_output
		 * with the expectedDesc.
		 */
		if (PyPg_record_CheckExact(fn_info->fi_output))
		{
			if (CALLED_AS_SRF(fcinfo))
			{
				ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;
				PyObj output;

				/*
				 * Caller screwed up.
				 */
				if (rsi == NULL || rsi->expectedDesc == NULL)
					elog(ERROR, "cannot execute ambiguous set returning functions");

				output = PyPgType_FromTupleDesc(rsi->expectedDesc);
				if (output == NULL)
					PyErr_ThrowPostgresError(
						"could not construct expected result type");

				r = Py_XACTREF(output);
				Py_DECREF(output);
				if (r == -1)
				{
					PyErr_ThrowPostgresError(
						"could not add expected result type to transaction scope");
				}
				fn_info->fi_output = output;
			}
		}

		/*
		 * Used to identify that the fn_info needs to be refreshed.
		 * (References in fn_extra are held by TransactionScope)
		 */
		fn_info->fi_xid = ext_xact_count;
	}
	else
	{
		/*
		 * fn_extra is holding the fn_info;
		 *
		 * TransactionScope is already the holding fi_func reference.
		 */
		pl_execution_context->fn_info = fn_info;
	}

	return(fn_info->fi_func);
}

/*
 * pl_handler - execute a Python procedure with given arguments.
 */
PG_FUNCTION_INFO_V1(pl_handler);
Datum
pl_handler(PG_FUNCTION_ARGS)
{
	Datum rd = 0;
	PyObj func = NULL;
	unsigned long stored_ist_count;
	volatile bool connected = false;
	struct pl_exec_state *previous = pl_execution_context;
	struct pl_exec_state current_exec_state = {
		NULL, CurrentMemoryContext, NULL,
	};

	PyGILState_STATE gs = NULL;

	/*
	 * This stored count is used to identify that all
	 * opened subtransactions have been closed on handler exit.
	 */
	stored_ist_count = ist_count;
	pl_execution_context = &current_exec_state;
	SXD("entering Python handler");

	gs = PyGILState_Ensure();

	Py_ALLOCATE_OWNER();
	{
		/*
		 * Determine entry point for execution and, well, enter.
		 */
		PG_TRY();
		{
			SPI_connect();
			connected = true;

			/*
			 * setup fn_extra and get the PyPgFunction object for the call
			 */
			func = initialize(fcinfo);
			/*
			 * TODO: Cache the sub-handler in fn_info.
			 */
			if (fcinfo->nargs == -1)
			{
				/*
				 * function preload protocol:
				 *  Result is NULL and a pointer to the module.
				 */
				rd = PointerGetDatum(current_exec_state.fn_info->fi_module);
				fcinfo->isnull = true;
			}
			else if (CALLED_AS_TRIGGER(fcinfo))
				rd = pull_trigger(fcinfo);
			else
			{
				if (!CALLED_AS_SRF(fcinfo))
					rd = call_function(fcinfo);
				else
				{
					if (SRF_SHOULD_MATERIALIZE(fcinfo))
						rd = srf_materialize(fcinfo);
					else if (SRF_VPC_REQUEST(fcinfo))
					{
						if (FN_INFO_HAS_STATE(fcinfo))
						{
							/*
							 * Grab the next value from the iterator stored in
							 * fi_internal_state, if it's initialized.
							 */
							rd = srf_vpcnext(fcinfo);
						}
						else
						{
							if (PyPgFunction_IsSRF(func))
							{
								/*
								 * It's an SRF, build init fi_internal_state and grab the first
								 * value.
								 */
								srf_vpcinit(fcinfo);
								rd = srf_vpcnext(fcinfo);
							}
							else
							{
								/*
								 * It's not an SRF, but it was invoked using the VPC
								 * protocol, so normal invocation is acceptable..
								 */
								rd = call_function(fcinfo);
							}
						}
					}
					else
					{
						elog(ERROR, "unsupported SRF mode requested");
					}
				}
			}

			connected = false;
			SPI_finish();
		}
		PG_CATCH();
		{
			/*
			 * It's already failing, so use ext_check_state(WARNING, ...)
			 *
			 * This will keep ext_check_state() from ever raising an error here.
			 */
			if (ext_state > 0)
				ext_state = ext_ready;

			/*
			 * Be sure to cleanup.
			 */
			if (connected)
				SPI_finish();

			/*
			 * It's already failing out, so only warn the user if the IST
			 * count is off. Either the user did something wrong, or
			 * the interpreter exited via longjmp(PL programming error).
			 */
			ext_check_state(WARNING, stored_ist_count);

			pl_execution_context = previous;

			/*
			 * Restore the caller's memory context.
			 */
			MemoryContextSwitchTo(current_exec_state.return_memory_context);

			/*
			 * Special case relays as there is a Python exception available
			 * for us to use. This failure case happens when DatumNew causes
			 * a *Python* exception.
			 */
			if (_PG_ERROR_IS_RELAY())
			{
				_PYRO_DEALLOCATE(); /* release references held by owner and pop */

				/*
				 * Don't need the error state, all the information is in the
				 * set Python error.
				 */
				FlushErrorState();

				PyGILState_Release(gs);

				/* Restore the caller's memory context. */
				PyErr_ThrowPostgresErrorWithContext(
					ERRCODE_PYTHON_EXCEPTION,
					"function raised a Python exception",
					&current_exec_state);

				/*
				 * The above throw call should have exited.
				 */
				Assert(false);
			}

			_PYRO_DEALLOCATE(); /* release references held by owner and pop */

			/*
			 * Should have been converted by now.
			 */
			Assert(!PyErr_Occurred());

			PyGILState_Release(gs);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	Py_DEALLOCATE_OWNER();

	pl_execution_context = previous;

	/*
	 * Restore the caller's memory context.
	 */
	MemoryContextSwitchTo(current_exec_state.return_memory_context);

	Assert(!PyErr_Occurred());

	/*
	 * If there are any open ISTs or ext_state != ext_ready,
	 * raise an exception.
	 */
	PyGILState_Release(gs);
	ext_check_state(ERROR, stored_ist_count);

	return(rd);
}
