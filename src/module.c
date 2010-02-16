/*
 * The Postgres module. (import Postgres)
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/hio.h"
#if (PG_VERSION_NUM >= 80400)
#include "access/sysattr.h"
#endif
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/catversion.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/async.h"
#include "commands/trigger.h"
#include "libpq/libpq.h"
#include "libpq/libpq-fs.h"
#include "libpq/pqformat.h"
#include "libpq/be-fsstubs.h"
#include "mb/pg_wchar.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/syscache.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/typcache.h"
#include "utils/plancache.h"
#include "storage/backendid.h"
#include "storage/large_object.h"
#include "executor/spi.h"

/*
 * Just about everything gets included here;
 * this is the module initialization file.
 */
#include "pypg/module.h"
#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/pl.h"
#include "pypg/error.h"
#include "pypg/errordata.h"
#include "pypg/triggerdata.h"
#include "pypg/strings.h"
#include "pypg/externs.h"

#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/array.h"
#include "pypg/type/record.h"
#include "pypg/type/bitwise.h"
#include "pypg/type/string.h"
#include "pypg/type/numeric.h"
#include "pypg/type/timewise.h"
#include "pypg/type/system.h"

#include "pypg/tupledesc.h"
#include "pypg/function.h"
#include "pypg/xact.h"
#include "pypg/statement.h"
#include "pypg/cursor.h"
#include "pypg/stateful.h"

/*
 * The source to the pure-Python portion of the 'Postgres' module.
 * See 'src/module.py' for the human readable version.
 */
static const char module_python[] = {
#include "module.py.cfrag"
};

static const char project_module_python[] = {
#include "project.py.cfrag"
};

static PyObj
py_get_Postgres_source(PyObj self)
{
	PyObj rob;
	rob = PyUnicode_Decode(module_python, strlen(module_python), "ascii", "");
	return(rob);
}

static PyObj
py_get_Postgres_project_source(PyObj self)
{
	PyObj rob;
	rob = PyUnicode_Decode(project_module_python, strlen(project_module_python), "ascii", "");
	return(rob);
}

/*
 * py_ereport - ereport() interface
 *
 * NOTE: This is usable in any PL state.
 */
static PyObj
py_ereport(PyObj self, PyObj args, PyObj kw)
{
	static char *words[] = {
		"severity", "message",
		"detail", "hint", "context", "sqlerrcode",
		"inhibit_pl_context", NULL
	};

	int elevel, sqlerrcode = 0;
	bool inhibit_pl_context;
	PyObj inhibit_pl_context_ob = NULL;
	PyObj message = NULL, detail = NULL, hint = NULL, context = NULL;
	volatile PyObj rob;

	if (!PyArg_ParseTupleAndKeywords(args, kw, "iO|OOOiO:ereport", words,
			&elevel, &message, &detail, &hint, &context, &sqlerrcode,
			&inhibit_pl_context_ob))
		return(NULL);

	/*
	 * Validate elevel
	 */
	switch (elevel)
	{
		case DEBUG5:
		case DEBUG4:
		case DEBUG3:
		case DEBUG2:
		case DEBUG1:
		case LOG:
		case COMMERROR:
		case INFO:
		case NOTICE:
		case WARNING:
		case ERROR:
		case FATAL:
		case PANIC:
			;
		break;

		default:
			PyErr_Format(PyExc_ValueError, "unknown reporting level '%l'", elevel);
			return(NULL);
		break;
	}

	/*
	 * Controls the ECC set by the handler that ultimately invoked this ereport().
	 */
	if (inhibit_pl_context_ob != NULL && inhibit_pl_context_ob != Py_None)
	{
		if (inhibit_pl_context_ob == Py_True)
			inhibit_pl_context = true;
		else if (inhibit_pl_context_ob == Py_False)
			inhibit_pl_context = false;
		else
		{
			PyErr_Format(PyExc_TypeError,
							"inhibit_pl_context keyword requires a bool, given '%s'",
							Py_TYPE(inhibit_pl_context_ob)->tp_name);
			return(NULL);
		}
	}
	else
	{
		/*
		 * Otherwise, don't disable the context.
		 */
		inhibit_pl_context = false;
	}

	Py_ALLOCATE_OWNER();
	{
		Py_ACQUIRE_SPACE();
		{
			PG_TRY();
			{
/*
 * Repetitious enough to annoy me into doing this. -jwp
 *
 * Coerce object into a string; if it fails, relay.
 */
#define ATTACH(CB, OBJ) do { \
	Py_INCREF(OBJ); \
	PyObject_StrBytes(&OBJ); \
	if (OBJ == NULL) \
		PyErr_RelayException(); \
	else \
	{ \
		Py_XREPLACE(OBJ); \
		CB("%s", PyBytes_AS_STRING(OBJ)); \
	} \
} while(0)

				if (errstart(elevel, "pg-python/src/module.c", 1, "<Postgres.ereport>", "python"))
				{
					if (sqlerrcode)
						errcode(sqlerrcode);

					ATTACH(errmsg, message);
					if (hint)
						ATTACH(errhint, hint);
					if (detail)
						ATTACH(errdetail, detail);
					if (context)
						ATTACH(errcontext, context);
					errfinish(0);
				}
#undef ATTACH
				rob = Py_None;
				Py_INCREF(rob);
			}
			PG_CATCH();
			{
				PyErr_SetPgError(false);

				/*
				 * Disable the inclusion of the traceback in the context?
				 *
				 * fetch and normalize, then
				 * set .inhibit_pl_context = True or False
				 */
				if (inhibit_pl_context)
				{
					PyObj exc, val, tb;
					PyErr_Fetch(&exc, &val, &tb);
					PyErr_NormalizeException(&exc, &val, &tb);
					PyObject_SetAttr(val, pg_inhibit_pl_context_str_ob, Py_True);
					PyErr_Restore(exc, val, tb);
				}

				rob = NULL;
			}
			PG_END_TRY();
		}
		Py_RELEASE_SPACE();
	}
	Py_DEALLOCATE_OWNER();

	return(rob);
}

/*
 * Get the execution context's invoking function. None if none.
 */
static PyObj
py_get_func(PyObj self)
{
	PyObj rob;

	if (PL_CONTEXT() && PL_FN_INFO())
	{
		rob = PL_FN_INFO()->fi_func;
		if (rob == NULL)
			rob = Py_None;
	}
	else
		rob = Py_None;

	Py_INCREF(rob);
	return(rob);
}

/*
 * Get the current list of search paths as Oids
 */
static PyObj
py_current_schemas_oid(PyObj self, PyObj args)
{
	volatile PyObj rob = NULL;
	PyObj include_temps_ob = NULL;
	bool include_temps;

	if (!PyArg_ParseTuple(args, "|O:current_schemas_oid", &include_temps_ob))
		return(NULL);

	if (include_temps_ob == Py_True)
		include_temps = true;
	else
		include_temps = false;

	PG_TRY();
	{
		List *l;
		int i = 0, len;

		l = fetch_search_path(include_temps);

		len = list_length(l);
		rob = PyTuple_New(len);

		if (rob != NULL)
		{
			ListCell *lc;

			foreach(lc, l)
			{
				if (i < 0)
				{
					list_free(l);
					ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("too many active namespaces in path")
					));
				}

				PyTuple_SET_ITEM(rob, i, PyLong_FromOid(lfirst_oid(lc)));
				if (PyTuple_GET_ITEM(rob, i) == NULL)
				{
					Py_DECREF(rob);
					rob = NULL;
					break;
				}
				++i;
			}
		}

		list_free(l);
	}
	PG_CATCH();
	{
		Py_XDECREF(rob);
		rob = NULL;
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return(rob);
}

/*
 * Get the current list of search path names as strings
 */
static PyObj
py_current_schemas(PyObj self, PyObj args)
{
	volatile PyObj rob = NULL;
	PyObj include_temps_ob = NULL;
	bool include_temps;

	if (!PyArg_ParseTuple(args, "|O:current_schemas", &include_temps_ob))
		return(NULL);

	if (include_temps_ob == Py_True)
		include_temps = true;
	else
		include_temps = false;

	PG_TRY();
	{
		List *l;
		int i = 0, len;

		l = fetch_search_path(true);
		len = list_length(l);

		rob = PyTuple_New(len);

		if (rob != NULL)
		{
			ListCell *lc;

			if (PyTuple_GET_SIZE(rob) != len)
			{
				list_free(l);
				ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					errmsg("too many active namespaces in path")
				));
			}

			foreach(lc, l)
			{
				HeapTuple ht;
				PyObj name;

				ht = SearchSysCache(NAMESPACEOID,
									ObjectIdGetDatum(lfirst_oid(lc)),
									0, 0, 0);

				if (HeapTupleIsValid(ht))
				{
					Form_pg_namespace ns;
					ns = (Form_pg_namespace) GETSTRUCT(ht);

					name = PyUnicode_FromCString(NameStr(ns->nspname));
					PyTuple_SET_ITEM(rob, i, name);
					ReleaseSysCache(ht);
					if (name == NULL)
					{
						Py_DECREF(rob);
						rob = NULL;
						break;
					}
				}
				else
				{
					/*
					 * Schema doesn't exist anymore? Just fill it with a None.
					 */
					PyTuple_SET_ITEM(rob, i, Py_None);
					Py_INCREF(Py_None);
				}

				++i;
			}
		}

		list_free(l);
	}
	PG_CATCH();
	{
		Py_XDECREF(rob);
		rob = NULL;
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return(rob);
}

/*
 * Utility function to be used with map() in order to wrap an object in a tuple.
 */
static PyObj
_return_args(PyObj self, PyObj args)
{
	Py_INCREF(args);
	return(args);
}

static PyObj
py_make_sqlstate(PyObj self, PyObj code)
{
	int r;

	if (!sqlerrcode_from_PyObject(code, &r))
		return(NULL);

	return(PyLong_FromLong((long) r));
}

/*
 * Convert to Python string, and emit notify.
 */
static PyObj
py_notify(PyObj self, PyObj args)
{
	PyObj strob;
	char *payload;
	Py_ssize_t payload_size;
	PyObj rob = NULL;

	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyArg_ParseTuple(args, "O|y#:notify", &strob, &payload, &payload_size))
		return(NULL);

	Py_INCREF(strob);
	PyObject_StrBytes(&strob);
	if (strob == NULL)
		return(NULL);

	PG_TRY();
	{
		Async_Notify(PyBytes_AS_STRING(strob));
		rob = Py_None;
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	Py_DECREF(strob);
	Py_XINCREF(rob);

	return(rob);
}

static PyObj
pypg_uid(void)
{
	return(PyPg_oid_FromObjectId(GetUserId()));
}

static PyObj
pypg_session_uid(void)
{
	return(PyPg_oid_FromObjectId(GetSessionUserId()));
}

static PyObj
py_transaction_timestamp(PyObj self)
{
	PyObj rob = NULL;
	TimestampTz ts;

	ts = GetCurrentTransactionStartTimestamp();
	rob = PyPgObject_New((PyObj) &PyPg_timestamptz_Type,
		TimestampTzGetDatum(ts));

	return(rob);
}

static PyObj
py_statement_timestamp(PyObj self)
{
	PyObj rob = NULL;
	TimestampTz ts;

	ts = GetCurrentStatementStartTimestamp();
	rob = PyPgObject_New((PyObj) &PyPg_timestamptz_Type,
		TimestampTzGetDatum(ts));

	return(rob);
}

static PyObj
py_clock_timestamp(PyObj self)
{
	PyObj rob = NULL;
	TimestampTz ts;

	ts = GetCurrentTimestamp();
	rob = PyPgObject_New((PyObj) &PyPg_timestamptz_Type,
		TimestampTzGetDatum(ts));

	return(rob);
}

static PyObj
py_quote_ident(PyObj self, PyObj ob)
{
	volatile PyObj rob = NULL;
	const char *instr, *outstr;

	Py_INCREF(ob);
	PyObject_StrBytes(&ob);
	if (ob == NULL)
		return(NULL);
	instr = PyBytes_AS_STRING(ob);

	PG_TRY();
	{
		outstr = quote_identifier(instr);
		rob = PyUnicode_FromCString(outstr);
		if (outstr != instr)
			pfree((void *) outstr);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		Py_XDECREF(rob);
		rob = NULL;
	}
	PG_END_TRY();

	Py_DECREF(ob);
	return(rob);
}

static PyObj
py_quote_literal(PyObj self, PyObj ob)
{
	volatile PyObj rob = NULL;
	text *s = NULL, *txt = NULL;

	Py_INCREF(ob);
	PyObject_StrBytes(&ob);
	if (ob == NULL)
		return(NULL);

	PG_TRY();
	{
		Py_ssize_t size = PyBytes_GET_SIZE(ob);
		/* s = cstring_to_text_with_len(PyBytes_AS_STRING(ob), PyBytes_GET_SIZE(ob)); */
		s = palloc((int) size + VARHDRSZ);
		SET_VARSIZE(s, (int) size + VARHDRSZ);
		Py_MEMCPY(VARDATA(s), PyBytes_AS_STRING(ob), size);
		txt = DatumGetTextP(DirectFunctionCall1(quote_literal, PointerGetDatum(s)));
		pfree(s);
		rob = PyUnicode_FromTEXT(txt);
		pfree(txt);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		Py_XDECREF(rob);
		rob = NULL;
	}
	PG_END_TRY();

	Py_DECREF(ob);
	return(rob);
}

static PyObj
py_quote_nullable(PyObj self, PyObj ob)
{
	if (ob == Py_None)
		return(PyUnicode_FromCString("NULL"));

	return(py_quote_literal(self, ob));
}

static PyObj
py_lo_create(PyObj self, PyObj args)
{
	Oid lo_oid = InvalidOid;
	PyObj oid_ob = NULL;

	if (!PyArg_ParseTuple(args, "|O:_lo_create", &oid_ob))
		return(NULL);

	if (oid_ob != NULL)
	{
		if (Oid_FromPyObject(oid_ob, &lo_oid))
			return(NULL);
	}

	if (DB_IS_NOT_READY())
		return(NULL);

	PG_TRY();
	{
		lo_oid = DirectFunctionCall1(lo_create, ObjectIdGetDatum(lo_oid));
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();

	return(PyLong_FromOid(lo_oid));
}

static PyObj
py_execute(PyObj self, PyObj sql_str)
{
	if (DB_IS_NOT_READY())
		return(NULL);

	if (PL_FN_READONLY())
	{
		PyErr_SetString(PyExc_RuntimeError, "cannot execute from a non-volatile function");
		return(NULL);
	}

	Py_INCREF(sql_str);
	PyObject_StrBytes(&sql_str);
	if (sql_str == NULL)
		return(NULL);

	SPI_push();
	PG_TRY();
	{
		execute_statements(PyBytes_AS_STRING(sql_str));
	}
	PG_CATCH();
	{
		Py_DECREF(sql_str);
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();
	SPI_pop();

	Py_DECREF(sql_str);
	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObj
py_lo_unlink(PyObj self, PyObj args)
{
	Oid lo_id;
	int32 status;
	PyObj oid_ob = NULL;

	if (!PyArg_ParseTuple(args, "O:_lo_unlink", &oid_ob))
		return(NULL);

	if (Oid_FromPyObject(oid_ob, &lo_id))
		return(NULL);

	if (DB_IS_NOT_READY())
		return(NULL);

	PG_TRY();
	{
		status = DatumGetInt32(DirectFunctionCall1(
								lo_unlink, ObjectIdGetDatum(lo_id)));
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();

	return(PyLong_FromLong((long) status));
}

static PyObj
py_lo_open(PyObj self, PyObj args)
{
	Oid lo_id;
	int32 fd, mode;
	PyObj oid_ob = NULL;

	if (!PyArg_ParseTuple(args, "Oi:_lo_open", &oid_ob, &mode))
		return(NULL);

	if (Oid_FromPyObject(oid_ob, &lo_id))
		return(NULL);

	if (DB_IS_NOT_READY())
		return(NULL);

	PG_TRY();
	{
		fd = DatumGetInt32(DirectFunctionCall2(
								lo_open, ObjectIdGetDatum(lo_id), mode));
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();

	return(PyLong_FromLong((long) fd));
}

static PyObj
py_lo_write(PyObj self, PyObj args)
{
	int fd, len, status;
	char *data;

	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyArg_ParseTuple(args, "iy#:_lo_write", &fd, &data, &len))
		return(NULL);

	PG_TRY();
	{
		status = lo_write(fd, data, (int) len);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();

	return(PyLong_FromLong((long) status));
}

static PyObj
py_lo_read(PyObj self, PyObj args)
{
	int fd, len;
	PyObj rob = NULL;

	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyArg_ParseTuple(args, "ii:_lo_read", &fd, &len))
		return(NULL);

	PG_TRY();
	{
		char *buf;
		int readbytes;

		buf = palloc(len);
		readbytes = lo_read(fd, buf, len);
		rob = PyBytes_FromStringAndSize(buf, readbytes);
		pfree(buf);
	}
	PG_CATCH();
	{
		Py_XDECREF(rob);
		rob = NULL;
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return(rob);
}

static PyObj
py_lo_close(PyObj self, PyObj args)
{
	int fd;

	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyArg_ParseTuple(args, "i:_lo_close", &fd))
		return(NULL);

	PG_TRY();
	{
		DirectFunctionCall1(lo_close, Int32GetDatum(fd));
		/*
		 * No status..
		 */
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();

	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObj
py_lo_tell(PyObj self, PyObj args)
{
	int32 fd, position;

	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyArg_ParseTuple(args, "i:_lo_tell", &fd))
		return(NULL);

	PG_TRY();
	{
		position = DirectFunctionCall1(lo_tell,
			Int32GetDatum(fd));
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();

	return(PyLong_FromLong((long) position));
}

static PyObj
py_lo_seek(PyObj self, PyObj args)
{
	int32 fd, offset, whence = 0, status = 0;

	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyArg_ParseTuple(args, "ii|i:_lo_seek", &fd, &offset, &whence))
		return(NULL);

	PG_TRY();
	{
		status = DirectFunctionCall3(lo_lseek,
			Int32GetDatum(fd),
			Int32GetDatum(offset),
			Int32GetDatum(whence));
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();

	return(PyLong_FromLong((long) status));
}

static PyObj
py_memstats(PyObj self)
{
	MemoryContextStats(PythonMemoryContext);

	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObj
py_cleartypecache(PyObj self)
{
	PyPgClearTypeCache();
	Py_INCREF(Py_None);
	return(Py_None);
}

static PyMethodDef PyPgModule_Methods[] = {
	{"_memstats", (PyCFunction) py_memstats, METH_NOARGS,
		PyDoc_STR("print PythonMemoryContext stats to stderr")},
	{"_cleartypecache", (PyCFunction) py_cleartypecache, METH_NOARGS,
		PyDoc_STR("clear the type cache dictionary")},

	{"__get_Postgres_source__", (PyCFunction) py_get_Postgres_source, METH_NOARGS,
		PyDoc_STR("get the Python source to the Postgres module")},
	{"__get_Postgres_project_source__", (PyCFunction) py_get_Postgres_project_source, METH_NOARGS,
		PyDoc_STR("get the Python source to the Postgres.project module")},

	{"__get_func__", (PyCFunction) py_get_func, METH_NOARGS,
		PyDoc_STR("get the function that executed the Python code")},

	{"current_schemas_oid", (PyCFunction) py_current_schemas_oid, METH_VARARGS,
		PyDoc_STR("get a tuple of Oids representing the current search_path")},
	{"current_schemas", (PyCFunction) py_current_schemas, METH_VARARGS,
		PyDoc_STR("get a tuple of names representing the current search_path")},

	{"_return_args", (PyCFunction) _return_args, METH_VARARGS,
		PyDoc_STR("wrap the argument in a tuple")},
	{"make_sqlstate", (PyCFunction) py_make_sqlstate, METH_O,
		PyDoc_STR("convert a string into an SQL-state integer")},
	{"ereport", (PyCFunction) py_ereport, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("emit a report using Postgres' ereport facility")},

	{"quote_ident", (PyCFunction) py_quote_ident, METH_O,
	PyDoc_STR("quote the identifier")},
	{"quote_literal", (PyCFunction) py_quote_literal, METH_O,
	PyDoc_STR("quote the literal")},
	{"quote_nullable", (PyCFunction) py_quote_nullable, METH_O,
	PyDoc_STR("quote the literal or return NULL if None")},

	{"transaction_timestamp", (PyCFunction) py_transaction_timestamp, METH_NOARGS,
	PyDoc_STR("get the transaction start timestamp")},
	{"statement_timestamp", (PyCFunction) py_statement_timestamp, METH_NOARGS,
	PyDoc_STR("get the statement start timestamp")},
	{"clock_timestamp", (PyCFunction) py_clock_timestamp, METH_NOARGS,
	PyDoc_STR("get the current wall-clock timestamp")},

	{"notify", (PyCFunction) py_notify, METH_VARARGS,
		PyDoc_STR("synonym for NOTIFY \"...\"")},
	{"execute", (PyCFunction) py_execute, METH_O,
	PyDoc_STR("execute multiple SQL statements; always returns None")},

	{"uid", (PyCFunction) pypg_uid, METH_NOARGS,
	PyDoc_STR("get the current UserId")},
	{"session_uid", (PyCFunction) pypg_session_uid, METH_NOARGS,
	PyDoc_STR("get the current SessionUserId")},

/* Large object support interfaces */
	{"_lo_create", (PyCFunction) py_lo_create, METH_VARARGS,
	PyDoc_STR("create a large object returning its Oid")},
	{"_lo_unlink", (PyCFunction) py_lo_unlink, METH_VARARGS,
	PyDoc_STR("remove a large object")},
	{"_lo_open", (PyCFunction) py_lo_open, METH_VARARGS,
	PyDoc_STR("open a large object")},
	{"_lo_write", (PyCFunction) py_lo_write, METH_VARARGS,
	PyDoc_STR("write the data to the opened large object")},
	{"_lo_read", (PyCFunction) py_lo_read, METH_VARARGS,
	PyDoc_STR("read data from the opened large object")},
	{"_lo_close", (PyCFunction) py_lo_close, METH_VARARGS,
	PyDoc_STR("close the large object")},
	{"_lo_tell", (PyCFunction) py_lo_tell, METH_VARARGS,
	PyDoc_STR("tell the internal position")},
	{"_lo_seek", (PyCFunction) py_lo_seek, METH_VARARGS,
	PyDoc_STR("move the internal position of the opened large object")},

	{NULL,},
};

PyDoc_STRVAR(PyPgModule_doc, "Backend Interface Module");

static struct PyModuleDef Postgres_Def = {
	PyModuleDef_HEAD_INIT,
	"Postgres", /* name of module */
	PyPgModule_doc, /* module documentation, may be NULL */
	-1, /* size of per-interpreter state of the module, or -1 if the module keeps state in global variables. */
	PyPgModule_Methods,
};

/*
 * Initializes the "Postgres" module.
 *
 * It follows the extension module initialization pattern,
 * but it's not a separate library file.
 */
PyMODINIT_FUNC init_Postgres(void);
PyMODINIT_FUNC
init_Postgres(void)
{
	PyObj mod, ob, md;

	mod = PyModule_Create(&Postgres_Def);
	if (mod == NULL)
		return(NULL);

	ob = PyDict_New();
	if (ob == NULL)
		goto fail;

	/*
	 * Build CONST dictionary.
	 *
	 * For each constant, set it in `ob`.
	 */
#	define TARGET ob
#	define APFUNC PyMapping_SetItemString
#		include "constants.c"
#	undef APFUNC
#	undef TARGET
	PyModule_AddObject(mod, "CONST", ob);

	/*
	 * Ready all the abstract Postgres type objects.
	 */
#define TYP(TYPE) (PyType_Ready((PyTypeObject *) &PyPg##TYPE##_Type) < 0) ||
	if (PYPG_TYPES(false))
		goto fail;
#undef TYP

	/*
	 * Ready all the Postgres DB types.
	 */
#define TYP(TYPE) (PyType_Ready((PyTypeObject *) &PyPg_##TYPE##_Type) < 0) ||
	if (PYPG_DB_TYPES(false))
		goto fail;
#undef TYP

	EmptyPyPgTupleDesc_Initialize();
	if (PyErr_Occurred())
		goto fail;

#define TYP(TYPE) (PyModule_AddObject(mod, #TYPE, (PyObj) &PyPg##TYPE##_Type) < 0) ||
	if (PYPG_TYPES(false))
		goto fail;
#undef TYP

	/*
	 * Run the pure-Python code in the new module's context.
	 * (See ./module.py for source)
	 */
	md = PyModule_GetDict(mod);
	PyDict_SetItemString(md, "__builtins__", Py_builtins_module);
	ob = Py_CompileString(module_python, "[Postgres]", Py_file_input);
	if (ob == NULL)
		goto fail;
	md = PyEval_EvalCode((PyCodeObject *) ob, md, md);
	Py_XDECREF(md);
	Py_DECREF(ob);

	if (PyErr_Occurred())
		goto fail;

	return(mod);
fail:
	Py_DECREF(mod);
	return(NULL);
}
