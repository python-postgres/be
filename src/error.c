/*
 * Postgres elog/ereport support functions 
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "mb/pg_wchar.h"
#include "catalog/pg_type.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/pl.h"
#include "pypg/strings.h"
#include "pypg/externs.h"
#include "pypg/errcodes.h"
#include "pypg/error.h"
#include "pypg/errordata.h"
#include "pypg/function.h"

/*
 * Given a str(code characters), or an Python int(sixbit representation),
 * return the code in errcode() acceptable form.
 *
 * Returns false when a Python exception is set by the function.
 */
bool
sqlerrcode_from_PyObject(PyObj errcode_ob, int *errcode_out)
{
	if (PyLong_Check(errcode_ob))
	{
		long errcode;
		errcode = PyLong_AsLong(errcode_ob);
		if (PyErr_Occurred())
			return(false);
		*errcode_out = errcode;
	}
	else
	{
		char *codestr;
		Py_INCREF(errcode_ob);
		PyObject_StrBytes(&errcode_ob);
		if (PyErr_Occurred())
			return(false);
		codestr = PyBytes_AS_STRING(errcode_ob);
		if (PyBytes_GET_SIZE(errcode_ob) != 5)
		{
			PyErr_Format(PyExc_ValueError, "invalid sqlerrcode '%s'", codestr);
			Py_DECREF(errcode_ob);
			return(false);
		}
		*errcode_out = MAKE_SQLSTATE(codestr[0], codestr[1], codestr[2], codestr[3], codestr[4]);
		Py_DECREF(errcode_ob);
	}

	return(true);
}

/*
 * Build out the entire Python traceback and exception information.
 */
static char *
context(bool incontext, bool display_full_exc)
{
	PyObj exc, val, tb, rob;
	Py_ssize_t errlen;
	char *r;

	PyErr_Fetch(&exc, &val, &tb);
	PyErr_NormalizeException(&exc, &val, &tb);
	if (tb == NULL)
	{
		tb = Py_None;
		Py_INCREF(tb);
	}
	if (exc == NULL)
	{
		exc = Py_None;
		Py_INCREF(exc);
	}
	if (val == NULL)
	{
		val = Py_None;
		Py_INCREF(val);
	}
	else if (!incontext)
	{
		if (!display_full_exc &&
			PyObject_SetAttrString(val, "_pg_inhibit_str", Py_True))
		{
			/*
			 * It's pretty unlikely, but chances are that something bad is
			 * happening or about to happen, so start flailing.
			 */
			elog(WARNING, "could not set \"_pg_inhibit_str\" attribute on exception");
			PyErr_Clear();
		}
	}

	if (exc != Py_None)
		rob = Py_Call(FormatTraceback, exc, val, tb);
	else
		rob = NULL;

	Py_XDECREF(exc);
	Py_XDECREF(val);
	Py_XDECREF(tb);
	if (rob == NULL)
	{
		/*
		 * Failed to format the traceback. Try getting this failure's
		 * info, but only once.
		 */
		if (exc == Py_None)
		{
			return(pstrdup(_("<Python exception indicated, but None was set>")));
		}
		else if (!incontext)
			return(context(true, false));
		else
			return(pstrdup("<could not get Python traceback>"));
	}
	else
	{
		PyObj tmp;
		tmp = PyUnicode_Join(NULL, rob);
		Py_DECREF(rob);
		rob = tmp;
		if (rob == NULL)
			return(pstrdup("<could not get Python traceback>"));
	}

	PyObject_StrBytes(&rob);
	if (rob != NULL)
	{
		errlen = PyBytes_GET_SIZE(rob);
		r = palloc(errlen+1);
		memcpy(r, PyBytes_AS_STRING(rob), PyBytes_GET_SIZE(rob));
		r[errlen] = '\0';
		Py_DECREF(rob);
	}
	else
	{
		PyErr_Clear();
		r = pstrdup("<could not encode traceback>");
	}

	return(r);
}

static void
emit_exception_errcontext(const char *desc, PyObj fn_filename)
{
	PyObj fn_bytes = NULL;
	const char *protitle = NULL;
	char *data;

	data = context(false, false);

	fn_bytes = fn_filename;
	if (fn_bytes != NULL)
	{
		Py_INCREF(fn_bytes);
		PyObject_StrBytes(&fn_bytes);
		if (fn_bytes)
			protitle = PyBytes_AS_STRING(fn_bytes);
		else
			PyErr_Clear();
	}

	if (protitle)
	{
		if (desc)
			errcontext("[exception from Python]\n%s\n[%s while %s]",
				data, protitle, desc);
		else
			errcontext("[exception from Python]\n%s\n[%s]", data, protitle);
	}
	else if (desc)
		errcontext("[exception from Python]\n%s\n[%s]", data, desc);
	else
		errcontext("[exception from Python]\n%s", data);

	Py_XDECREF(fn_bytes);
	pfree(data);
}

void
collect_errcontext_params(
	struct pl_exec_state *pl_ctx,
	const char **desc, PyObj *filename)
{
	*desc = NULL;
	*filename = NULL;
	if (pl_ctx)
	{
		*desc = pl_ctx->description;

		if (pl_ctx->fn_info)
		{
			PyObj fn = pl_ctx->fn_info->fi_func;
			if (fn)
				*filename = PyPgFunction_GetFilename(fn);
		}
	}
}

/*
 * ecc_context - context() initiated via ErrorContextCallback
 *
 * Second, append the errcontext and clear the Python error.
 * The PL will be exiting via a thrown error soon.
 */
static void
ecc_context(void *arg)
{
	struct pl_exec_state *pl_ctx = arg;
	PyObj filename;
	const char *desc;

	collect_errcontext_params(pl_ctx, &desc, &filename);
	emit_exception_errcontext(desc, filename);
}

void
PyErr_RelayException(void)
{
	Assert(PyErr_Occurred());

	ereport(ERROR,
		(errcode(ERRCODE_PYTHON_RELAY),
		errmsg("python exception is being relayed")));
	Assert(false);
}

/*
 * PyErr_ThrowPostgresErrorWithContext - put the traceback into the errcontext()
 *
 * This function is needed by the root handler as when it re-throws any raised
 * errors, the pl_exec_state has been restored to the prior execution context.
 * (needed an explicit pl_exec_state parameter)
 */
void
PyErr_ThrowPostgresErrorWithContext(int code, const char *errstr, struct pl_exec_state *pl_ctx)
{
	bool inhibit_pl_context = false;
	PyObj exc, val, tb;
	PyObj errdata_ob = NULL;
	ErrorContextCallback ecc;

	/*
	 * Normalize the exception and check for a pg_errordata attribute.
	 */
	PyErr_Fetch(&exc, &val, &tb);
	PyErr_NormalizeException(&exc, &val, &tb);
	if (val != NULL)
	{
		if (PyObject_HasAttr(val, pg_errordata_str_ob))
		{
			errdata_ob = PyObject_GetAttr(val, pg_errordata_str_ob);
			if (errdata_ob == Py_None)
			{
				Py_DECREF(errdata_ob);
				errdata_ob = NULL;
			}
		}

		/*
		 * Exception specific override for inhibit_pl_context?
		 */
		if (PyObject_HasAttr(val, pg_inhibit_pl_context_str_ob))
		{
			PyObj no_tb_ob;

			no_tb_ob = PyObject_GetAttr(val, pg_inhibit_pl_context_str_ob);
			if (no_tb_ob == Py_True)
				inhibit_pl_context = true;
			else if (no_tb_ob == Py_False)
				inhibit_pl_context = false;

			if (no_tb_ob != NULL)
				Py_DECREF(no_tb_ob);
			else
			{
				/*
				 * This is more possible than expected. An Exception subclass
				 * could have been raised with a buggy __getattr__
				 * implementation. (yeah, there's a test for this too)
				 */
				ereport(WARNING,
						(errmsg("could not get \"inhibit_pl_context\" attribute on \"%s\" exception",
								Py_TYPE(val)->tp_name),
						errdetail("A non-fatal Python exception occurred while building the traceback.")));

				/*
				 * Tried to notify the user of the issue, now clear it and move
				 * on.
				 */
				PyErr_Clear();
			}
		}
	}
	/*
	 * Don't inhibit if it's a Python exception.
	 * Chances are that it's not intentional.
	 */
	if (inhibit_pl_context == true &&
		!PyObject_IsSubclass(exc, PyExc_PostgresException))
		inhibit_pl_context = false;

	PyErr_Restore(exc, val, tb);

	if (errdata_ob != NULL && PyPgErrorData_Check(errdata_ob))
	{
		/*
		 * Throw the original error, but with the Python traceback in the context.
		 */
		PG_TRY();
		{
			ReThrowError(PyPgErrorData_GetErrorData(errdata_ob));
		}
		PG_CATCH();
		{
			Py_DECREF(errdata_ob);

			if (inhibit_pl_context)
			{
				/*
				 * Don't include context.
				 */
				PyErr_Clear();
			}
			else
			{
				PyObj filename;
				const char *desc;

				collect_errcontext_params(pl_ctx, &desc, &filename);
				emit_exception_errcontext(desc, filename);
			}

			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	else
	{
		if (inhibit_pl_context)
		{
			/*
			 * Don't include context.
			 */
			PyErr_Clear();
		}
		else
		{
			ecc.callback = ecc_context;
			ecc.previous = error_context_stack;
			ecc.arg = (void *) pl_ctx;
			error_context_stack = &ecc;
		}

		/*
		 * pg_errordata was not an Postgres.ErrorData object.
		 */
		Py_XDECREF(errdata_ob);

		/*
		 * The exception will still be reported, but go ahead and
		 * trump the code and errstr as the PL needs to exit.
		 */
		CHECK_FOR_INTERRUPTS();

		/*
		 * Throw an error referring to a Python exception.
		 */
		ereport(ERROR,(errcode(code), errmsg("%s", errstr)));
	}

	/*
	 * Should never get here.
	 */
	Assert(false);
}

void
PyErr_ThrowPostgresErrorWithCode(int code, const char *errstr)
{
	static struct pl_exec_state pl_ctx = {NULL,NULL,NULL,};

	PyErr_ThrowPostgresErrorWithContext(code, errstr,
		pl_execution_context ? pl_execution_context : &pl_ctx);
}

void
PyErr_ThrowPostgresError(const char *errstr)
{
	PyErr_ThrowPostgresErrorWithCode(ERRCODE_PYTHON_ERROR, errstr);
}

/*
 * This will emit the Python error as a WARNING.
 * 
 * Primarily, this is used in type dealloc functions
 * where some strange failure happened while
 * freeing memory.
 */
void
PyErr_EmitPostgresWarning(const char *errstr)
{
	ErrorContextCallback ecc;
	ecc.callback = ecc_context;
	ecc.arg = (void *) NULL;
	ecc.previous = error_context_stack;
	error_context_stack = &ecc;

	/*
	 * Throw a warning about the Python exception.
	 */
	elog(WARNING, "%s", errstr);
	error_context_stack = ecc.previous;
	Assert(!PyErr_Occurred());
}

/*
 * PyErr_SetPgError() - Set the current Postgres error as the Python exception.
 *
 * Also, mark the DB as being in error(keeps track that we pulled and flushed
 * the error data).
 */
void
PyErr_SetPgError(bool inhibit_warning)
{
	MemoryContext former = CurrentMemoryContext;
	PyObj errdata = NULL;
	PyObj exc, val, tb;

	/*
	 * When inhibit warning is true, the caller is indicating that the error
	 * should not cause a failed transaction error. Often, this is used by
	 * callers that are causing an error in a controlled situation or when an
	 * OOM error occurs.
	 */
	if (!inhibit_warning)
	{
		if (pl_state == pl_in_failed_transaction)
		{
			/*
			 * If this is ever emitted, it is likely a programming error(PL level).
			 * [ie, a PG_TRY() block was not properly protected from in-error use]
			 */
			elog(WARNING, "database already in error state");
		}

		if (pl_state == pl_ready_for_access)
			pl_state = pl_in_failed_transaction;
	}

	/*
	 * If it's a relayed exception, ignore the ErrorData.
	 * (we're actually going to the raise the currently set Python error)
	 */
	if (!_PG_ERROR_IS_RELAY())
	{
		errdata = PyPgErrorData_FromCurrent();

		/*
		 * The above _FromCurrent will set the error if it failed.
		 */
		if (errdata != NULL)
		{
			if (!inhibit_warning)
			{
				ErrorData *ed;
				ed = PyPgErrorData_GetErrorData(errdata);

				/*
				 * Check for interrupt code.
				 */
				switch (ed->sqlerrcode)
				{
					case ERRCODE_OPERATOR_INTERVENTION:
					case ERRCODE_QUERY_CANCELED:
					case ERRCODE_ADMIN_SHUTDOWN:
						/*
						 * Interrupted. Things need to stop.
						 */
						pl_state = pl_in_failed_transaction;
					break;

					default:
						;
					break;
				}
			}

			PyErr_SetObject(PyExc_PostgresException, errdata);
			PyErr_Fetch(&exc, &val, &tb);
			PyErr_NormalizeException(&exc, &val, &tb);
			Py_DECREF(errdata);
			PyErr_Restore(exc, val, tb);
		}
	}
	else
	{
		/*
		 * It's a relay; meaning the ErrorData is "empty", and we should
		 * expect a Python error to be set.
		 */
		PG_TRY();
		{
			/* Don't need it at all */
			FlushErrorState();
		}
		PG_CATCH();
		{
			/* can't happen? */
			elog(WARNING, "could not flush error state");
		}
		PG_END_TRY();

		/*
		 * huh, relayed exception did *not* have a Python
		 * error set?
		 */
		if (!PyErr_Occurred())
		{
			PyErr_SetString(PyExc_RuntimeError,
				"exception relay did not set exception"
			);
		}
	}
	MemoryContextSwitchTo(former);

	/*
	 * A Python exception *should* be set.
	 * If PyPgErrorData could not be created, a RuntimeError will be set.
	 */
	Assert(PyErr_Occurred());
}

void
PyErr_EmitPgErrorAsWarning(const char *msg)
{
	char *data;

	if (!_PG_ERROR_IS_RELAY())
	{
		PyObj exc, val, tb;

		/*
		 * Store the current exception, if any.
		 */
		PyErr_Fetch(&exc, &val, &tb);

		/*
		 * Don't modify pl_state.
		 */
		PyErr_SetPgError(true);

		data = context(false, true);
		/*
		 * Throw a warning about the PG error.
		 */
		ereport(WARNING,(
			errmsg("%s", msg),
			errcontext("[Postgres ERROR]\n%s", data)));
		pfree(data);

		PyErr_Restore(exc, val, tb);
	}
	else
	{
		/*
		 * Just a relay, don't need the ErrorData.
		 */
		FlushErrorState();

		/*
		 * Emitting the currently set Python exception as a warning.
		 */
		data = context(false, true);
		/*
		 * Throw a warning about the Python exception.
		 */
		ereport(WARNING,(
			errmsg("%s", msg),
			errcontext("[exception from Python]\n%s", data)));
		pfree(data);
	}
}

/*
 * Set when the database is accessed between transactions or prior to
 * initialization.
 */
bool
PyErr_SetDatabaseAccessDenied(void)
{
	PyErr_SetString(PyExc_RuntimeError, "database not ready");
	return(true);
}

/*
 * Used by the DB_IS_NOT_READY() macro.
 */
bool
PyErr_SetInFailedTransaction(void)
{
	PG_TRY();
	{
		ereport(ERROR,(
			errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
			errmsg("database action attempted while in failed transaction")
		));
	}
	PG_CATCH();
	{
		PyErr_SetPgError(true); /* Error data flushed. */
	}
	PG_END_TRY();

	return(true);
}
