/*
 * do.c - inline handler implementation
 *
 * Feature found in PG 8.5/9.0 and greater.
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "postgres.h"
/*
 * DO statements exist in 8.5 and greater.
 * FIXME: This should probably be an ifdef determined by a probe.
 */
#if (PG_VERSION_NUM >= 80500)
#include "fmgr.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "access/transam.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "storage/itemptr.h"
#include "nodes/parsenodes.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/strings.h"
#include "pypg/externs.h"
#include "pypg/pl.h"
#include "pypg/error.h"
#include "pypg/ist.h"
#include "pypg/type/type.h"
#include "pypg/function.h"
#include "pypg/tupledesc.h"

#include "pypg/do.h"

/*
 * The sub-executor for inline functions: DO '...' LANGUAGE python;
 *
 * Fake fcinfo, flinfo, and fn_info. Inline execution goes through
 * the pl_handler.
 */
struct pl_fn_info pl_inline_executor_info;
FmgrInfo pl_inline_executor_fmgrinfo;
FunctionCallInfoData pl_inline_executor_fcinfo = {NULL,};

/*
 * Create the necessary function information for executing an inline procedure.
 * The executor is actually implemented in pure-Python in module.py, this
 * initialization path constructs the function information for that executor.
 *
 * By taking this route, we execute inline statements in the same context as
 * other functions, which allows us to leverage the PL-state checking already
 * available in pl_handler().
 */
static void
initialize_inline_executor(Oid langOid)
{
	MemoryContext former = CurrentMemoryContext;
	const char *attname = "prosrc";
	Oid cstringoid = CSTRINGOID;
	TupleDesc argdesc = NULL;
	PyObj func, module = NULL, output;
	PyObj fn_oid_int, fn_oid_str;
	PyObj nspname_str_ob, filename_str_ob;

	/*
	 * The fake function object will take a single cstring argument and return
	 * VOID.
	 */
	func = PyPgFunction_NEW();
	if (func == NULL)
	{
		PyErr_ThrowPostgresError("could not create the inline executor");
	}

	PyPgFunction_SetOid(func, InvalidOid);
	PyPgFunction_SetLanguage(func, langOid);
	PyPgFunction_SetNamespace(func, InvalidOid);
	PyPgFunction_SetStateful(func, false);
	PyPgFunction_SetXMin(func, InvalidTransactionId);
	PyPgFunction_SetReturnsSet(func, false);
	PyPgFunction_SetVolatile(func, 'v');
	PyPgFunction_SetPGFunction(func, NULL);

	Py_INCREF(Py_None);
	PyPgFunction_SetSource(func, Py_None);

	fn_oid_int = PyLong_FromLong(0);
	fn_oid_str = PyObject_Str(fn_oid_int);
	PyPgFunction_SetPyLongOid(func, fn_oid_int);
	PyPgFunction_SetPyUnicodeOid(func, fn_oid_str);

	filename_str_ob = PyUnicode_FromString("__inline_executor__");
	PyPgFunction_SetFilename(func, filename_str_ob);

	nspname_str_ob = PyUnicode_FromString("");
	PyPgFunction_SetNamespaceName(func, nspname_str_ob);

	/*
	 * Looking up types, the inline executor can't be initialized before first
	 * call.
	 */
	PG_TRY();
	{
		PyObj input;

		argdesc = TupleDesc_FromNamesAndOids(1, &attname, &cstringoid);
		input = PyPgTupleDesc_FromCopy(argdesc);
		PyPgFunction_SetInput(func, input);
		pl_inline_executor_info.fi_input = input;

		FreeTupleDesc(argdesc);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(true);
	}
	PG_END_TRY();
	MemoryContextSwitchTo(former);

	output = PyPgType_FromOid(VOIDOID);
	PyPgFunction_SetOutput(func, output);

	module = PyObject_GetAttrString(Py_Postgres_module, "InlineExecutor");
	if (PyErr_Occurred())
	{
		Py_DECREF(func);
		Py_XDECREF(module);
		PyErr_ThrowPostgresError("could not initialize the inline executor");
	}

	pl_inline_executor_info.fi_xid = 1;
	pl_inline_executor_info.fi_func = func;
	/*
	 * The fake module is the primary contrast between the inline executor and
	 * real functions. The executor's fn_info is permanent and points to the
	 * Postgres.InlineExecutor object that provides a "main" attribute that will
	 * create and manage the temporary module to run the script in.
	 *
	 * This is how the inline executor avoids the usual initialize() procedure
	 * that real functions go through. It's *always* current.
	 */
	pl_inline_executor_info.fi_module = module;
	pl_inline_executor_info.fi_state = NULL;
	pl_inline_executor_info.fi_output = output;

	pl_inline_executor_fmgrinfo.fn_extra = &pl_inline_executor_info;
	pl_inline_executor_fmgrinfo.fn_oid = InvalidOid;
	pl_inline_executor_fmgrinfo.fn_addr = NULL;
	pl_inline_executor_fmgrinfo.fn_nargs = 1;
	pl_inline_executor_fmgrinfo.fn_strict = false;
	pl_inline_executor_fmgrinfo.fn_retset = false;
	pl_inline_executor_fmgrinfo.fn_stats = '\0';
	pl_inline_executor_fmgrinfo.fn_expr = NULL;

	pl_inline_executor_fcinfo.context = NULL;
	pl_inline_executor_fcinfo.resultinfo = NULL;
	pl_inline_executor_fcinfo.isnull = false;
	pl_inline_executor_fcinfo.nargs = 1;
	pl_inline_executor_fcinfo.flinfo = &pl_inline_executor_fmgrinfo;
}

PG_FUNCTION_INFO_V1(pl_inline);
Datum
pl_inline(PG_FUNCTION_ARGS)
{
	InlineCodeBlock *codeblock = (InlineCodeBlock *) DatumGetPointer(PG_GETARG_DATUM(0));
	Datum rd;

	Assert(IsA(codeblock, InlineCodeBlock));

	if (pl_inline_executor_fcinfo.flinfo == NULL)
	{
		if (pl_state == pl_not_initialized)
			pl_first_call();

		/*
		 * Initialize the pseudo-function for executing DO-blocks.
		 */
		initialize_inline_executor(codeblock->langOid);
	}

	/*
	 * Be sure to update the inline executor function if these
	 * parameters need to change.
	 */
	pl_inline_executor_fmgrinfo.fn_mcxt = CurrentMemoryContext;
	pl_inline_executor_fcinfo.argnull[0] = false;
	pl_inline_executor_fcinfo.arg[0] = PointerGetDatum(codeblock->source_text);

	rd = pl_handler(&pl_inline_executor_fcinfo);

	fcinfo->argnull[0] = true;
	fcinfo->arg[0] = PointerGetDatum(NULL);
	pl_inline_executor_fmgrinfo.fn_mcxt = NULL;

	return(rd);
}
#endif
