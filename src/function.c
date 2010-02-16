/*
 * Postgres functions
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <compile.h>
#include <structmember.h>

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/heapam.h"
#include "access/tupdesc.h"
#include "access/transam.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "mb/pg_wchar.h"
#include "nodes/params.h"
#include "parser/parse_func.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/tuplestore.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/pl.h"
#include "pypg/strings.h"
#include "pypg/externs.h"
#include "pypg/error.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/system.h"
#include "pypg/type/record.h"
#include "pypg/tupledesc.h"
#include "pypg/function.h"

/*
 * PyPgFunction_get_source - get the function's prosrc
 */
PyObj
PyPgFunction_get_source(PyObj func)
{
	PyObj src = NULL;

	Assert(func != NULL);
	Assert(PyPgFunction_Check(func));

	src = PyPgFunction_GetSource(func);
	Assert(src != NULL);
	Py_INCREF(src);

	return(src);
}

/*
 * PyPgFunction_get_code - compile the code from the source
 */
PyObj
PyPgFunction_get_code(PyObj func)
{
	PyObj prosrc, cargs, rob;

	Assert(func != NULL);
	Assert(PyPgFunction_Check(func));
	Assert(PyPgFunction_GetFilename(func) != NULL);

	prosrc = PyPgFunction_get_source(func);
	if (prosrc == NULL)
		return(NULL);

	if (prosrc == Py_None)
	{
		PyErr_SetString(PyExc_TypeError, "function has no source to compile");
		Py_DECREF(prosrc);
		return(NULL);
	}

	cargs = PyTuple_New(3);
	if (cargs == NULL)
		return(NULL);

	PyTuple_SET_ITEM(cargs, 0, prosrc);
	PyTuple_SET_ITEM(cargs, 1, PyPgFunction_GetFilename(func));
	PyTuple_SET_ITEM(cargs, 2, exec_str_ob);
	Py_INCREF(PyPgFunction_GetFilename(func));
	Py_INCREF(exec_str_ob);

	rob = PyObject_CallObject(Py_compile_ob, cargs);
	Py_DECREF(cargs);

	return(rob);
}

/*
 * PyPgFunction_load_module - create and load the module
 *
 * This execute the module body in a new module object iff the module does not
 * already exist in sys.modules.
 *
 * The PL protocol checks are not done here because this could be called from
 * the code of another function. It's the caller's responsibility to ensure that
 * the appropriate checks are being made at the appropriate time.
 */
PyObj
PyPgFunction_load_module(PyObj func)
{
	PyObj modules, module, modname, d, code, evalr;
	int rv;

	Assert(func != NULL);
	Assert(PyPgFunction_Check(func));

	modules = PyImport_GetModuleDict();

	rv = PySequence_Contains(modules, PyPgFunction_GetPyUnicodeOid(func));
	if (rv == -1)
		return(NULL);
	else if (rv == 1)
	{
		/*
		 * If this returns NULL, it's probably some weird race condition...
		 * The check above said it exists, so let's trust it.
		 */
		return(PyObject_GetItem(modules, PyPgFunction_GetPyUnicodeOid(func)));
	}

	/*
	 * Hasn't been loaded into sys.modules yet.
	 */

	code = PyPgFunction_get_code(func);
	if (code == NULL)
		return(NULL);

	modname = PyPgFunction_GetPyUnicodeOid(func);
	Py_INCREF(modname);
	PyObject_StrBytes(&modname);
	if (modname == NULL)
	{
		Py_DECREF(code);
		return(NULL);
	}

	module = PyModule_New(PyBytes_AS_STRING(modname));
	Py_DECREF(modname);
	if (module == NULL)
	{
		Py_DECREF(code);
		return(NULL);
	}

	d = PyModule_GetDict(module);
	if (PyDict_SetItemString(d, "__builtins__", Py_builtins_module) != 0)
		goto fail;
	/*
	 * __loader__ PEP302 support.
	 * This is what linecache uses to access the function's source
	 */
	if (PyDict_SetItemString(d, "__loader__", func) != 0)
		goto fail;
	if (PyDict_SetItemString(d, "__func__", func) != 0)
		goto fail;
	if (PyDict_SetItemString(d, "__file__", PyPgFunction_GetFilename(func)) != 0)
		goto fail;

	/*
	 * Module has to exist in sys.modules before code evaluates.
	 */
	if (PyObject_SetItem(modules, PyPgFunction_GetPyUnicodeOid(func), module) != 0)
		goto fail;

	/*
	 * Module context, therefore locals and globals are the same object.
	 */
	evalr = PyEval_EvalCode((PyCodeObject *) code, d, d);
	if (evalr == NULL)
	{
		/*
		 * Code evaluation failed. Remove the junk module from sys.modules.
		 */
		PyObject_DelItem(modules, PyPgFunction_GetPyUnicodeOid(func));
		goto fail;
	}
	Py_DECREF(evalr);
	Py_DECREF(code);

	return(module);
fail:
	Py_DECREF(code);
	Py_DECREF(module);
	return(NULL);
}

/*
 * Create a TupleDesc from a given pg_proc tuple's proarginfo column
 */
TupleDesc
TupleDesc_From_pg_proc_arginfo(HeapTuple ht)
{
	int16 pronargs;
	Datum proargtypes_datum, proargnames_datum;
	bool isnull;

	Oid *types;
	char **names = NULL;
	int nnames;

	TupleDesc td;

	pronargs = DatumGetInt16(SysCacheGetAttr(PROCOID, ht,
		Anum_pg_proc_pronargs, &isnull)
	);
	if (isnull)
		elog(ERROR, "pronargs is NULL");

	proargtypes_datum = SysCacheGetAttr(PROCOID, ht,
		Anum_pg_proc_proargtypes, &isnull);
	if (isnull)
		elog(ERROR, "proargtypes is NULL");

	types = (Oid *) ARR_DATA_PTR(DatumGetArrayTypeP(proargtypes_datum));

	proargnames_datum = SysCacheGetAttr(PROCOID, ht,
		Anum_pg_proc_proargnames, &isnull);
	if (isnull == false)
	{
		ArrayType *name_array = NULL;
		Datum *name_datums;
		bool *name_nulls;

		/* INOUT Parameters may adjust the location of the argument name */
		bool modes_isnull;
		Datum proargmodes_datum;

		name_array = DatumGetArrayTypeP(proargnames_datum);

		deconstruct_array(
			name_array, ARR_ELEMTYPE(name_array),
			-1, false, 'i', &name_datums,
			&name_nulls, &nnames
		);
		if (nnames < pronargs)
			elog(ERROR, "too few names in proargnames");

		if (nnames > 0)
		{
			names = palloc0(sizeof(char *) * pronargs);

			proargmodes_datum = SysCacheGetAttr(PROCOID, ht,
				Anum_pg_proc_proargmodes, &modes_isnull);
			if (modes_isnull == false)
			{
				char *modes;
				int i, k;

				modes = (char *) ARR_DATA_PTR(DatumGetArrayTypeP(proargmodes_datum));
				for (i = 0, k = 0; i < nnames; ++i)
				{
					switch (modes[i])
					{
						case 'b':
						case 'i':
						{
							Datum name_d;
							name_d = name_datums[i];
							names[k] = (char *) DirectFunctionCall1(textout, name_d);
							++k;
						}
						break;

						case 'o':
						break;

						default:
						{
							elog(ERROR, "unknown mode \"%c\" for argument %d",
								modes[i], i);
						}
						break;
					}
				}
			}
			else /* Don't process proargnames down here if modes is valid */
			{
				int i;

				for (i = 0; i < nnames; ++i)
				{
					Datum name_d;
					name_d = name_datums[i];
					names[i] = (char *) DirectFunctionCall1(textout, name_d);
				}
			}
		}
	}

	td = TupleDesc_FromNamesAndOids(pronargs, (const char **) names, types);
	if (names != NULL)
	{
		int i;
		if (nnames > 0)
			for (i = 0; i < pronargs; ++i)
				pfree(names[i]);
		pfree(names);
	}
	return(td);
}

/*
 * invalid_fullname - Used by the PEP302 interfaces
 *
 * This will determine whether or not the given "fullname" argument
 * corresponds to an identifier of the function.
 *
 * While normally used only in conjunction with a reference to "__name__",
 * other forms of the identifier are allowed for mere convenience.
 */
static bool
invalid_fullname(PyObj self, PyObj args, PyObj kw)
{
	char *words[] = {"fullname", NULL};
	PyObj fullname = NULL;
	Oid fn_oid;

	if (!PyPgFunction_CheckExact(self))
	{
		PyErr_SetString(PyExc_TypeError,
			"loader method requires Postgres.Function type");
		return(true);
	}

	if (!PyArg_ParseTupleAndKeywords(args, kw, "|O", words, &fullname))
		return(true);

	if (fullname == NULL
	|| fullname == PyPgFunction_GetPyUnicodeOid(self)
	|| fullname == PyPgFunction_GetPyLongOid(self))
		return(false);

	/*
	 * fullname is given and it's not an exact Oid object,
	 * so convert it to a real Oid and compare.
	 */
	if (Oid_FromPyObject(fullname, &fn_oid))
		return(true);

	if (PyPgFunction_GetOid(self) == fn_oid)
		return(false);

	PyErr_Format(PyExc_ImportError,
		"loader cannot import module named '%lu'", fn_oid);
	return(true);
}

/*
 * Postgres FUNCTIONs are not packages. [PEP302]
 */
static PyObj
is_package(PyObj self, PyObj args, PyObj kw)
{
	if (invalid_fullname(self, args, kw))
		return(NULL);
	Py_INCREF(Py_False);
	return(Py_False);
}

/*
 * Get the function source code. [PEP302]
 */
static PyObj
get_source(PyObj self, PyObj args, PyObj kw)
{
	if (invalid_fullname(self, args, kw))
		return(NULL);

	return(PyPgFunction_get_source(self));
}

/*
 * Get the function's compiled code object. [PEP302]
 */
static PyObj
get_code(PyObj self, PyObj args, PyObj kw)
{
	if (invalid_fullname(self, args, kw))
		return(NULL);

	return(PyPgFunction_get_code(self));
}

/*
 * def load_module(fullname) -> types.ModuleType
 *
 * Evaluate the function's code in a new module
 * or if the fullname exists in sys.modules, return
 * the existing module. [PEP302]
 *
 * This code must go through pl_handler in order to properly load the
 * module. The execution context can dictate much about what happens during
 * load time.
 *
 * i.e., tricky shit happens here. It may be preferrable to make the execution
 * context rigging more accessible, but for now, it's the handler's job.
 */
static PyObj
load_module(PyObj self, PyObj args, PyObj kw)
{
	MemoryContext former = CurrentMemoryContext;
	volatile PyObj rob = NULL;
	FmgrInfo flinfo;
	FunctionCallInfoData fcinfo;

	if (invalid_fullname(self, args, kw))
		return(NULL);

	/*
	 * Disallow execution of "anonymous" functions.
	 */
	flinfo.fn_addr = PyPgFunction_GetPGFunction(self);
	flinfo.fn_oid = PyPgFunction_GetOid(self);
	flinfo.fn_retset = false;
	if (flinfo.fn_addr == NULL || flinfo.fn_oid == InvalidOid)
	{
		PyErr_SetString(PyExc_TypeError, "internal functions cannot be preloaded");
		return(NULL);
	}

	flinfo.fn_nargs = -1;
	flinfo.fn_extra = NULL;
	flinfo.fn_mcxt = CurrentMemoryContext;
	flinfo.fn_expr = NULL;
	fcinfo.nargs = -1;
	fcinfo.flinfo = &flinfo;
	fcinfo.context = NULL;
	fcinfo.resultinfo = NULL;
	/*
	 * Better be true afterwards.
	 */
	fcinfo.isnull = false;

	SPI_push();
	PG_TRY();
	{
		rob = (PyObj) DatumGetPointer(FunctionCallInvoke(&fcinfo));
	}
	PG_CATCH();
	{
		rob = NULL;
		PyErr_SetPgError(false);
	}
	PG_END_TRY();
	SPI_pop();
	MemoryContextSwitchTo(former);

	if (fcinfo.isnull == false)
	{
		PyErr_SetString(PyExc_RuntimeError,
			"function module load protocol did not set isnull");
		rob = NULL;
	}

	Py_XINCREF(rob);
	return(rob);
}

/*
 * Create a PyPgFunction from an Oid object
 */
static PyObj
find_module(PyObj typ, PyObj args, PyObj kw)
{
	char *words[] = {"fullname", "path", NULL};
	PyObj fullname, path;
	Oid fn_oid;

	if (typ != (PyObj) &PyPgFunction_Type)
	{
		PyErr_SetString(PyExc_TypeError,
			"find_module expects Postgres.Function as its first argument");
		return(NULL);
	}

	if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O", words, &fullname, &path))
		return(NULL);

	if (path != NULL && path != Py_None)
	{
		PyErr_SetString(PyExc_ImportError, "Postgres functions are top-level modules");
		return(NULL);
	}

	if (Oid_FromPyObject(fullname, &fn_oid))
		return(NULL);

	return(PyPgFunction_FromOid(fn_oid));
}

static PyMethodDef PyPgFunction_Methods[] = {
	/*
	 * PEP 302 Interfaces
	 *
	 * These interfaces should only be used on Python functions
	 */
	{"is_package", (PyCFunction) is_package, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("Always returns false. [PEP302 interface]")},
	{"get_source", (PyCFunction) get_source, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("Get the function's procedure source [PEP302 interface]")},
	{"get_code", (PyCFunction) get_code, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("Return the code of the source. [PEP302 interface]")},
	{"load_module", (PyCFunction) load_module, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("Return a module based on the source. [PEP302 interface]")},
	{"find_module", (PyCFunction) find_module, METH_VARARGS|METH_KEYWORDS|METH_CLASS,
		PyDoc_STR("Get a Postgres.Function--loader--object based on the given fullname. [PEP302 interface]")},
	{NULL}
};

static PyObj
func_get_pronamespace(PyObj self, void *closure)
{
	PyObj rob;
	rob = PyLong_FromUnsignedLong(PyPgFunction_GetNamespace(self));
	return(rob);
}

static PyObj
func_get_prolang(PyObj self, void *closure)
{
	PyObj rob;
	rob = PyLong_FromUnsignedLong(PyPgFunction_GetLanguage(self));
	return(rob);
}

static PyGetSetDef PyPgFunction_GetSet[] = {
	{"namespace", func_get_pronamespace, NULL, PyDoc_STR("get the namespace oid")},
	{"language", func_get_prolang, NULL, PyDoc_STR("get the language oid")},
	{NULL,}
};

static PyMemberDef PyPgFunction_Members[] = {
	{"oid", T_OBJECT, offsetof(struct PyPgFunction, fn_oid_int), READONLY,
	PyDoc_STR("pg_proc entry's Oid")},
	{"oidstr", T_OBJECT, offsetof(struct PyPgFunction, fn_oid_str), READONLY,
	PyDoc_STR("pg_proc entry's Oid as a str object")},
	{"nspname", T_OBJECT, offsetof(struct PyPgFunction, fn_nspname_str), READONLY,
	PyDoc_STR("namespace name when the function was loaded")},
	{"filename", T_OBJECT, offsetof(struct PyPgFunction, fn_filename_str), READONLY,
	PyDoc_STR("regprocedure-like representation of the procedure's identifier")},
	{"input", T_OBJECT, offsetof(struct PyPgFunction, fn_input), READONLY,
	PyDoc_STR("the function's parameter descriptor")},
	{"output", T_OBJECT, offsetof(struct PyPgFunction, fn_output), READONLY,
	PyDoc_STR("the function's results")},
	{"stateful", T_BOOL, offsetof(struct PyPgFunction, fn_stateful), 0,
	PyDoc_STR("the function returns a send'able iterator(generator)")},
	{NULL,}
};

static void
func_dealloc(PyObj self)
{
	PyObj ob;

	ob = PyPgFunction_GetOutput(self);
	if (ob != NULL)
	{
		PyPgFunction_SetOutput(self, NULL);
		Py_DECREF(ob);
	}

	ob = PyPgFunction_GetInput(self);
	if (ob != NULL)
	{
		PyPgFunction_SetInput(self, NULL);
		Py_DECREF(ob);
	}

	ob = PyPgFunction_GetPyUnicodeOid(self);
	if (ob != NULL)
	{
		PyPgFunction_SetPyUnicodeOid(self, NULL);
		Py_DECREF(ob);
	}

	ob = PyPgFunction_GetPyLongOid(self);
	if (ob != NULL)
	{
		PyPgFunction_SetPyLongOid(self, NULL);
		Py_DECREF(ob);
	}

	ob = PyPgFunction_GetNamespaceName(self);
	if (ob != NULL)
	{
		PyPgFunction_SetNamespaceName(self, NULL);
		Py_DECREF(ob);
	}

	ob = PyPgFunction_GetFilename(self);
	if (ob != NULL)
	{
		PyPgFunction_SetFilename(self, NULL);
		Py_DECREF(ob);
	}

	ob = PyPgFunction_GetSource(self);
	if (ob != NULL)
	{
		PyPgFunction_SetSource(self, NULL);
		Py_DECREF(ob);
	}

	self->ob_type->tp_free(self);
}

static PyObj
func_call(PyObj self, PyObj args, PyObj kw)
{
	MemoryContext former = CurrentMemoryContext;
	PyObj fn_input, fn_output, input, rob = NULL;
	TupleDesc td;
	FmgrInfo flinfo;
	FunctionCallInfoData fcinfo;
	volatile Datum datum = 0;

	/*
	 * Disallow execution of "anonymous" functions.
	 */
	flinfo.fn_addr = PyPgFunction_GetPGFunction(self);
	flinfo.fn_oid = PyPgFunction_GetOid(self);
	flinfo.fn_retset = PyPgFunction_GetReturnsSet(self);
	if (flinfo.fn_addr == NULL || flinfo.fn_oid == InvalidOid)
	{
		PyErr_SetString(PyExc_TypeError, "internal functions are not directly callable");
		return(NULL);
	}
	if (flinfo.fn_retset)
	{
		PyErr_SetString(PyExc_NotImplementedError,
			"cannot directly execute set returning functions");
		return(NULL);
	}

	fn_input = PyPgFunction_GetInput(self);
	fn_output = PyPgFunction_GetOutput(self);
	if (PyPgTupleDesc_IsPolymorphic(fn_input) ||
		PyPgType_IsPolymorphic(fn_output))
	{
		PyErr_SetString(PyExc_NotImplementedError,
			"cannot directly execute polymorphic functions");
		return(NULL);
	}

	if (PyPgType_GetOid(fn_output) == TRIGGEROID)
	{
		PyErr_SetString(PyExc_NotImplementedError,
			"cannot directly execute TRIGGER returning functions");
		return(NULL);
	}

	/* No access if failed transaction */
	if (DB_IS_NOT_READY())
		return(NULL);

	td = PyPgTupleDesc_GetTupleDesc(fn_input);

	/*
	 * Normalize the parameters.
	 */
	input = PyTuple_FromTupleDescAndParameters(td, args, kw);
	if (input == NULL)
		return(NULL);

	flinfo.fn_nargs = td->natts;
	flinfo.fn_extra = NULL;
	flinfo.fn_mcxt = CurrentMemoryContext;
	flinfo.fn_expr = NULL;
	fcinfo.flinfo = &flinfo;
	fcinfo.context = NULL;
	fcinfo.resultinfo = NULL;
	fcinfo.isnull = false;
	/*
	 * Custom built descriptor; no dropped attributes.
	 */
	fcinfo.nargs = td->natts;

	SPI_push();
	PG_TRY();
	{
		Py_BuildDatumsAndNulls(td,
			PyPgTupleDesc_GetTypesTuple(fn_input),
			input, fcinfo.arg, fcinfo.argnull);

		datum = FunctionCallInvoke(&fcinfo);

		/*
		 * Special casing void to avoid the singleton.
		 */
		if (fcinfo.isnull ||
			PyPgType_GetOid(fn_output) == VOIDOID)
		{
			rob = Py_None;
			Py_INCREF(rob);
		}
		else
		{
			/*
			 * Some functions will return a parameter that its given.
			 * This is problematic if we are going to free the output
			 * after re-allocating as a Postgres.Object.
			 */
			if (PyPgType_ShouldFree(fn_output))
			{
				int i;

				/*
				 * Scan for !typbyval parameters.
				 * When one is found, compare the datum to the result datum.
				 */
				for (i = 0; i < PyTuple_GET_SIZE(input); ++i)
				{
					PyObj param = PyTuple_GET_ITEM(input, i);
					/*
					 * It's tempting to check the types first, but in situations
					 * of functions doing binary compatible coercion, it would be a
					 * mistake.
					 */
					if (PyPgType_ShouldFree(Py_TYPE(param)))
					{
						if (PyPgObject_GetDatum(param) == datum)
						{
							/*
							 * It's the same Datum of an argument,
							 * inc the ref and return the param.
							 */
							if (fn_output == (PyObj) Py_TYPE(param))
							{
								rob = param;
								Py_INCREF(rob);
							}
							else
							{
								/*
								 * It's the same Datum, but a different type.
								 * Make a Copy.
								 */
								rob = PyPgObject_New(fn_output, datum);
							}

							break;
						}
					}
				}

				/*
				 * It's a newly allocated result? (not an argument)
				 */
				if (rob == NULL)
				{
					/*
					 * New result, Datum is copied into the PythonMemoryContext
					 */
					rob = PyPgObject_New(fn_output, datum);
					/*
					 * Cleanup.
					 */
					pfree(DatumGetPointer(datum));
				}
			}
			else
			{
				/* Not pfree'ing typbyval, so no need to check parameters. */
				rob = PyPgObject_New(fn_output, datum);
			}
		}
	}
	PG_CATCH();
	{
		Py_XDECREF(rob);
		rob = NULL;
		PyErr_SetPgError(false);
	}
	PG_END_TRY();
	SPI_pop();

	Py_DECREF(input);

	MemoryContextSwitchTo(former);
	return(rob);
}

static PyObj
func_new_from_oid(PyTypeObject *subtype, Oid fn_oid, PyObj fn_oid_int, PyObj fn_oid_str)
{
	volatile HeapTuple ht = NULL;
	volatile PyObj rob = NULL;

	Assert(OidIsValid(fn_oid));
	Assert(fn_oid_int != NULL);
	Assert(fn_oid_str != NULL);

	rob = subtype->tp_alloc(subtype, 0);
	if (rob == NULL)
		return(NULL);

	PyPgFunction_SetOid(rob, fn_oid);
	PyPgFunction_SetStateful(rob, false);

	Py_INCREF(fn_oid_int);
	Py_INCREF(fn_oid_str);

	PyPgFunction_SetPyLongOid(rob, fn_oid_int);
	PyPgFunction_SetPyUnicodeOid(rob, fn_oid_str);

	/*
	 * Collect the Function information from the system cache
	 */
	PG_TRY();
	{
		Form_pg_proc ps;
		Form_pg_namespace ns;
		FmgrInfo flinfo;
		text *prosrc;
		Datum prosrc_datum;
		bool isnull = true;
		const char *filename = NULL, *nspname, *q_nspname;
		TupleDesc argdesc = NULL, result_desc = NULL;
		Oid prorettype = InvalidOid;
		PyObj id_str_ob = NULL, nspname_str_ob = NULL;
		PyObj filename_str_ob = NULL, q_nspname_str_ob = NULL;
		PyObj output = NULL, src = NULL;
		PyObj input;

		ht = SearchSysCache(PROCOID, fn_oid, 0, 0, 0);
		if (!HeapTupleIsValid(ht))
		{
			ereport(ERROR,(
				errcode(ERRCODE_UNDEFINED_FUNCTION),
				errmsg("failed to find function at oid %d", fn_oid)
			));
		}

		PyPgFunction_SetXMin(rob, HeapTupleHeaderGetXmin(ht->t_data));
		PyPgFunction_SetItemPointer(rob, &(ht->t_self));

		ps = (Form_pg_proc) GETSTRUCT(ht);

		PyPgFunction_SetNamespace(rob, ps->pronamespace);
		PyPgFunction_SetLanguage(rob, ps->prolang);
		PyPgFunction_SetReturnsSet(rob, ps->proretset);
		PyPgFunction_SetVolatile(rob, ps->provolatile);

		prorettype = ps->prorettype;
		prosrc_datum = SysCacheGetAttr(
			PROCOID, ht, Anum_pg_proc_prosrc, &isnull);
		if (!isnull)
		{
			prosrc = DatumGetTextPCopy(prosrc_datum);
			src = PyUnicode_FromTEXT(prosrc);
			PyPgFunction_SetSource(rob, src);
			pfree(prosrc);
			prosrc = NULL;
		}
		else
		{
			src = Py_None;
			Py_INCREF(src);
			PyPgFunction_SetSource(rob, src);
		}
		if (src == NULL)
			PyErr_RelayException();

		/*
		 * Get the function's address.
		 */
		fmgr_info(fn_oid, &flinfo);
		PyPgFunction_SetPGFunction(rob, flinfo.fn_addr);

		/*
		 * Build function parameters TupleDesc
		 */
		if (ps->pronargs > 0)
		{
			argdesc = TupleDesc_From_pg_proc_arginfo(ht);
			input = PyPgTupleDesc_FromCopy(argdesc);
			if (input == NULL)
				PyErr_RelayException();
			PyPgFunction_SetInput(rob, input);
			FreeTupleDesc(argdesc);
		}
		else
		{
			Py_INCREF(EmptyPyPgTupleDesc);
			PyPgFunction_SetInput(rob, EmptyPyPgTupleDesc);
		}

		/*
		 * If it's a registered composite,
		 * PyPgType_FromOid will resolve that below.
		 */
		if (prorettype == RECORDOID)
		{
			/*
			 * Otherwise, build out a function result tupdesc.
			 */
			result_desc = build_function_result_tupdesc_t(ht);
			if (result_desc != NULL)
			{
				/*
				 * Anonymous composite returned by function.
				 */
				output = PyPgType_FromTupleDesc(result_desc);
				PyPgFunction_SetOutput(rob, output);
				FreeTupleDesc(result_desc);
				/*
				 * We will certainly be using it, so bless it right now iff
				 * it's *not* polymorphic.
				 */
				if (output && !PyPgType_IsPolymorphic(output))
					BlessTupleDesc(PyPgType_GetTupleDesc(output));
			}
			else
			{
				/*
				 * ew..
				 */
				goto lookup_output_type;
			}
		}
		else
		{
lookup_output_type:
			output = PyPgType_FromOid(prorettype);
			if (output == NULL)
				PyErr_RelayException();
			PyPgFunction_SetOutput(rob, output);
		}

		RELEASESYSCACHE(&ht);

		/*
		 * Don't worry *too* much about leaking memory.
		 */
		filename = format_procedure(fn_oid);
		Assert(filename != NULL);

		ht = SearchSysCache(NAMESPACEOID,
							PyPgFunction_GetNamespace(rob), 0, 0, 0);
		if (!HeapTupleIsValid(ht))
		{
			pfree((char *) filename);
			elog(ERROR, "function %u namespace %u does not exist",
						fn_oid, PyPgFunction_GetNamespace(rob));
		}

		ns = (Form_pg_namespace) GETSTRUCT(ht);
		nspname = pstrdup(NameStr(ns->nspname));
		RELEASESYSCACHE(&ht);

		/*
		 * Build the filename string.
		 */
		q_nspname = quote_identifier(nspname);

		nspname_str_ob = PyUnicode_FromCString(nspname);
		PyPgFunction_SetNamespaceName(rob, nspname_str_ob);
		if (nspname_str_ob == NULL)
		{
			/*
			 * Invalid encoded string?
			 */
			if (nspname != q_nspname)
				pfree((char *) q_nspname);
			pfree((char *) nspname);
			PyErr_RelayException();
		}

		q_nspname_str_ob = PyUnicode_FromCString(q_nspname);
		if (nspname != q_nspname)
			pfree((char *) q_nspname);
		pfree((char *) nspname);
		/*
		 * Ignore the potential exception for a moment.
		 */
		id_str_ob = PyUnicode_FromCString(filename);

		/*
		 * Skip the filename_str_ob if either of the above failed.
		 */
		if (id_str_ob != NULL && q_nspname_str_ob != NULL)
		{
			if (FunctionIsVisible(fn_oid))
				filename_str_ob = PyUnicode_FromFormat("%U.%U", q_nspname_str_ob, id_str_ob);
			else
			{
				filename_str_ob = id_str_ob;
				Py_INCREF(id_str_ob);
			}
		}
		PyPgFunction_SetFilename(rob, filename_str_ob);
		Py_XDECREF(q_nspname_str_ob);
		Py_XDECREF(id_str_ob);

		pfree((char *) filename);

		if (filename_str_ob == NULL)
			PyErr_RelayException();
	}
	PG_CATCH();
	{
		Py_XDECREF(rob);
		rob = NULL;

		PyErr_SetPgError(false);

		if (ht != NULL)
			ReleaseSysCache(ht);
	}
	PG_END_TRY();

	return(rob);
}

static PyObj
func_new(PyTypeObject *subtype, PyObj args, PyObj kw)
{
	char *words[] = {"oid", NULL};
	Oid fn_oid = InvalidOid;
	PyObj source = NULL, lo, so, rob;

	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyArg_ParseTupleAndKeywords(args, kw, "O", words, &source))
		return(NULL);

	if (Oid_FromPyObject(source, &fn_oid))
		return(NULL);

	lo = PyLong_FromUnsignedLong(fn_oid);
	if (lo == NULL)
		return(NULL);
	so = PyObject_Str(lo);
	if (so == NULL)
	{
		Py_DECREF(lo);
		return(NULL);
	}

	rob = func_new_from_oid(subtype, fn_oid, lo, so);
	Py_DECREF(lo);
	Py_DECREF(so);

	return(rob);
}

PyDoc_STRVAR(PyPgFunction_Type_Doc, "Postgres function interface type");

PyTypeObject PyPgFunction_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"Postgres.Function",					/* tp_name */
	sizeof(struct PyPgFunction),			/* tp_basicsize */
	0,										/* tp_itemsize */
	func_dealloc,							/* tp_dealloc */
	NULL,									/* tp_print */
	NULL,									/* tp_getattr */
	NULL,									/* tp_setattr */
	NULL,									/* tp_compare */
	NULL,									/* tp_repr */
	NULL,									/* tp_as_number */
	NULL,									/* tp_as_sequence */
	NULL,									/* tp_as_mapping */
	NULL,									/* tp_hash */
	func_call,								/* tp_call */
	NULL,									/* tp_str */
	NULL,									/* tp_getattro */
	NULL,									/* tp_setattro */
	NULL,									/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,						/* tp_flags */
	PyPgFunction_Type_Doc,					/* tp_doc */
	NULL,									/* tp_traverse */
	NULL,									/* tp_clear */
	NULL,									/* tp_richcompare */
	0,										/* tp_weaklistoffset */
	NULL,									/* tp_iter */
	NULL,									/* tp_iternext */
	PyPgFunction_Methods,					/* tp_methods */
	PyPgFunction_Members,					/* tp_members */
	PyPgFunction_GetSet,					/* tp_getset */
	NULL,									/* tp_base */
	NULL,									/* tp_dict */
	NULL,									/* tp_descr_get */
	NULL,									/* tp_descr_set */
	0,										/* tp_dictoffset */
	NULL,									/* tp_init */
	NULL,									/* tp_alloc */
	func_new,								/* tp_new */
};

PyObj
PyPgFunction_FromOid(Oid fn_oid)
{
	PyObj lo, so, rob;

	lo = PyLong_FromUnsignedLong(fn_oid);
	if (lo == NULL)
		return(NULL);

	so = PyObject_Str(lo);
	if (so == NULL)
	{
		Py_DECREF(lo);
		return(NULL);
	}

	rob = func_new_from_oid(&PyPgFunction_Type, fn_oid, lo, so);
	Py_DECREF(lo);
	Py_DECREF(so);

	return(rob);
}

/*
 * PyPgFunction_IsCurrent - determine if the current pg_proc entry is newer than 'func'
 */
bool
PyPgFunction_IsCurrent(PyObj func)
{
	HeapTuple ht;
	ItemPointerData fn_tid;
	TransactionId fn_xmin, last_fn_xmin;

	last_fn_xmin = PyPgFunction_GetXMin(func);
	if (last_fn_xmin == InvalidTransactionId)
	{
		/* pseudo-function */
		return(true);
	}

	ht = SearchSysCache(PROCOID, PyPgFunction_GetOid(func), 0, 0, 0);
	if (!HeapTupleIsValid(ht))
		return(false);

	fn_xmin = HeapTupleHeaderGetXmin(ht->t_data);
	fn_tid = ht->t_self;
	ReleaseSysCache(ht);

	if (last_fn_xmin != fn_xmin ||
		!ItemPointerEquals(PyPgFunction_GetItemPointer(func), &fn_tid))
	{
		return(false);
	}

	if (!PyPgTupleDesc_IsCurrent(PyPgFunction_GetInput(func)))
	{
		return(false);
	}

	if (!PyPgType_IsCurrent(PyPgFunction_GetOutput(func)))
		return(false);

	return(true);
}

/*
 * Remove the function module from sys.modules
 *
 * >>> del sys.modules[func.oidstr]
 *
 * -1 on error.
 */
int
PyPgFunction_RemoveModule(PyObj func)
{
	int rv;
	PyObj modules;

	modules = PyImport_GetModuleDict();

	rv = PySequence_Contains(modules, PyPgFunction_GetPyUnicodeOid(func));
	if (rv == 1)
		rv = PyObject_DelItem(modules, PyPgFunction_GetPyUnicodeOid(func));

	return(rv);
}
