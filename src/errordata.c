/*
 * Postgres ErrorData interfaces
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
#include "pypg/errordata.h"
#include "pypg/error.h"

static PyObj
errdata_throw(PyObj self)
{
	MemoryContext former = CurrentMemoryContext;

	PG_TRY();
	{
		ReThrowError(PyPgErrorData_GetErrorData(self));
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	MemoryContextSwitchTo(former);

	return(NULL);
}

static PyMethodDef PyPgErrorData_Methods[] = {
	{"throw", (PyCFunction) errdata_throw, METH_NOARGS,
		PyDoc_STR("re-throw a Postgres error")},
	{NULL}
};

static PyObj
errdata_get_elevel(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);

	return(PyLong_FromLong(ed->elevel));
}

static PyObj
errdata_get_lineno(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);

	return(PyLong_FromLong(ed->lineno));
}

static PyObj
errdata_get_sqlerrcode(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);

	return(PyLong_FromLong(ed->lineno));
}

static PyObj
errdata_get_internalpos(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);

	return(PyLong_FromLong(ed->internalpos));
}

static PyObj
errdata_get_cursorpos(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);

	return(PyLong_FromLong(ed->cursorpos));
}

static PyObj
errdata_get_saved_errno(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);

	return(PyLong_FromLong(ed->saved_errno));
}

static PyObj
errdata_get_message(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);
	if (ed->message)
		return(PyUnicode_FromCString(ed->message));
	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObj
errdata_get_detail(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);
	if (ed->detail)
		return(PyUnicode_FromCString(ed->detail));
	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObj
errdata_get_detail_log(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);
	if (ed->detail)
		return(PyUnicode_FromCString(ed->detail_log));
	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObj
errdata_get_context(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);
	if (ed->context)
		return(PyUnicode_FromCString(ed->context));
	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObj
errdata_get_hint(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);
	if (ed->hint)
		return(PyUnicode_FromCString(ed->hint));
	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObj
errdata_get_domain(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);
	if (ed->domain)
		return(PyUnicode_FromCString(ed->domain));
	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObj
errdata_get_filename(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);
	if (ed->filename)
		return(PyUnicode_FromCString(ed->filename));
	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObj
errdata_get_funcname(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);
	if (ed->filename)
		return(PyUnicode_FromCString(ed->funcname));
	Py_INCREF(Py_None);
	return(Py_None);
}

/* conversions */

static PyObj
errdata_get_severity(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);

	/*
	 * Very unlikely that PyPgErrorData will contain anything other
	 * than an ERROR level, but support them all anyways.
	 */
	switch (ed->elevel)
	{
#define ELEVEL(ELNAME) case ELNAME: return(PyUnicode_FromString(#ELNAME)); break;
		_pg_elevels_()
#undef ELEVEL
	}

	/*
	 * Probably more useful than None...
	 */
	PyErr_Format(PyExc_ValueError, "unknown elevel '%d'", ed->elevel);
	return(NULL);
}

static PyObj
errdata_get_code(PyObj self, void *arg)
{
	ErrorData *ed = PyPgErrorData_GetErrorData(self);
	char code[6];
	int i, val;

	val = ed->sqlerrcode;
	for (i = 0; i < 5; i++)
	{
		code[i] = PGUNSIXBIT(val);
		val >>= 6;
	}
	code[5] = '\0';

	return(PyUnicode_FromString(code));
}


static PyGetSetDef PyPgErrorData_GetSet[] = {
	{"message", errdata_get_message, NULL, PyDoc_STR("get message field"), NULL},
	{"elevel", errdata_get_elevel, NULL, PyDoc_STR("get elevel field"), NULL},
	{"detail", errdata_get_detail, NULL, PyDoc_STR("get DETAIL field"), NULL},
	{"detail_log", errdata_get_detail_log, NULL, PyDoc_STR("get detail_log field"), NULL},
	{"context", errdata_get_context, NULL, PyDoc_STR("get CONTEXT field"), NULL},
	{"hint", errdata_get_hint, NULL, PyDoc_STR("get the HINT field"), NULL},
	{"domain", errdata_get_domain, NULL, PyDoc_STR("get domain field"), NULL},
	{"filename", errdata_get_filename, NULL, PyDoc_STR("get filename field"), NULL},
	{"line", errdata_get_lineno, NULL, PyDoc_STR("get lineno field"), NULL},
	{"lineno", errdata_get_lineno, NULL, PyDoc_STR("get lineno field"), NULL},
	{"function", errdata_get_funcname, NULL, PyDoc_STR("get funcname field"), NULL},
	{"funcname", errdata_get_funcname, NULL, PyDoc_STR("get funcname field"), NULL},
	{"sqlerrcode", errdata_get_sqlerrcode, NULL, PyDoc_STR("get the sqlerrcode field(encoded state)"), NULL},
	{"cursorpos", errdata_get_cursorpos, NULL, PyDoc_STR("get the cursorpos field"), NULL},
	{"internalpos", errdata_get_internalpos, NULL, PyDoc_STR("get the internalpos field"), NULL},
	{"saved_errno", errdata_get_saved_errno, NULL, PyDoc_STR("get the saved_errno field"), NULL},
	{"errno", errdata_get_saved_errno, NULL, PyDoc_STR("get the saved_errno field"), NULL},
/* conversions */
	{"severity", errdata_get_severity, NULL, PyDoc_STR("get the severity of the error(elevel derived)"), NULL},
	{"code", errdata_get_code, NULL, PyDoc_STR("get the SQL state code(decoded characters)"), NULL},
/* aliases */
	{"position", errdata_get_cursorpos, NULL, PyDoc_STR("get the cursorpos field"), NULL},
	{"internal_position", errdata_get_internalpos, NULL, PyDoc_STR("get the internalpos field"), NULL},
	{NULL,}
};

static void
errdata_dealloc(PyObj self)
{
	ErrorData *ed;
	ed = PyPgErrorData_GetErrorData(self);
	PyPgErrorData_SetErrorData(self, NULL);

	if (ed != NULL)
	{
		MemoryContext former = CurrentMemoryContext;

		PG_TRY();
		{
			if (ed != NULL)
				FreeErrorData(ed);

			/*
			 * When PLPY_STRANGE_THINGS is defined.
			 */
			RaiseAStrangeError
		}
		PG_CATCH();
		{
			/*
			 * Don't bother using PyErr_EmitPgErrorAsWarning() in
			 * fear of this happening again.
			 */
			FlushErrorState();
			elog(WARNING, "failed to deallocate ErrorData object");
		}
		PG_END_TRY();
		MemoryContextSwitchTo(former);
	}

	Py_TYPE(self)->tp_free(self);
}

const char PyPgErrorData_Doc[] = "Postgres ErrorData interface";
PyTypeObject PyPgErrorData_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"Postgres.ErrorData",						/* tp_name */
	sizeof(struct PyPgErrorData),				/* tp_basicsize */
	0,											/* tp_itemsize */
	errdata_dealloc,							/* tp_dealloc */
	NULL,										/* tp_print */
	NULL,										/* tp_getattr */
	NULL,										/* tp_setattr */
	NULL,										/* tp_compare */
	NULL,										/* tp_repr */
	NULL,										/* tp_as_number */
	NULL,										/* tp_as_sequence */
	NULL,										/* tp_as_mapping */
	NULL,										/* tp_hash */
	NULL,										/* tp_call */
	NULL,										/* tp_str */
	NULL,										/* tp_getattro */
	NULL,										/* tp_setattro */
	NULL,										/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,					  		/* tp_flags */
	PyPgErrorData_Doc,							/* tp_doc */
	NULL,										/* tp_traverse */
	NULL,										/* tp_clear */
	NULL,										/* tp_richcompare */
	0,											/* tp_weaklistoffset */
	NULL,										/* tp_iter */
	NULL,										/* tp_iternext */
	PyPgErrorData_Methods,						/* tp_methods */
	NULL,										/* tp_members */
	PyPgErrorData_GetSet,						/* tp_getset */
	NULL,										/* tp_base */
	NULL,										/* tp_dict */
	NULL,										/* tp_descr_get */
	NULL,										/* tp_descr_set */
	0,											/* tp_dictoffset */
	NULL,										/* tp_init */
	NULL,										/* tp_alloc */
	NULL,										/* tp_new */
};

PyObj
PyPgErrorData_Initialize(PyObj self, ErrorData *ed)
{
	PyPgErrorData_SetErrorData(self, ed);
	return(self);
}

PyObj
PyPgErrorData_FromCurrent(void)
{
	bool raised = false;
	PyObj rob;
	MemoryContext former;

	rob = PyPgErrorData_NEW();
	if (rob == NULL)
		return(NULL);
	PyPgErrorData_SetErrorData(rob, NULL);

	/*
	 * It's a Python object now, so into PMC it goes.
	 */
	former = MemoryContextSwitchTo(PythonMemoryContext);
	PG_TRY();
	{
		PyPgErrorData_SetErrorData(rob, CopyErrorData());
		FlushErrorState();
	}
	PG_CATCH();
	{
		/*
		 * Avoid calling PyErr_SetPgError() in fear of this happening again.
		 */
		FlushErrorState();
		PyErr_SetString(PyExc_RuntimeError,
			"failed to CopyErrorData for Python exception");
	}
	PG_END_TRY();
	MemoryContextSwitchTo(former);

	if (raised)
	{
		Py_DECREF(rob);
		rob = NULL;
	}

	return(rob);
}
