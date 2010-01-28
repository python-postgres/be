/*
 * Postgres subtransactions
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
#include "catalog/pg_type.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/pl.h"
#include "pypg/error.h"
#include "pypg/ist.h"
#include "pypg/xact.h"


static PyObj
xact_enter(PyObj self)
{
	if (PyPgTransaction_GetId(self) != 0)
	{
		PyErr_SetString(PyExc_RuntimeError, "Postgres.Transaction already used");
		return(NULL);
	}
	if (!pl_ist_begin(PyPgTransaction_GetState(self)))
		return(NULL);
	PyPgTransaction_SetId(self, pl_ist_count);
	PyPgTransaction_SetState(self, pl_ist_open);

	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObj
xact_exit(PyObj self, PyObj args)
{
	PyObj typ = NULL, val = NULL, tb = NULL;
	unsigned long xid;
	char state;

	if (!PyArg_ParseTuple(args, "OOO", &typ, &val, &tb))
		return(NULL);

	xid = PyPgTransaction_GetId(self);
	if (PyPgTransaction_GetId(self) == 0)
	{
		PyErr_SetString(PyExc_RuntimeError, "Postgres.Transaction not entered");
		return(NULL);
	}

	state = PyPgTransaction_GetState(self);

	if (typ == Py_None)
	{
		/*
		 * No Python exception, attempt commit.
		 */
		if (!pl_ist_commit(xid, state))
			return(NULL);
		PyPgTransaction_SetState(self, pl_ist_committed);
	}
	else
	{
		/* Exception occurred, abort. */
		if (!pl_ist_abort(xid, state))
			return(NULL);
		PyPgTransaction_SetState(self, pl_ist_aborted);
	}

	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObj
xact_context(PyObj self)
{
	Py_INCREF(self);
	return(self);
}

static PyMethodDef PyPgTransaction_Methods[] = {
	{"__enter__", (PyCFunction) xact_enter, METH_NOARGS,
		PyDoc_STR("start a subtransaction in a with block")},
	{"__exit__", (PyCFunction) xact_exit, METH_VARARGS,
		PyDoc_STR("commit or rollback a substraction")},
	{"__context__", (PyCFunction) xact_context, METH_NOARGS,
		PyDoc_STR("return the transaction object")},
	{NULL}
};

static PyObj
xact_new(PyTypeObject *subtype, PyObj args, PyObj kw)
{
	static char *words[] = {NULL};

	if (!PyArg_ParseTupleAndKeywords(args, kw, "", words))
		return(NULL);
	return(PyPgTransaction_NEW((PyTypeObject *) subtype));
}

const char PyPgTransaction_Doc[] = "Postgres Internal Subtransaction interface";
PyTypeObject PyPgTransaction_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"Postgres.Transaction",				/* tp_name */
	sizeof(struct PyPgTransaction),		/* tp_basicsize */
	0,									/* tp_itemsize */
	NULL,								/* tp_dealloc */
	NULL,								/* tp_print */
	NULL,								/* tp_getattr */
	NULL,								/* tp_setattr */
	NULL,								/* tp_compare */
	NULL,								/* tp_repr */
	NULL,								/* tp_as_number */
	NULL,								/* tp_as_sequence */
	NULL,								/* tp_as_mapping */
	NULL,								/* tp_hash */
	NULL,								/* tp_call */
	NULL,								/* tp_str */
	NULL,								/* tp_getattro */
	NULL,								/* tp_setattro */
	NULL,								/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,			  		/* tp_flags */
	PyPgTransaction_Doc,				/* tp_doc */
	NULL,								/* tp_traverse */
	NULL,								/* tp_clear */
	NULL,								/* tp_richcompare */
	0,									/* tp_weaklistoffset */
	NULL,								/* tp_iter */
	NULL,								/* tp_iternext */
	PyPgTransaction_Methods,			/* tp_methods */
	NULL,								/* tp_members */
	NULL,								/* tp_getset */
	NULL,								/* tp_base */
	NULL,								/* tp_dict */
	NULL,								/* tp_descr_get */
	NULL,								/* tp_descr_set */
	0,									/* tp_dictoffset */
	NULL,								/* tp_init */
	NULL,								/* tp_alloc */
	xact_new,							/* tp_new */
};

PyObj
PyPgTransaction_NEW(PyTypeObject *subtype)
{
	PyObj rob;

	rob = subtype->tp_alloc(subtype, 0);
	if (rob != NULL)
	{
		PyPgTransaction_SetState(rob, pl_ist_new);
		PyPgTransaction_SetId(rob, 0);
	}

	return(rob);
}
