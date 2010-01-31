/*
 * stateful.c - Postgres.Stateful
 *
 * This object provides a means for the call state to be used by a given Python
 * FUNCTION. It does this using the generator's protocol.
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <compile.h>
#include <structmember.h>

#include "postgres.h"
#include "fmgr.h"
#include "access/tupdesc.h"
#include "utils/memutils.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/strings.h"
#include "pypg/externs.h"
#include "pypg/pl.h"
#include "pypg/stateful.h"

/*
 * Stateful.__call__
 */
static PyObj
stateful_call(PyObj self, PyObj args, PyObj kw)
{
	bool used_label = false;
	struct pl_exec_state *pl_exec;
	struct pl_fn_info *fn_info;
	PyObj source, state, rob = NULL;

	if (kw != NULL)
	{
		PyErr_SetString(PyExc_TypeError,
			"Postgres.Stateful does not accept keyword arguments");
	}

	pl_exec = PL_CONTEXT();
	if (pl_exec == NULL)
	{
		PyErr_SetString(PyExc_RuntimeError,
			"cannot call Stateful without execution context");
		return(NULL);
	}

	fn_info = pl_exec->fn_info;
	if (fn_info == NULL)
	{
		PyErr_SetString(PyExc_RuntimeError,
			"cannot call Stateful without execution context function");
		return(NULL);
	}

	if (fn_info->fi_func != PyPgStateful_GetFunction(self))
	{
		PyErr_SetString(PyExc_RuntimeError,
			"cannot call Stateful across execution contexts");
		return(NULL);
	}

	source = PyPgStateful_GetSource(self);

	/*
	 * This label is used in StopIteration cases where a new state object
	 * needs to be created.
	 */
getstate:
	if (fn_info->fi_state == NULL)
	{
		/*
		 * Need a state object.
		 */
		state = PyObject_CallObject(source, args);

		if (!PyIter_Check(state))
		{
			Py_DECREF(state);
			PyErr_SetString(PyExc_TypeError,
				"state source did not return an iterator");
			return(NULL);
		}

		rob = PyIter_Next(state);

		/*
		 * Requires at least one value.
		 */
		if (rob == NULL)
		{
			Py_DECREF(state);

			/*
			 * If no exception was set; the iterator was empty.
			 *
			 * This is explicitly prohibited.
			 */
			if (!PyErr_Occurred())
				PyErr_SetString(PyExc_RuntimeError, "empty state object");

			return(NULL);
		}
		else
		{
			/*
			 * Got the first return object.
			 */
			int r;
			PyObj meth;

			/*
			 * It may be wiser to use callmethod, but let's optim1ze!!
			 */
			meth = PyObject_GetAttr(state, send_str_ob);
			/*
			 * meth should be holding the reference now.
			 */
			Py_DECREF(state);

			if (meth == NULL)
			{
				Py_DECREF(rob);
				PyErr_SetString(PyExc_TypeError,
					"stateful source did not produce an object with a 'send' method");
				return(NULL);
			}

			r = PySet_Add(TransactionScope, meth);
			Py_DECREF(meth); /* TransactionScope has reference */
			if (r)
			{
				PyErr_SetString(PyExc_RuntimeError,
					"could not store state in transaction scope");
				return(NULL);
			}

			/*
			 * We now have state. =D
			 */
			fn_info->fi_state = meth;
		}
	}
	else
	{
		PyObj allargs;

		allargs = PyTuple_New(1);
		if (allargs == NULL)
		{
			PyErr_SetString(PyExc_RuntimeError,
				"could not create tuple for 'send' argument");
			return(NULL);
		}

		PyTuple_SET_ITEM(allargs, 0, args);
		Py_INCREF(args);

		rob = PyObject_CallObject(fn_info->fi_state, allargs);
		/*
		 * Don't DECREF args/allargs. If the exception is a StopIteration,
		 * it needs to goto getstate.
		 */

		if (rob == NULL)
		{
			if (PyErr_ExceptionMatches(PyExc_StopIteration) && used_label == false)
			{
				PySet_Discard(TransactionScope, fn_info->fi_state);
				fn_info->fi_state = NULL;

				PyErr_Clear();
				used_label = true;
				/*
				 * Don't need the 'allargs' tuple anymore, but 'args' is going
				 * to the main_ob call.
				 */
				Py_DECREF(allargs);
				goto getstate;
			}
		}

		/*
		 * Done; succesful return.
		 */
		Py_DECREF(allargs);
	}

	return(rob);
}

/*
 * Stateful.__new__
 */
static PyObj
stateful_new(PyTypeObject *typ, PyObj args, PyObj kw)
{
	char *words[] = {"source", NULL};
	struct pl_exec_state *pl_exec;
	struct pl_fn_info *fn_info;
	PyObj source, rob;

	if (!PyArg_ParseTupleAndKeywords(args, kw, "O", words, &source))
		return(NULL);

	pl_exec = PL_CONTEXT();
	if (pl_exec == NULL)
	{
		PyErr_SetString(PyExc_RuntimeError,
			"cannot create Stateful without execution context");
		return(NULL);
	}

	fn_info = pl_exec->fn_info;
	if (pl_exec->fn_info == NULL || fn_info->fi_func == NULL)
	{
		PyErr_SetString(PyExc_RuntimeError,
			"cannot create Stateful without execution context function");
		return(NULL);
	}

	rob = PyPgStateful_New();
	if (rob == NULL)
		return(rob);

	PyPgStateful_SetFunction(rob, fn_info->fi_func);
	Py_INCREF(fn_info->fi_func);

	PyPgStateful_SetSource(rob, source);
	Py_INCREF(source);

	return(rob);
}

static void
stateful_dealloc(PyObj self)
{
	PyObj ob;

	ob = PyPgStateful_GetSource(self);
	Py_DECREF(ob);

	ob = PyPgStateful_GetFunction(self);
	Py_DECREF(ob);

	self->ob_type->tp_free(self);
}

const char PyPgStateful_Doc[] = "Function [call] state manager";
PyTypeObject PyPgStateful_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"Postgres.Stateful",				/* tp_name */
	sizeof(struct PyPgStateful),		/* tp_basicsize */
	0,									/* tp_itemsize */
	stateful_dealloc,					/* tp_dealloc */
	NULL,								/* tp_print */
	NULL,								/* tp_getattr */
	NULL,								/* tp_setattr */
	NULL,								/* tp_compare */
	NULL,								/* tp_repr */
	NULL,								/* tp_as_number */
	NULL,								/* tp_as_sequence */
	NULL,								/* tp_as_mapping */
	NULL,								/* tp_hash */
	stateful_call,						/* tp_call */
	NULL,								/* tp_str */
	NULL,								/* tp_getattro */
	NULL,								/* tp_setattro */
	NULL,								/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,			  		/* tp_flags */
	PyPgStateful_Doc,					/* tp_doc */
	NULL,								/* tp_traverse */
	NULL,								/* tp_clear */
	NULL,								/* tp_richcompare */
	0,									/* tp_weaklistoffset */
	NULL,								/* tp_iter */
	NULL,								/* tp_iternext */
	NULL,								/* tp_methods */
	NULL,								/* tp_members */
	NULL,								/* tp_getset */
	NULL,								/* tp_base */
	NULL,								/* tp_dict */
	NULL,								/* tp_descr_get */
	NULL,								/* tp_descr_set */
	0,									/* tp_dictoffset */
	NULL,								/* tp_init */
	NULL,								/* tp_alloc */
	stateful_new,						/* tp_new */
};
