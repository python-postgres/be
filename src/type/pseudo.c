/*
 * pseudo objects
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#include "postgres.h"
#include "access/heapam.h"
#include "access/hash.h"
#include "access/transam.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_namespace.h"
#include "lib/stringinfo.h"
#include "nodes/params.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "parser/parse_oper.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/catcache.h"
#include "utils/datum.h"
#include "utils/geo_decls.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/tuplestore.h"
#include "mb/pg_wchar.h"

#include "pypg/environment.h"
#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/pl.h"
#include "pypg/error.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/pseudo.h"

static PyObj
pseudo_new(PyTypeObject *subtype, PyObj args, PyObj kw)
{
	Py_INCREF((PyObj) subtype);
	return((PyObj) subtype);
}

PyDoc_STRVAR(PyPgPsuedo_Type_Doc, "Psuedo-types basetype");

PyPgTypeObject PyPgPseudo_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"pseudo",										/* tp_name */
	sizeof(struct PyPgObject),						/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	NULL,											/* tp_as_number */
	NULL,											/* tp_as_sequence */
	NULL,											/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|
	Py_TPFLAGS_BASETYPE,							/* tp_flags */
	PyPgPsuedo_Type_Doc,							/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	pseudo_new,										/* tp_new */
},
	PYPG_INIT_TYPINFO(invalid)
};

PyObj PyPg_void_object = NULL;
static PyObj
void_new(PyTypeObject *subtype, PyObj args, PyObj kw)
{
	static char *kwlist[] = {NULL};
	PyObj ob;

	Assert(subtype != NULL);

	if (!PyArg_ParseTupleAndKeywords(args, kw, "O", kwlist, &ob))
		return(NULL);

	Assert(ob != NULL);

	if (Py_TYPE(ob) == subtype)
	{
		Py_INCREF(ob);
		return(ob);
	}

	if (ob != Py_None)
	{
		PyErr_Format(PyExc_ValueError, "pseudo type '%s' requires None",
			subtype->tp_name);
		return(NULL);
	}

	if (PyPg_void_object == NULL)
	{
		PyPg_void_object = PyPgObject_New(subtype, 0);
	}

	Py_XINCREF(PyPg_void_object);
	return(PyPg_void_object);
}

PyDoc_STRVAR(PyPg_void_Type_Doc, "VOID singleton type");
#define void_new_datum invalid_new_datum
PyPgTypeObject PyPg_void_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"void",											/* tp_name */
	sizeof(struct PyPgObject),						/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	NULL,											/* tp_as_number */
	NULL,											/* tp_as_sequence */
	NULL,											/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,								/* tp_flags */
	PyPg_void_Type_Doc,								/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgPseudo_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	void_new,										/* tp_new */
},
	PYPG_INIT_TYPINFO(void)
};

static PyObj
trigger_new(PyTypeObject *subtype, PyObj args, PyObj kw)
{
	PyErr_SetString(PyExc_TypeError, "cannot instantiate TRIGGER types");
	return(NULL);
}

PyDoc_STRVAR(PyPg_trigger_Type_Doc, "un-instantiatable TRIGGER type");
#define trigger_new_datum invalid_new_datum
PyPgTypeObject PyPg_trigger_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"trigger",										/* tp_name */
	sizeof(struct PyPgObject),						/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	NULL,											/* tp_as_number */
	NULL,											/* tp_as_sequence */
	NULL,											/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,								/* tp_flags */
	PyPg_trigger_Type_Doc,							/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgPseudo_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	trigger_new,									/* tp_new */
},
	PYPG_INIT_TYPINFO(trigger)
};
