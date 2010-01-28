/*
 * string types
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "postgres.h"
#include "fmgr.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "access/transam.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opclass.h"
#include "catalog/namespace.h"
#include "storage/block.h"
#include "storage/off.h"
#include "nodes/params.h"
#include "parser/parse_func.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/relcache.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/pl.h"
#include "pypg/error.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/string.h"

static PyObj
string_add(PyObj self, PyObj with)
{
	return(PyPgObject_Operate("||", self, with));
}

static PyNumberMethods string_as_number = {
	string_add,		/* nb_add */
	NULL,
};

PyDoc_STRVAR(PyPgString_Type_Doc, "strings basetype");
#define STRING_new_datum NULL
PyPgTypeObject PyPgString_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.String",								/* tp_name */
	sizeof(struct PyPgObject),						/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	&string_as_number,								/* tp_as_number */
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
	PyPgString_Type_Doc,							/* tp_doc */
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
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(STRING)
};

#define name_new_datum NULL

PyDoc_STRVAR(PyPg_name_Type_Doc, "A fixed size character sequence type");
PyPgTypeObject PyPg_name_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.name",							/* tp_name */
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
	PyPg_name_Type_Doc,								/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgString_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
}, 
	PYPG_INIT_TYPINFO(name)
};

static Py_ssize_t
text_length(PyObj self)
{
	volatile Datum d;
	d = PyPgObject_GetDatum(self);

	PG_TRY();
	{
		d = DirectFunctionCall1(textlen, d);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		return(-1);
	}
	PG_END_TRY();

	return((Py_ssize_t) DatumGetInt32(d));
}

static PyObj
text_repeat(PyObj self, Py_ssize_t ntimes)
{
	volatile Datum rd = 0;
	volatile PyObj rob = NULL;

	PG_TRY();
	{
		rd = DirectFunctionCall2(repeat,
			PyPgObject_GetDatum(self), Int32GetDatum((int32) ntimes));

		rob = PyPgObject_New(Py_TYPE(self), rd);
		if (rd != PyPgObject_GetDatum(self))
			pfree(DatumGetPointer(rd));
	}
	PG_CATCH();
	{
		Py_XDECREF(rob);
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();

	return(rob);
}

static PySequenceMethods text_as_sequence = {
	text_length,			/* sq_length */
	string_add,				/* sq_concat */
	text_repeat,			/* sq_repeat */
	NULL,					/* sq_item */
	NULL,					/* sq_slice */
	NULL,					/* sq_ass_item */
	NULL,					/* sq_ass_slice */
	NULL,					/* sq_contains */
	NULL,					/* sq_inplace_concat */
	NULL,					/* sq_inplace_repeat */
};

static PyObj
text_multiply(PyObj self, PyObj n)
{
	Py_ssize_t s;

	s = PyNumber_AsSsize_t(n, NULL);
	return(text_repeat(self, s));
}

/*
 * repeat is being masked by ObjectAsNumber's multiply.
 */
static PyNumberMethods text_as_number = {
	NULL,				/* nb_add */
	NULL,				/* nb_subtract */
	text_multiply,		/* nb_multiply */
	NULL,				/* nb_remainder */
	NULL,				/* nb_divmod */
	NULL,				/* nb_power */
	NULL,				/* nb_negative */
	NULL,				/* nb_positive */
	NULL,				/* nb_absolute */
	NULL,				/* nb_bool */
	NULL,				/* nb_invert */
	NULL,				/* nb_lshift */
	NULL,				/* nb_rshift */
	NULL,				/* nb_and */
	NULL,				/* nb_xor */
	NULL,				/* nb_or */
	NULL,				/* nb_int */
	NULL,				/* nb_reserved */
	NULL,				/* nb_float */

	NULL,				/* nb_inplace_add */
	NULL,				/* nb_inplace_subtract */
	NULL,				/* nb_inplace_multiply */
	NULL,				/* nb_inplace_remainder */
	NULL,				/* nb_inplace_power */
	NULL,				/* nb_inplace_lshift */
	NULL,				/* nb_inplace_rshift */
	NULL,				/* nb_inplace_and */
	NULL,				/* nb_inplace_xor */
	NULL,				/* nb_inplace_or */

	NULL,				/* nb_floor_divide */
	NULL,				/* nb_true_divide */
	NULL,				/* nb_inplace_floor_divide */
	NULL,				/* nb_inplace_true_divide */

	NULL				/* nb_index */
};

/*
 * typinput works nicely.
 */
#define text_new_datum NULL

PyDoc_STRVAR(PyPg_text_Type_Doc, "A variable size character sequence type");
PyPgTypeObject PyPg_text_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.text",							/* tp_name */
	sizeof(struct PyPgObject),						/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	&text_as_number,								/* tp_as_number */
	&text_as_sequence,								/* tp_as_sequence */
	NULL,											/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|
	Py_TPFLAGS_BASETYPE,							/* tp_flags */
	PyPg_text_Type_Doc,								/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgString_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(text)
};


#define char_new_datum NULL

PyDoc_STRVAR(PyPg_char_Type_Doc, "A single [small] character");
PyPgTypeObject PyPg_char_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.char",							/* tp_name */
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
	PyPg_char_Type_Doc,								/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgString_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(char)
};


#define bpchar_new_datum NULL

PyDoc_STRVAR(PyPg_bpchar_Type_Doc, "characters");
PyPgTypeObject PyPg_bpchar_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.bpchar",						/* tp_name */
	sizeof(struct PyPgObject),						/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	NULL,											/* tp_as_number */
	&text_as_sequence,								/* tp_as_sequence */
	NULL,											/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|
	Py_TPFLAGS_BASETYPE,							/* tp_flags */
	PyPg_bpchar_Type_Doc,							/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgString_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(bpchar)
};


#define varchar_new_datum NULL

PyDoc_STRVAR(PyPg_varchar_Type_Doc, "varchar type interface");
PyPgTypeObject PyPg_varchar_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.varchar",						/* tp_name */
	sizeof(struct PyPgObject),						/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	NULL,											/* tp_as_number */
	&text_as_sequence,								/* tp_as_sequence */
	NULL,											/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|
	Py_TPFLAGS_BASETYPE,							/* tp_flags */
	PyPg_varchar_Type_Doc,							/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgString_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(varchar)
};

#define unknown_new_datum NULL
PyDoc_STRVAR(PyPg_unknown_Type_Doc,
	"An \"unknown\" variable character sequence"
);
PyPgTypeObject PyPg_unknown_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.unknown",						/* tp_name */
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
	PyPg_unknown_Type_Doc,							/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgString_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(unknown)
};

#define cstring_new_datum NULL
PyDoc_STRVAR(PyPg_cstring_Type_Doc,
	"A zero terminated variable character sequence"
);
PyPgTypeObject PyPg_cstring_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.cstring",						/* tp_name */
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
	PyPg_cstring_Type_Doc,							/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgString_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(cstring)
};
