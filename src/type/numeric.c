/*
 * number types
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
#include "pypg/extension.h"
#include "pypg/error.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/numeric.h"

static PyObj
int2_int(PyObj self)
{
	PyObj rob;
	rob = PyLong_FromLong((long) DatumGetInt16(PyPgObject_GetDatum(self)));
	return(rob);
}

static PyObj
int2_float(PyObj self)
{
	PyObj rob;
	int2 src;
	src = DatumGetInt16(PyPgObject_GetDatum(self));
	rob = PyFloat_FromDouble((double) src);
	return(rob);
}

static PyNumberMethods int2_as_number = {
	NULL,					/* nb_add */
	NULL,					/* nb_subtract */
	NULL,					/* nb_multiply */
	NULL,					/* nb_remainder */
	NULL,					/* nb_divmod */
	NULL,					/* nb_power */
	NULL,					/* nb_negative */
	NULL,					/* nb_positive */
	NULL,					/* nb_absolute */
	NULL,					/* nb_bool */
	NULL,					/* nb_invert */
	NULL,					/* nb_lshift */
	NULL,					/* nb_rshift */
	NULL,					/* nb_and */
	NULL,					/* nb_xor */
	NULL,					/* nb_or */
	int2_int,				/* nb_int */
	NULL,					/* nb_reserved */
	int2_float,				/* nb_float */

	NULL,
};

static void
int2_new_datum(PyObj subtype, PyObj ob, int32 mod, Datum *out, bool *isnull)
{
	long the_int;
	int16 i16out;
	PyObj intob;

	intob = PyNumber_Long(ob);
	if (intob == NULL)
		PyErr_RelayException();

	the_int = PyLong_AsLong(intob);
	Py_DECREF(intob);
	if (PyErr_Occurred())
		PyErr_RelayException();

	if (the_int < -0x8000 || the_int > 0x7FFF)
	{
		ereport(ERROR,(
			errmsg("Python object overflows int2")
		));
	}

	i16out = (int16) the_int;

	*out = Int16GetDatum(i16out);
	*isnull = false;
}

PyDoc_STRVAR(PyPg_int2_Type_Doc, "int2 interface type");
PyPgTypeObject PyPg_int2_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.int2",					/* tp_name */
	sizeof(struct PyPgObject),				/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	&int2_as_number,							/* tp_as_number */
	NULL,											/* tp_as_sequence */
	NULL,											/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|
	Py_TPFLAGS_BASETYPE,						/* tp_flags */
	PyPg_int2_Type_Doc,						/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,	/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
}, 
	PYPG_INIT_TYPINFO(int2)
};

static PyObj
int4_int(PyObj self)
{
	PyObj rob;
	rob = PyLong_FromLong((long) DatumGetInt32(PyPgObject_GetDatum(self)));
	return(rob);
}

static PyObj
int4_float(PyObj self)
{
	PyObj rob;
	int4 src;
	src = DatumGetInt32(PyPgObject_GetDatum(self));
	rob = PyFloat_FromDouble((double) src);
	return(rob);
}

static PyNumberMethods int4_as_number = {
	NULL,					/* nb_add */
	NULL,					/* nb_subtract */
	NULL,					/* nb_multiply */
	NULL,					/* nb_remainder */
	NULL,					/* nb_divmod */
	NULL,					/* nb_power */
	NULL,					/* nb_negative */
	NULL,					/* nb_positive */
	NULL,					/* nb_absolute */
	NULL,					/* nb_bool */
	NULL,					/* nb_invert */
	NULL,					/* nb_lshift */
	NULL,					/* nb_rshift */
	NULL,					/* nb_and */
	NULL,					/* nb_xor */
	NULL,					/* nb_or */
	int4_int,				/* nb_int */
	NULL,					/* nb_reserved */
	int4_float,				/* nb_float */

	NULL,
};

static void
int4_new_datum(PyObj subtype, PyObj ob, int32 mod, Datum *out, bool *isnull)
{
	long the_int;
	PyObj intob;

	intob = PyNumber_Long(ob);
	if (intob == NULL)
		PyErr_RelayException();

	the_int = PyLong_AsLong(intob);
	Py_DECREF(intob);
	if (PyErr_Occurred())
		PyErr_RelayException();

#if SIZEOF_LONG > 4
	/*
	 * hooray for C!
	 */
	if (the_int < -INT64CONST(0x80000000) || the_int > INT64CONST(0x7FFFFFFF))
	{
		PyErr_SetString(PyExc_OverflowError,
		            "Python long too large to convert to int4");
		PyErr_RelayException();
	}
#endif

	*out = Int32GetDatum((int32) the_int);
	*isnull = false;
}

PyDoc_STRVAR(PyPg_int4_Type_Doc, "pg_catalog.int4 interface type");
PyPgTypeObject PyPg_int4_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.int4",					/* tp_name */
	sizeof(struct PyPgObject),				/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	&int4_as_number,							/* tp_as_number */
	NULL,											/* tp_as_sequence */
	NULL,											/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|
	Py_TPFLAGS_BASETYPE,						/* tp_flags */
	PyPg_int4_Type_Doc,						/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,	/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
}, 
	PYPG_INIT_TYPINFO(int4)
};

static PyObj
int8_int(PyObj self)
{
	int64 ll;
	ll = DatumGetInt64(PyPgObject_GetDatum(self));
	return(PyLong_FromLongLong(ll));
}

static PyObj
int8_float(PyObj self)
{
	int64 ll;
	ll = DatumGetInt64(PyPgObject_GetDatum(self));
	return(PyFloat_FromDouble((double) ll));
}

static PyNumberMethods int8_as_number = {
	NULL,					/* nb_add */
	NULL,					/* nb_subtract */
	NULL,					/* nb_multiply */
	NULL,					/* nb_remainder */
	NULL,					/* nb_divmod */
	NULL,					/* nb_power */
	NULL,					/* nb_negative */
	NULL,					/* nb_positive */
	NULL,					/* nb_absolute */
	NULL,					/* nb_bool */
	NULL,					/* nb_invert */
	NULL,					/* nb_lshift */
	NULL,					/* nb_rshift */
	NULL,					/* nb_and */
	NULL,					/* nb_xor */
	NULL,					/* nb_or */
	int8_int,			/* nb_int */
	NULL,					/* nb_reserved */
	int8_float,			/* nb_float */

	NULL,
};

static void
int8_new_datum(PyObj subtype, PyObj ob, int32 mod, Datum *out, bool *isnull)
{
	int64 the_int;
	PyObj intob;

	intob = PyNumber_Long(ob);
	if (intob == NULL)
		PyErr_RelayException();

	the_int = (int64) PyLong_AsLongLong(intob);
	Py_DECREF(intob);
	if (PyErr_Occurred())
		PyErr_RelayException();

	*out = Int64GetDatum(the_int);
	*isnull = false;
}

PyDoc_STRVAR(PyPg_int8_Type_Doc, "pg_catalog.int8 interface type");
PyPgTypeObject PyPg_int8_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.int8",					/* tp_name */
	sizeof(struct PyPgObject),				/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	&int8_as_number,							/* tp_as_number */
	NULL,											/* tp_as_sequence */
	NULL,											/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|
	Py_TPFLAGS_BASETYPE,						/* tp_flags */
	PyPg_int8_Type_Doc,						/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,	/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
}, 
	PYPG_INIT_TYPINFO(int8)
};

static PyObj
float4_float(PyObj self)
{
	float4 src;
	src = DatumGetFloat4(PyPgObject_GetDatum(self));
	return(PyFloat_FromDouble((double) src));
}

static PyNumberMethods float4_as_number = {
	NULL,					/* nb_add */
	NULL,					/* nb_subtract */
	NULL,					/* nb_multiply */
	NULL,					/* nb_remainder */
	NULL,					/* nb_divmod */
	NULL,					/* nb_power */
	NULL,					/* nb_negative */
	NULL,					/* nb_positive */
	NULL,					/* nb_absolute */
	NULL,					/* nb_bool */
	NULL,					/* nb_invert */
	NULL,					/* nb_lshift */
	NULL,					/* nb_rshift */
	NULL,					/* nb_and */
	NULL,					/* nb_xor */
	NULL,					/* nb_or */
	NULL,					/* nb_int */
	NULL,					/* nb_reserved */
	float4_float,		/* nb_float */

	NULL,
};

static void
float4_new_datum(PyObj subtype, PyObj ob, int32 mod, Datum *out, bool *isnull)
{
	double the_float;
	PyObj fob;

	fob = PyNumber_Float(ob);
	if (fob == NULL)
		PyErr_RelayException();

	the_float = PyFloat_AsDouble(fob);
	Py_DECREF(fob);
	if (PyErr_Occurred())
		PyErr_RelayException();

	*out = Float4GetDatum((float) the_float);
	*isnull = false;
}

PyDoc_STRVAR(PyPg_float4_Type_Doc, "float4 interface type");
PyPgTypeObject PyPg_float4_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.float4",				/* tp_name */
	sizeof(struct PyPgObject),				/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	&float4_as_number,						/* tp_as_number */
	NULL,											/* tp_as_sequence */
	NULL,											/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|
	Py_TPFLAGS_BASETYPE,						/* tp_flags */
	PyPg_float4_Type_Doc,					/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,	/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
}, 
	PYPG_INIT_TYPINFO(float4)
};

static PyObj
float8_float(PyObj self)
{
	double x;
	x = DatumGetFloat8(PyPgObject_GetDatum(self));
	return(PyFloat_FromDouble(x));
}

static PyNumberMethods float8_as_number = {
	NULL,					/* nb_add */
	NULL,					/* nb_subtract */
	NULL,					/* nb_multiply */
	NULL,					/* nb_remainder */
	NULL,					/* nb_divmod */
	NULL,					/* nb_power */
	NULL,					/* nb_negative */
	NULL,					/* nb_positive */
	NULL,					/* nb_absolute */
	NULL,					/* nb_bool */
	NULL,					/* nb_invert */
	NULL,					/* nb_lshift */
	NULL,					/* nb_rshift */
	NULL,					/* nb_and */
	NULL,					/* nb_xor */
	NULL,					/* nb_or */
	NULL,					/* nb_int */
	NULL,					/* nb_reserved */
	float8_float,		/* nb_float */
	NULL,
};

static void
float8_new_datum(PyObj subtype, PyObj ob, int32 mod, Datum *out, bool *isnull)
{
	double the_float;
	PyObj fob;

	fob = PyNumber_Float(ob);
	if (fob == NULL)
		PyErr_RelayException();

	the_float = PyFloat_AsDouble(fob);
	Py_DECREF(fob);
	if (PyErr_Occurred())
		PyErr_RelayException();

	*out = Float8GetDatum(the_float);
	*isnull = false;
}

PyDoc_STRVAR(PyPg_float8_Type_Doc, "float8 interface type");

PyPgTypeObject PyPg_float8_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.float8",				/* tp_name */
	sizeof(struct PyPgObject),				/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	&float8_as_number,						/* tp_as_number */
	NULL,											/* tp_as_sequence */
	NULL,											/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|
	Py_TPFLAGS_BASETYPE,						/* tp_flags */
	PyPg_float8_Type_Doc,					/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,	/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
}, 
	PYPG_INIT_TYPINFO(float8)
};


#define numeric_new_datum NULL

PyDoc_STRVAR(PyPg_numeric_Type_Doc, "arbitrary precision number");
PyPgTypeObject PyPg_numeric_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.numeric",				/* tp_name */
	sizeof(struct PyPgObject),				/* tp_basicsize */
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
	Py_TPFLAGS_BASETYPE,						/* tp_flags */
	PyPg_numeric_Type_Doc,					/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,	/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(numeric)
};

#define cash_new_datum NULL
PyDoc_STRVAR(PyPg_cash_Type_Doc, "Monetary quantity");
PyPgTypeObject PyPg_cash_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.cash",					/* tp_name */
	sizeof(struct PyPgObject),				/* tp_basicsize */
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
	Py_TPFLAGS_BASETYPE,						/* tp_flags */
	PyPg_cash_Type_Doc,						/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,	/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(cash)
};
