/*
 * bit types
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#include <bytes_methods.h>

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
#include "utils/builtins.h"
#if !(PG_VERSION_NUM < 80500)
#include "utils/bytea.h"
#endif
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/relcache.h"
#include "lib/stringinfo.h"

#include "pypg/environment.h"
#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/pl.h"
#include "pypg/error.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/bitwise.h"

static void
bool_new_datum(PyObj subtype, PyObj ob, int32 mod, Datum *out, bool *isnull)
{
	int r = PyObject_IsTrue(ob);
	if (r == -1)
		PyErr_RelayException();
	*out = r ? BoolGetDatum(true) : BoolGetDatum(false);
	*isnull = false;
}

PyDoc_STRVAR(PyPg_bool_Type_Doc, "pg_catalog.bool interface type");
PyPgTypeObject PyPg_bool_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"bool",											/* tp_name */
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
	PyPg_bool_Type_Doc,								/* tp_doc */
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
	PYPG_INIT_TYPINFO(bool)
};

#define get_bytea_length(DATUM) (VARSIZE(DATUM) - VARHDRSZ)

#ifdef NOT_USED
/*
 * Method implementations that create the return object.
 */
#define XMETH(NAME) \
static PyObj bytea_##NAME(PyObj self) \
{ \
	char *buf; Py_ssize_t len; \
	if (PyObject_AsReadBuffer(self, (const void **) &buf, &len)) \
		return(NULL); \
	return(_Py_bytes_##NAME(buf, len)); \
}
BYTES_RETURNS_OBJECT
#undef XMETH

/*
 * Method implementations that fill the pre-allocated results.
 */
#define XMETH(NAME) \
static PyObj bytea_##NAME(PyObj self) \
{ \
	volatile PyObj rob = NULL; \
	char *buf; Py_ssize_t len; \
	if (PyObject_AsReadBuffer(self, (const void **) &buf, &len)) \
		return(NULL); \
	PG_TRY(); \
	{ \
		char *var; \
		var = palloc(len+VARHDRSZ); \
		SET_VARSIZE(var, len + VARHDRSZ); \
		rob = PyPgObject_NEW(Py_TYPE(self)); \
		if (rob != NULL) \
		{ \
			PyPgObject_SetDatum(rob, PointerGetDatum(var)); \
			_Py_bytes_##NAME(VARDATA(var), buf, len); \
		} \
		else \
			pfree(var); \
	} \
	PG_CATCH(); \
	{ \
		Py_XDECREF(rob); \
		rob = NULL; \
		PyErr_SetPgError(false); \
	} \
	PG_END_TRY(); \
	return(rob); \
}
BYTES_FILLS_OBJECT
#undef XMETH

static PyMethodDef PyPg_bytea_Methods[] = {
#define XMETH(name) \
	{#name, (PyCFunction) bytea_##name, METH_NOARGS, _Py_##name##__doc__},
	BYTES_RETURNS_OBJECT
	BYTES_FILLS_OBJECT
#undef XMETH
	{NULL,}
};
#endif /* NOT_USED */

static PyMethodDef PyPg_bytea_Methods[] = {
	{NULL,}
};

static Py_ssize_t
bytea_length(PyObj self)
{
	Datum d;

	if (!PyPg_bytea_Check(self))
	{
		PyErr_SetString(PyExc_TypeError, "requires a bytea instance");
		return(-1);
	}
	d = PyPgObject_GetDatum(self);

	return((Py_ssize_t) get_bytea_length(d));
}

static PyObj
bytea_concat(PyObj self, PyObj seq)
{
	return(PyPgObject_Operate("||", self, seq));
}

static PyObj
bytea_item(PyObj self, Py_ssize_t item)
{
	volatile PyObj rob = NULL;
	Datum datum;
	int32 len = 0;

	if (!PyPg_bytea_Check(self))
	{
		PyErr_SetString(PyExc_TypeError, "requires a bytea instance");
		return(NULL);
	}

	datum = PyPgObject_GetDatum(self);

	if (item >= get_bytea_length(datum))
	{
		PyErr_Format(PyExc_IndexError,
			"index %d is out of range(%d)", item, len);
		return(NULL);
	}

	PG_TRY();
	{
		Datum rd;
		rd = (Datum) DatumGetByteaPSlice(datum, item, 1);
		rob = PyPgObject_New(Py_TYPE(self), rd);
		pfree(DatumGetPointer(rd));
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
bytea_slice(PyObj self, Py_ssize_t from, Py_ssize_t to)
{
	Datum datum;
	volatile PyObj rob = NULL;

	if (!PyPg_bytea_Check(self))
	{
		PyErr_SetString(PyExc_TypeError, "requires a bytea instance");
		return(NULL);
	}

	datum = PyPgObject_GetDatum(self);

	PG_TRY();
	{
		Datum rd;

		rd = PointerGetDatum(DatumGetByteaPSlice(datum, from, to - from));
		rob = PyPgObject_New(Py_TYPE(self), rd);
		pfree(DatumGetPointer(rd));
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

static PySequenceMethods bytea_as_sequence = {
	bytea_length,		/* sq_length */
	bytea_concat,		/* sq_concat */
	NULL,				/* sq_repeat */
	bytea_item,			/* sq_item */
	bytea_slice,		/* sq_slice */
	NULL,				/* sq_ass_item */
	NULL,				/* sq_ass_slice */
	NULL,				/* sq_contains */
	NULL,				/* sq_inplace_concat */
	NULL,				/* sq_inplace_repeat */
};

static PyNumberMethods bytea_as_number = {
	bytea_concat,	/* nb_add */
	NULL,
};

/*
 * buffer interfaces
 */
static int
bytea_getbuffer(PyObj self, Py_buffer *view, int flags)
{
	Datum d;
	char *data;
	Py_ssize_t len;

	if (!PyPg_bytea_Check(self))
	{
		PyErr_SetString(PyExc_TypeError, "requires a bytea instance");
		return(-1);
	}

	d = PyPgObject_GetDatum(self);
	data = VARDATA(d);
	len = get_bytea_length(d);

	return(PyBuffer_FillInfo(view, self, (void *) data, len, 1, flags));
}

static PyBufferProcs PyPg_bytea_Buffer = {
	bytea_getbuffer,
	NULL,
};

static PyObj
bytea_subscript(PyObj self, PyObj sub)
{
	volatile PyObj rob = NULL;
	char *data;
	Py_ssize_t len;

	if (PyObject_AsReadBuffer(self, (const void **) &data, &len))
		return(NULL);

	if (PySlice_Check(sub))
	{
		PySliceObject *slice = (PySliceObject *) sub;
		Py_ssize_t start, stop, step, slicelength;

		if (!PyPg_bytea_Check(self))
		{
			PyErr_SetString(PyExc_TypeError, "requires a bytea instance");
			return(NULL);
		}

		if (PySlice_GetIndicesEx(slice, len, &start, &stop, &step, &slicelength))
			return(NULL);

		PG_TRY();
		{
			char *rslice;
			Py_ssize_t offset, i;
			bytea *bytes;

			bytes = palloc(slicelength+VARHDRSZ);
			SET_VARSIZE(bytes, slicelength + VARHDRSZ);
			rslice = VARDATA(bytes);

			for (offset = start, i = 0; i < slicelength; offset = offset + step, ++i)
			{
				rslice[i] = data[offset];
			}

			rob = PyPgObject_New(Py_TYPE(self), PointerGetDatum(bytes));
			pfree(bytes);
		}
		PG_CATCH();
		{
			Py_XDECREF(rob);
			rob = NULL;
			PyErr_SetPgError(false);
		}
		PG_END_TRY();
	}
	else
	{
		Py_ssize_t i = PyNumber_AsSsize_t(sub, NULL);
		if (PyErr_Occurred())
			return(NULL);

		if (i < 0)
			i = i + len;
		/*
		 * _item does the type check
		 */
		rob = bytea_item(self, i);
	}

	return(rob);
}

static PyMappingMethods bytea_as_mapping = {
	bytea_length,
	bytea_subscript,
	NULL,
};

static void
bytea_new_datum(PyObj subtype, PyObj ob, int32 mod, Datum *out, bool *isnull)
{
	const void *data;
	Py_ssize_t len;
	bytea *bytes = NULL;

	/*
	 * If the object is a read buffer, it can be made into a bytea.
	 */
	if (PyObject_AsReadBuffer(ob, &data, &len))
		PyErr_RelayException();

	bytes = palloc(len+VARHDRSZ);
	SET_VARSIZE(bytes, len + VARHDRSZ);
	memcpy(VARDATA(bytes), (char *) data, len);

	*out = PointerGetDatum(bytes);
	*isnull = false;
}

PyDoc_STRVAR(PyPg_bytea_Type_Doc, "bytea interface type");
PyPgTypeObject PyPg_bytea_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"bytea",										/* tp_name */
	sizeof(struct PyPgObject),						/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	&bytea_as_number,								/* tp_as_number */
	&bytea_as_sequence,								/* tp_as_sequence */
	&bytea_as_mapping,								/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	&PyPg_bytea_Buffer,								/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|
	Py_TPFLAGS_BASETYPE,							/* tp_flags */
	PyPg_bytea_Type_Doc,							/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	PyPg_bytea_Methods,								/* tp_methods */
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
	PYPG_INIT_TYPINFO(bytea)
};

#define bit_new_datum NULL

PyDoc_STRVAR(PyPg_bit_Type_Doc, "bit interface type");
PyPgTypeObject PyPg_bit_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"bit",											/* tp_name */
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
	PyPg_bit_Type_Doc,								/* tp_doc */
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
	PYPG_INIT_TYPINFO(bit)
};

#define varbit_new_datum NULL

PyDoc_STRVAR(PyPg_varbit_Type_Doc, "varbit interface type");
PyPgTypeObject PyPg_varbit_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"varbit",										/* tp_name */
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
	PyPg_varbit_Type_Doc,							/* tp_doc */
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
	PYPG_INIT_TYPINFO(varbit)
};
