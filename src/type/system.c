/*
 * system types
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#include "postgres.h"
#include "fmgr.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "access/transam.h"
#include "access/xact.h"
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

#include "pypg/environment.h"
#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/array.h"
#include "pypg/type/system.h"

static int
oid_bool(PyObj self)
{
	return(OidIsValid(DatumGetObjectId(PyPgObject_GetDatum(self))) ? 1 : 0);
}

static PyObj
oid_long(PyObj self)
{
	PyObj rob;
	rob = PyLong_FromUnsignedLong((unsigned long) DatumGetObjectId(PyPgObject_GetDatum(self)));
	return(rob);
}

static PyObj
oid_float(PyObj self)
{
	PyObj rob;
	Oid objid;
	objid = DatumGetObjectId(PyPgObject_GetDatum(self));
	rob = PyFloat_FromDouble((double) objid);
	return(rob);
}

static PyNumberMethods oid_as_number = {
	NULL,					/* nb_add */
	NULL,					/* nb_subtract */
	NULL,					/* nb_multiply */
	NULL,					/* nb_remainder */
	NULL,					/* nb_divmod */
	NULL,					/* nb_power */
	NULL,					/* nb_negative */
	NULL,					/* nb_positive */
	NULL,					/* nb_absolute */
	oid_bool,				/* nb_bool */
	NULL,					/* nb_invert */
	NULL,					/* nb_lshift */
	NULL,					/* nb_rshift */
	NULL,					/* nb_and */
	NULL,					/* nb_xor */
	NULL,					/* nb_or */
	oid_long,				/* nb_long */
	NULL,					/* nb_reserved */
	oid_float,				/* nb_float */
	NULL,
};

#define oid_new_datum NULL
PyDoc_STRVAR(PyPg_oid_Type_Doc, "Object identifier interface type");
PyPgTypeObject PyPg_oid_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"oid",											/* tp_name */
	sizeof(struct PyPgObject),						/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	&oid_as_number,									/* tp_as_number */
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
	PyPg_oid_Type_Doc,								/* tp_doc */
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
	PYPG_INIT_TYPINFO(oid)
};

static int
cid_bool(PyObj self)
{
	return(1);
}

static PyObj
cid_int(PyObj self)
{
	CommandId cid;
	PyObj rob;
	cid = DatumGetCommandId(PyPgObject_GetDatum(self));
	rob = PyLong_FromLong((long) cid);
	return(rob);
}

static PyObj
cid_float(PyObj self)
{
	PyObj rob;
	CommandId cid;
	cid = DatumGetCommandId(PyPgObject_GetDatum(self));
	rob = PyFloat_FromDouble((double) cid);
	return(rob);
}

static PyNumberMethods cid_as_number = {
	NULL,					/* nb_add */
	NULL,					/* nb_subtract */
	NULL,					/* nb_multiply */
	NULL,					/* nb_remainder */
	NULL,					/* nb_divmod */
	NULL,					/* nb_power */
	NULL,					/* nb_negative */
	NULL,					/* nb_positive */
	NULL,					/* nb_absolute */
	cid_bool,				/* nb_bool */
	NULL,					/* nb_invert */
	NULL,					/* nb_lshift */
	NULL,					/* nb_rshift */
	NULL,					/* nb_and */
	NULL,					/* nb_xor */
	NULL,					/* nb_or */
	cid_int,				/* nb_int */
	NULL,					/* nb_reserved */
	cid_float,				/* nb_float */

	NULL,
};

#define cid_new_datum NULL
PyDoc_STRVAR(PyPg_cid_Type_Doc, "Command identifier interface type");
PyPgTypeObject PyPg_cid_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"cid",											/* tp_name */
	sizeof(struct PyPgObject),						/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	&cid_as_number,									/* tp_as_number */
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
	PyPg_cid_Type_Doc,								/* tp_doc */
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
	PYPG_INIT_TYPINFO(cid)
};

static int
xid_bool(PyObj self)
{
	TransactionId xid = DatumGetTransactionId(PyPgObject_GetDatum(self));
	return(TransactionIdIsValid(xid) ? 1 : 0);
}

static PyObj
xid_int(PyObj self)
{
	PyObj rob;
	TransactionId xid = DatumGetTransactionId(PyPgObject_GetDatum(self));
	rob = PyLong_FromUnsignedLong((unsigned long) xid);
	return(rob);
}

static PyNumberMethods xid_as_number = {
	NULL,					/* nb_add */
	NULL,					/* nb_subtract */
	NULL,					/* nb_multiply */
	NULL,					/* nb_remainder */
	NULL,					/* nb_divmod */
	NULL,					/* nb_power */
	NULL,					/* nb_negative */
	NULL,					/* nb_positive */
	NULL,					/* nb_absolute */
	xid_bool,				/* nb_bool */
	NULL,					/* nb_invert */
	NULL,					/* nb_lshift */
	NULL,					/* nb_rshift */
	NULL,					/* nb_and */
	NULL,					/* nb_xor */
	NULL,					/* nb_or */
	xid_int,				/* nb_int */
	NULL,					/* nb_reserved */
	NULL,					/* nb_float */
	NULL,
};

static PyObj
xid_current(PyObj typ)
{
	PyObj rob;
	Datum d = TransactionIdGetDatum(GetCurrentTransactionId());
	rob = PyPgObject_New(typ, d);
	return(rob);
}

static PyObj
xid_current_sub(PyObj typ)
{
	PyObj rob;
	Datum d = TransactionIdGetDatum(GetCurrentSubTransactionId());
	rob = PyPgObject_New(typ, d);
	return(rob);
}

static PyMethodDef PyPg_xid_Methods[] = {
	{"current", (PyCFunction) xid_current, METH_CLASS|METH_NOARGS,
	PyDoc_STR("create an xid instance using the current xid")},
	{"current_sub", (PyCFunction) xid_current_sub, METH_CLASS|METH_NOARGS,
	PyDoc_STR("create an xid instance using the current sub-xid")},
	{NULL,}
};

#define xid_new_datum NULL
PyDoc_STRVAR(PyPg_xid_Type_Doc, "Transaction identifier interface type");
PyPgTypeObject PyPg_xid_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"xid",											/* tp_name */
	sizeof(struct PyPgObject),						/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	&xid_as_number,									/* tp_as_number */
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
	PyPg_xid_Type_Doc,								/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	PyPg_xid_Methods,								/* tp_methods */
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
	PYPG_INIT_TYPINFO(xid)
};

#define tid_new_datum NULL
PyDoc_STRVAR(PyPg_tid_Type_Doc, "TID interface type");
PyPgTypeObject PyPg_tid_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"tid",											/* tp_name */
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
	PyPg_tid_Type_Doc,								/* tp_doc */
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
	PYPG_INIT_TYPINFO(tid)
};

#define aclitem_new_datum NULL
PyDoc_STRVAR(PyPg_aclitem_Type_Doc, "ACL Item interface type");
PyPgTypeObject PyPg_aclitem_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"aclitem",										/* tp_name */
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
	PyPg_aclitem_Type_Doc,							/* tp_doc */
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
	PYPG_INIT_TYPINFO(aclitem)
};

#define refcursor_new_datum NULL
PyDoc_STRVAR(PyPg_refcursor_Type_Doc, "Refcursor interface type");
PyPgTypeObject PyPg_refcursor_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"refcursor",									/* tp_name */
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
	PyPg_refcursor_Type_Doc,						/* tp_doc */
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
	PYPG_INIT_TYPINFO(refcursor)
};

#define regprocedure_new_datum NULL
PyDoc_STRVAR(PyPg_regprocedure_Type_Doc, "regprocedure interface type");
PyPgTypeObject PyPg_regprocedure_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"regprocedure",									/* tp_name */
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
	PyPg_regprocedure_Type_Doc,						/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPg_oid_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(regprocedure)
};

#define regproc_new_datum NULL
PyDoc_STRVAR(PyPg_regproc_Type_Doc, "regproc interface type");
PyPgTypeObject PyPg_regproc_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"regproc",										/* tp_name */
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
	PyPg_regproc_Type_Doc,							/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPg_oid_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(regproc)
};


#define regclass_new_datum NULL
PyDoc_STRVAR(PyPg_regclass_Type_Doc, "regclass interface type");
PyPgTypeObject PyPg_regclass_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"regclass",										/* tp_name */
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
	PyPg_regclass_Type_Doc,							/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPg_oid_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(regclass)
};


#define regtype_new_datum NULL
PyDoc_STRVAR(PyPg_regtype_Type_Doc, "regtype interface type");
PyPgTypeObject PyPg_regtype_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"regtype",										/* tp_name */
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
	PyPg_regtype_Type_Doc,							/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPg_oid_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(regtype)
};

#define regoper_new_datum NULL
PyDoc_STRVAR(PyPg_regoper_Type_Doc, "regoper interface type");
PyPgTypeObject PyPg_regoper_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"regoper",										/* tp_name */
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
	PyPg_regoper_Type_Doc,							/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPg_oid_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(regoper)
};

#define regoperator_new_datum NULL
PyDoc_STRVAR(PyPg_regoperator_Type_Doc, "regoperator interface type");
PyPgTypeObject PyPg_regoperator_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"regoperator",									/* tp_name */
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
	PyPg_regoperator_Type_Doc,						/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPg_oid_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(regoperator)
};

#define oidvector_new_datum NULL
PyDoc_STRVAR(PyPg_oidvector_Type_Doc, "oidvector interface type");
PyPgTypeObject PyPg_oidvector_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"oidvector",									/* tp_name */
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
	PyPg_oidvector_Type_Doc,						/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgArray_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(oidvector)
};

#define int2vector_new_datum NULL
PyDoc_STRVAR(PyPg_int2vector_Type_Doc, "int2vector interface type");
PyPgTypeObject PyPg_int2vector_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"int2vector",									/* tp_name */
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
	PyPg_int2vector_Type_Doc,						/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	NULL,											/* tp_getset */
	(PyTypeObject *) &PyPgArray_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(int2vector)
};
