/*
 * abstract object
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
#include "parser/parse_coerce.h"
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

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/extension.h"
#include "pypg/error.h"
#include "pypg/tupledesc.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/bitwise.h"
#include "pypg/type/record.h"

static PyMemberDef PyPgObject_Members[] = {
	{"datum", T_DATUM,
	offsetof(struct PyPgObject, pg_datum), READONLY,
	PyDoc_STR("The PyPgObject's datum as a PyLong object")},
	{NULL}
};

/*
 * Python only supports prefix unary operators.
 */
static PyObj
unary_operate(const char *op, PyObj right)
{
	PyObj rob = NULL;
	Datum dright = PyPgObject_GetDatum(right);
	Oid right_oid = PyPgType_GetOid(Py_TYPE(right));

	Py_ALLOCATE_OWNER();
	{
		PyObj rtype;
		Operator opt;
		volatile Datum rd = 0;
		List * volatile namelist = NULL;

		PG_TRY();
		{
			struct FmgrInfo flinfo = {0,};
			struct FunctionCallInfoData fcinfo = {0,};
			Form_pg_operator ops;
			Oid declared, result_type, fn_oid;

			namelist = stringToQualifiedNameList(op);
			opt = oper(NULL, (List *) namelist, InvalidOid, right_oid, false, 1);

			ops = (Form_pg_operator) GETSTRUCT(opt);
			fn_oid = ops->oprcode;
			declared = ops->oprright;
			result_type = ops->oprresult;
			ReleaseSysCache((HeapTuple) opt);

			result_type = enforce_generic_type_consistency(
					&right_oid, &declared, 1, result_type, true);

			rtype = PyPgType_FromOid(result_type);
			Py_XACQUIRE(rtype);

			list_free((List *) namelist);
			namelist = NULL;

			if (rtype == NULL)
				elog(ERROR, "operator result type could not be created");

			fmgr_info(fn_oid, &flinfo);

			fcinfo.flinfo = &flinfo;
			fcinfo.nargs = 1;

			fcinfo.arg[0] = dright;
			fcinfo.argnull[0] = false;

			rd = FunctionCallInvoke(&fcinfo);
			if (fcinfo.isnull)
			{
				rob = Py_None;
				Py_INCREF(rob);
				Py_ACQUIRE(rob);
			}
			else
			{
				rob = PyPgObject_New(rtype, rd);
				Py_XACQUIRE(rob);
				if (PyPgType_ShouldFree(rtype))
					pfree(DatumGetPointer(rd));
			}
		}
		PG_CATCH();
		{
			PyErr_SetPgError(false);
			rob = NULL;
		}
		PG_END_TRY();

		Py_XINCREF(rob);
	}
	Py_DEALLOCATE_OWNER();

	return(rob);
}

static PyObj
binary_operate(const char *op, PyObj left, PyObj right)
{
	PyObj base = PyPgObject_Check(left) ? left : right;
	PyObj rob = NULL;
	Datum dleft, dright;
	Datum dcoerce;
	bool lisnull = false, risnull = false, coerce_isnull = true;
	Oid left_oid, right_oid;

	Py_ALLOCATE_OWNER();
	{
		volatile Datum rd = 0;
		List * volatile namelist = NULL;
		PyObj rtype;
		PyObj coerce = NULL;

		PG_TRY();
		{
			struct FmgrInfo flinfo = {0,};
			struct FunctionCallInfoData fcinfo = {0,};
			Operator opt;
			Form_pg_operator ops;
			Oid actual[2];
			Oid declared[2];
			Oid result_type, fn_oid;

			/*
			 * base and coerce are used to manage preliminary coercion.
			 * If either side of the operator is not a PyPgObject, convert the
			 * object to the type of the other side.
			 */

			if (base == left)
			{
				if (!PyPgObject_Check(right))
					coerce = right;
			}
			else
				coerce = left;

			if (coerce != NULL)
			{
				PyPgType_DatumNew((PyObj) Py_TYPE(base),
					coerce, -1, &dcoerce, &coerce_isnull);

				if (base == left)
				{
					dleft = PyPgObject_GetDatum(left);
					lisnull = false;
					dright = dcoerce;
					risnull = coerce_isnull;
				}
				else
				{
					dleft = dcoerce;
					lisnull = coerce_isnull;
					dright = PyPgObject_GetDatum(right);
					risnull = false;
				}

				/*
				 * Both are the same type as base due to coercion.
				 */
				left_oid = right_oid = PyPgType_GetOid(Py_TYPE(base));
			}
			else
			{
				/*
				 * Both objects are PyPgObjects.
				 */
				dleft = PyPgObject_GetDatum(left);
				left_oid = PyPgType_GetOid(Py_TYPE(left));
				dright = PyPgObject_GetDatum(right);
				right_oid = PyPgType_GetOid(Py_TYPE(right));
			}

			namelist = stringToQualifiedNameList(op);

			opt = oper(NULL, (List *) namelist, left_oid, right_oid, false, 1);
			ops = (Form_pg_operator) GETSTRUCT(opt);
			fn_oid = ops->oprcode;
			declared[0] = ops->oprleft;
			declared[1] = ops->oprright;
 			actual[0] = left_oid;
			actual[1] = right_oid;
			result_type = ops->oprresult;
			ReleaseSysCache((HeapTuple) opt);

			result_type = enforce_generic_type_consistency(
					actual, declared, 2, result_type, true);

			rtype = PyPgType_FromOid(result_type);
			rtype = Py_XACQUIRE(rtype);

			list_free((List *) namelist);
			namelist = NULL;

			if (rtype == NULL)
				PyErr_RelayException();

			fmgr_info(fn_oid, &flinfo);

			fcinfo.flinfo = &flinfo;
			fcinfo.nargs = 2;

			fcinfo.arg[0] = dleft;
			fcinfo.argnull[0] = lisnull;
			fcinfo.arg[1] = dright;
			fcinfo.argnull[1] = risnull;

			rd = FunctionCallInvoke(&fcinfo);
			if (fcinfo.isnull)
				rob = Py_None;
			else
			{
				rob = PyPgObject_New(rtype, rd);
				Py_XACQUIRE(rob);
				if (PyPgType_ShouldFree(rtype))
					pfree(DatumGetPointer(rd));
			}

			if (!coerce_isnull && PyPgType_ShouldFree(Py_TYPE(base)))
				pfree(DatumGetPointer(dcoerce));
		}
		PG_CATCH();
		{
			PyErr_SetPgError(false);
			rob = NULL;
		}
		PG_END_TRY();

		Py_XINCREF(rob);
	}
	Py_DEALLOCATE_OWNER();

	return(rob);
}

PyObj
PyPgObject_Operate(const char *op, PyObj fst, PyObj snd)
{
	PyObj rob;

	MemoryContext former;
	if (DB_IS_NOT_READY())
		return(NULL);

	former = CurrentMemoryContext;
	if (snd == NULL)
		rob = unary_operate(op, fst);
	else
		rob = binary_operate(op, fst, snd);
	MemoryContextSwitchTo(former);

	return(rob);
}

static PyObj
obj_add(PyObj self, PyObj with)
{
	return(PyPgObject_Operate("+", self, with));
}

static PyObj
obj_subtract(PyObj self, PyObj with)
{
	return(PyPgObject_Operate("-", self, with));
}

static PyObj
obj_multiply(PyObj self, PyObj with)
{
	return(PyPgObject_Operate("*", self, with));
}

static PyObj
obj_remainder(PyObj self, PyObj with)
{
	return(PyPgObject_Operate("%", self, with));
}

static PyObj
obj_divide(PyObj self, PyObj with)
{
	return(PyPgObject_Operate("/", self, with));
}

static PyObj
obj_divmod(PyObj self, PyObj with)
{
	PyObj div, mod, rob;

	rob = PyTuple_New(2);
	if (rob == NULL)
		return(NULL);

	div = PyPgObject_Operate("/", self, with);
	if (div == NULL)
		goto error;
	PyTuple_SET_ITEM(rob, 0, div);
	mod = PyPgObject_Operate("%", self, with);
	if (mod == NULL)
		goto error;
	PyTuple_SET_ITEM(rob, 1, mod);

	return(rob);
error:
	Py_DECREF(rob);
	return(NULL);
}

static PyObj
obj_power(PyObj self, PyObj with, PyObj andthis)
{
	return(PyPgObject_Operate("^", self, with));
}

static PyObj
obj_negative(PyObj self)
{
	return(PyPgObject_Operate("-", self, NULL));
}

static PyObj
obj_positive(PyObj self)
{
	return(PyPgObject_Operate("+", self, NULL));
}

/*
 * XXX: If type methods ever come along, hopefully this will be implemented
 * using a more generalized function.
 */
static PyObj
obj_absolute(PyObj self)
{
	MemoryContext former;
	Oid typoid;
	volatile PyObj rob = NULL;

	if (DB_IS_NOT_READY() || PyPgObjectType_Require(Py_TYPE(self)))
		return(NULL);

	typoid = PyPgType_GetOid(Py_TYPE(self));

	former = CurrentMemoryContext;
	PG_TRY();
	{
		HeapTuple procTuple;
		Datum rd = 0;
		Oid procoid, roid;
		List *qnl;

		qnl = stringToQualifiedNameList("abs");
		procoid = LookupFuncName(qnl, 1, &(typoid), true);
		list_free(qnl);
		if (procoid == InvalidOid)
		{
			PyErr_Format(PyExc_LookupError,
					"no such function named 'abs' for type %u", typoid);
			return(NULL);
		}
		procTuple = SearchSysCache(PROCOID, procoid, 0, 0, 0);
		if (procTuple == NULL)
		{
			PyErr_Format(PyExc_LookupError,
					"no procedure with Oid %u", procoid);
			return(NULL);
		}
		roid = ((Form_pg_proc) GETSTRUCT(procTuple))->prorettype;
		ReleaseSysCache(procTuple);

		rd = OidFunctionCall1(procoid, PyPgObject_GetDatum(self));

		rob = PyPgObject_FromTypeOidAndDatum(roid, rd);
		if (PyPgType_ShouldFree(Py_TYPE(rob)))
		{
			/*
			 * That's our datum...
			 */
			if (PyPgObject_GetDatum(self) != rd)
				pfree(DatumGetPointer(rd));
		}
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

static int
obj_bool(PyObj self)
{
	int r;
	/*
	 * Use self's Datum if it's a bool object.
	 * Otherwise, cast to a bool.
	 */
	if (PyPg_bool_Check(self))
		r = DatumGetBool(PyPgObject_GetDatum(self)) ? 1 : 0;
	else
	{
		Datum d;
		bool isnull;

		PG_TRY();
		{
			PyPgType_typcast((PyObj) &PyPg_bool_Type, self, -1, &d, &isnull);
			if (isnull)
				r = 0;
			else
				r = DatumGetBool(d) ? 1 : 0;
		}
		PG_CATCH();
		{
			r = -1;
			PyErr_SetPgError(false);
		}
		PG_END_TRY();
	}

	return(r);
}

static PyObj
obj_int(PyObj self)
{
	PyObj s, i;

	/*
	 * Statically allocated types are encouraged to optimize this.
	 */
	s = PyObject_Str(self);
	if (s == NULL)
		return(NULL);
	i = PyNumber_Long(s);
	Py_DECREF(s);

	return(i);
}

static PyObj
obj_invert(PyObj self)
{
	return(PyPgObject_Operate("~", self, NULL));
}

static PyObj
obj_lshift(PyObj self, PyObj with)
{
	return(PyPgObject_Operate("<<", self, with));
}

static PyObj
obj_rshift(PyObj self, PyObj with)
{
	return(PyPgObject_Operate(">>", self, with));
}

static PyObj
obj_and(PyObj self, PyObj with)
{
	return(PyPgObject_Operate("&", self, with));
}

static PyObj
obj_xor(PyObj self, PyObj with)
{
	return(PyPgObject_Operate("#", self, with));
}

static PyObj
obj_or(PyObj self, PyObj with)
{
	return(PyPgObject_Operate("|", self, with));
}

static PyNumberMethods PyPgObjectAsNumber = {
	obj_add,		/* nb_add */
	obj_subtract,	/* nb_subtract */
	obj_multiply,	/* nb_multiply */
	obj_remainder,	/* nb_remainder */
	obj_divmod,		/* nb_divmod */
	obj_power,		/* nb_power */
	obj_negative,	/* nb_negative */
	obj_positive,	/* nb_positive */
	obj_absolute,	/* nb_absolute */
	obj_bool,		/* nb_bool */
	obj_invert,		/* nb_invert */
	obj_lshift,		/* nb_lshift */
	obj_rshift,		/* nb_rshift */
	obj_and,		/* nb_and */
	obj_xor,		/* nb_xor */
	obj_or,			/* nb_or */
	obj_int,		/* nb_int */
	NULL,			/* nb_reserved */
	NULL,			/* nb_float */

	NULL,			/* nb_inplace_add */
	NULL,			/* nb_inplace_subtract */
	NULL,			/* nb_inplace_multiply */
	NULL,			/* nb_inplace_remainder */
	NULL,			/* nb_inplace_power */
	NULL,			/* nb_inplace_lshift */
	NULL,			/* nb_inplace_rshift */
	NULL,			/* nb_inplace_and */
	NULL,			/* nb_inplace_xor */
	NULL,			/* nb_inplace_or */

	NULL,			/* nb_floor_divide */
	obj_divide,		/* nb_true_divide */
	NULL,			/* nb_inplace_floor_divide */
	NULL,			/* nb_inplace_true_divide */

	obj_int			/* nb_index */
};

static long
obj_hash(PyObj self)
{
	PyPgTypeInfo typinfo;
	Datum ob_datum = PyPgObject_GetDatum(self);
	long rv = 0;

	typinfo = PyPgTypeInfo(Py_TYPE(self));
	if (typinfo->typbyval)
	{
		rv = ((long) ob_datum);
	}
	else if (typinfo->typlen > -1 && typinfo->typlen <= sizeof(long))
	{
		rv = (*((long *) DatumGetPointer(ob_datum)));
	}
	else
	{
		int len;
		switch(typinfo->typlen)
		{
			case -2:
				len = strlen((char *) DatumGetPointer(ob_datum));
			break;

			case -1:
				len = VARSIZE(ob_datum) - VARHDRSZ;
				ob_datum = PointerGetDatum(VARDATA(ob_datum));
			break;

			default:
				len = (int) typinfo->typlen;
			break;
		}
		rv = hash_any((unsigned char *) DatumGetPointer(ob_datum), len);
	}

	return(rv);
}

static PyObj
obj_richcompare(PyObj self, PyObj with, int comparison)
{
	char *op = NULL;
	PyObj boolob, rob;

	if (with == Py_None)
	{
		Py_INCREF(Py_NotImplemented);
		return(Py_NotImplemented);
	}

	switch (comparison)
	{
		case Py_LT:
			op = "<";
		break;

		case Py_LE:
			op = "<=";
		break;

		case Py_EQ:
			op = "=";
		break;

		case Py_NE:
			op = "<>";
		break;

		case Py_GT:
			op = ">";
		break;

		case Py_GE:
			op = ">=";
		break;

		default:
			Py_INCREF(Py_NotImplemented);
			return(Py_NotImplemented);
		break;
	}

	boolob = PyPgObject_Operate(op, self, with);
	if (boolob == NULL)
		return(NULL);
	if (obj_bool(boolob))
		rob = Py_True;
	else
		rob = Py_False;

	Py_DECREF(boolob);
	Py_INCREF(rob);
	return(rob);
}

static void
obj_dealloc(PyObj self)
{
	Datum d = PyPgObject_GetDatum(self);

	PyPgObject_SetDatum(self, 0);
	if (PyPgType_ShouldFree(Py_TYPE(self)))
	{
		if (PointerIsValid(DatumGetPointer(d)))
		{
			MemoryContext former = CurrentMemoryContext;
			PG_TRY();
			{
				pfree(DatumGetPointer(d));

				/*
				 * When PLPY_STRANGE_THINGS is defined.
				 */
				RaiseAStrangeError
			}
			PG_CATCH();
			{
				PyErr_EmitPgErrorAsWarning("failed to deallocate Datum");
			}
			PG_END_TRY();
			MemoryContextSwitchTo(former);
		}
	}

	Py_TYPE(self)->tp_free(self);
}

static PyObj
obj_str(PyObj self)
{
	return(PyPgType_typoutput((PyObj) Py_TYPE(self), self));
}

static PyObj
obj_repr(PyObj self)
{
	PyObj typname, strpart, rob;

	strpart = PyPgType_typoutput((PyObj) Py_TYPE(self), self);
	if (strpart == NULL)
		return(NULL);
	typname = PyPgType_GetTypeName(Py_TYPE(self));

	Assert(typname != NULL);
	rob = PyUnicode_FromFormat("%U(%R)", typname, strpart);
	Py_DECREF(strpart);

	return(rob);
}

static PyObj
obj_new(PyTypeObject *subtype, PyObj args, PyObj kw)
{
	static char *words[] = {"source", "mod", NULL};
	PyObj src = NULL, mod = NULL, rob = NULL, typmodin_ob = NULL;
	Datum d = 0;
	bool isnull = true;
	int32 typmod = -1;

	if (DB_IS_NOT_READY())
		return(NULL);

	/*
	 * The type *must* be a PyPgType instance.
	 * Use CheckExact for speed.
	 * Subclassing PyPgType_Type is not supported.
	 */
	if (PyPgObjectType_Require(subtype))
		return(NULL);

	/*
	 * Grab a single "source" argument and an
	 * optional "mod" from the args and kw.
	 */
	if (!PyArg_ParseTupleAndKeywords(args, kw, "O|O", words, &src, &mod))
		return(NULL);

	if (mod != NULL && mod != Py_None)
	{
		PyObj lo;

		if (!PyList_CheckExact(mod))
		{
			lo = Py_Call((PyObj) &PyList_Type, mod);
			if (lo == NULL)
			{
				PyErr_SetString(PyExc_ValueError, "'mod' keyword must be a sequence");
				return(NULL);
			}
		}
		else
		{
			Py_INCREF(mod);
			lo = mod;
		}

		typmodin_ob = Py_Call(PyPg_cstring_Array_Type, lo);
		Py_DECREF(lo);
		if (typmodin_ob == NULL)
		{
			PyErr_SetString(PyExc_ValueError, "invalid typmod object");
			return(NULL);
		}
	}
	else if (Py_TYPE(src) == subtype)
	{
		/*
		 * Exact type and no typmod.
		 */
		rob = src;
		Py_INCREF(rob);
		return(rob);
	}

	PG_TRY();
	{
		if (typmodin_ob != NULL)
		{
			typmod = PyPgType_modin((PyObj) subtype, typmodin_ob);
		}

		if (src == Py_None)
		{
			d = 0;
			isnull = true;
		}
		else
		{
			PyPgType_DatumNew((PyObj) subtype, src, typmod, &d, &isnull);
		}

		if (isnull)
		{
			rob = Py_None;
			Py_INCREF(rob);
		}
		else
		{
			rob = PyPgObject_New(subtype, d);
			if (PyPgType_ShouldFree(subtype))
			{
				Datum fd = d;
				d = 0;
				isnull = true;
				/*
				 * If it fails to pfree, don't try it again in
				 * the catch.
				 */
				pfree(DatumGetPointer(fd));
			}
		}
	}
	PG_CATCH();
	{
		Py_XDECREF(rob);
		rob = NULL;

		if (!isnull && PyPgType_ShouldFree(subtype))
			pfree(DatumGetPointer(d));

		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	Py_XDECREF(typmodin_ob);

	return(rob);
}

PyDoc_STRVAR(PyPgObject_Type_Doc, "Postgres object interface type");
PyPgTypeObject PyPgObject_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.Object",						/* tp_name */
	sizeof(struct PyPgObject),				/* tp_basicsize */
	0,										/* tp_itemsize */
	obj_dealloc,							/* tp_dealloc */
	NULL,									/* tp_print */
	NULL,									/* tp_getattr */
	NULL,									/* tp_setattr */
	NULL,									/* tp_reserved */
	obj_repr,								/* tp_repr */
	&PyPgObjectAsNumber,					/* tp_as_number */
	NULL,									/* tp_as_sequence */
	NULL,									/* tp_as_mapping */
	obj_hash,								/* tp_hash */
	NULL,									/* tp_call */
	obj_str,								/* tp_str */
	NULL,									/* tp_getattro */
	NULL,									/* tp_setattro */
	NULL,									/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|
	Py_TPFLAGS_BASETYPE,					/* tp_flags */
	PyPgObject_Type_Doc,					/* tp_doc */
	NULL,									/* tp_traverse */
	NULL,									/* tp_clear */
	obj_richcompare,						/* tp_richcompare */
	0,										/* tp_weaklistoffset */
	NULL,									/* tp_iter */
	NULL,									/* tp_iternext */
	NULL,									/* tp_methods */
	PyPgObject_Members,						/* tp_members */
	NULL,									/* tp_getset */
	NULL,									/* tp_base */
	NULL,									/* tp_dict */
	NULL,									/* tp_descr_get */
	NULL,									/* tp_descr_set */
	0,										/* tp_dictoffset */
	NULL,									/* tp_init */
	NULL,									/* tp_alloc */
	obj_new,								/* tp_new */
	NULL,									/* tp_free */
},
	PYPG_INIT_TYPINFO(invalid)
};

PyObj
PyPgObject_Initialize(PyObj self, Datum d)
{
	PyPgTypeInfo typinfo = PyPgTypeInfo(Py_TYPE(self));
	MemoryContext former = CurrentMemoryContext;

	MemoryContextSwitchTo(PythonMemoryContext);
	d = Py_datumCopy(d, typinfo->typbyval, typinfo->typlen);
	MemoryContextSwitchTo(former);

	if (!typinfo->typbyval && !PointerIsValid(DatumGetPointer(d)))
	{
		Py_DECREF(self);
		return(NULL);
	}

	PyPgObject_SetDatum(self, d);
	return(self);
}

PyObj
PyPgObject_FromTypeOidAndDatum(Oid typeoid, Datum d)
{
	PyObj typ, rob;

	typ = PyPgType_FromOid(typeoid);
	if (typ == NULL)
		return(NULL);
	rob = PyPgObject_New(typ, d);
	Py_DECREF(typ);

	return(rob);
}

PyObj
PyPgObject_FromTypeOidAndPyObject(Oid typeoid, PyObj ob)
{
	PyObj typ, args, rob;

	typ = PyPgType_FromOid(typeoid);
	if (typ == NULL)
		return(NULL);
	args = PyTuple_New(1);
	PyTuple_SET_ITEM(args, 0, ob);
	Py_INCREF(ob);
	rob = PyObject_CallObject(typ, args);
	Py_DECREF(args);
	Py_DECREF(typ);

	return(rob);
}
