/*
 * TupleDesc type
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#include <abstract.h>

#include "postgres.h"
#include "tcop/tcopprot.h"
#include "access/attnum.h"
#include "access/htup.h"
#include "access/hio.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/typcache.h"
#include "utils/tuplestore.h"
#include "mb/pg_wchar.h"

/*
 * Needed to expose the varlen attributes.
 */
#include "catalog/pg_attribute.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/pl.h"
#include "pypg/extension.h"
#include "pypg/error.h"
#include "pypg/tupledesc.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/record.h"

/*
 * Used to form the pg_attribute tuple for tupd_item().
 */
#define GetNULL(ARG) PointerGetDatum(NULL)
#define GetNAME(ARG) PointerGetDatum(&ARG)
/*
 * XXX: Is there a better way to get the pg_attribute fields? :(
 * (This is for instantiating a pg_attribute instance by __getitem__)
 */

#if PG_VERSION_NUM >= 90200
#warning 9.2 and greater
#define FormData_pg_attribute_Fields(...) \
	FIELD(attrelid, 1, ObjectIdGetDatum) \
	FIELD(attname, 2, GetNAME) \
	FIELD(atttypid, 3, ObjectIdGetDatum) \
	FIELD(attstattarget, 4, Int32GetDatum) \
	FIELD(attlen, 5, Int16GetDatum) \
	FIELD(attnum, 6, Int16GetDatum) \
	FIELD(attndims, 7, Int32GetDatum) \
	FIELD(attcacheoff, 8, Int32GetDatum) \
	FIELD(atttypmod, 9, Int32GetDatum) \
	FIELD(attbyval, 10, BoolGetDatum) \
	FIELD(attstorage, 11, CharGetDatum) \
	FIELD(attalign, 12, CharGetDatum) \
	FIELD(attnotnull, 13, BoolGetDatum) \
	FIELD(atthasdef, 14, BoolGetDatum) \
	FIELD(attisdropped, 15, BoolGetDatum) \
	FIELD(attislocal, 16, BoolGetDatum) \
	FIELD(attinhcount, 17, Int32GetDatum) \
	FIELD(attcollation, 18, ObjectIdGetDatum) \
	FIELD(attacl, 19, GetNULL) \
	FIELD(attoptions, 20, GetNULL) \
	FIELD(attfdwoptions, 21, GetNULL) \
	__VA_ARGS__

#elif PG_VERSION_NUM >= 90100
#warning 9.1
#define FormData_pg_attribute_Fields(...) \
	FIELD(attrelid, 1, ObjectIdGetDatum) \
	FIELD(attname, 2, GetNAME) \
	FIELD(atttypid, 3, ObjectIdGetDatum) \
	FIELD(attstattarget, 4, Int32GetDatum) \
	FIELD(attlen, 5, Int16GetDatum) \
	FIELD(attnum, 6, Int16GetDatum) \
	FIELD(attndims, 7, Int32GetDatum) \
	FIELD(attcacheoff, 8, Int32GetDatum) \
	FIELD(atttypmod, 9, Int32GetDatum) \
	FIELD(attbyval, 10, BoolGetDatum) \
	FIELD(attstorage, 11, CharGetDatum) \
	FIELD(attalign, 12, CharGetDatum) \
	FIELD(attnotnull, 13, BoolGetDatum) \
	FIELD(atthasdef, 14, BoolGetDatum) \
	FIELD(attisdropped, 15, BoolGetDatum) \
	FIELD(attislocal, 16, BoolGetDatum) \
	FIELD(attinhcount, 17, Int32GetDatum) \
	FIELD(attcollation, 18, ObjectIdGetDatum) \
	FIELD(attacl, 19, GetNULL) \
	FIELD(attoptions, 20, GetNULL) \
	__VA_ARGS__

#elif PG_VERSION_NUM >= 80500
/*
 * 8.5
 */
#warning 8.5/9.0
#define FormData_pg_attribute_Fields(...) \
	FIELD(attrelid, 1, ObjectIdGetDatum) \
	FIELD(attname, 2, GetNAME) \
	FIELD(atttypid, 3, ObjectIdGetDatum) \
	FIELD(attstattarget, 4, Int32GetDatum) \
	FIELD(attlen, 5, Int16GetDatum) \
	FIELD(attnum, 6, Int16GetDatum) \
	FIELD(attndims, 7, Int32GetDatum) \
	FIELD(attcacheoff, 8, Int32GetDatum) \
	FIELD(atttypmod, 9, Int32GetDatum) \
	FIELD(attbyval, 10, BoolGetDatum) \
	FIELD(attstorage, 11, CharGetDatum) \
	FIELD(attalign, 12, CharGetDatum) \
	FIELD(attnotnull, 13, BoolGetDatum) \
	FIELD(atthasdef, 14, BoolGetDatum) \
	FIELD(attisdropped, 15, BoolGetDatum) \
	FIELD(attislocal, 16, BoolGetDatum) \
	FIELD(attinhcount, 17, Int32GetDatum) \
	FIELD(attacl, 18, GetNULL) \
	FIELD(attoptions, 19, GetNULL) \
	__VA_ARGS__

#else
#warning 8.4
#define FormData_pg_attribute_Fields(...) \
	FIELD(attrelid, 1, ObjectIdGetDatum) \
	FIELD(attname, 2, GetNAME) \
	FIELD(atttypid, 3, ObjectIdGetDatum) \
	FIELD(attstattarget, 4, Int32GetDatum) \
	FIELD(attlen, 5, Int16GetDatum) \
	FIELD(attnum, 6, Int16GetDatum) \
	FIELD(attndims, 7, Int32GetDatum) \
	FIELD(attcacheoff, 8, Int32GetDatum) \
	FIELD(atttypmod, 9, Int32GetDatum) \
	FIELD(attbyval, 10, BoolGetDatum) \
	FIELD(attstorage, 11, CharGetDatum) \
	FIELD(attalign, 12, CharGetDatum) \
	FIELD(attnotnull, 13, BoolGetDatum) \
	FIELD(atthasdef, 14, BoolGetDatum) \
	FIELD(attisdropped, 15, BoolGetDatum) \
	FIELD(attislocal, 16, BoolGetDatum) \
	FIELD(attinhcount, 17, Int32GetDatum) \
	FIELD(attacl, 18, GetNULL) \
	__VA_ARGS__

#endif

PyObj EmptyPyPgTupleDesc = NULL;

/*
 * There should be only one.
 */
void
EmptyPyPgTupleDesc_Initialize(void)
{
	PG_TRY();
	{
		TupleDesc td;
		td = CreateTemplateTupleDesc(0, false);
		EmptyPyPgTupleDesc = PyPgTupleDesc_FromCopy(td);
		FreeTupleDesc(td);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(true);
	}
	PG_END_TRY();
}

/*
 * Create a PyTupleObject of !attisdropped attribute names.
 */
PyObj
TupleDesc_BuildNames(TupleDesc td, int natts)
{
	int i;
	PyObj rob;

	/* tuple() large enough for not dropped natts */
	rob = PyTuple_New(natts);
	if (rob == NULL)
		return(NULL);

	for (i = 0, natts = 0; i < td->natts; ++i)
	{
		if (!td->attrs[i]->attisdropped)
		{
			PyObj s;
			s = PyUnicode_FromCString(NameStr(td->attrs[i]->attname));
			if (s == NULL)
			{
				Py_DECREF(rob);
				return(NULL);
			}
			PyTuple_SET_ITEM(rob, natts, s);

			++natts;
		}
	}

	return(rob);
}

/*
 * TupleDesc_GetPyPgTypes - Create a PyTupleObject of PyPgType's
 *
 * For each attribute in the TupleDesc, lookup the PyPgType corresponding
 * with the attribute's atttypid and place it in the new PyTupleObject at the
 * corresponding offset.
 *
 * NOTE:
 *  This includes all attributes in the returned tuple object.
 *  Dropped attributes are represented with Py_None.
 */
PyObj
TupleDesc_GetPyPgTypes(TupleDesc td)
{
	PyObj rob;
	Form_pg_attribute *att;
	int i;

	rob = PyTuple_New(td->natts);
	if (rob == NULL)
		return(NULL);

	att = td->attrs;
	for (i = 0; i < td->natts; ++i)
	{
		if (att[i]->attisdropped)
		{
			PyTuple_SET_ITEM(rob, i, Py_None);
			Py_INCREF(Py_None);
		}
		else
		{
			PyObj typob = PyPgType_FromOid(att[i]->atttypid);
			if (typob == NULL)
			{
				Py_DECREF(rob);
				Assert(PyErr_Occurred());
				return(NULL);
			}
			PyTuple_SET_ITEM(rob, i, typob);
		}
	}

	return(rob);
}

/*
 * build_name_map - map attribute names to [live] attribute offset
 */
static PyObj
build_name_map(PyObj names)
{
	PyObj rob;
	Py_ssize_t i, l;

	rob = PyDict_New();
	if (rob == NULL)
		return(NULL);

	l = PyTuple_GET_SIZE(names);

	for (i = 0; i < l; ++i)
	{
		PyObj key, val;

		key = PyTuple_GET_ITEM(names, i);
		val = PyLong_FromSsize_t(i);

		if (PyDict_SetItem(rob, key, val))
		{
			/* SetItem failure? ouch. */
			Py_DECREF(val);
			Py_DECREF(rob);
			return(NULL);
		}

		Py_DECREF(val); /* setitem grabs its reference */
	}

	return(rob);
}

/*
 * TupleDesc_FromNamesAndOids - create a TupleDesc from some basic elements
 *
 * Given a count of attributes, an array of names and type Oids, build and
 * return a TupleDesc.
 */
TupleDesc
TupleDesc_FromNamesAndOids(int ac, const char **names, Oid *types)
{
	unsigned int i;
	volatile TupleDesc td = NULL;

	PG_TRY();
	{
		td = CreateTemplateTupleDesc(ac, false);
		for (i = 0; i < ac; ++i)
		{
			const char *name = NULL;

			if (names)
				name = names[i];
			if (name == NULL)
				name = "";

			TupleDescInitEntry(td, i + 1, name, types[i], -1, 0);
		}
	}
	PG_CATCH();
	{
		if (td != NULL)
			FreeTupleDesc(td);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return(td);
}

/*
 * Build a tuple from the given args and kw using the TupleDesc to
 * create an appropriately ordered PyTupleObject of arguments and
 * keywords.
 *
 * The output of this can then be casted as needed.
 */
PyObj
PyTuple_FromTupleDescAndParameters(TupleDesc td, PyObj args, PyObj kw)
{
	PyObj rob;
	Py_ssize_t args_len, kw_len;

	Assert(td != NULL);
	Assert(args != NULL);

	/*
	 * Python allows NULL kw parameters to be given,
	 * so compensate below.
	 *
	 * Note that abstract interfaces are being used:
	 *  args can be any sequence and kw can be any mapping.
	 */

	args_len = PyObject_Length(args);
	kw_len = kw != NULL ? PyObject_Length(kw) : 0;

	if (args_len == -1 || kw_len == -1)
		return(NULL);

	if ((args_len + kw_len) != td->natts)
	{
		PyErr_Format(PyExc_TypeError,
			"requires exactly %d arguments, given %d",
			td->natts, (args_len + kw_len));
		return(NULL);
	}

	rob = PyTuple_New(td->natts);

	/*
	 * There are a few ways this could be implemented,
	 * but it seems the most reasonable is to first set
	 * any available keywords and fill in the gaps with
	 * the positional args.
	 */
	if (kw_len > 0)
	{
		PyObj ob_key, kw_iter;

		kw_iter = PyObject_GetIter(kw);
		if (kw_iter == NULL)
		{
			Py_DECREF(rob);
			return(NULL);
		}

		while ((ob_key = PyIter_Next(kw_iter)) != NULL)
		{
			PyObj ob, ob_str;
			char *obstr;
			int i;

			ob_str = ob_key;
			Py_INCREF(ob_str);
			PyObject_StrBytes(&ob_str);
			if (ob_str == NULL)
			{
				Py_DECREF(kw_iter);
				Py_DECREF(rob);
				return(NULL);
			}
			obstr = PyBytes_AS_STRING(ob_str);

			/*
			 * Scan TupleDesc for attribute number. O(NM) :(
			 */
			for (i = 0; i < td->natts; ++i)
			{
				if (!strcmp(NameStr(td->attrs[i]->attname), obstr))
					break;
			}
			Py_DECREF(ob_str);

			/*
			 * No such attribute.
			 */
			if (i == td->natts)
			{
				PyObj invalid_kw_param;
				Py_DECREF(rob);
				Py_DECREF(kw_iter);
				invalid_kw_param = PyUnicode_FromFormat(
					"invalid keyword parameter %R", ob_key);
				if (invalid_kw_param != NULL)
				{
					PyErr_SetObject(PyExc_TypeError, invalid_kw_param);
					Py_DECREF(invalid_kw_param);
				}
				Py_DECREF(ob_key);
				return(NULL);
			}

			ob = PyObject_GetItem(kw, ob_key);
			Py_DECREF(ob_key);
			PyTuple_SET_ITEM(rob, i, ob);
		}
	}

	if (args_len > 0)
	{
		int i, ai;

		for (i = 0, ai = 0; i < td->natts && ai < args_len; ++i)
		{
			PyObj ob;
			if (PyTuple_GET_ITEM(rob, i) != NULL)
				continue;

			ob = PySequence_GetItem(args, ai);
			if (ob == NULL)
			{
				Py_DECREF(rob);
				return(NULL);
			}
			PyTuple_SET_ITEM(rob, i, ob);
			++ai;
		}
	}

	return(rob);
}

/*
 * Get an offset from a number or a string.
 *
 * If a string, translate it using the name map.
 * If a number, return it as a Py_ssize_t
 */
Py_ssize_t
Offset_FromPyPgTupleDescAndPyObject(PyObj tdo, PyObj ob)
{
	if (PyUnicode_Check(ob))
	{
		int contains;
		PyObj namemap = PyPgTupleDesc_GetNameMap(tdo); /* borrowed */

		contains = PySequence_Contains(namemap, ob);
		if (contains == 1)
		{
			ob = PyDict_GetItem(namemap, ob); /* borrowed */
			return(PyNumber_AsSsize_t(ob, NULL));
		}

		return(-2);
	}

	return(-1);
}

/*
 * The TypesTuple includes dropped attributes, if there are dropped attributes
 * create and return a tuple of types without dropped attribute types.
 */
PyObj
PyPgTupleDesc_GetTypes(PyObj tdo)
{
	Py_ssize_t i, j, l;
	PyObj rob, types;

	types = PyPgTupleDesc_GetTypesTuple(tdo);
	if (PyTuple_GET_SIZE(types) == PyPgTupleDesc_GetNatts(tdo))
	{
		/* No dropped attributes */
		Py_INCREF(types);
		return(types);
	}

	rob = PyTuple_New(PyPgTupleDesc_GetNatts(tdo));
	if (rob == NULL)
		return(NULL);

	/*
	 * After this point, [python] failure isn't really possible...
	 */

	l = PyTuple_GET_SIZE(types);
	/*
	 * Fill the return tuple with !None objects
	 */
	for (i = 0, j = 0; i < l; ++i)
	{
		PyObj t = PyTuple_GET_ITEM(types, i);

		if (t != Py_None)
		{
			PyTuple_SET_ITEM(rob, j, t);
			Py_INCREF(t);
			++j;

			/*
			 * Reached the end of the !dropped attributes? Break out.
			 */
			if (j == PyTuple_GET_SIZE(rob))
				break;
		}
	}
	Assert(j == PyTuple_GET_SIZE(rob));

	return(rob);
}

PyObj
PyPgTupleDesc_GetTypeOids(PyObj tdo)
{
	Py_ssize_t i, j, l;
	PyObj rob, types;

	types = PyPgTupleDesc_GetTypesTuple(tdo);

	rob = PyTuple_New(PyPgTupleDesc_GetNatts(tdo));
	if (rob == NULL)
		return(NULL);

	/*
	 * After this point, [python] failure isn't really possible...
	 */

	l = PyTuple_GET_SIZE(types);
	/*
	 * Fill the return tuple with !None objects
	 */
	for (i = 0, j = 0; i < l; ++i)
	{
		PyObj t = PyTuple_GET_ITEM(types, i);

		if (t != Py_None)
		{
			PyObj typoid_ob = PyPgType_GetOid_PyLong(t);
			Py_INCREF(typoid_ob);
			PyTuple_SET_ITEM(rob, j, typoid_ob);
			++j;

			if (j == PyTuple_GET_SIZE(rob))
				break;
		}
	}

	return(rob);
}

/*
 * PyPgTupleDesc_IsCurrent - determine if all the column types are current
 */
bool
PyPgTupleDesc_IsCurrent(PyObj tdo)
{
	PyObj types = PyPgTupleDesc_GetTypesTuple(tdo);
	Py_ssize_t i;

	for (i = 0; i < PyTuple_GET_SIZE(types); ++i)
	{
		PyObj t;
		t = PyTuple_GET_ITEM(types, i);
		if (t == Py_None)
			continue;

		if (!PyPgType_IsCurrent(t))
			return(false);
	}

	return(true);
}

static PyObj
tupd_get_column_types(PyObj self, void *unused)
{
	return(PyPgTupleDesc_GetTypes(self));
}

static PyObj
tupd_get_pg_column_types(PyObj self, void *unused)
{
	return(PyPgTupleDesc_GetTypeOids(self));
}

static PyGetSetDef PyPgTupleDesc_GetSet[] = {
	{"column_types", tupd_get_column_types, NULL,
		PyDoc_STR("the descriptor's attribute types that have not been dropped")},
	{"pg_column_types", tupd_get_pg_column_types, NULL,
		PyDoc_STR("the descriptor's attribute type Oids that have not been dropped")},
	{NULL}
};

static PyMemberDef PyPgTupleDesc_Members[] = {
	{"column_names", T_OBJECT, offsetof(struct PyPgTupleDesc, td_names), READONLY,
		PyDoc_STR("the descriptor's attribute names that have not been dropped")},
	{"column_count", T_INT, offsetof(struct PyPgTupleDesc, td_natts), READONLY,
		PyDoc_STR("the count of the descriptor's attributes that have not been dropped ")},
	{NULL}
};

static Py_ssize_t
tupd_length(PyObj self)
{
	return((Py_ssize_t) PyPgTupleDesc_GetTupleDesc(self)->natts);
}

static PyObj
tupd_item(PyObj self, Py_ssize_t i)
{
	volatile PyObj rob = NULL;
	TupleDesc td;
	HeapTuple ht;
	Form_pg_attribute att;
	MemoryContext former = CurrentMemoryContext;

	td = PyPgTupleDesc_GetTupleDesc(self);
	if (i >= td->natts || i < 0)
	{
		PyErr_Format(PyExc_IndexError,
			"Postgres.TupleDesc index(%d) is out of range %d", i, td->natts);
		return(NULL);
	}

	i = PyPgTupleDesc_GetAttributeIndex(self, i);
	att = td->attrs[i];

	PG_TRY();
	{
		int initd_natts = 0;
		Datum pg_att_datums[Natts_pg_attribute];
		bool pg_att_nulls[Natts_pg_attribute] = {false,};

#ifdef Anum_pg_attribute_attacl
		pg_att_nulls[Anum_pg_attribute_attacl-1] = true;
#endif

#ifdef Anum_pg_attribute_attoptions
		pg_att_nulls[Anum_pg_attribute_attoptions-1] = true;
#endif

#ifdef Anum_pg_attribute_attfdwoptions
		pg_att_nulls[Anum_pg_attribute_attfdwoptions-1] = true;
#endif

		/*
		 * XXX: Need a better way to construct a pg_attribute Datum.
		 */
#define FIELD(NAME, NUM, DATUMIZER) \
		pg_att_datums[NUM-1] = DATUMIZER(att->NAME); initd_natts = initd_natts + 1;

		FormData_pg_attribute_Fields();
#undef FIELD

		if (initd_natts != Natts_pg_attribute)
			elog(ERROR, "failed to initialize all pg_attribute fields");

		ht = heap_form_tuple(PyPgType_GetTupleDesc(PyPg_pg_attribute_Type),
			pg_att_datums, pg_att_nulls);

		MemoryContextSwitchTo(PythonMemoryContext);
		rob = PyPgObject_FromPyPgTypeAndHeapTuple(PyPg_pg_attribute_Type, ht);
		MemoryContextSwitchTo(former);

		heap_freetuple(ht);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(former);
		Py_XDECREF(rob);
		rob = NULL;
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return(rob);
}

static PySequenceMethods PyPgTupleDescAsSequence = {
	tupd_length,			/* sq_length */
	NULL,					/* sq_concat */
	NULL,					/* sq_repeat */
	tupd_item,				/* sq_item */
	NULL,					/* sq_slice */
	NULL,					/* sq_ass_item */
	NULL,					/* sq_ass_slice */
	NULL,					/* sq_contains */
	NULL,					/* sq_inplace_concat */
	NULL,					/* sq_inplace_repeat */
};

static void
tupd_dealloc(PyObj self)
{
	MemoryContext former = CurrentMemoryContext;

	Py_XDECREF(PyPgTupleDesc_GetNames(self));
	PyPgTupleDesc_SetNames(self, NULL);

	Py_XDECREF(PyPgTupleDesc_GetNameMap(self));
	PyPgTupleDesc_SetNameMap(self, NULL);

	Py_XDECREF(PyPgTupleDesc_GetTypesTuple(self));
	PyPgTupleDesc_SetTypesTuple(self, NULL);

	PG_TRY();
	{
		TupleDesc td;
		int *map;

		td = PyPgTupleDesc_GetTupleDesc(self);
		PyPgTupleDesc_SetTupleDesc(self, NULL);
		if (td != NULL)
			FreeTupleDesc(td);

		map = PyPgTupleDesc_GetIndexMap(self);
		PyPgTupleDesc_SetIndexMap(self, NULL);
		if (map != NULL)
			pfree(map);

		map = PyPgTupleDesc_GetFreeMap(self);
		PyPgTupleDesc_SetFreeMap(self, NULL);
		if (map != NULL)
			pfree(map);

		/*
		 * When PLPY_STRANGE_THINGS is defined.
		 */
		RaiseAStrangeError
	}
	PG_CATCH();
	{
		PyErr_EmitPgErrorAsWarning("could not deallocate Postgres.TupleDesc fields");
	}
	PG_END_TRY();
	/*
	 * Normally, this doesn't matter, but it is possible for the context
	 * to be switched by the above code..
	 */
	MemoryContextSwitchTo(former);

	Py_TYPE(self)->tp_free(self);
}

static PyObj
tupd_richcompare(PyObj self, PyObj ob, int op)
{
	TupleDesc pri, sec;

	if (!PyPgTupleDesc_Check(ob) || op != Py_EQ)
		return(Py_NotImplemented);

	pri = PyPgTupleDesc_GetTupleDesc(self);
	sec = PyPgTupleDesc_GetTupleDesc(ob);

	if (equalTupleDescs(pri, sec))
	{
		Py_INCREF(Py_True);
		return(Py_True);
	}

	Py_INCREF(Py_False);
	return(Py_False);
}

static PyObj
tupd_new(PyTypeObject *subtype, PyObj args, PyObj kw)
{
	PyErr_SetString(PyExc_NotImplementedError,
			"custom TupleDesc creation not supported");
	return(NULL);
}

PyDoc_STRVAR(PyPgTupleDesc_Doc,
"Python interface to the Postgres tuple descriptor");

PyTypeObject PyPgTupleDesc_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"Postgres.TupleDesc",				/* tp_name */
	sizeof(struct PyPgTupleDesc),		/* tp_basicsize */
	0,									/* tp_itemsize */
	tupd_dealloc,						/* tp_dealloc */
	NULL,								/* tp_print */
	NULL,								/* tp_getattr */
	NULL,								/* tp_setattr */
	NULL,								/* tp_reserved */
	NULL,								/* tp_repr */
	NULL,								/* tp_as_number */
	&PyPgTupleDescAsSequence,			/* tp_as_sequence */
	NULL,								/* tp_as_mapping */
	(hashfunc) _Py_HashPointer,			/* tp_hash */
	NULL,								/* tp_call */
	NULL,								/* tp_str */
	NULL,								/* tp_getattro */
	NULL,								/* tp_setattro */
	NULL,								/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,			   		/* tp_flags */
	PyPgTupleDesc_Doc,					/* tp_doc */
	NULL,								/* tp_traverse */
	NULL,								/* tp_clear */
	tupd_richcompare,					/* tp_richcompare */
	0,									/* tp_weaklistoffset */
	PySeqIter_New,						/* tp_iter */
	NULL,								/* tp_iternext */
	NULL,								/* tp_methods */
	PyPgTupleDesc_Members,				/* tp_members */
	PyPgTupleDesc_GetSet,				/* tp_getset */
	NULL,								/* tp_base */
	NULL,								/* tp_dict */
	NULL,								/* tp_descr_get */
	NULL,								/* tp_descr_set */
	0,									/* tp_dictoffset */
	NULL,								/* tp_init */
	NULL,								/* tp_alloc */
	tupd_new,							/* tp_new */
};

PyObj
PyPgTupleDesc_NEW(PyTypeObject *subtype, TupleDesc td)
{
	int i, anatts = 0, byrefnatts = 0, poly = -1;
	int attnum, free_att;
	int *idxmap, *freemap = NULL;
	MemoryContext former;
	PyObj typs, namemap, names, rob;

	rob = ((PyObj) subtype->tp_alloc(subtype, 0));
	if (rob == NULL)
	{
		/*
		 * Subsequent failures will free the TupleDesc via dealloc,
		 * so be consistent and clear it here as we couldn't get 'rob'.
		 */
		Py_FreeTupleDesc(td);
		return(NULL);
	}
	PyPgTupleDesc_SetTupleDesc(rob, td);

	typs = TupleDesc_GetPyPgTypes(td);
	if (typs == NULL)
	{
		Py_DECREF(rob);
		return(NULL);
	}
	PyPgTupleDesc_SetTypesTuple(rob, typs);

	for (i = 0; i < td->natts; ++i)
	{
		if (!td->attrs[i]->attisdropped)
		{
			++anatts;
			if (!td->attrs[i]->attbyval)
				++byrefnatts;

			if (poly == -1 && IsPolymorphicType(td->attrs[i]->atttypid))
				poly = i;
		}
	}
	PyPgTupleDesc_SetNatts(rob, anatts); /* active attributes */
	PyPgTupleDesc_SetPolymorphic(rob, poly); /* first polymorphic att */

	names = TupleDesc_BuildNames(td, anatts);
	if (names == NULL)
	{
		Py_DECREF(rob);
		return(NULL);
	}
	PyPgTupleDesc_SetNames(rob, names);

	namemap = build_name_map(names);
	if (namemap == NULL)
	{
		Py_DECREF(rob);
		return(NULL);
	}
	PyPgTupleDesc_SetNameMap(rob, namemap);

	/*
	 * Build IndexMap and FreeMap
	 *
	 * Fast access to !dropped attributes and
	 * Fast access to !byval attributes.
	 */

	/*
	 * Dropped attribute exist. In this case,
	 * fast access to the actual attribute number is
	 * desired, so create the index map for the TupleDesc
	 * to speed things along.
	 *
	 * In situations where Datums and Nulls are built,
	 * fast access to the !typbyval Datum indexes is provided
	 * by the freemap.
	 */
	former = MemoryContextSwitchTo(PythonMemoryContext);
	idxmap = Py_palloc(sizeof(int) * (anatts + 1));

	if (idxmap != NULL)
	{
		freemap = Py_palloc(sizeof(int) * (byrefnatts + 1));
		if (freemap == NULL)
		{
			pfree(idxmap);
			idxmap = NULL;
		}
	}
	MemoryContextSwitchTo(former);

	if (idxmap == NULL)
	{
		Py_DECREF(rob);
		return(NULL);
	}

	/*
	 * No more Python or Postgres calls are made from here out.
	 */
	for (i = 0, attnum = 0, free_att = 0; i < td->natts; ++i)
	{
		if (attnum == anatts)
		{
			/* The rest are dropped; exit loop. */
			break;
		}

		/*
		 * If it's dropped, it will not take part in the index map,
		 * nor will a Datum ever need to be freed at its index.
		 */
		if (td->attrs[i]->attisdropped)
			continue;

		if (!td->attrs[i]->attbyval)
		{
			freemap[free_att] = i;
			++free_att;
		}
		idxmap[attnum] = i;
		++attnum;
	}

	/* Terminate the maps */
	freemap[byrefnatts] = -1;
	idxmap[anatts] = -1;

	PyPgTupleDesc_SetIndexMap(rob, idxmap);
	PyPgTupleDesc_SetFreeMap(rob, freemap);

	return(rob);
}

/*
 * Polymorph the polymorphic types in the TupleDesc to the appropriate type
 * related to the 'target' base type.
 *
 * Polymorphic types are pseudo types and thus Postgres won't store them,
 * so 'self' will never be a relation type.
 */
PyObj
PyPgTupleDesc_Polymorph(PyObj self, PyObj target)
{
	int i;
	TupleDesc td;
	PyObj td_types, rob;
	MemoryContext former;

	Assert(PyPgTupleDesc_CheckExact(self));
	Assert(PyPgTupleDesc_GetPolymorphic(self) != -1);
	Assert(PyPgType_Check(target));
	Assert(!PyPgType_IsPolymorphic(target));

	td_types = PyPgTupleDesc_GetTypesTuple(self);
	td = PyPgTupleDesc_GetTupleDesc(self);

	/*
	 * We need to update any polymorphic attributes, so
	 * grab a copy.
	 */
	former = MemoryContextSwitchTo(PythonMemoryContext);
	if (td->constr != NULL)
		td = Py_CreateTupleDescCopyConstr(td);
	else
		td = Py_CreateTupleDescCopy(td);
	MemoryContextSwitchTo(former);
	if (td == NULL)
		return(NULL);

	for (i = 0; i < td->natts; ++i)
	{
		PyObj polymorphic_type, polymorphed_type;
		PyPgTypeInfo typinfo;

		if (!IsPolymorphicType(td->attrs[i]->atttypid))
			continue;

		polymorphic_type = PyTuple_GET_ITEM(td_types, i);
		polymorphed_type = PyPgType_Polymorph(polymorphic_type, target);
		if (polymorphed_type == NULL)
		{
			Py_FreeTupleDesc(td);
			return(NULL);
		}

		typinfo = PyPgTypeInfo(polymorphed_type);
		/*
		 * It's not a pseudo type anymore; change the oid.
		 */
		td->attrs[i]->atttypid = typinfo->typoid;
		td->attrs[i]->attalign = typinfo->typalign;
		td->attrs[i]->attbyval = typinfo->typbyval;
		td->attrs[i]->attlen = typinfo->typlen;

		/*
		 * Done with that type for now.
		 * PyPgTupleDesc_New() will grab a new reference when it builds the types
		 * tuple.
		 */
		Py_DECREF(polymorphed_type);
	}

	/*
	 * Make and return the PyPgTupleDesc object..
	 */
	rob = PyPgTupleDesc_New(td);
	Assert(PyPgTupleDesc_GetPolymorphic(rob) == -1);

	return(rob);
}

PyObj
PyPgTupleDesc_FromCopy(TupleDesc tupd)
{
	PyObj rob;
	TupleDesc tdcpy;
	MemoryContext former;

	former = MemoryContextSwitchTo(PythonMemoryContext);
	if (tupd->constr != NULL)
		tdcpy = Py_CreateTupleDescCopyConstr(tupd);
	else
		tdcpy = Py_CreateTupleDescCopy(tupd);
	MemoryContextSwitchTo(former);
	if (tdcpy == NULL)
		return(NULL);

	/*
	 * If _New fails, it will free the TupleDesc.
	 */
	rob = PyPgTupleDesc_New(tdcpy);

	return(rob);
}
