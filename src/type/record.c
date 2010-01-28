/*
 * composite types
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "postgres.h"
#include "access/heapam.h"
#include "access/hash.h"
#include "access/transam.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_namespace.h"
#include "nodes/params.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "parser/parse_oper.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "storage/block.h"
#include "storage/off.h"
#include "utils/array.h"
#include "utils/catcache.h"
#include "utils/datum.h"
#include "utils/geo_decls.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/tuplestore.h"
#include "lib/stringinfo.h"
#include "funcapi.h"

#include "pypg/environment.h"
#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/pl.h"
#include "pypg/error.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/record.h"
#include "pypg/tupledesc.h"

#define size(REC) \
	PyPgTupleDesc_GetNatts(PyPgType_GetPyPgTupleDesc(Py_TYPE(REC)))

static int
get_offset(PyObj tdo, PyObj ob, Py_ssize_t *out)
{
	*out = Offset_FromPyPgTupleDescAndPyObject(tdo, ob);
	if (*out == -2)
	{
		/*
		 * It was a string object, set the key error.
		 */
		PyErr_SetObject(PyExc_KeyError, ob);
		return(-1);
	}
	else if (*out >= 0)
		return(0);

	/*
	 * It wasn't a name-based index.
	 */
	if (!PyIndex_Check(ob))
	{
		PyErr_SetString(PyExc_TypeError,
			"attribute selector must be index or string");
		return(-1);
	}

	*out = PyNumber_AsSsize_t(ob, NULL);

	return(0);
}

static Py_ssize_t
get_index(PyObj self, PyObj idx)
{
	PyObj tdo;
	Py_ssize_t r, x;
	int natts;

	tdo = PyPgType_GetPyPgTupleDesc((PyObj) Py_TYPE(self));
	natts = PyPgTupleDesc_GetNatts(tdo);

	r = Offset_FromPyPgTupleDescAndPyObject(tdo, idx);
	if (r >= 0)
		return(r);
	else if (r == -2)
	{
		PyErr_SetObject(PyExc_KeyError, idx);
		return(-1);
	}

	if (!PyIndex_Check(idx))
	{
		PyErr_SetString(PyExc_TypeError,
			"attribute selector must be index or string");
		return(-1);
	}

	natts = PyPgTupleDesc_GetNatts(tdo);

	x = PyNumber_AsSsize_t(idx, NULL);
	if (x == PY_SSIZE_T_MIN || x == PY_SSIZE_T_MIN)
	{
		PyErr_Format(PyExc_IndexError,
				"index (overflow) out of range %d", natts);
		return(-1);
	}

	if (x < 0)
		r = natts + x;
	else
		r = x;

	if (r >= natts || r < 0)
	{
		PyErr_Format(PyExc_IndexError,
				"index %d out of range %d", x, natts);
		return(-1);
	}

	return(r);
}

static PyObj
item(PyObj self, PyObj tdo, Py_ssize_t attnum)
{
	Datum datum;
	bool isnull;
	HeapTupleData ht;
	PyObj atttyp, rob;

	Assert(attnum < PyPgTupleDesc_GetNatts(tdo));
	Assert(attnum >= 0);

	/*
	 * Convert the given index to the appropriate
	 * Attribute Offset.
	 * HINT: This is where attisdropped is minded.
	 */
	attnum = PyPgTupleDesc_GetAttributeIndex(tdo, attnum);

	/*
	 * The types tuple is sized at td->natts, not GetNatts().
	 */
	atttyp = PyPgTupleDesc_GetAttributeType(tdo, attnum);
	if (atttyp == Py_None)
	{
		/*
		 * Can't happen error; the attnum was mapped to a dropped attribute.
		 */
		PyErr_SetString(PyExc_RuntimeError, "index resolved to dropped attribute");
		return(NULL);
	}

	ht.t_tableOid = PyPgType_GetTableOid(Py_TYPE(self));
	ht.t_data = PyPg_record_GetHeapTupleHeader(self);
	ht.t_len = HeapTupleHeaderGetDatumLength(ht.t_data);

	datum = fastgetattr(&ht, attnum+1, PyPgTupleDesc_GetTupleDesc(tdo), &isnull);
	if (isnull)
	{
		rob = Py_None;
		Py_INCREF(rob);
	}
	else
		rob = PyPgObject_New(atttyp, datum);

	return(rob);
}

/*
 * slicing - extract a tuple
 */
static PyObj
slicing(PyObj self, Py_ssize_t from, Py_ssize_t to, Py_ssize_t step)
{
	PyObj tdo, rob;
	Py_ssize_t n, i, offset, natts, skip;

	Assert(step != 0);
	Assert(to <= size(self));
	Assert(from >= -1);

	tdo = PyPgType_GetPyPgTupleDesc(Py_TYPE(self));
	natts = PyPgTupleDesc_GetNatts(tdo);

	if (step < 0)
	{
		skip = -step;
		n = ((skip - 1) + (from - to)) / skip;
	}
	else
	{
		skip = step;
		n = ((skip - 1) + (to - from)) / skip;
	}
	if (n < 0)
		n = 0;
	else if (n > natts)
		n = natts;

	rob = PyTuple_New(n);
	if (rob == NULL)
		return(NULL);

	for (offset = from, i = 0; i < n; offset = offset + step, ++i)
	{
		PyObj ob;

		ob = item(self, tdo, offset);
		if (ob == NULL)
			goto error;

		PyTuple_SET_ITEM(rob, i, ob);
	}

	return(rob);
error:
	Py_DECREF(rob);
	return(NULL);
}

/*
 * rec_transform - create a new instance using the callables associated with the
 * columns that will be transformed.
 *
 * Consider the composite, (i int, t text) AS ct:
 *
 * >>> r = ct((123, "string data"))
 * >>> r.transform(t = lambda x: x + " more string data")
 * (123, "string data more string data")
 *
 * The general process is:
 *
 *  Allocate datums and nulls for the new DatumTuple
 *  Fill datums from transformed positional parameters
 *  Fill datums from transformed keyword parameters
 *  Form new tuple
 *  Cleanup datums and nulls
 *  Create PyPgObject from new tuple
 */
static PyObj
rec_transform(PyObj self, PyObj args, PyObj kw)
{
	MemoryContext former = CurrentMemoryContext;
	PyObj tdo, typs;
	int i, ii;
	TupleDesc td;
	volatile HeapTuple ht = NULL;
	volatile PyObj rob = NULL;
	Datum *datums, *indatums;
	bool *nulls;
	bool *replaced;

	if (PyPg_record_Require(self) || DB_IS_NOT_READY())
		return(NULL);

	tdo = PyPgType_GetPyPgTupleDesc(Py_TYPE(self));
	typs = PyPgTupleDesc_GetTypesTuple(tdo);
	td = PyPgTupleDesc_GetTupleDesc(tdo);

	if (PyTuple_GET_SIZE(args) > PyPgTupleDesc_GetNatts(tdo))
	{
		PyErr_Format(PyExc_OverflowError,
			"too many positional transformations (%d) for "
			"record with %d attributes",
			PyTuple_GET_SIZE(args), PyPgTupleDesc_GetNatts(tdo));
		return(NULL);
	}

	Py_ALLOCATE_OWNER();
	{
		PG_TRY();
		{
			HeapTupleData htd;

			datums = palloc(sizeof(Datum) * td->natts);
			indatums = palloc(sizeof(Datum) * td->natts);
			nulls = palloc(sizeof(bool) * td->natts);
			/*
			 * Relies on the natts being !replaced[x] by default.
			 */
			replaced = palloc0(sizeof(bool) * td->natts);

			Py_ACQUIRE_SPACE();
			{
				for (i = 0; i < PyTuple_GET_SIZE(args); ++i)
				{
					PyObj xf, att, r, typ;
					xf = PyTuple_GET_ITEM(args, i);
					if (xf == Py_None)
						continue;

					ii = PyPgTupleDesc_GetAttributeIndex(tdo, i);
					Assert(ii != -1);
					typ = PyTuple_GET_ITEM(typs, ii);

					/*
					 * GetAttributeIndex should resolve to a not dropped attribute.
					 */
					Assert(typ != Py_None);

					att = item(self, tdo, i);
					if (att == NULL)
						break;

					r = Py_Call(xf, att);
					Py_DECREF(att);

					if (r == NULL)
						break;
					Py_XREPLACE(r);

					PyPgType_DatumNew(typ, r, -1, &(datums[ii]), &(nulls[ii]));
					replaced[ii] = true;
				}
			}
			Py_RELEASE_SPACE();

			/*
			 * If it ended early, fail out, and goto cleanup.
			 */
			if (i < PyTuple_GET_SIZE(args))
				goto cleanup;

			/*
			 * Process keywords. Yes, they can override args.
			 */
			if (kw != NULL)
			{
				PyObj key, value;
				Py_ssize_t si = 0;

				Py_ACQUIRE_SPACE();
				{
					while (PyDict_Next(kw, &si, &key, &value))
					{
						PyObj att, typ, r;

						/*
						 * Resolve the keyword's index for replacement
						 */
						i = get_index(self, key);
						if (i == -1)
							break;

						att = item(self, tdo, i);
						if (att == NULL)
							break;

						r = Py_Call(value, att);
						Py_DECREF(att);

						if (r == NULL)
							break;
						Py_XREPLACE(r);

						ii = PyPgTupleDesc_GetAttributeIndex(tdo, i);
						typ = PyTuple_GET_ITEM(typs, ii);

						Assert(typ != Py_None);

						/*
						 * Overwriting the previous entry(from the positionals), if any...
						 */
						if (!nulls[ii] && replaced[ii] && !td->attrs[ii]->attbyval)
						{
							replaced[ii] = false;
							pfree(DatumGetPointer(datums[ii]));
							datums[ii] = PointerGetDatum(NULL);
						}

						PyPgType_DatumNew(typ, r, -1, &(datums[ii]), &(nulls[ii]));
						replaced[ii] = true;

						/* identify break for end-of-loop */
						key = NULL;
					}
				}
				Py_RELEASE_SPACE();

				/*
				 * Not NULL? It broke out before the end of the dictionary.
				 */
				if (key != NULL)
					goto cleanup;
			}

			/*
			 * Fill in the remaining !replaced datums with the originals
			 * from 'self'.
			 */
			htd.t_data = (HeapTupleHeader) PyPgObject_GetDatum(self);
			htd.t_len = HeapTupleHeaderGetDatumLength(htd.t_data);

			for (i = 0; i < td->natts; ++i)
			{
				if (td->attrs[i]->attisdropped)
				{
					nulls[i] = true;
					Assert(!replaced[i]);
					continue;
				}

				if (replaced[i])
					indatums[i] = datums[i];
				else
				{
					indatums[i] = fastgetattr(&htd, i+1, td, &(nulls[i]));
				}
			}

			ht = heap_form_tuple(td, indatums, nulls);

cleanup:
			/*
			 * Be kind and cleanup. Aren't we nice & pretty.
			 */
			for (i = 0; i < td->natts; ++i)
			{
				if (!nulls[i] && !(td->attrs[i]->attbyval) && replaced[i])
					pfree(DatumGetPointer(datums[i]));
			}
			pfree(datums);
			pfree(indatums);
			pfree(nulls);
			pfree(replaced);

			if (ht != NULL)
			{
				/*
				 * If heap_form_tuple() was successful:
				 */
				MemoryContextSwitchTo(PythonMemoryContext);
				rob = PyPgObject_FromPyPgTypeAndHeapTuple((PyObj) Py_TYPE(self), ht);
				MemoryContextSwitchTo(former);

				Py_XACQUIRE(rob);
				heap_freetuple(ht);
			}
		}
		PG_CATCH();
		{
			rob = NULL; /* refowner will decref if necessary */
			MemoryContextSwitchTo(former);
			PyErr_SetPgError(false);
		}
		PG_END_TRY();

		/*
		 * Grab reference for return if it didn't fail.
		 */
		Py_XINCREF(rob);
	}
	Py_DEALLOCATE_OWNER();

	return(rob);
}

/*
 * rec_replace - create a new instance using the replacements.
 *
 * NOTE: transform & replace are nearly identical, if changes are necessary
 * in one, it is likely needed in the other.
 *
 * Consider the composite, (i int, t text) AS ct:
 *
 * >>> r = ct((123, "string data"))
 * >>> r.replace(321, t = "new string data")
 * (321, "new string data")
 *
 * The general process is:
 *
 *  Allocate datums and nulls for the new DatumTuple
 *  Fill datums from replacement positional parameters
 *  Fill datums from replacement keyword parameters
 *  Form new tuple
 *  Cleanup datums and nulls
 *  Create PyPgObject from new tuple
 */
static PyObj
rec_replace(PyObj self, PyObj args, PyObj kw)
{
	MemoryContext former = CurrentMemoryContext;
	PyObj tdo, typs;
	int i, ii;
	TupleDesc td;
	volatile HeapTuple ht = NULL;
	volatile PyObj rob = NULL;
	Datum *datums, *indatums;
	bool *nulls;
	bool *replaced;

	if (PyPg_record_Require(self) || DB_IS_NOT_READY())
		return(NULL);

	tdo = PyPgType_GetPyPgTupleDesc(Py_TYPE(self));
	typs = PyPgTupleDesc_GetTypesTuple(tdo);
	td = PyPgTupleDesc_GetTupleDesc(tdo);

	Py_ALLOCATE_OWNER();
	{
		PG_TRY();
		{
			HeapTupleData htd;

			datums = palloc(sizeof(Datum) * td->natts);
			indatums = palloc(sizeof(Datum) * td->natts);
			nulls = palloc(sizeof(bool) * td->natts);
			/*
			 * Code relies on the natts being !replaced[x] by default.
			 */
			replaced = palloc0(sizeof(bool) * td->natts);

			for (i = 0; i < PyTuple_GET_SIZE(args); ++i)
			{
				PyObj newatt, typ;

				newatt = PyTuple_GET_ITEM(args, i);

				ii = PyPgTupleDesc_GetAttributeIndex(tdo, i);
				typ = PyTuple_GET_ITEM(typs, ii);

				Assert(typ != Py_None);

				PyPgType_DatumNew(typ, newatt, -1, &(datums[ii]), &(nulls[ii]));
				replaced[ii] = true;
			}

			/*
			 * Process keywords. Yes, they override args.
			 */
			if (kw != NULL)
			{
				PyObj key, value;
				Py_ssize_t si = 0;

				while (PyDict_Next(kw, &si, &key, &value))
				{
					PyObj typ;

					/*
					 * Resolve the keyword's index for replacement
					 */
					i = get_index(self, key);
					if (i == -1)
						break;

					ii = PyPgTupleDesc_GetAttributeIndex(tdo, i);
					typ = PyTuple_GET_ITEM(typs, ii);

					Assert(typ != Py_None);

					/*
					 * Overwriting the previous entry, if any...
					 */
					if (!nulls[ii] && replaced[ii] && !td->attrs[ii]->attbyval)
					{
						replaced[ii] = false;
						pfree(DatumGetPointer(datums[ii]));
					}

					PyPgType_DatumNew(typ, value, -1, &(datums[ii]), &(nulls[ii]));
					replaced[ii] = true;

					/* identify break for end-of-loop */
					key = NULL;
				}

				/*
				 * Not NULL? It broke out before the end of the dictionary.
				 */
				if (key != NULL)
					goto cleanup;
			}

			/*
			 * Fill in the remaining !replaced datums with the originals
			 * from 'self'.
			 */
			htd.t_data = (HeapTupleHeader) PyPgObject_GetDatum(self);
			htd.t_len = HeapTupleHeaderGetDatumLength(htd.t_data);

			for (i = 0; i < td->natts; ++i)
			{
				if (!replaced[i])
					datums[i] = fastgetattr(&htd, i+1, td, &(nulls[i]));

				/*
				 * heap_form_tuple scribbles on datums, so make a copy.
				 */
				indatums[i] = datums[i];
			}

			ht = heap_form_tuple(td, indatums, nulls);

cleanup:
			/*
			 * Be kind and cleanup. Aren't we nice & pretty.
			 */
			for (i = 0; i < td->natts; ++i)
			{
				if (!nulls[i] && !(td->attrs[i]->attbyval) && replaced[i])
					pfree(DatumGetPointer(datums[i]));
			}
			pfree(datums);
			pfree(indatums);
			pfree(nulls);
			pfree(replaced);

			if (ht != NULL)
			{
				/*
				 * If heap_form_tuple() was successful:
				 */
				MemoryContextSwitchTo(PythonMemoryContext);
				rob = PyPgObject_FromPyPgTypeAndHeapTuple((PyObj) Py_TYPE(self), ht);
				MemoryContextSwitchTo(former);

				Py_XACQUIRE(rob);
				heap_freetuple(ht);
			}
		}
		PG_CATCH();
		{
			rob = NULL;
			MemoryContextSwitchTo(former);
			PyErr_SetPgError(false);
		}
		PG_END_TRY();

		/*
		 * Grab reference for return if it didn't fail.
		 */
		Py_XINCREF(rob);
	}
	Py_DEALLOCATE_OWNER();

	return(rob);
}

static PyObj
rec_keys(PyObj self)
{
	PyObj tdo, rob;

	if (PyPg_record_Require(self))
		return(NULL);

	tdo = PyPgType_GetPyPgTupleDesc(Py_TYPE(self));
	rob = PyPgTupleDesc_GetNames(tdo);
	Py_INCREF(rob);
	return(rob);
}

static PyObj
rec_values(PyObj self)
{
	if (PyPg_record_Require(self))
		return(NULL);

	Py_INCREF(self);
	return(self);
}

static PyObj
rec_items(PyObj self)
{
	PyObj tdo, names, rob;

	if (PyPg_record_Require(self))
		return(NULL);

	tdo = PyPgType_GetPyPgTupleDesc(Py_TYPE(self));
	names = PyPgTupleDesc_GetNames(tdo);

	rob = Py_Call((PyObj) &PyZip_Type, names, self);
	return(rob);
}

static PyMethodDef PyPg_record_Methods[] = {
	{"transform", (PyCFunction) rec_transform, METH_KEYWORDS|METH_VARARGS,
	PyDoc_STR("transform the record using callables assigned to the an attribute")},
	{"replace", (PyCFunction) rec_replace, METH_KEYWORDS|METH_VARARGS,
	PyDoc_STR("replace values using the identified attributes")},
	{"keys", (PyCFunction) rec_keys, METH_NOARGS,
	PyDoc_STR("return the descriptor's column_names tuple")},
	{"values", (PyCFunction) rec_values, METH_NOARGS,
	PyDoc_STR("return the record object")},
	{"items", (PyCFunction) rec_items, METH_NOARGS,
	PyDoc_STR("zip(x.column_names, x)")},
	{NULL,}
};

/*
 * yay, aliases.
 */
static PyObj
record_get_column_names(PyObj self, void *unused)
{
	PyObj names, tdo;

	tdo = PyPgType_GetPyPgTupleDesc(Py_TYPE(self));
	names = PyPgTupleDesc_GetNames(tdo);
	Py_INCREF(names);
	return(names);
}

static PyObj
record_get_column_types(PyObj self, void *unused)
{
	PyObj tdo;

	tdo = PyPgType_GetPyPgTupleDesc(Py_TYPE(self));
	return(PyPgTupleDesc_GetTypes(tdo));
}

static PyObj
record_get_pg_column_types(PyObj self, void *unused)
{
	PyObj tdo;

	tdo = PyPgType_GetPyPgTupleDesc(Py_TYPE(self));
	return(PyPgTupleDesc_GetTypeOids(tdo));
}

static PyGetSetDef record_getset[] = {
	{"column_names", record_get_column_names, NULL,
		PyDoc_STR("name of the columns of the composite")},
	{"column_types", record_get_column_types, NULL,
		PyDoc_STR("types of the columns of the composite")},
	{"pg_column_types", record_get_pg_column_types, NULL,
		PyDoc_STR("type Oids of the composite's columns")},
	{NULL}
};

static Py_ssize_t
rec_length(PyObj self)
{
	if (PyPg_record_Require(self))
		return(-1);

	return(size(self));
}

static PyObj
rec_item(PyObj self, Py_ssize_t attnum)
{
	PyObj tdo;

	if (PyPg_record_Require(self))
		return(NULL);

	tdo = PyPgType_GetPyPgTupleDesc(Py_TYPE(self));

	if (attnum >= PyPgTupleDesc_GetNatts(tdo))
	{
		PyErr_Format(PyExc_IndexError,
				"index %d out of range %d", attnum, PyPgTupleDesc_GetNatts(tdo));
		return(NULL);
	}

	return(item(self, tdo, attnum));
}

static PyObj
rec_slice(PyObj self, Py_ssize_t from, Py_ssize_t to)
{
	if (PyPg_record_Require(self))
		return(NULL);

	return(slicing(self, from, to, 1));
}

static PySequenceMethods record_as_sequence = {
	rec_length,			/* sq_length */
	NULL,					/* sq_concat */
	NULL,					/* sq_repeat */
	rec_item,			/* sq_item */
	rec_slice,			/* sq_slice */
	NULL,					/* sq_ass_item */
	NULL,					/* sq_ass_slice */
	NULL,					/* sq_contains */
};

static PyObj
rec_subscript(PyObj self, PyObj sub)
{
	PyObj tdo;
	PyObj rob = NULL;
	Py_ssize_t natts;

	if (PyPg_record_Require(self))
		return(NULL);

	tdo = PyPgType_GetPyPgTupleDesc(Py_TYPE(self));
	natts = size(self);

	if (PySlice_Check(sub))
	{
		PySliceObject *slice = (PySliceObject *) sub;
		Py_ssize_t start, stop, step;

		if (slice->step == Py_None)
			step = 1;
		else
			step = PyNumber_AsSsize_t(slice->step, NULL);

		if (step == 0)
		{
			PyErr_SetString(PyExc_ValueError, "slice step cannot be zero");
			return(NULL);
		}

		if (slice->start == Py_None)
		{
			if (step < 0)
				start = natts-1;
			else
				start = 0;
		}
		else if (!get_offset(tdo, slice->start, &start))
		{
			if (start < 0)
				start = start + natts;
		}
		else
			return(NULL); /* get_offset failed */

		if (slice->stop == Py_None)
		{
			if (step < 0)
				stop = -1;
			else
				stop = natts;
		}
		else if (!get_offset(tdo, slice->stop, &stop))
		{
			if (stop < 0)
				stop = stop + natts;
		}
		else
			return(NULL); /* get_offset failed */

		/*
		 * Limit the boundaries so slicing() can get a proper tuple size.
		 */
		if (stop < 0)
			stop = -1;
		else if (stop > natts)
			stop = natts;

		if (start < 0)
			start = 0;
		else if (start > natts)
			start = natts - 1;

		rob = slicing(self, start, stop, step);
	}
	else
	{
		Py_ssize_t i;

		i = get_index(self, sub);
		if (i == -1)
			return(NULL);

		rob = item(self, tdo, i);
	}

	return(rob);
}

static PyMappingMethods record_as_mapping = {
	rec_length,				/* mp_length */
	rec_subscript,			/* mp_subscript */
	NULL,						/* mp_ass_subscript */
};

static void
record_new_datum(
	PyObj subtype, PyObj ob, int32 mod, Datum *out, bool *isnull)
{
	volatile PyObj row;
	PyObj tdo, typs;
	TupleDesc td;
	HeapTuple ht;
	HeapTupleHeader hth;
	Datum *datums;
	bool *nulls;
	int i;

	tdo = PyPgType_GetPyPgTupleDesc(subtype);
	td = PyPgTupleDesc_GetTupleDesc(tdo);
	typs = PyPgTupleDesc_GetTypesTuple(tdo);

	/*
	 * Normalize row for DatumTuple construction.
	 */
	row = Py_NormalizeRow(
		PyPgTupleDesc_GetNatts(tdo),
		PyPgTupleDesc_GetTupleDesc(tdo),
		PyPgTupleDesc_GetNameMap(tdo), ob
	);
	if (row == NULL)
		PyErr_RelayException();

	PG_TRY();
	{
		datums = palloc(sizeof(Datum) * td->natts);
		nulls = palloc(sizeof(bool) * td->natts);
		Py_BuildDatumsAndNulls(td, typs, row, datums, nulls);
		Py_DECREF(row);
		row = NULL;

		ht = heap_form_tuple(td, datums, nulls);
		hth = palloc(ht->t_len);
		memcpy(hth, ht->t_data, ht->t_len);
		heap_freetuple(ht);
		for (i = 0; i < td->natts; ++i)
		{
			if (!nulls[i] && !(td->attrs[i]->attbyval))
				pfree(DatumGetPointer(datums[i]));
		}
		pfree(datums);
		pfree(nulls);

		*out = PointerGetDatum(hth);
		*isnull = false;
	}
	PG_CATCH();
	{
		Py_XDECREF(row);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

PyDoc_STRVAR(PyPg_record_Type_Doc, "record interface type");
PyPgTypeObject PyPg_record_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.record",						/* tp_name */
	sizeof(struct PyPgObject),						/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	NULL,											/* tp_as_number */
	&record_as_sequence,							/* tp_as_sequence */
	&record_as_mapping,								/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_BASETYPE|
	Py_TPFLAGS_DEFAULT,								/* tp_flags */
	PyPg_record_Type_Doc,							/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	PySeqIter_New,									/* tp_iter */
	NULL,											/* tp_iternext */
	PyPg_record_Methods,							/* tp_methods */
	NULL,											/* tp_members */
	record_getset,									/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
	NULL,											/* tp_free */
},
	PYPG_INIT_TYPINFO(record)
};

/*
 * PyPgObject_FromPyPgTypeAndHeapTuple
 *
 * Given a PyPg_record_Type subclass and a HeapTuple, create an instance
 * of the class using the HeapTuple.
 */
PyObj
PyPgObject_FromPyPgTypeAndHeapTuple(PyObj subtype, HeapTuple ht)
{
	HeapTupleHeader hth;
	PyObj rob;

	hth = palloc(ht->t_len);
	memcpy(hth, ht->t_data, ht->t_len);

	HeapTupleHeaderSetDatumLength(hth, ht->t_len);
	HeapTupleHeaderSetTypeId(hth, PyPgType_GetOid(subtype));
	HeapTupleHeaderSetTypMod(hth, PyPgType_GetTupleDesc(subtype)->tdtypmod);
	HeapTupleHeaderSetNatts(hth, HeapTupleHeaderGetNatts(ht->t_data));

	rob = PyPgObject_NEW(subtype);
	if (rob == NULL)
	{
		pfree(hth);
		return(NULL);
	}

	PyPgObject_SetDatum(rob, PointerGetDatum(hth));
	return(rob);
}

/*
 * Create a PyPg_record_Type instance from the given HeapTuple.
 */
PyObj
PyPgObject_FromHeapTuple(HeapTuple ht)
{
	PyObj typrel_Type, rob;

	typrel_Type = PyPgType_FromTableOid(ht->t_tableOid);
	if (typrel_Type == NULL)
		return(NULL);

	Py_DECREF(typrel_Type); /* borrow reference from type_cache */
	rob = PyPgObject_FromPyPgTypeAndHeapTuple(typrel_Type, ht);
	return(rob);
}

void
PyPg_record_Reform(PyObj subtype, PyObj ob, int mod, Datum *outdatum, bool *outnull)
{
	PyPgTypeInfo typinfo = PyPgTypeInfo(subtype);
	TupleDesc src, target;
	HeapTupleHeader hth;

	target = PyPgType_GetTupleDesc(subtype);
	src = PyPgType_GetTupleDesc(Py_TYPE(ob));

	if (equalTupleDescs(target, src))
	{
		/*
		 * Exact descriptors.
		 */
		*outdatum = datumCopy(PyPgObject_GetDatum(ob),
			typinfo->typbyval, typinfo->typlen);

		*outnull = false;
	}
	else
		elog(ERROR, "incompatible record types");

	hth = (HeapTupleHeader) *outdatum;
	HeapTupleHeaderSetTypeId(hth, PyPgType_GetOid(subtype));
	HeapTupleHeaderSetTypMod(hth, target->tdtypmod);
}
