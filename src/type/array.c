/*
 * Array base type
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#include "postgres.h"
#include "fmgr.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opclass.h"
#include "catalog/namespace.h"
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
#include "utils/relcache.h"
#include "utils/typcache.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/extension.h"
#include "pypg/error.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/bitwise.h"
#include "pypg/type/string.h"
#include "pypg/type/array.h"

/*
 * py_list_depth - get the ndims of nested PyLists
 *
 * If the depth exceeds MAXDIM, an error will be thrown.
 * This will protect against recursive lists.
 */
static int
py_list_depth(PyObj seq)
{
	int d = 0;

	Assert(PyList_CheckExact(seq));

	while (PyList_CheckExact(seq))
	{
		++d;
		if (d > MAXDIM)
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("depth of list exceeds the maximum allowed dimensions (%d)",
							MAXDIM),
					 errhint("A recursive list object can also be the cause this error.")));
		}
		if (PyList_GET_SIZE(seq) > 0)
			seq = PyList_GET_ITEM(seq, 0);
		else
		{
			/*
			 * Not a PyList, then it's not a "dimension".
			 */
			break;
		}
	}

	return(d);
}

/*
 * py_list_dimensions - given a PyList, calculate the dimensions and ndims
 *
 * dims should be a MAXDIM sized int array. ndims is returned.
 */
static int
py_list_dimensions(PyObj seq, int *dims)
{
	int i, ndims;

	/*
	 * Get the number of dimensions, and make an array for holding
	 * their measurements.
	 */
	ndims = py_list_depth(seq);

	/*
	 * The depth is restricted to the counted ndims (py_list_depth).
	 * This guarantees that 'seq' will always be a PyListObject, so
	 * use the optimized routines.
	 */
	for (i = 0; i < ndims; ++i)
	{
		dims[i] = PyList_GET_SIZE(seq);
		if (dims[i] > 0)
			seq = PyList_GET_ITEM(seq, 0);
	}

	return(ndims);
}

/*
 * fill_element - create all the Datums and NULLs using PyPgType_DatumNew
 *
 * This is the nasty routine that navigates through the nested lists coercing
 * the objects into the array's element type.
 */
static void
fill_elements(
	PyObj element_type, PyObj listob, int mod,
	unsigned int nelems, int ndims, int *dims,
	Datum *datums, bool *nulls)
{
	int elements_per = dims[ndims-1];
	int position[MAXDIM] = {0,};
	PyObj dstack[MAXDIM] = {listob, NULL,};
	unsigned int i = 0; /* current, absolute element position */
	int j, axis = 0; /* top */

	Assert(PyList_GET_SIZE(listob) == dims[0]);

	/*
	 * The filling of the Datum array ends when the total number of elements
	 * have been processed. datums and nulls *must* be allocated to fit nelems.
	 */
	while (i < nelems)
	{
		/*
		 * push until we are at element depth.
		 */
		while (axis < ndims - 1)
		{
			PyObj pushed;

			++axis; /* go deeper */

			/*
			 * use the position of the previous axis to identify which list will be used
			 * for this one.
			 */
			pushed = dstack[axis] = PyList_GET_ITEM(dstack[axis-1], position[axis-1]);
			position[axis] = 0; /* just started */

			/* just consumed position[axis-1], so increment */
			position[axis-1] = position[axis-1] + 1;

			/*
			 * Check the object.
			 */
			if (!PyList_CheckExact(pushed))
			{
				/*
				 * Do *not* be nice and instantiate the list because we don't
				 * want to be holding any references.
				 */
				PyErr_Format(PyExc_ValueError,
					"array boundaries must be list objects not '%s'",
					Py_TYPE(pushed)->tp_name);
				PyErr_RelayException();
			}

			/*
			 * Make sure it's consistent with the expectations.
			 *
			 * This check never hits the root list object, but that's fine
			 * because the dims[] is derived from the list lengths.
			 */
			if (PyList_GET_SIZE(pushed) != dims[axis])
			{
				PyErr_Format(PyExc_ValueError,
					"cannot make array from unbalanced lists", dims[axis]);
				PyErr_RelayException();
			}
		}

		/*
		 * Build the element datums.
		 */
		for (j = 0; j < elements_per; ++j)
		{
			PyPgType_DatumNew(element_type, PyList_GET_ITEM(dstack[ndims-1], j),
				mod, &(datums[i]), &(nulls[i]));
			++i;
		}

		/*
		 * pop it like it's hot
		 *
		 * No need to DECREF anything as PyList_GET_ITEM borrows.
		 *
		 * Also, the root list is never popped, so stop before zero.
		 */
		while (axis > 0)
		{
			/*
			 * Stop pop'ing when we identify a position that has more lists to
			 * process.
			 */
			--axis;

			if (position[axis] < dims[axis])
				break;
		}
	}
}

/*
 * array_from_py_list - given an element type and a list(), build an array
 */
static ArrayType *
array_from_py_list(PyObj element_type, PyObj listob, int elemmod)
{
	PyPgTypeInfo typinfo = PyPgTypeInfo(element_type);
	Datum * volatile datums = NULL;
	bool * volatile nulls = NULL;
	unsigned int nelems;
	int i, ndims;
	int dims[MAXDIM];
	int lbs[MAXDIM];
	ArrayType *rat = NULL;

	ndims = py_list_dimensions(listob, dims);
	Assert(ndims <= MAXDIM);

	/*
	 * From the dimensions, calculate the expected number of elements.
	 * At this point it is not known if the array is balanced
	 */
	nelems = dims[ndims-1];
	for (i = ndims-2; i > -1; --i)
	{
		unsigned int n = nelems * dims[i];
		if (n < nelems)
		{
			elog(ERROR, "too many elements for array");
		}
		nelems = n;
	}

	if (nelems == 0 && ndims > 1)
		elog(ERROR, "malformed nesting of list objects");

	PG_TRY();
	{
		/*
		 * palloc0 as cleanup in the PG_CATCH() will depend on this.
		 */
		nulls = palloc0(sizeof(bool) * nelems);
		datums = palloc0(sizeof(Datum) * nelems);

		/*
		 * elog's on failure, this will validate the balance/sizes of the
		 * dimensions.
		 */
		fill_elements(element_type, listob, elemmod, nelems, ndims, dims,
			(Datum *) datums, (bool *) nulls);

		/*
		 * Arrays built from lists don't support custom lower bounds,
		 * so initialize it to the default '1'.
		 */
		for (i = 0; i < ndims; ++i)
			lbs[i] = 1;

		/*
		 * Everything has been allocated, make the array.
		 */
		rat = construct_md_array(
			(Datum *) datums, (bool *) nulls,
			ndims, dims, lbs,
			typinfo->typoid,
			typinfo->typlen,
			typinfo->typbyval,
			typinfo->typalign);

		/*
		 * Cleanup.
		 */
		if (!typinfo->typbyval)
		{
			for (i = 0; i < nelems; ++i)
			{
				/*
				 * Array construction completed successfully,
				 * so go over the entire array of datums.
				 */
				if (!nulls[i])
					pfree(DatumGetPointer(datums[i]));
			}
		}
		pfree((bool *) nulls);
		pfree((Datum *) datums);
	}
	PG_CATCH();
	{
		/*
		 * Try and cleanup as much memory as possible.
		 *
		 * Currently, this code will run in the procedure context,
		 * so whatever leaks here will remain allocated for the duration of the
		 * procedure.
		 */

		if (rat != NULL)
		{
			/*
			 * When rat != NULL, failure occurred after the array
			 * was built, which means it had trouble freeing the resources.
			 * Attempt to free rat, but leave it at that.
			 */
			pfree(rat);
		}
		else
		{
			if (datums != NULL && nulls != NULL)
			{
				if (!typinfo->typbyval)
				{
					/*
					 * This is a bit different from the non-error case;
					 * rather than pfree'ing everything, we watch for
					 * NULL pointers..
					 */
					for (i = 0; i < nelems; ++i)
					{
						char *p = DatumGetPointer(datums[i]);
						if (nulls[i])
							continue;

						if (PointerIsValid(p))
							pfree(p);
						else
							break;
					}
				}
			}

			if (datums != NULL)
				pfree((Datum *) datums);
			if (nulls != NULL)
				pfree((bool *) nulls);
		}

		PG_RE_THROW();
	}
	PG_END_TRY();

	return(rat);
}

/*
 * array_from_list - given an element type and a list(), build an array
 * using the described structure.
 *
 * Sets a Python error and returns NULL on failure.
 */
static ArrayType *
array_from_list_and_info(PyObj element_type, PyObj listob, int elemmod,
	int ndims, int *dims, int *lbs)
{
	PyPgTypeInfo typinfo = PyPgTypeInfo(element_type);
	unsigned int nelems;
	int i;
	Datum * volatile datums = NULL;
	bool * volatile nulls = NULL;
	ArrayType * volatile rat = NULL;

	Assert(PyList_CheckExact(listob));

	nelems = PyList_GET_SIZE(listob);

	PG_TRY();
	{
		/*
		 * palloc0 as cleanup in the PG_CATCH() will depend on this.
		 */
		nulls = palloc0(sizeof(bool) * nelems);
		datums = palloc0(sizeof(Datum) * nelems);

		for (i = 0; i < nelems; ++i)
		{
			PyPgType_DatumNew(element_type, PyList_GET_ITEM(listob, i),
				elemmod, (Datum *) &(datums[i]), (bool *) &(nulls[i]));
		}

		/*
		 * Everything has been allocated, make the array.
		 */
		rat = construct_md_array(
			(Datum *) datums, (bool *) nulls,
			ndims, dims, lbs,
			typinfo->typoid,
			typinfo->typlen,
			typinfo->typbyval,
			typinfo->typalign);

		/*
		 * Cleanup.
		 */
		if (!typinfo->typbyval)
		{
			for (i = 0; i < nelems; ++i)
			{
				/*
				 * Array construction completed successfully,
				 * so go over the entire array of datums.
				 */
				if (!nulls[i])
					pfree(DatumGetPointer(datums[i]));
			}
		}
		pfree((bool *) nulls);
		pfree((Datum *) datums);
	}
	PG_CATCH();
	{
		/*
		 * Try and cleanup as much memory as possible.
		 *
		 * Currently, this code will run in the procedure context,
		 * so whatever leaks here will remain allocated for the duration of the
		 * procedure. If failure is often the part of a loop, the leaks could
		 * be problematic.
		 */

		if (rat != NULL)
		{
			/*
			 * When rat != NULL, failure occurred after the array
			 * was built, which means it had trouble freeing the resources.
			 * Attempt to free rat, but leave it at that.
			 */
			pfree((char *) rat);
		}
		else
		{
			if (datums != NULL && nulls != NULL)
			{
				if (!typinfo->typbyval)
				{
					/*
					 * This is a bit different from the non-error case;
					 * rather than pfree'ing everything, we watch for
					 * NULL pointers..
					 */
					for (i = 0; i < nelems; ++i)
					{
						char *p = DatumGetPointer(datums[i]);
						if (nulls[i])
							continue;

						if (PointerIsValid(p))
							pfree(p);
						else
							break;
					}
				}
			}

			if (datums != NULL)
				pfree((Datum *) datums);
			if (nulls != NULL)
				pfree((bool *) nulls);
		}

		PyErr_SetPgError(false);
		rat = NULL;
	}
	PG_END_TRY();

	return((ArrayType *) rat);
}

/*
 * Concatenate, but subjectively for supporting the distinction drawn by PyList
 * objects.
 */
static PyObj
array_add(PyObj self, PyObj with)
{
	PyObj wrapper, rob;

	if (!PyList_CheckExact(with) && !PyPgObject_Check(with))
	{
		/*
		 * It's probably an element object.
		 */
		wrapper = PyList_New(1);
		if (wrapper == NULL)
			return(NULL);
		PyList_SET_ITEM(wrapper, 0, with);
		Py_INCREF(with);
	}
	else
	{
		wrapper = with;
		Py_INCREF(wrapper);
	}

	rob = PyPgObject_Operate("||", self, wrapper);
	Py_DECREF(wrapper);

	return(rob);
}

static PyNumberMethods array_as_number = {
	array_add,		/* nb_add */
	NULL,
};

/*
 * len(o) - Python semantics
 */
static Py_ssize_t
py_array_length(PyObj self)
{
	ArrayType *at;
	at = DatumGetArrayTypeP(PyPgObject_GetDatum(self));
	if (ARR_NDIM(at) == 0)
		return(0);
	else
		return(ARR_DIMS(at)[0]);
}

static PyObj
array_item(PyObj self, Py_ssize_t item)
{
	volatile PyObj rob = NULL;
	PyPgTypeInfo typinfo, atypinfo;
	ArrayType *at;
	Datum rd;
	bool isnull = false;
	int index = (int) item;
	PyObj elm;

	elm = PyPgType_GetElementType(Py_TYPE(self));
	typinfo = PyPgTypeInfo(elm);
	atypinfo = PyPgTypeInfo(Py_TYPE(self));
	at = DatumGetArrayTypeP(PyPgObject_GetDatum(self));

	/* convert index */
	++index;

	if (ARR_NDIM(at) == 0)
	{
		PyErr_SetString(PyExc_IndexError, "empty array");
		return(NULL);
	}

	/*
	 * Note that the comparison is '>', not '>='.
	 */
	if (index > ARR_DIMS(at)[0])
	{
		PyErr_Format(PyExc_IndexError, "index %d out of range %d",
			item, ARR_DIMS(at)[0]);
		return(NULL);
	}

	/*
	 * Single dimenion array? Get an element.
	 */
	if (ARR_NDIM(at) == 1)
	{
		PG_TRY();
		{
			rd = array_ref(at, 1, &index, atypinfo->typlen,
				typinfo->typlen, typinfo->typbyval, typinfo->typalign, &isnull);

			if (isnull)
			{
				rob = Py_None;
				Py_INCREF(rob);
			}
			else
			{
				/*
				 * It points into the array structure, so there's no need to free.
				 */
				rob = PyPgObject_New(elm, rd);
			}
		}
		PG_CATCH();
		{
			Py_XDECREF(rob);
			rob = NULL;
			PyErr_SetPgError(false);
			return(NULL);
		}
		PG_END_TRY();
	}
	else
	{
		ArrayType *rat;
		int lower[MAXDIM] = {index,0,};
		int upper[MAXDIM] = {index,0,};

		/*
		 * Multiple dimensions, so get a slice.
		 */
		PG_TRY();
		{
			ArrayType *xat;
			Datum *elements;
			bool *nulls;
			int nelems;
			int ndims, i;
			int lbs[MAXDIM];
			int dims[MAXDIM];

			xat = array_get_slice(at, 1, upper, lower, atypinfo->typlen,
						typinfo->typlen, typinfo->typbyval, typinfo->typalign);

			/*
			 * Eventually, this should probably be changed to change the already
			 * allocated ArrayType at 'xat', but for now use the available
			 * interfaces for creating the expected result.
			 */
			deconstruct_array(xat,
				typinfo->typoid, typinfo->typlen, typinfo->typbyval, typinfo->typalign,
				&elements, &nulls, &nelems
			);

			/*
			 * Alter dims, lbs, and ndims: we are removing the first dimension.
			 */
			ndims = ARR_NDIM(xat);
			for (i = 1; i < ndims; ++i)
				lbs[i-1] = ARR_LBOUND(xat)[i];
			for (i = 1; i < ndims; ++i)
				dims[i-1] = ARR_DIMS(xat)[i];
			--ndims;

			/*
			 * Construct the expected result to a Python itemget call.
			 */
			rat = construct_md_array(elements, nulls, ndims, dims, lbs,
				typinfo->typoid, typinfo->typlen, typinfo->typbyval, typinfo->typalign);

			pfree(elements);
			pfree(nulls);
			pfree(xat);

			rob = PyPgObject_New(Py_TYPE(self), PointerGetDatum(rat));
			pfree(rat);
		}
		PG_CATCH();
		{
			PyErr_SetPgError(false);
			return(NULL);
		}
		PG_END_TRY();
	}

	return(rob);
}

static PyObj
array_slice(PyObj self, Py_ssize_t from, Py_ssize_t to)
{
	PyObj elm;
	PyPgTypeInfo etc;
	ArrayType *at, *rat = NULL;
	PyObj rob = NULL;
	int idx_lower[MAXDIM] = {(int) from+1, 0,};
	int idx_upper[MAXDIM] = {(int) to+1, 0,};

	elm = PyPgType_GetElementType(Py_TYPE(self));
	Assert(elm != NULL);

	etc = PyPgTypeInfo(elm);
	Assert(etc != NULL);

	at = DatumGetArrayTypeP(PyPgObject_GetDatum(self));
	Assert(at != NULL);

	PG_TRY();
	{
		rat = array_get_slice(at, 1, idx_upper, idx_lower,
			PyPgTypeInfo(Py_TYPE(self))->typlen,
			etc->typlen, etc->typbyval, etc->typalign);

		rob = PyPgObject_New(Py_TYPE(self), PointerGetDatum(rat));
		if (rob == NULL)
			pfree(rat);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();

	return(rob);
}

static PySequenceMethods array_as_sequence = {
	py_array_length,	/* sq_length */
	NULL,				/* sq_concat */
	NULL,				/* sq_repeat */
	array_item,			/* sq_item */
	array_slice,		/* sq_slice */
	NULL,				/* sq_ass_item */
	NULL,				/* sq_ass_slice */
	NULL,				/* sq_contains */
	NULL,				/* sq_inplace_concat */
	NULL,				/* sq_inplace_repeat */
};

static PyObj
array_subscript(PyObj self, PyObj arg)
{
	Py_ssize_t len = py_array_length(self);

	if (PyIndex_Check(arg))
	{
		Py_ssize_t i = PyNumber_AsSsize_t(arg, PyExc_IndexError);

		if (i == -1 && PyErr_Occurred())
			return(NULL);

		if (i < 0)
			i += py_array_length(self);

		return(array_item(self, i));
	}
	else if (PySlice_Check(arg))
	{
		Py_ssize_t start, stop, step, slicelength;
		int r;

		r = PySlice_GetIndicesEx((PySliceObject *) arg, len,
			&start, &stop, &step, &slicelength);
		if (r < 0)
		  return(NULL);

		if (step != 1)
		{
			/* TODO: implement custom step values for array subscript */
			PyErr_Format(PyExc_NotImplementedError,
				"unsupported step value in array subscript");
			return(NULL);
		}

		if (slicelength == len && start == 0)
		{
			Py_INCREF(self);
			return(self);
		}

		return(array_slice(self, start, stop));
	}
	else
	{
		PyErr_Format(PyExc_TypeError, "array indexes must be integers, not %.200s",
			Py_TYPE(arg)->tp_name);

		return(NULL);
	}
}

static PyMappingMethods array_as_mapping = {py_array_length, array_subscript,};

static PyObj
array_get_lowerbounds(PyObj self, void *closure)
{
	ArrayType *at;
	PyObj rob;
	int i, ndim, *lbs;

	at = DatumGetArrayTypeP(PyPgObject_GetDatum(self));
	ndim = ARR_NDIM(at);
	lbs = ARR_LBOUND(at);

	rob = PyTuple_New(ndim);

	for (i = 0; i < ndim; ++i)
	{
		PyObj ob;
		ob = PyLong_FromLong(lbs[i]);
		if (ob == NULL)
		{
			Py_DECREF(rob);
			return(NULL);
		}
		PyTuple_SET_ITEM(rob, i, ob);
	}

	return(rob);
}

static PyObj
array_get_dimensions(PyObj self, void *closure)
{
	ArrayType *at;
	PyObj rob;
	int i, ndim, *dims;

	at = DatumGetArrayTypeP(PyPgObject_GetDatum(self));
	ndim = ARR_NDIM(at);
	dims = ARR_DIMS(at);
	rob = PyTuple_New(ndim);

	for (i = 0; i < ndim; ++i)
	{
		PyObj ob;
		ob = PyLong_FromLong(dims[i]);
		if (ob == NULL)
		{
			Py_DECREF(rob);
			return(NULL);
		}
		PyTuple_SET_ITEM(rob, i, ob);
	}

	return(rob);
}

static PyObj
array_get_ndim(PyObj self, void *closure)
{
	PyObj rob;

	rob = PyLong_FromLong(ARR_NDIM(DatumGetArrayTypeP(PyPgObject_GetDatum(self))));

	return(rob);
}

static PyObj
array_has_null(PyObj self, void *closure)
{
	PyObj rob;

	if (ARR_HASNULL(DatumGetArrayTypeP(PyPgObject_GetDatum(self))))
		rob = Py_True;
	else
		rob = Py_False;

	Py_INCREF(rob);
	return(rob);
}

static PyObj
array_get_nelements(PyObj self, void *closure)
{
	long nelements;
	ArrayType *at;
	int ndim, *dims;

	at = DatumGetArrayTypeP(PyPgObject_GetDatum(self));
	ndim = ARR_NDIM(at);
	dims = ARR_DIMS(at);

	if (ndim == 0)
		nelements = 0;
	else
	{
		int i;
		nelements = 1;

		for (i = 0; i < ndim; ++i)
		{
			nelements = (dims[i] * nelements);
		}
	}

	return(PyLong_FromLong(nelements));
}

/*
 * Very similar to type_get_Element.
 */
static PyObj
array_get_Element_type(PyObj self, void *closure)
{
	PyObj rob;
	PyPgTypeInfo typinfo = PyPgTypeInfo(Py_TYPE(self));

	rob = typinfo->array.x_yes.typelem_Type;

	/*
	 * The array type shouldn't exist without having an element type.
	 */
	Assert(rob != NULL);
	Py_INCREF(rob);

	return(rob);
}

static PyGetSetDef array_getset[] = {
	{"Element", array_get_Element_type, NULL,
	PyDoc_STR("The array's element type")},
	{"dimensions", array_get_dimensions, NULL,
	PyDoc_STR("The array's dimensions")},
	{"has_null", array_has_null, NULL,
	PyDoc_STR("Whether the array has a NULL inside of it")},
	{"lowerbounds", array_get_lowerbounds, NULL,
	PyDoc_STR("The array's lower bounds")},
	{"ndim", array_get_ndim, NULL,
	PyDoc_STR("The number of array dimensions")},
	{"nelements", array_get_nelements, NULL,
	PyDoc_STR("The number of elements in the array")},
	{NULL}
};

/*
 * Array.get_element(indexes) - Get an element from the array.
 *
 * This uses Python sequence semantics(zero-based indexes, IndexError's).
 */
static PyObj
array_get_element(PyObj self, PyObj indexes_ob)
{
	PyObj tup, element_type, rob = NULL;
	PyPgTypeInfo atypinfo, typinfo;
	ArrayType *at;
	int i, nindexes, indexes[MAXDIM] = {0,};

	/*
	 * Convert the indexes_ob into a tuple and extract the values
	 * into the indexes[] array. Do any necessary checks along the way.
	 */
	tup = Py_Call((PyObj) &PyTuple_Type, indexes_ob);
	if (tup == NULL)
		return(NULL);

	nindexes = (int) PyTuple_GET_SIZE(tup);

	if (!(nindexes > 0))
	{
		Py_DECREF(tup);
		PyErr_SetString(PyExc_ValueError, "empty index tuple");
		return(NULL);
	}

	at = DatumGetArrayTypeP(PyPgObject_GetDatum(self));
	Assert(at != NULL);

	if (nindexes != ARR_NDIM(at))
	{
		Py_DECREF(tup);
		if (ARR_NDIM(at) == 0)
			PyErr_SetString(PyExc_IndexError, "no elements in array");
		else
			PyErr_Format(PyExc_ValueError, "element access requires exactly %d indexes, given %d",
				ARR_NDIM(at), nindexes);
		return(NULL);
	}

	for (i = 0; i < nindexes; ++i)
	{
		int index;
		index = (int) PyNumber_AsSsize_t(PyTuple_GET_ITEM(tup, i),
											NULL);
		if (PyErr_Occurred())
		{
			Py_DECREF(tup);
			return(NULL);
		}

		/*
		 * Adjust for backwards based access. (feature of get_element)
		 */
		if (index < 0)
			indexes[i] = index + ARR_DIMS(at)[i];
		else
			indexes[i] = index;

		if (indexes[i] >= ARR_DIMS(at)[i] || indexes[i] < 0)
		{
			PyErr_Format(PyExc_IndexError, "index %d out of range %d for axis %d",
				index, ARR_DIMS(at)[0], i);
			Py_DECREF(tup);
			return(NULL);
		}

		/*
		 * Adjust by the lowerbounds..
		 */
		indexes[i] = indexes[i] + ARR_LBOUND(at)[i];
	}

	Py_DECREF(tup);

	atypinfo = PyPgTypeInfo(Py_TYPE(self));
	element_type = PyPgType_GetElementType(Py_TYPE(self));
	typinfo = PyPgTypeInfo(element_type);

	PG_TRY();
	{
		Datum rd;
		bool isnull = false;

		rd = array_ref(at, nindexes, indexes, atypinfo->typlen,
			typinfo->typlen, typinfo->typbyval, typinfo->typalign, &isnull);

		if (isnull)
		{
			rob = Py_None;
			Py_INCREF(rob);
		}
		else
		{
			/*
			 * It points into the array structure, so there's no need to free.
			 */
			rob = PyPgObject_New(element_type, rd);
		}
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return(rob);
}

static PyObj
array_sql_get_element(PyObj self, PyObj indexes_ob)
{
	PyObj tup, element_type, rob = NULL;
	PyPgTypeInfo atypinfo, typinfo;
	ArrayType *at;
	int i, nindexes, indexes[MAXDIM] = {0,};

	/*
	 * Convert the dimensions keyword into a tuple and extract the values
	 * into the dims[] array.
	 */

	tup = Py_Call((PyObj) &PyTuple_Type, indexes_ob);
	if (tup == NULL)
		return(NULL);

	at = DatumGetArrayTypeP(PyPgObject_GetDatum(self));
	Assert(at != NULL);

	nindexes = (int) PyTuple_GET_SIZE(tup);
	if (nindexes != ARR_NDIM(at))
	{
		Py_DECREF(tup);
		Py_INCREF(Py_None);
		return(Py_None);
	}

	for (i = 0; i < nindexes; ++i)
	{
		indexes[i] = (int) PyNumber_AsSsize_t(PyTuple_GET_ITEM(tup, i),
											NULL);
		if (PyErr_Occurred())
		{
			Py_DECREF(tup);
			return(NULL);
		}
	}

	Py_DECREF(tup);

	atypinfo = PyPgTypeInfo(Py_TYPE(self));
	element_type = PyPgType_GetElementType(Py_TYPE(self));
	typinfo = PyPgTypeInfo(element_type);

	/*
	 * Single dimenion array? Get an element.
	 */
	PG_TRY();
	{
		Datum rd;
		bool isnull = false;

		rd = array_ref(at, nindexes, indexes, atypinfo->typlen,
			typinfo->typlen, typinfo->typbyval, typinfo->typalign, &isnull);

		if (isnull)
		{
			rob = Py_None;
			Py_INCREF(rob);
		}
		else
		{
			/*
			 * It points into the array structure, so there's no need to free.
			 */
			rob = PyPgObject_New(element_type, rd);
		}
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return(rob);
}

/*
 * array_element - get an iterator to all the elements in the array
 *
 * The short: deconstruct and build a list of element instances.
 */
static PyObj
array_elements(PyObj self)
{
	PyObj element_type;
	volatile PyObj rob = NULL;
	PyPgTypeInfo typinfo;

	element_type = PyPgType_GetElementType(Py_TYPE(self));
	typinfo = PyPgTypeInfo(element_type);

	/*
	 * Multiple dimensions, so get a slice.
	 */
	PG_TRY();
	{
		Datum *elements;
		bool *nulls;
		int i, nelems;
		ArrayType *at;

		at = DatumGetArrayTypeP(PyPgObject_GetDatum(self));

		deconstruct_array(at,
			typinfo->typoid, typinfo->typlen, typinfo->typbyval, typinfo->typalign,
			&elements, &nulls, &nelems
		);

		rob = PyList_New(nelems);
		for (i = 0; i < nelems; ++i)
		{
			PyObj ob;
			if (nulls[i])
			{
				ob = Py_None;
				Py_INCREF(ob);
			}
			else
				ob = PyPgObject_New(element_type, elements[i]);

			if (ob == NULL)
			{
				Py_DECREF(rob);
				rob = NULL;
				break;
			}

			PyList_SET_ITEM(rob, i, ob);
		}

		pfree(elements);
		pfree(nulls);
	}
	PG_CATCH();
	{
		Py_XDECREF(rob);
		rob = NULL;
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();

	return(rob);
}

/*
 * array_from_elements - classmethod to build an array
 */
static PyObj
array_from_elements(PyObj self, PyObj args, PyObj kw)
{
	static char *kwlist[] = {"elements", "dimensions", "lowerbounds", NULL};
	PyObj rob = NULL, iter, listob, dims_ob = NULL, lbs_ob = NULL;
	int dims[MAXDIM];
	int lbs[MAXDIM] = {1, 0,};
	int ndims = 1, nelems;
	Py_ssize_t i;
	ArrayType *rat = NULL;

	if (!PyArg_ParseTupleAndKeywords(args, kw, "O|OO:from_elements",
		kwlist, &iter, &dims_ob, &lbs_ob))
	{
		return(NULL);
	}

	listob = Py_Call((PyObj) &PyList_Type, iter);
	if (listob == NULL)
		return(NULL);
	Assert(PyList_CheckExact(listob));

	/*
	 * Convert the dimensions keyword into a tuple and extract the values
	 * into the dims[] array.
	 */
	if (dims_ob != NULL && dims_ob != Py_None)
	{
		PyObj tdims_ob;

		tdims_ob = Py_Call((PyObj) &PyTuple_Type, dims_ob);
		if (tdims_ob == NULL)
			goto fail;

		ndims = PyTuple_GET_SIZE(tdims_ob);
		if (ndims > MAXDIM)
		{
			Py_DECREF(tdims_ob);
			PyErr_Format(PyExc_ValueError, "too many dimensions (%d) for array",
							ndims);
			goto fail;
		}

		if (ndims > 0)
		{
			for (i = 0; i < ndims; ++i)
			{
				dims[i] = (int) PyNumber_AsSsize_t(PyTuple_GET_ITEM(tdims_ob, i),
													NULL);
				if (PyErr_Occurred())
				{
					Py_DECREF(tdims_ob);
					goto fail;
				}
			}
		}
		else
			dims[0] = 0;

		Py_DECREF(tdims_ob);
	}
	else
	{
		dims[0] = PyList_GET_SIZE(listob);
		if (dims[0] == 0)
			ndims = 0;
	}

	nelems = dims[0];
	if (ndims > 1)
	{
		for (i = 1; i < ndims; ++i)
			nelems = nelems * dims[i];
	}

	if (nelems != PyList_GET_SIZE(listob))
	{
		PyErr_Format(PyExc_ValueError,
			"dimension capacity (%d) does not accommodate the given elements (%d)",
			nelems, PyList_GET_SIZE(listob));
		goto fail;
	}

	if (lbs_ob != NULL && lbs_ob != Py_None)
	{
		PyObj tlbs_ob;

		tlbs_ob = Py_Call((PyObj) &PyTuple_Type, lbs_ob);
		if (tlbs_ob == NULL)
			goto fail;

		if (PyTuple_GET_SIZE(tlbs_ob) > MAXDIM)
		{
			Py_DECREF(tlbs_ob);
			PyErr_SetString(PyExc_ValueError, "too many dimensions for array");
			goto fail;
		}

		if (PyTuple_GET_SIZE(tlbs_ob) != ndims)
		{
			Py_DECREF(tlbs_ob);
			PyErr_Format(PyExc_ValueError, "number of lower bounds (%d) is "
				"inconsistent with dimensions (%d)",
				PyTuple_GET_SIZE(tlbs_ob), ndims);
			goto fail;
		}

		for (i = 0; i < PyTuple_GET_SIZE(tlbs_ob); ++i)
		{
			lbs[i] = (int) PyNumber_AsSsize_t(PyTuple_GET_ITEM(tlbs_ob, i),
												NULL);
			if (PyErr_Occurred())
			{
				Py_DECREF(tlbs_ob);
				goto fail;
			}
		}

		Py_DECREF(tlbs_ob);
	}
	else
	{
		/*
		 * No lower bounds specified, fill in with 1's.
		 */
		for (i = 0; i < ndims; ++i)
		{
			lbs[i] = 1;
		}
	}

	rat = array_from_list_and_info(
		PyPgType_GetElementType(self),
		listob, -1, ndims, dims, lbs);
	Py_DECREF(listob);

	if (rat != NULL)
	{
		rob = PyPgObject_New(self, PointerGetDatum(rat));
		pfree(rat);
	}

	return(rob);
fail:
	Py_XDECREF(listob);
	return(NULL);
}

static PyMethodDef array_methods[] = {
	{"sql_get_element", (PyCFunction) array_sql_get_element, METH_O,
	PyDoc_STR("get a single element in the array")},
	{"get_element", (PyCFunction) array_get_element, METH_O,
	PyDoc_STR("get a single element in the array using zero-based indexes")},
	{"elements", (PyCFunction) array_elements, METH_NOARGS,
	PyDoc_STR("get the ArrayElementsIter for the array")},
	{"from_elements", (PyCFunction) array_from_elements,
		METH_VARARGS|METH_KEYWORDS|METH_CLASS,
		PyDoc_STR("build an array from elements")},
	{NULL,},
};

static void
array_new_datum(PyObj subtype, PyObj ob, int32 mod, Datum *rdatum, bool *isnull)
{
	ArrayType *at;

	if (!PyList_CheckExact(ob))
	{
		/*
		 * If it's a string object, it should never get here.
		 */
		PyErr_SetString(PyExc_TypeError,
			"array constructor requires a list or string object");
		PyErr_RelayException();
	}
	else
	{
		at = array_from_py_list(PyPgType_GetElementType(subtype), ob, mod);
		*rdatum = PointerGetDatum(at);
		*isnull = false;
	}
}

PyDoc_STRVAR(PyPgArray_Type_Doc, "abstract array type (base type for arrays)");
#define ARRAY_new_datum array_new_datum
PyPgTypeObject PyPgArray_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.Array",								/* tp_name */
	sizeof(struct PyPgObject),						/* tp_basicsize */
	0,												/* tp_itemsize */
	NULL,											/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	&array_as_number,								/* tp_as_number */
	&array_as_sequence,								/* tp_as_sequence */
	&array_as_mapping,								/* tp_as_mapping */
	NULL,											/* tp_hash */
	NULL,											/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT|
	Py_TPFLAGS_BASETYPE,							/* tp_flags */
	PyPgArray_Type_Doc,								/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	PySeqIter_New,									/* tp_iter */
	NULL,											/* tp_iternext */
	array_methods,									/* tp_methods */
	NULL,											/* tp_members */
	array_getset,									/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(ARRAY)
};
