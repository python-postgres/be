/*
 * Postgres interface functions
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#include "postgres.h"
#include "fmgr.h"
#include "tcop/tcopprot.h"
#include "access/htup.h"
#include "access/hio.h"
#include "access/heapam.h"
#include "access/tupdesc.h"
#include "access/transam.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "parser/parse_type.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/tuplestore.h"
#include "mb/pg_wchar.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/pl.h"
#include "pypg/strings.h"
#include "pypg/externs.h"
#include "pypg/error.h"
#include "pypg/tupledesc.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/system.h"

/*
 * It's "Postgres data", so put it here instead of pl.c.
 */
PyObj py_my_datname_str_ob = NULL;

/*
 * palloc interface that catches any OOM errors.
 */
void *
Py_palloc(Size memsize)
{
	void * volatile rptr = NULL;

	PG_TRY();
	{
		rptr = palloc(memsize);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return(rptr);
}

Datum
Py_datumCopy(Datum d, bool typbyval, int typlen)
{
	volatile Datum rd = 0;

	if (typbyval)
		return(d);

	PG_TRY();
	{
		/* -1 == VARLENA */
		if (typlen == -1)
		{
			/*
			 * Always work with detoasted data.
			 */
			rd = PointerGetDatum(PG_DETOAST_DATUM_COPY(d));
		}
		else
			rd = datumCopy(d, false, typlen);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return((Datum) rd);
}

TupleDesc
Py_CreateTupleDescCopy(TupleDesc td)
{
	volatile TupleDesc ntd = NULL;

	PG_TRY();
	{
		ntd = CreateTupleDescCopy(td);
		ntd->tdrefcount = -1;
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return((TupleDesc) ntd);
}

TupleDesc
Py_CreateTupleDescCopyConstr(TupleDesc td)
{
	volatile TupleDesc ntd = NULL;

	PG_TRY();
	{
		ntd = CreateTupleDescCopyConstr(td);
		ntd->tdrefcount = -1;
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return(ntd);
}

void
Py_FreeTupleDesc(TupleDesc td)
{
	PG_TRY();
	{
		FreeTupleDesc(td);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
	}
	PG_END_TRY();
}

/*
 * Convert the Python object to an Oid.
 */
int
Oid_FromPyObject(PyObj ob, Oid *out)
{
	const static unsigned long maxoid = 0xFFFFFFFF;
	unsigned long tmpul;
	PyObj tmp;

	*out = InvalidOid;

	/*
	 * Special case the reg* types and Oid itself.
	 */
	if (PyPgObject_Check(ob))
	{
		Oid typeoid = PyPgType_GetOid(Py_TYPE(ob));

		switch (typeoid)
		{
			case REGPROCEDUREOID:
			case REGCLASSOID:
			case REGOPEROID:
			case REGOPERATOROID:
			case REGPROCOID:
			case REGTYPEOID:
			case OIDOID:
				*out = DatumGetObjectId(PyPgObject_GetDatum(ob));
				return(0);
			break;

			default:
				/*
				 * Fall through to PyNumber_Long conversion.
				 */
			break;
		}
	}

	/*
	 * Convert to long and get the unsigned long.
	 */
	tmp = PyNumber_Long(ob);
	if (tmp == NULL)
		return(-1);

	tmpul = PyLong_AsUnsignedLong(tmp);
	Py_DECREF(tmp);

	if (PyErr_Occurred())
		return(-1);

	if (tmpul > maxoid)
	{
		PyErr_SetString(PyExc_OverflowError,
			"number object overflows Oid");
	}

	*out = (Oid) tmpul;
	return(0);
}

/*
 * Py_NormalizeRow - unroll the given PyObject into an PyTupleObject
 *
 * This routine is used by type/record.c and pl.c to construct a tuple
 * object that is appropriate for use with Py_BuildDatumsAndNulls
 */
PyObj
Py_NormalizeRow(int rnatts, TupleDesc td, PyObj namemap, PyObj row)
{
	PyObj rob;

	if (PyDict_CheckExact(row))
	{
		Py_ssize_t pos = 0;
		PyObj key, val;
		rob = PyTuple_New(rnatts);
		if (rob == NULL)
			return(NULL);

		while (PyDict_Next(row, &pos, &key, &val))
		{
			unsigned long l;
			PyObj idx;

			if (!PySequence_Contains(namemap, key))
			{
				Py_DECREF(rob);
				PyErr_SetObject(PyExc_KeyError, key);
				return(NULL);
			}

			idx = PyDict_GetItem(namemap, key);

			l = PyLong_AsUnsignedLong(idx);
			if (l >= rnatts || l < 0)
			{
				/*
				 * Near "can't happen" case as the namemap
				 * is built against the given TupleDesc.
				 */
				PyErr_SetString(PyExc_RuntimeError,
					"key resolved to invalid offset");
				Py_DECREF(rob);
				return(NULL);
			}

			PyTuple_SET_ITEM(rob, (Py_ssize_t) l, val);
			Py_INCREF(val);
		}

		/*
		 * Fill in the NULLs with None.
		 */
		for (pos = 0; pos < rnatts; ++pos)
		{
			val = PyTuple_GET_ITEM(rob, pos);
			if (val == NULL)
			{
				PyTuple_SET_ITEM(rob, pos, Py_None);
				Py_INCREF(Py_None);
			}
		}
	}
	else if (PySequence_Check(row) || PyIter_Check(row))
	{
		if (PyTuple_Check(row))
		{
			rob = row;
			Py_INCREF(rob);
		}
		else
		{
			rob = Py_Call((PyObj) &PyTuple_Type, row);
			if (rob == NULL)
				return(NULL);
		}

		if (PyTuple_GET_SIZE(rob) != rnatts)
		{
			PyErr_Format(PyExc_TypeError,
				"descriptor requires exactly %d attributes, given sequence has %d items",
				rnatts, PyTuple_GET_SIZE(rob));
			Py_DECREF(rob);
			rob = NULL;
		}
	}
	else
	{
		PyErr_Format(PyExc_TypeError,
			"cannot normalize row from '%s'", Py_TYPE(row)->tp_name);
		rob = NULL;
	}

	return(rob);
}

/*
 * Py_BuildDatumsAndNulls - create datums from PyPgTypes and and objects.
 *
 * Given a TupleDesc, td, an array of PyPgTypes, typs, and an array
 * of arbitrary Python objects, fill in the preallocated values and
 * nulls with the result of the tp_new_datum calls.
 *
 * On failure, a Postgres error is raised.
 */
void
Py_BuildDatumsAndNulls(
	TupleDesc td,
	PyObj typs, PyObj row,
	Datum *values, bool *nulls)
{
	Form_pg_attribute *att = td->attrs;
	int a, i, natts = td->natts;

	for (i = 0, a = 0; i < natts; ++i)
	{
		PyObj ob, typ;

		if (att[i]->attisdropped)
		{
			nulls[i] = true;
			values[i] = 0;
			continue;
		}

		ob = PyTuple_GET_ITEM(row, a);
		typ = PyTuple_GET_ITEM(typs, i);
		if (ob == Py_None)
		{
			nulls[i] = true;
			values[i] = 0;
		}
		else
		{
			/*
			 * Postgres ERROR thrown on failure.
			 */
			PyPgType_DatumNew(
				typ, ob, att[i]->atttypmod,
				&(values[i]), &(nulls[i]));
		}

		++a;
	}
}

void
FreeReferences(int *byrefmap, Datum *values, bool *nulls)
{
	int i = 0, attoff;

	while ((attoff = byrefmap[i]) != -1)
	{
		if (!nulls[attoff])
			pfree(DatumGetPointer(values[attoff]));
		++i;
	}
}

void
FreeDatumsAndNulls(int *byrefmap, Datum *values, bool *nulls)
{
	FreeReferences(byrefmap, values, nulls);
	pfree(nulls);
	pfree(values);
}

void
raise_spi_error(int spi_error)
{
	switch (spi_error)
	{
		case SPI_ERROR_COPY:
			ereport(ERROR, (
				errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
				errmsg("copy commands prohibited")));
		break;

		case SPI_ERROR_TRANSACTION:
			ereport(ERROR, (
				errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
				errmsg("transaction commands prohibited"),
				errhint("The \"xact()\" context manager can be used to start a subtransaction.")));
		break;

		case SPI_ERROR_PARAM:
			/*
			 * This case *should* be caught before it's possible.
			 */
			elog(ERROR, "statement requires parameters");
		break;

		case SPI_ERROR_ARGUMENT:
			/* PL programming error */
			elog(ERROR, "invalid arguments given to SPI interface");
		break;

		default:
			elog(ERROR, "unexpected SPI error(%d): %s",
				spi_error, SPI_result_code_string(spi_error));
		break;
	}
}
