/*
 * Postgres cursors
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#include "postgres.h"
#include "access/heapam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/catversion.h"
#include "executor/executor.h"
#include "executor/execdesc.h"
#include "nodes/params.h"
#include "parser/analyze.h"
#include "tcop/tcopprot.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/palloc.h"
#include "utils/portal.h"
#include "utils/relcache.h"
#include "utils/typcache.h"
#include "utils/tuplestore.h"
#include "executor/spi.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/strings.h"
#include "pypg/externs.h"
#include "pypg/pl.h"
#include "pypg/error.h"
#include "pypg/function.h"

#include "pypg/tupledesc.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/record.h"

#include "pypg/statement.h"
#include "pypg/cursor.h"

#define whence__INVALID -1
#define whence__ABSOLUTE 0
#define whence__RELATIVE 1
#define whence__FROM_END 2

#define whence__ABSOLUTE_txt "ABSOLUTE"
#define whence__RELATIVE_txt "RELATIVE"
#define whence__FROM_END_txt "FROM_END"

/*
 * Given a Python object, return the appropriate whence__* integer(0, or 1, or 2).
 *
 * returns -1 on error
 */
static int
resolve_whence(PyObj whence_ob)
{
	int whence;

	if (PyUnicode_Check(whence_ob))
	{
		PyObj sbytes;
		char *s;

		sbytes = PyObject_AsASCIIString(whence_ob);
		if (sbytes == NULL)
		{
			PyErr_SetString(PyExc_ValueError,
				"could not convert whence parameter to byte string");
			return(whence__INVALID);
		}

		s = PyBytes_AS_STRING(whence_ob);

		if (strcasecmp(s, whence__ABSOLUTE_txt) == 0)
			whence = whence__ABSOLUTE;
		else if (strcasecmp(s, whence__RELATIVE_txt) == 0)
			whence = whence__RELATIVE;
		else if (strcasecmp(s, whence__FROM_END_txt) == 0)
			whence = whence__FROM_END;
		else
		{
			PyErr_SetString(PyExc_ValueError, "invalid whence");
			return(whence__INVALID);
		}
		Py_DECREF(whence_ob);
	}
	else
	{
		int overflow;
		long l;
		PyObj lob;

		lob = PyNumber_Long(whence_ob);
		if (lob == NULL)
		{
			PyErr_SetString(PyExc_ValueError,
				"invalid whence");
			return(whence__INVALID);
		}

		l = PyLong_AsLongAndOverflow(lob, &overflow);
		Py_DECREF(lob);
		if (overflow)
		{
			PyErr_SetString(PyExc_ValueError, "invalid whence");
			return(whence__INVALID);
		}

		switch(l)
		{
			case whence__ABSOLUTE:
			case whence__RELATIVE:
			case whence__FROM_END:
				return(l);
			break;
			default:
				PyErr_SetString(PyExc_ValueError,
					"invalid whence");
				whence = whence__INVALID;
			break;
		}
	}

	return(whence);
}

/*
 * If the cursor is closed, set a database error.
 */
static int
cursor_is_closed(PyObj self, const char *action)
{
	if (PyPgCursor_IsClosed(self))
	{
		/*
		 * It's closed.
		 */
		PG_TRY();
		{
			ereport(ERROR, (
				errmsg("cannot use '%s' operation on a closed cursor", action),
				errhint("All Postgres.Cursor objects are closed at the end of a transaction.")
			));
		}
		PG_CATCH();
		{
			PyErr_SetPgError(false);
		}
		PG_END_TRY();

		return(-1);
	}
	return(0);
}

/*
 * Get more rows from the Portal in the PyPgCursor
 *
 * Caller needs to protect from improper use(DB_IS_NOT_READY()).
 */
static PyObj
get_more(PyObj self, bool forward, long count)
{
	MemoryContext former = CurrentMemoryContext;
	PyObj output = PyPgCursor_GetOutput(self);
	Portal p = PyPgCursor_GetPortal(self);
	volatile PyObj rob = NULL;
	uint32 i = 0;

	PG_TRY();
	{
		SPI_cursor_fetch(p, forward, count);
		if (SPI_result < 0)
			raise_spi_error(SPI_result);
		i = 1;

		Assert(equalTupleDescs(PyPgType_GetTupleDesc(output), SPI_tuptable->tupdesc));

		/*
		 * Allocate a list for all the fetched tuples.
		 */
		rob = PyList_New(SPI_processed);

		/*
		 * Switch to Python context for DatumTuple allocations.
		 */
		MemoryContextSwitchTo(PythonMemoryContext);
		for (i = 0; i < SPI_processed; ++i)
		{
			HeapTuple ht;
			PyObj row;

			ht = SPI_tuptable->vals[i];
			row = PyPgObject_FromPyPgTypeAndHeapTuple(output, ht);
			if (row == NULL)
			{
				Py_DECREF(rob);
				rob = NULL;
				break;
			}
			PyList_SET_ITEM(rob, i, row);
		}

		SPI_freetuptable(SPI_tuptable);
		MemoryContextSwitchTo(former);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		Py_XDECREF(rob);
		rob = NULL;
		MemoryContextSwitchTo(former);
	}
	PG_END_TRY();

	return(rob);
}

/*
 * Get more rows from the Portal in the PyPgCursor (for column cursors)
 *
 * Caller needs to protect from improper use(DB_IS_NOT_READY()).
 */
static PyObj
column_get_more(PyObj self, bool forward, long count)
{
	MemoryContext former = CurrentMemoryContext;
	PyObj output = PyPgCursor_GetOutput(self);
	PyObj output_column;
	Portal p = PyPgCursor_GetPortal(self);
	volatile PyObj rob = NULL;
	uint32 i = 0;

	output_column = PyPgType_GetPyPgTupleDesc(output);
	output_column = PyPgTupleDesc_GetTypesTuple(output_column);

	assert(PyTuple_GET_ITEM(output_column, 0) != Py_None);
	output_column = PyTuple_GET_ITEM(output_column, 0);

	PG_TRY();
	{
		SPI_cursor_fetch(p, forward, count);
		if (SPI_result < 0)
			raise_spi_error(SPI_result);
		i = 1;

		Assert(equalTupleDescs(PyPgType_GetTupleDesc(output), SPI_tuptable->tupdesc));

		/*
		 * Allocate a list for all the fetched tuples.
		 */
		rob = PyList_New(SPI_processed);

		for (i = 0; i < SPI_processed; ++i)
		{
			HeapTuple ht;
			PyObj att;
			Datum firstatt;
			bool isnull;

			ht = SPI_tuptable->vals[i];
			firstatt = fastgetattr(ht, 1, SPI_tuptable->tupdesc, &isnull);

			if (isnull)
			{
				att = Py_None;
				Py_INCREF(att);
			}
			else
			{
				att = PyPgObject_New(output_column, firstatt);
				if (att == NULL)
				{
					Py_DECREF(rob);
					rob = NULL;
					break;
				}
			}
			PyList_SET_ITEM(rob, i, att);
		}

		SPI_freetuptable(SPI_tuptable);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		Py_XDECREF(rob);
		rob = NULL;
	}
	PG_END_TRY();

	MemoryContextSwitchTo(former);
	return(rob);
}

static PyObj
cursor_get_column_names(PyObj self, void *unused)
{
	PyObj output, tdo, names;

	if (!PyPgStatement_ReturnsRows(self))
	{
		Py_INCREF(Py_None);
		return(Py_None);
	}

	output = PyPgCursor_GetOutput(self);
	tdo = PyPgType_GetPyPgTupleDesc(output);

	names = PyPgTupleDesc_GetNames(tdo);
	Py_INCREF(names);

	return(names);
}

static PyObj
cursor_get_column_types(PyObj self, void *unused)
{
	PyObj output, tdo;

	output = PyPgCursor_GetOutput(self);
	tdo = PyPgType_GetPyPgTupleDesc(output);

	return(PyPgTupleDesc_GetTypes(tdo));
}

static PyObj
cursor_get_pg_column_types(PyObj self, void *unused)
{
	PyObj output, tdo;

	output = PyPgCursor_GetOutput(self);
	tdo = PyPgType_GetPyPgTupleDesc(output);

	return(PyPgTupleDesc_GetTypeOids(tdo));
}

static PyObj
cursor_get_direction(PyObj self, void *unused)
{
	PyObj rob;

	if (!PyPgCursor_IsDeclared(self))
		rob = Py_True;
	else
	{
		if (PyPgCursor_GetChunksize(self) == CUR_SCROLL_FORWARD)
			rob = Py_True;
		else
			rob = Py_False;
	}

	Py_INCREF(rob);
	return(rob);
}

static int
cursor_set_direction(PyObj self, PyObj val, void *unused)
{
	int truth;

	if (!PyPgCursor_IsDeclared(self))
	{
		PyErr_SetString(PyExc_RuntimeError, "cannot change direction with NO SCROLL cursors");
		return(-1);
	}

	truth = PyObject_IsTrue(val);
	if (truth == -1)
		return(-1);

	PyPgCursor_SetChunksize(self, truth ? 1 : -1);

	return(0);
}

static PyObj
cursor_get_chunksize(PyObj self, void *unused)
{
	if (PyPgCursor_IsDeclared(self))
		return(PyLong_FromSsize_t(1));
	else if (PyPgCursor_IsRows(self))
		return(PyLong_FromSsize_t(PyPgCursor_GetRowsReadSize(self)));
	else
		return(PyLong_FromSsize_t(PyPgCursor_GetChunksReadSize(self)));
}

static int
cursor_set_chunksize(PyObj self, PyObj val, void *unused)
{
	Py_ssize_t size;

	if (PyPgCursor_IsDeclared(self) || PyPgCursor_IsColumn(self))
	{
		PyErr_Format(PyExc_RuntimeError, "chunksize is immutable for \"%s\" cursors",
						PyPgCursor_IsDeclared(self) ? "declared" : "column");
	}

	size = PyNumber_AsSsize_t(val, NULL) + 1;
	if (size < 1)
	{
		PyErr_Format(PyExc_ValueError, "invalid chunksize %l", (long) size);
		return(-1);
	}

	if (PyPgCursor_IsRows(self))
		PyPgCursor_SetChunksize(self, -(size + 1));
	else
		PyPgCursor_SetChunksize(self, size);

	return(0);
}

static PyGetSetDef PyPgCursor_GetSet[] = {
	{"column_names", cursor_get_column_names, NULL,
		PyDoc_STR("name of the columns produced by the cursor")},
	{"column_types", cursor_get_column_types, NULL,
		PyDoc_STR("types of the columns produced by the cursor")},
	{"pg_column_types", cursor_get_pg_column_types, NULL,
		PyDoc_STR("type Oids of the columns returned by the cursor")},
	/* for scrollable cursors */
	{"direction", cursor_get_direction, cursor_set_direction,
		PyDoc_STR("default direction of the cursor (True or False)")},
	{"chunksize", cursor_get_chunksize, cursor_set_chunksize,
		PyDoc_STR("change the number of tuples to be read from "
						"the Portal when more are needed")},
	{NULL}
};

static PyMemberDef PyPgCursor_Members[] = {
	{"output", T_OBJECT, offsetof(struct PyPgCursor, cur_output), READONLY,
		PyDoc_STR("A Postgres.Type of the statement's result")},
	{"statement", T_OBJECT, offsetof(struct PyPgCursor, cur_statement), READONLY,
		PyDoc_STR("The Postgres.Statement object that created the cursor")},
	{"parameters", T_OBJECT, offsetof(struct PyPgCursor, cur_parameters), READONLY,
		PyDoc_STR("The original parameters given to statement")},
	{NULL}
};

/*
 * Explicitly close the cursor.
 */
static PyObj
cursor_close(PyObj self)
{
	if (PyPgCursor_Close(self))
		return(NULL);

	Py_INCREF(Py_None);
	return(Py_None);
}

static PyObj
cursor_clone(PyObj self)
{
	PyObj rob;

	if (DB_IS_NOT_READY())
		return(NULL);

	rob = PyPgCursor_NEW(
		Py_TYPE(self),
		PyPgCursor_GetStatement(self),
		PyPgCursor_GetParameters(self),
		NULL, /* keywords */
		PyPgCursor_GetChunksize(self));

	return(rob);
}

static PyObj
cursor_seek(PyObj self, PyObj args, PyObj kw)
{
	static char *words[] = {"position", "whence", NULL};
	Py_ssize_t chunksize;
	PyObj whence_ob = NULL;
	Portal p;
	PyObj rob;
	int whence;
	long count;

	if (DB_IS_NOT_READY() || cursor_is_closed(self, "seek"))
		return(NULL);

	if (!PyPgCursor_IsDeclared(self))
	{
		PyErr_SetString(PyExc_RuntimeError, "cannot seek NO SCROLL cursors");
		return(NULL);
	}

	chunksize = PyPgCursor_GetChunksize(self);

	if (!PyArg_ParseTupleAndKeywords(args, kw, "l|O:seek", words,
			&count, &whence_ob))
		return(NULL);

	whence = whence__ABSOLUTE;
	if (whence_ob)
	{
		whence = resolve_whence(whence_ob);
		if (whence == whence__INVALID)
			return(NULL);
	}

	p = PyPgCursor_GetPortal(self);

	PG_TRY();
	{
		bool dir = chunksize == 1 ? true : false;

		if (dir == false)
		{
			/*
			 * Configured direction is backwards.
			 * Reverse whence and count as needed.
			 */
			if (whence == whence__ABSOLUTE)
				whence = whence__FROM_END;
			else if (whence == whence__RELATIVE)
				count = -count;
			else
				whence = whence__ABSOLUTE;
		}

		switch (whence)
		{
			case whence__ABSOLUTE:
				if (count >= 0)
					SPI_scroll_cursor_move(p, FETCH_ABSOLUTE, count);
				else if (count < 0)
					elog(ERROR, "cannot seek to negative offset using ABSOLUTE");
			break;
			case whence__RELATIVE:
				if (count > 0)
					SPI_scroll_cursor_move(p, FETCH_FORWARD, count);
				else if (count < 0)
					SPI_scroll_cursor_move(p, FETCH_BACKWARD, -count);
				/*
				 * Nothing needs to be done if it's zero.
				 */
			break;
			case whence__FROM_END:
				if (count < 0)
					elog(ERROR, "cannot seek to negative offset using FROM_END");
				/*
				 * Go to the last record.
				 */
				SPI_scroll_cursor_move(p, FETCH_ABSOLUTE, -1);

				if (count == 0)
					SPI_scroll_cursor_move(p, FETCH_FORWARD, 1); /* the very end */
				else if (count - 1 != 0)
					SPI_scroll_cursor_move(p, FETCH_BACKWARD, count-1);
			break;
		}

		rob = PyLong_FromUnsignedLong(SPI_processed);
	}
	PG_CATCH();
	{
		rob = NULL;
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return(rob);
}

static PyObj
cursor_read(PyObj self, PyObj args, PyObj kw)
{
	static char *words[] = {"quantity", "direction", NULL};
	long count = -1;
	bool forward;
	int dir;
	PyObj direction = NULL, rob;

	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyPgCursor_IsDeclared(self))
	{
		PyErr_SetString(PyExc_RuntimeError, "cannot use read method with NO SCROLL cursors");
		return(NULL);
	}

	if (cursor_is_closed(self, "read"))
		return(NULL);

	if (!PyArg_ParseTupleAndKeywords(args, kw, "|lO:read", words, &count, &direction))
		return(NULL);

	/*
	 * PyPgCursor's have a configured direction when SCROLL.
	 * 1 means FORWARD and -1 means BACKWARD
	 */
	dir = PyPgCursor_GetChunksize(self);

	if (direction != NULL && direction != Py_None)
	{
		int is_true;

		if (PyUnicode_Check(direction))
		{
			PyObj bytes = PyUnicode_AsASCIIString(direction);
			if (bytes == NULL)
			{
				PyErr_SetString(PyExc_ValueError,
					"invalid direction parameter given to Postgres.Cursor.read");
				return(NULL);
			}

			if (strcasecmp("FORWARD", PyBytes_AS_STRING(bytes)) == 0)
				is_true = 1;
			else if (strcasecmp("BACKWARD", PyBytes_AS_STRING(bytes)) == 0)
				is_true = 0;
			else
			{
				PyErr_SetString(PyExc_ValueError, "unrecognized direction string");
				is_true = -1;
			}
			Py_DECREF(bytes);
		}
		else
			is_true = PyObject_IsTrue(direction);

		if (is_true == -1)
			return(NULL); /* exception */

		/*
		 * FORWARD direction on FORWARD configured cursor goes FORWARD
		 * BACKWARD direction on FORWARD configured cursor goes BACKWARD
		 * FORWARD direction on BACKWARD configured cursor goes BACKWARD
		 * BACKWARD direction on BACKWARD configured cursor goes FORWARD
		 */
		dir = dir * (is_true ? 1 : -1);
	}

	if (dir == 1)
		forward = true;
	else
		forward = false; /* -1 */

	if (count < 0)
	{
		Py_ssize_t last_count;

		rob = PyList_New(0);

		/*
		 * read more and append until the last_count != request size.
		 *
		 * either the end will be reached via last_count < 100 or last_count
		 * will be explicitly set to 0 in order to break from the loop.
		 */
		do
		{
			PyObj rows = get_more(self, forward, 100);

			if (rows)
			{
				PyObj newrob = PySequence_Concat(rob, rows);

				/* zero will indicate loop needs to stop */
				last_count = newrob ? PyList_GET_SIZE(rows) : 0;

				/*
				 * Don't need these anymore; there is a new return object.
				 */
				Py_DECREF(rob);
				Py_DECREF(rows);

				/*
				 * If PySequence_Concat fails, newrob is NULL. Now, rob is NULL and
				 * the loop will break due to last_count being set to zero above.
				 */
				rob = newrob;
			}
			else
			{
				Py_DECREF(rob);
				last_count = 0;
				rob = NULL;
			}
		} while (last_count == 100);
	}
	else
	{
		/*
		 * Explicit count requested.
		 */
		rob = get_more(self, forward, count);
	}

	return(rob);
}

static PyMethodDef PyPgCursor_Methods[] = {
	{"close", (PyCFunction) cursor_close, METH_NOARGS,
		PyDoc_STR("close the cursor; further use will cause an exception")},
	{"clone", (PyCFunction) cursor_clone, METH_NOARGS,
		PyDoc_STR("create a copy of the statement")},
	{"seek", (PyCFunction) cursor_seek, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("change the position of the cursor")},
	{"read", (PyCFunction) cursor_read, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("read the given number of rows from the cursor")},
	{NULL}
};

static void
cursor_dealloc(PyObj self)
{
	PyObj ob;

	ob = PyPgCursor_GetStatement(self);
	PyPgCursor_SetStatement(self, NULL);
	Py_XDECREF(ob);

	ob = PyPgCursor_GetParameters(self);
	PyPgCursor_SetParameters(self, NULL);
	Py_XDECREF(ob);

	ob = PyPgCursor_GetOutput(self);
	PyPgCursor_SetOutput(self, NULL);
	Py_XDECREF(ob);

	ob = PyPgCursor_GetBuffer(self);
	PyPgCursor_SetBuffer(self, NULL);
	Py_XDECREF(ob);

	if (!PyPgCursor_IsClosed(self))
	{
		Portal p = PyPgCursor_GetPortal(self);
		PyPgCursor_SetPortal(self, NULL);

		/*
		 * Don't bother closing the cursor unless we're inside a transaction.
		 */
		if (pl_state >= 0)
		{
			MemoryContext former = CurrentMemoryContext;

			/*
			 * Only close the cursor if we are *not* between transactions.
			 *
			 * It's pretty much a waste of time and we don't know if the portal
			 * has been dropped yet or not.
			 */
			PG_TRY();
			{
				SPI_cursor_close(p);

				/*
				 * When PLPY_STRANGE_THINGS is defined.
				 */
				RaiseAStrangeError
			}
			PG_CATCH();
			{
				PyErr_EmitPgErrorAsWarning("failed to close Postgres.Cursor's Portal");
			}
			PG_END_TRY();
			MemoryContextSwitchTo(former);
		}
	}

	Py_TYPE(self)->tp_free(self);
}

/*
 * Just return self.
 */
static PyObj
cursor_iter(PyObj self)
{
	Py_INCREF(self);
	return(self);
}

static PyObj
cursor_next(PyObj self)
{
	PyObj rob = NULL;

	if (DB_IS_NOT_READY() || cursor_is_closed(self, "next"))
		return(NULL);

	if (PyPgCursor_IsDeclared(self))
	{
		/* Scrollable Cursor */
		bool forward = PyPgCursor_GetDirection(self);

		rob = get_more(self, forward, 1);
		if (rob == NULL)
			return(NULL);
		if (PyList_Size(rob) == 0)
		{
			Py_DECREF(rob);
			rob = NULL;
		}
		else
		{
			PyObj row;
			row = PyList_GET_ITEM(rob, 0);
			Py_INCREF(row);
			Py_DECREF(rob);
			rob = row;
		}
	}
	else if (PyPgCursor_IsChunks(self))
	{
		Py_ssize_t chunksize = PyPgCursor_GetChunksReadSize(self);
		/* Get next chunk */
		rob = get_more(self, true, chunksize);
		if (rob == NULL)
			return(NULL);

		if (PyList_Size(rob) == 0)
		{
			Py_DECREF(rob);
			rob = NULL;
		}
	}
	else
	{
		PyObj buf = PyPgCursor_GetBuffer(self);

		/*
		 * Active buffer iterable? Get the next row.
		 * If it's the edge of `buf`, run get_more(self)
		 */
		if (buf != NULL)
			rob = PyIter_Next(buf);

		/*
		 * There is no iterator, or it's at the end of the iterator.
		 */
		if (rob == NULL)
		{
			PyObj chunk;
			/*
			 * Need a new chunk.
			 */
			if (PyPgCursor_IsColumn(self))
				chunk = column_get_more(self, true, 50);
			else
				chunk = get_more(self, true, PyPgCursor_GetRowsReadSize(self));

			if (chunk == NULL)
				return(NULL);

			/*
			 * Got chunk, apply the iterator of the chunk to the cursor.
			 */
			PyPgCursor_SetBuffer(self, PyObject_GetIter(chunk));
			Py_DECREF(chunk);

			/*
			 * Release old iterator if there was one.
			 */
			Py_XDECREF(buf);

			buf = PyPgCursor_GetBuffer(self);
			/*
			 * GetIter threw error? Exit.
			 */
			if (buf == NULL)
				return(NULL);

			/*
			 * At this point, if rob is NULL, there is
			 * nothing left in the cursor.
			 */
			rob = PyIter_Next(buf);
		}
	}

	return(rob);
}

PyDoc_STRVAR(cursor_doc, "Postgres cursor object (Portal interface)");
PyTypeObject PyPgCursor_Type = {
	PyVarObject_HEAD_INIT(&PyType_Type, 0)
	"Postgres.Cursor",					/* tp_name */
	sizeof(struct PyPgCursor),			/* tp_basicsize */
	0,									/* tp_itemsize */
	cursor_dealloc,						/* tp_dealloc */
	NULL,								/* tp_print */
	NULL,								/* tp_getattr */
	NULL,								/* tp_setattr */
	NULL,								/* tp_compare */
	NULL,								/* tp_repr */
	NULL,								/* tp_as_number */
	NULL,								/* tp_as_sequence */
	NULL,								/* tp_as_mapping */
	NULL,								/* tp_hash */
	NULL,								/* tp_call */
	NULL,								/* tp_str */
	NULL,								/* tp_getattro */
	NULL,								/* tp_setattro */
	NULL,								/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,					/* tp_flags */
	cursor_doc,							/* tp_doc */
	NULL,								/* tp_traverse */
	NULL,								/* tp_clear */
	NULL,								/* tp_richcompare */
	0,									/* tp_weaklistoffset */
	cursor_iter,						/* tp_iter */
	cursor_next,						/* tp_iternext */
	PyPgCursor_Methods,					/* tp_methods */
	PyPgCursor_Members,					/* tp_members */
	PyPgCursor_GetSet,					/* tp_getset */
	NULL,								/* tp_base */
	NULL,								/* tp_dict */
	NULL,								/* tp_descr_get */
	NULL,								/* tp_descr_set */
	0,									/* tp_dictoffset */
	NULL,								/* tp_init */
	NULL,								/* tp_alloc */
	NULL,								/* tp_new */
};

PyObj
PyPgCursor_NEW(
	PyTypeObject *subtype,
	PyObj statement,
	PyObj args, PyObj kw,
	Py_ssize_t chunksize)
{
	PyObj tdo = PyPgStatement_GetInput(statement);
	TupleDesc td = PyPgTupleDesc_GetTupleDesc(tdo);
	SPIPlanPtr plan;
	PyObj output = PyPgStatement_GetOutput(statement);
	Portal p = NULL;
	PyObj pargs, rob = NULL;

	if (chunksize == CUR_UNINITIALIZED)
	{
		/*
		 * "Impossible", but protect against it anyways.
		 * (Exposed statement interfaces don't take a chunksize parameter)
		 */
		PyErr_SetString(PyExc_ValueError, "invalid chunksize parameter");
		return(NULL);
	}
	else if (chunksize == -1 || chunksize == 1)
	{
		plan = PyPgStatement_GetScrollPlan(statement);
	}
	else
	{
		plan = PyPgStatement_GetPlan(statement);
	}

	/*
	 * The plan fields are not initialized on statement creation.
	 */
	if (plan == NULL)
		return(NULL);

	/* Statement Parameters */
	pargs = Py_NormalizeRow(
		PyPgTupleDesc_GetNatts(tdo), td,
		PyPgTupleDesc_GetNameMap(tdo),
		args
	);
	if (pargs == NULL)
		return(NULL);

	rob = subtype->tp_alloc(subtype, 0);
	if (rob == NULL)
		return(NULL);
	PyPgCursor_SetPortal(rob, NULL);

	Py_INCREF(args);
	PyPgCursor_SetParameters(rob, args);

	Py_INCREF(statement);
	PyPgCursor_SetStatement(rob, statement);

	Py_INCREF(output);
	PyPgCursor_SetOutput(rob, output);

	PyPgCursor_SetChunksize(rob, chunksize);
	PyPgCursor_SetXid(rob, pl_xact_count);

	PG_TRY();
	{
		int *freemap = PyPgTupleDesc_GetFreeMap(tdo);
		Datum *datums;
		bool *nulls;
		char *cnulls;
		int i;

		datums = palloc(sizeof(Datum) * td->natts);
		nulls = palloc(sizeof(bool) * td->natts);
		cnulls = palloc(sizeof(char) * td->natts);

		Py_BuildDatumsAndNulls(
			td, PyPgTupleDesc_GetTypesTuple(tdo),
			pargs, datums, nulls);

		/* sigh */
		for (i = 0; i < td->natts; ++i)
		{
			cnulls[i] = nulls[i] ? 'n' : ' ';
		}

		p = SPI_cursor_open(NULL, plan, datums, cnulls, PL_FN_READONLY());
		PyPgCursor_SetPortal(rob, p);
		PyPgCursor_SetXid(rob, pl_xact_count);
		PyPgCursor_SetSubXid(rob, pl_subxact_rollback_count);

		FreeDatumsAndNulls(freemap, datums, nulls);
		datums = NULL;
		nulls = NULL;
		pfree(cnulls);
	}
	PG_CATCH();
	{
		Py_DECREF(rob);
		rob = NULL;
		Py_DECREF(pargs);
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();
	Py_DECREF(pargs);

	return(rob);
}

int
PyPgCursor_Close(PyObj self)
{
	if (PyPgCursor_IsClosed(self))
		PyPgCursor_SetPortal(self, NULL);
	else
	{
		Portal p;
		p = PyPgCursor_GetPortal(self);
		PyPgCursor_SetPortal(self, NULL);

		PG_TRY();
		{
			SPI_cursor_close(p);
		}
		PG_CATCH();
		{
			PyErr_SetPgError(false);
			return(-1);
		}
		PG_END_TRY();
	}

	return(0);
}
