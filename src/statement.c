/*
 * Postgres prepared statements
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#include "postgres.h"
#include "access/xact.h"
#include "commands/trigger.h"
#include "catalog/pg_proc.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#if PG_VERSION_NUM >= 80400
#include "utils/snapmgr.h"
#endif
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "executor/spi_priv.h"
#include "parser/analyze.h"
#include "mb/pg_wchar.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/extension.h"
#include "pypg/pl.h"
#include "pypg/error.h"
#include "pypg/function.h"

#include "pypg/tupledesc.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"

#include "pypg/statement.h"
#include "pypg/cursor.h"

/*
 * resolve_parameters - do some *args checks
 *
 * - Validate that kw is NULL--no methods using this take keywords.
 * - Make sure the statement's parameters havent been specified if len(args) > 0
 */
static int
resolve_parameters(PyObj self, PyObj *args, PyObj *kw)
{
	PyObj params = PyPgStatement_GetParameters(self);

	/*
	 * No keywords taken by execution methods..
	 */
	if (*kw != NULL)
	{
		PyErr_SetString(PyExc_TypeError,
			"Postgres.Statement methods do not take keywords");
		return(-1);
	}

	/*
	 * If the statement was built with constant parameters,
	 * then reject invocations with arguments.
	 */
	if (params != Py_None)
	{
		if (PySequence_Length(*args) != 0)
		{
			PyErr_SetString(PyExc_TypeError,
				"Postgres.Statement object's parameters have already been specified");
			return(-1);
		}
		else
		{
			/*
			 * Borrow the statement object's reference to the parameters.
			 */
			*args = params;
		}
	}

	return(0);
}

static PyObj
statement_get_column_names(PyObj self, void *unused)
{
	PyObj output, tdo, names;

	if (!PyPgStatement_ReturnsRows(self))
	{
		Py_INCREF(Py_None);
		return(Py_None);
	}

	output = PyPgStatement_GetOutput(self);
	tdo = PyPgType_GetPyPgTupleDesc(output);

	names = PyPgTupleDesc_GetNames(tdo);
	Py_INCREF(names);

	return(names);
}

static PyObj
statement_get_column_types(PyObj self, void *unused)
{
	PyObj output, tdo;

	if (!PyPgStatement_ReturnsRows(self))
	{
		Py_INCREF(Py_None);
		return(Py_None);
	}

	output = PyPgStatement_GetOutput(self);
	tdo = PyPgType_GetPyPgTupleDesc(output);

	return(PyPgTupleDesc_GetTypes(tdo));
}

static PyObj
statement_get_parameter_types(PyObj self, void *unused)
{
	PyObj tdo;

	tdo = PyPgStatement_GetInput(self);

	return(PyPgTupleDesc_GetTypes(tdo));
}

static PyObj
statement_get_pg_parameter_types(PyObj self, void *unused)
{
	PyObj tdo;

	tdo = PyPgStatement_GetInput(self);

	return(PyPgTupleDesc_GetTypeOids(tdo));
}

static PyObj
statement_get_pg_column_types(PyObj self, void *unused)
{
	PyObj tdo;

	if (!PyPgStatement_ReturnsRows(self))
	{
		Py_INCREF(Py_None);
		return(Py_None);
	}

	tdo = PyPgType_GetPyPgTupleDesc(PyPgStatement_GetOutput(self));

	return(PyPgTupleDesc_GetTypeOids(tdo));
}

static PyGetSetDef PyPgStatement_GetSet[] = {
	{"column_names", statement_get_column_names, NULL,
		PyDoc_STR("name of the columns produced by the statement")},
	{"column_types", statement_get_column_types, NULL,
		PyDoc_STR("types of the columns produced by the statement")},
	{"pg_column_types", statement_get_pg_column_types, NULL,
		PyDoc_STR("type Oids of the columns returned by the statement")},
	{"parameter_types", statement_get_parameter_types, NULL,
		PyDoc_STR("types of the parameters taken by the statement")},
	{"pg_parameter_types", statement_get_pg_parameter_types, NULL,
		PyDoc_STR("type Oids of the parameters taken by the statement")},
	{NULL}
};

static PyMemberDef PyPgStatement_Members[] = {
	{"input", T_OBJECT, offsetof(struct PyPgStatement, ps_input), READONLY,
		PyDoc_STR("A Postgres.TupleDesc of the statement's parameters")},
	{"output", T_OBJECT, offsetof(struct PyPgStatement, ps_output), READONLY,
		PyDoc_STR("A Postgres.Type of the statement's result")},
	{"string", T_OBJECT, offsetof(struct PyPgStatement, ps_string), READONLY,
		PyDoc_STR("The SQL that defines this statement; may or may not be a str object")},
	{"command", T_OBJECT, offsetof(struct PyPgStatement, ps_command), READONLY,
		PyDoc_STR("The CommandTag of the statement")},
	{"parameters", T_OBJECT, offsetof(struct PyPgStatement, ps_parameters), READONLY,
		PyDoc_STR("constant statement parameters; None if none.")},
	{NULL}
};

static PyObj
statement_clone(PyObj self)
{
	if (DB_IS_NOT_READY())
		return(NULL);

	return(PyPgStatement_NEW(
		Py_TYPE(self),
		PyPgStatement_GetString(self),
		PyPgStatement_GetParameters(self)
	));
}

static PyObj
statement_rows(PyObj self, PyObj args, PyObj kw)
{
	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyPgStatement_ReturnsRows(self))
	{
		PyErr_SetString(PyExc_TypeError, "statement does not return rows");
		return(NULL);
	}

	if (resolve_parameters(self, &args, &kw))
		return(NULL);

	return(PyPgCursor_New(self, args, kw, CUR_ROWS(50)));
}

static PyObj
statement_column(PyObj self, PyObj args, PyObj kw)
{
	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyPgStatement_ReturnsRows(self))
	{
		PyErr_SetString(PyExc_TypeError, "statement does not return rows");
		return(NULL);
	}

	if (resolve_parameters(self, &args, &kw))
		return(NULL);

	return(PyPgCursor_New(self, args, kw, CUR_COLUMN));
}

static PyObj
statement_first(PyObj self, PyObj args, PyObj kw)
{
	MemoryContext former = CurrentMemoryContext;
	PyObj c, rob = NULL;

	if (resolve_parameters(self, &args, &kw))
		return(NULL);

	if (DB_IS_NOT_READY())
		return(NULL);

	if (PyPgStatement_ReturnsRows(self))
	{
		c = PyPgCursor_New(self, args, kw, CUR_ROWS(1));

		if (c != NULL)
		{
			PyObj r;

			r = PyIter_Next(c);
			if (!PyErr_Occurred() && r == NULL)
			{
				r = Py_None;
				Py_INCREF(r);
			}

			if (PyPgCursor_Close(c))
			{
				Py_DECREF(c);
				Py_XDECREF(r);
				return(NULL);
			}

			if (r != NULL)
			{
				if (r == Py_None)
					rob = r;
				else
				{
					Py_ssize_t s = PySequence_Size(r);

					if (s == -1)
						rob = NULL;
					else if (s == 1)
					{
						rob = PySequence_GetItem(r, 0);
						Py_DECREF(r);
					}
					else
					{
						/*
						 * It has multiple columns, so return the first row.
						 */
						rob = r;
					}
				}
			}
			Py_DECREF(c);
		}
	}
	else
	{
		SPIPlanPtr plan;
		PyObj tdo = PyPgStatement_GetInput(self);
		TupleDesc td = PyPgTupleDesc_GetTupleDesc(tdo);
		PyObj pargs;

		plan = PyPgStatement_GetPlan(self);
		if (plan == NULL)
			return(NULL);

		pargs = Py_NormalizeRow(
			PyPgTupleDesc_GetNatts(tdo), td,
			PyPgTupleDesc_GetNameMap(tdo),
			args
		);
		if (pargs == NULL)
			return(NULL);

		PG_TRY();
		{
			int r;
			Datum *datums;
			bool *nulls;
			char *cnulls;
			int *freemap = PyPgTupleDesc_GetFreeMap(tdo);

			datums = palloc(sizeof(Datum) * td->natts);
			nulls = palloc(sizeof(bool) * td->natts);
			cnulls = palloc(sizeof(char) * td->natts);

			Py_BuildDatumsAndNulls(
				td, PyPgTupleDesc_GetTypesTuple(tdo), pargs,
				datums, nulls
			);

			for (r = 0; r < td->natts; ++r)
			{
				cnulls[r] = nulls[r] ? 'n' : ' ';
			}
			r = SPI_execute_plan(plan, datums, cnulls, PL_FN_READONLY(), 1);
			if (r < 0)
				raise_spi_error(r);
			rob = PyLong_FromUnsignedLong(SPI_processed);

			FreeDatumsAndNulls(freemap, datums, nulls);
			pfree(cnulls);
		}
		PG_CATCH();
		{
			PyErr_SetPgError(false);
			Py_XDECREF(rob);
			rob = NULL;
		}
		PG_END_TRY();

		Py_DECREF(pargs);
	}
	MemoryContextSwitchTo(former);

	return(rob);
}

static PyObj
statement_chunks(PyObj self, PyObj args, PyObj kw)
{
	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyPgStatement_ReturnsRows(self))
	{
		PyErr_SetString(PyExc_TypeError, "statement does not return rows");
		return(NULL);
	}

	if (resolve_parameters(self, &args, &kw))
		return(NULL);

	return(PyPgCursor_New(self, args, kw, CUR_CHUNKS(100)));
}

static PyObj
statement_declare(PyObj self, PyObj args, PyObj kw)
{
	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyPgStatement_ReturnsRows(self))
	{
		PyErr_SetString(PyExc_TypeError, "statement does not return rows");
		return(NULL);
	}

	if (resolve_parameters(self, &args, &kw))
		return(NULL);

	return(PyPgCursor_New(self, args, kw, CUR_SCROLL_FORWARD));
}

static int
load_rows(PyObj self, PyObj row_iter, uint32 *total)
{
	MemoryContext former = CurrentMemoryContext;
	volatile PyObj row;
	Datum *datums;
	bool *nulls;
	char *cnulls;
	int r = 0;
	SPIPlanPtr plan;

	Assert(!ext_state);
	Assert(PyIter_Check(row_iter));

	plan = PyPgStatement_GetPlan(self);
	if (plan == NULL)
		return(-1);

	PG_TRY();
	{
		PyObj tdo = PyPgStatement_GetInput(self);
		PyObj typs = PyPgTupleDesc_GetTypesTuple(tdo);
		PyObj namemap = PyPgTupleDesc_GetNameMap(tdo);
		TupleDesc td = PyPgTupleDesc_GetTupleDesc(tdo);
		int rnatts = PyPgTupleDesc_GetNatts(tdo);
		int *freemap = PyPgTupleDesc_GetFreeMap(tdo);
		int spi_r;

		datums = palloc(sizeof(Datum) * td->natts);
		nulls = palloc(sizeof(bool) * td->natts);
		cnulls = palloc(sizeof(char) * td->natts);

		while ((row = PyIter_Next(row_iter)))
		{
			PyObj pargs;
			pargs = Py_NormalizeRow(rnatts, td, namemap, row);
			Py_DECREF(row);
			if (pargs == NULL)
			{
				r = -1;
				break;
			}
			row = pargs;

			Py_BuildDatumsAndNulls(td, typs, row, datums, nulls);

			Py_DECREF(row);
			row = NULL;

			/* borrow spi_r for a moment */
			for (spi_r = 0; spi_r < td->natts; ++spi_r)
			{
				cnulls[spi_r] = nulls[spi_r] ? 'n' : ' ';
			}
			spi_r = SPI_execute_plan(plan, datums, cnulls, false, 1);

			/*
			 * Free the built datums.
			 */
			FreeReferences(freemap, datums, nulls);

			if (spi_r < 0)
				raise_spi_error(spi_r);

			*total = *total + SPI_processed;
		}

		pfree(datums);
		pfree(nulls);
		pfree(cnulls);
	}
	PG_CATCH();
	{
		/*
		 * WARNING: Leaks datums & nulls on error. Yay, procCxt.
		 */
		PyErr_SetPgError(false);
		Py_XDECREF(row);
		r = -1;
	}
	PG_END_TRY();
	MemoryContextSwitchTo(former);

	return(r);
}

static PyObj
statement_load_rows(PyObj self, PyObj args, PyObj kw)
{
	char *words[] = {"rows_iter", NULL};
	PyObj row_iter, rob;
	uint32 total = 0;

	if (PyPgStatement_GetParameters(self) != Py_None)
	{
		PyErr_SetString(PyExc_TypeError,
			"cannot use load_rows with constant parameters");
		return(NULL);
	}

	if (!PyArg_ParseTupleAndKeywords(args, kw, "O:load_rows", words, &row_iter))
		return(NULL);

	if (DB_IS_NOT_READY())
		return(NULL);

	row_iter = PyObject_GetIter(row_iter);
	if (row_iter == NULL)
		return(NULL);

	if (load_rows(self, row_iter, &total))
		rob = NULL;
	else
		rob = PyLong_FromUnsignedLong(total);

	Py_DECREF(row_iter);

	return(rob);
}

static PyObj
statement_load_chunks(PyObj self, PyObj args, PyObj kw)
{
	char *words[] = {"chunks_iter", NULL};
	PyObj chunk_iter, ob;
	uint32 total = 0;

	if (PyPgStatement_GetParameters(self) != Py_None)
	{
		PyErr_SetString(PyExc_TypeError,
			"cannot use load_rows with constant parameters");
		return(NULL);
	}

	if (!PyArg_ParseTupleAndKeywords(args, kw, "O:load_chunks", words, &chunk_iter))
		return(NULL);

	if (DB_IS_NOT_READY())
		return(NULL);

	chunk_iter = PyObject_GetIter(chunk_iter);
	if (chunk_iter == NULL)
		return(NULL);

	while ((ob = PyIter_Next(chunk_iter)))
	{
		PyObj row_iter;
		int r;

		row_iter = PyObject_GetIter(ob);
		Py_DECREF(ob);
		if (row_iter == NULL)
		{
			Py_DECREF(chunk_iter);
			return(NULL);
		}

		r = load_rows(self, row_iter, &total);
		Py_DECREF(row_iter);

		if (r)
		{
			Py_DECREF(chunk_iter);
			return(NULL);
		}
	}
	Py_DECREF(chunk_iter);

	return(PyLong_FromUnsignedLong(total));
}

static PyMethodDef PyPgStatement_Methods[] = {
	{"clone", (PyCFunction) statement_clone, METH_NOARGS,
		PyDoc_STR("create a copy of the statement")},
	{"rows", (PyCFunction) statement_rows, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("return an iterable to the rows produced by the statement")},
	{"column", (PyCFunction) statement_column, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("return an iterable to the first column of the rows produced by the statement")},
	{"first", (PyCFunction) statement_first, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("return the first attribute or row in the result set")},
	{"chunks", (PyCFunction) statement_chunks, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("return an iterable to the chunks of rows produced by the statement")},
	{"declare", (PyCFunction) statement_declare, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("declare a scrollable cursor using the given parameters")},
	{"load_rows", (PyCFunction) statement_load_rows, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("load the sequence of parameters using the statement")},
	{"load_chunks", (PyCFunction) statement_load_chunks, METH_VARARGS|METH_KEYWORDS,
		PyDoc_STR("load the sequence of parameter chunks using the statement")},
	{NULL}
};

static void
statement_dealloc(PyObj self)
{
	PyPgStatement ps;
	SPIPlanPtr plan, splan;
	MemoryContext memory;
	PyObj ob;

	ob = PyPgStatement_GetString(self);
	PyPgStatement_SetString(self, NULL);
	Py_XDECREF(ob);

	ob = PyPgStatement_GetInput(self);
	PyPgStatement_SetInput(self, NULL);
	Py_XDECREF(ob);

	ob = PyPgStatement_GetOutput(self);
	PyPgStatement_SetOutput(self, NULL);
	Py_XDECREF(ob);

	ob = PyPgStatement_GetParameters(self);
	PyPgStatement_SetParameters(self, NULL);
	Py_XDECREF(ob);

	ps = (PyPgStatement) self;
	plan = ps->ps_plan;
	splan = ps->ps_scroll_plan;
	ps->ps_plan = NULL;
	ps->ps_scroll_plan = NULL;
	memory = PyPgStatement_GetMemory(self);

	/*
	 * It's allocated in the statment's memory context, so just NULL it.
	 */
	PyPgStatement_SetParameterTypes(self, NULL);
	PyPgStatement_SetPath(self, NULL);

	if (plan != NULL || splan != NULL || memory != NULL)
	{
		MemoryContext former = CurrentMemoryContext;

		PG_TRY();
		{
			if (memory)
				MemoryContextDelete(memory);
			if (plan)
				SPI_freeplan(plan);
			if (splan)
				SPI_freeplan(splan);

			/*
			 * When PLPY_STRANGE_THINGS is defined.
			 */
			RaiseAStrangeError
		}
		PG_CATCH();
		{
			PyErr_EmitPgErrorAsWarning("could not deallocate statement plans");
		}
		PG_END_TRY();

		MemoryContextSwitchTo(former);
	}

	Py_TYPE(self)->tp_free(self);
}

/*
 * statement_call - execute the plan
 *
 * If the statement returns rows, return all the rows as a list.
 * If the statement does *not* return rows, return the command and count
 * in a tuple. e.g. ("INSERT", 10)
 */
static PyObj
statement_call(PyObj self, PyObj args, PyObj kw)
{
	PyObj rob = NULL;

	if (resolve_parameters(self, &args, &kw))
		return(NULL);

	if (DB_IS_NOT_READY())
		return(NULL);

	if (PyPgStatement_ReturnsRows(self))
	{
		PyObj rows, curs;

		/*
		 * Just use a chunking cursor. Nothing fancy.
		 */
		curs = PyPgCursor_New(self, args, kw, CUR_CHUNKS(10000));
		if (curs == NULL)
			return(NULL);
		rob = PyList_New(0);

		/*
		 * Concat all the chunks produced by the PyPgCursor
		 */
		while ((rows = PyIter_Next(curs)))
		{
			PyObj newrob;
			newrob = PySequence_Concat(rob, rows);
			Py_DECREF(rob);
			Py_DECREF(rows);
			rob = newrob;
		}
		Py_DECREF(curs);

		if (PyErr_Occurred())
		{
			Py_DECREF(rob);
			rob = NULL;
		}
	}
	else
	{
		/*
		 * DML/DDL return ("COMMAND", <count>) pair.
		 */
		PyObj count;

		rob = PyTuple_New(2);
		Py_INCREF(PyPgStatement_GetCommand(self));
		PyTuple_SET_ITEM(rob, 0, PyPgStatement_GetCommand(self));

		count = statement_first(self, args, kw);
		if (count == NULL)
		{
			Py_DECREF(rob);
			rob = NULL;
		}
		else
			PyTuple_SET_ITEM(rob, 1, count);
	}

	return(rob);
}

static PyObj
statement_iter(PyObj self)
{
	PyObj rob = NULL;
	PyObj args;

	args = PyTuple_New(0);
	rob = statement_rows(self, args, NULL);
	Py_DECREF(args);

	return(rob);
}

static PyObj
statement_new(PyTypeObject *subtype, PyObj args, PyObj kw)
{
	PyObj source = NULL, params = NULL, rob;
	Py_ssize_t i, nargs;

	if (kw != NULL)
	{
		PyErr_SetString(PyExc_TypeError, "Postgres.Statement does not take keyword parameters");
		return(NULL);
	}

	nargs = PyTuple_GET_SIZE(args);
	if (nargs == 0)
	{
		PyErr_SetString(PyExc_TypeError, "Postgres.Statement requires at least one argument");
		return(NULL);
	}

	source = PyTuple_GET_ITEM(args, 0);

	if (nargs > 1)
	{
		params = PyTuple_New(nargs - 1);
		for (i = 1; i < nargs; ++i)
		{
			PyObj ob = PyTuple_GET_ITEM(args, i);
			Py_INCREF(ob);
			PyTuple_SET_ITEM(params, i - 1, ob);
		}
	}
	else
	{
		params = Py_None;
		Py_INCREF(params);
	}

	if (DB_IS_NOT_READY())
		rob = NULL;
	else
		rob = PyPgStatement_NEW(subtype, source, params);

	Py_DECREF(params);

	return(rob);
}

PyDoc_STRVAR(statement_doc, "Postgres [SPI] prepared statement");
PyTypeObject PyPgStatement_Type = {
	PyVarObject_HEAD_INIT(&PyType_Type, 0)
	"Postgres.Statement",							/* tp_name */
	sizeof(struct PyPgStatement),					/* tp_basicsize */
	0,												/* tp_itemsize */
	statement_dealloc,								/* tp_dealloc */
	NULL,											/* tp_print */
	NULL,											/* tp_getattr */
	NULL,											/* tp_setattr */
	NULL,											/* tp_compare */
	NULL,											/* tp_repr */
	NULL,											/* tp_as_number */
	NULL,											/* tp_as_sequence */
	NULL,											/* tp_as_mapping */
	NULL,											/* tp_hash */
	statement_call,									/* tp_call */
	NULL,											/* tp_str */
	NULL,											/* tp_getattro */
	NULL,											/* tp_setattro */
	NULL,											/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,								/* tp_flags */
	statement_doc,									/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	statement_iter,									/* tp_iter */
	NULL,											/* tp_iternext */
	PyPgStatement_Methods,							/* tp_methods */
	PyPgStatement_Members,							/* tp_members */
	PyPgStatement_GetSet,							/* tp_getset */
	NULL,											/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	statement_new,									/* tp_new */
};

/*
 * statement_error_callback
 *
 * Add context information when a query can't be parsed and analyzed.
 *
 * Equivalent to the SPI error callback.
 */
static void
statement_error_callback(void *arg)
{
	const char *query = (const char *) arg;
	int			syntaxerrposition;

	/*
	 * If there is a syntax error position, convert to internal syntax error;
	 * otherwise treat the query as an item of context stack
	 */
	syntaxerrposition = geterrposition();
	if (syntaxerrposition > 0)
	{
		errposition(0);
		internalerrposition(syntaxerrposition);
		internalerrquery(query);
	}
	else
		errcontext("SQL statement \"%s\"", query);
}

/*
 * Parse the statement, extract the parameter types, commandTag, and resultDesc.
 */
static void
statement_get_metadata(const char *src, int *num_params,
					Oid **param_types, const char **commandTag,
					TupleDesc *resultDesc)
{
	List *raw_parsetree_list;
	Node *rpt;
	Query *query;
	ErrorContextCallback errccb;

	errccb.callback = statement_error_callback;
	errccb.arg = (void *) src;
	errccb.previous = error_context_stack;
	error_context_stack = &errccb;

	/*
	 * Parse the request string into a list of raw parse trees.
	 */
	raw_parsetree_list = pg_parse_query(src);
	if (list_length(raw_parsetree_list) != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("cannot insert multiple commands into a prepared statement")));
	}

	rpt = (Node *) linitial(raw_parsetree_list);

	/*
	 * Validate that it fits the basic criteria for a single SPI statement.
	 *
	 * Don't wait for execution time to fail on these.
	 */
	if (IsA(rpt, TransactionStmt))
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("cannot execute transaction statements"),
				errhint("The \"xact()\" context manager can be used to manage subtransactions.")));

	}

	if (IsA(rpt, CopyStmt) && ((CopyStmt *) rpt)->filename == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("cannot execute COPY TO STDOUT or FROM STDIN statements")));
	}

	/*
	 * for repalloc
	 */
	*num_params = 0;
	*param_types = (Oid *) palloc(sizeof(Oid) * 0);

	/*
	 * Don't bother running copyObject on the rpt because
	 * it's just going to be thrown away anyways.
	 */
	query = parse_analyze_varparams(rpt, src, param_types, num_params);

	*commandTag = CreateCommandTag(rpt);
	*resultDesc = PlanCacheComputeResultDesc(list_make1(query));

	error_context_stack = errccb.previous;
}

PyObj
PyPgStatement_NEW(PyTypeObject *subtype, PyObj source, PyObj parameters)
{
	MemoryContext former = CurrentMemoryContext;
	PyObj rob, source_str;
	PyObj output = NULL;

	Assert(!ext_state);

	source_str = source;
	Py_INCREF(source_str);
	PyObject_StrBytes(&source_str);
	if (source_str == NULL)
		return(NULL);

	rob = subtype->tp_alloc(subtype, 0);
	if (rob == NULL)
	{
		Py_DECREF(source_str);
		return(NULL);
	}

	if (parameters == NULL)
		parameters = Py_None;
	Py_INCREF(parameters);
	PyPgStatement_SetParameters(rob, parameters);

	Py_INCREF(source);
	PyPgStatement_SetString(rob, source);

	((PyPgStatement) rob)->ps_plan = NULL;
	((PyPgStatement) rob)->ps_scroll_plan = NULL;

	MemoryContextSwitchTo(PythonWorkMemoryContext);
	PG_TRY();
	{
		PyObj cmd;
		int num_params;
		Oid *param_types;
		TupleDesc resultDesc = NULL;
		const char *commandTag;
		MemoryContext memory;
		OverrideSearchPath *path;

		memory = AllocSetContextCreate(PythonMemoryContext,
			"StatementMemoryContext",
			ALLOCSET_SMALL_MINSIZE,
			ALLOCSET_SMALL_INITSIZE,
			ALLOCSET_SMALL_MAXSIZE);
		PyPgStatement_SetMemory(rob, memory);
		path = GetOverrideSearchPath(memory);
		PyPgStatement_SetPath(rob, path);

		statement_get_metadata(PyBytes_AS_STRING(source_str),
								&num_params, &param_types,
								&commandTag, &resultDesc);

		/*
		 * Make CommandTag PyUnicode object.
		 */
		if (commandTag != NULL)
		{
			cmd = PyUnicode_FromString(commandTag);
			if (cmd == NULL)
				PyErr_RelayException();
		}
		else
		{
			cmd = Py_None;
			Py_INCREF(cmd);
		}
		PyPgStatement_SetCommand(rob, cmd);

		if (num_params > 0)
		{
			TupleDesc input_td;
			PyObj input;
			Oid *stored_param_types;

			input_td = TupleDesc_FromNamesAndOids(num_params, NULL, param_types);
			input = PyPgTupleDesc_FromCopy(input_td);
			if (input == NULL)
				PyErr_RelayException();
			PyPgStatement_SetInput(rob, input);

			stored_param_types = MemoryContextAlloc(memory,
													sizeof(Oid) * num_params);
			memcpy(stored_param_types, param_types, sizeof(Oid) * num_params);
			PyPgStatement_SetParameterTypes(rob, stored_param_types);
			/*
			 * TupleDesc will be freed by PythonWork reset.
			 */
		}
		else
		{
			Py_INCREF(EmptyPyPgTupleDesc);
			PyPgStatement_SetInput(rob, EmptyPyPgTupleDesc);
		}

		if (resultDesc != NULL)
			output = PyPgType_FromTupleDesc(resultDesc);
		else
		{
			output = Py_None;
			Py_INCREF(output);
		}
		PyPgStatement_SetOutput(rob, output);
		if (output == NULL)
			PyErr_RelayException();
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		Py_XDECREF(rob);
		rob = NULL;
	}
	PG_END_TRY();
	MemoryContextSwitchTo(former);
	MemoryContextReset(PythonWorkMemoryContext);

	Py_DECREF(source_str);

	return(rob);
}

/*
 * Build a plan.
 */
static SPIPlanPtr
make_plan(PyObj self, bool with_scroll)
{
	volatile SPIPlanPtr splan = NULL;
	PyObj source_str = PyPgStatement_GetString(self);

	Assert(!ext_state);
	/* it should return rows if we are SCROLLing... */
	Assert(!with_scroll || (with_scroll && PyPgStatement_ReturnsRows(self)));

	Py_INCREF(source_str);
	PyObject_StrBytes(&source_str);
	if (source_str == NULL)
		return(NULL);

	PG_TRY();
	{
		SPIPlanPtr plan;
		TupleDesc checkDesc = NULL;
		ListCell *lc;
		PyObj output;
		OverrideSearchPath *path;

		path = PyPgStatement_GetPath(self);
		PushOverrideSearchPath(path);

		PG_TRY();
		{
			plan = SPI_prepare_cursor(
				PyBytes_AS_STRING(source_str),
				PyPgTupleDesc_GetNatts(PyPgStatement_GetInput(self)),
				PyPgStatement_GetParameterTypes(self),
				with_scroll ? CURSOR_OPT_SCROLL : 0
			);
		}
		PG_CATCH();
		{
			PopOverrideSearchPath();
			PG_RE_THROW();
		}
		PG_END_TRY();
		PopOverrideSearchPath();

		if (plan == NULL)
			raise_spi_error(SPI_result);

		if (PyPgStatement_ReturnsRows(self))
		{
			/*
			 * Validate that the established expectations are consistent.
			 */
			foreach(lc, plan->plancache_list)
			{
				CachedPlanSource *plans = (CachedPlanSource *) lfirst(lc);
				if (plans->resultDesc != NULL && checkDesc != NULL)
				{
					SPI_freeplan(plan);
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("cannot insert multiple commands into a prepared statement")));
				}
				checkDesc = plans->resultDesc;
			}

			output = PyPgStatement_GetOutput(self);
			if (!equalTupleDescs(checkDesc, PyPgType_GetTupleDesc(output)))
			{
				SPI_freeplan(plan);
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR), /* XXX: not the right error code */
						errmsg("statement output changed since creation")));
			}
		}

		splan = SPI_saveplan(plan);
		SPI_freeplan(plan);
	}
	PG_CATCH();
	{
		if (splan != NULL)
			SPI_freeplan(splan);
		splan = NULL;
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	Py_DECREF(source_str);
	return(splan);
}

SPIPlanPtr
PyPgStatement_GetPlan(PyObj self)
{
	PyPgStatement s = (PyPgStatement) self;
	SPIPlanPtr plan;

	plan = s->ps_plan;

	if (plan == NULL)
	{
		plan = s->ps_plan = make_plan(self, false);
	}

	return(plan);
}

SPIPlanPtr
PyPgStatement_GetScrollPlan(PyObj self)
{
	PyPgStatement s = (PyPgStatement) self;
	SPIPlanPtr plan;

	plan = s->ps_scroll_plan;

	if (plan == NULL)
	{
		plan = s->ps_scroll_plan = make_plan(self, true);
	}

	return(plan);
}

/*
 * execute_statements - execute multiple statements in the string
 */
int
execute_statements(const char *src)
{
	int n = 0;
	List *raw_parsetree_list;
	ListCell *list_item;
	DestReceiver *dest;
	ErrorContextCallback errccb;

	errccb.callback = statement_error_callback;
	errccb.arg = (void *) src;
	errccb.previous = error_context_stack;
	error_context_stack = &errccb;

	/*
	 * Parse the request string into a list of raw parse trees.
	 */
	raw_parsetree_list = pg_parse_query(src);
	dest = CreateDestReceiver(DestNone);

	/*
	 * Iterate over the parsetree list, executing each statement.
	 */
	foreach(list_item, raw_parsetree_list)
	{
		Node	   *parsetree = (Node *) lfirst(list_item);
		List	   *stmt_list;
		ListCell	*lc2;

		++n;

		/*
		 * Before each statement.
		 */
		CommandCounterIncrement();

		/*
		 * No parameters.
		 */
		stmt_list = pg_analyze_and_rewrite(parsetree, src, NULL, 0);
		stmt_list = pg_plan_queries(stmt_list, 0, NULL);

		foreach(lc2, stmt_list)
		{
			Node	   *stmt = (Node *) lfirst(lc2);

			if (!IsA(stmt, PlannedStmt))
			{
				/*
				 * Filter prohibited statements.
				 */
				if (IsA(stmt, CopyStmt))
				{
					CopyStmt   *cstmt = (CopyStmt *) stmt;

					if (cstmt->filename == NULL)
					{
						ereport(ERROR,(
							errmsg("cannot execute COPY statements")
						));
					}
				}
				else if (IsA(stmt, TransactionStmt))
				{
					ereport(ERROR,(
						errmsg("cannot execute transaction statements")
					));
				}
			}

			/*
			 * Before each command.
			 */
			CommandCounterIncrement();
			PushActiveSnapshot(GetTransactionSnapshot());

			if (IsA(stmt, PlannedStmt) &&
				((PlannedStmt *) stmt)->utilityStmt == NULL)
			{
				QueryDesc  *qdesc;

				qdesc = CreateQueryDesc(
										(PlannedStmt *) stmt, src,
										GetActiveSnapshot(), /* xact snapshot */
										InvalidSnapshot, /* no crosscheck */
										dest, NULL, false);
				AfterTriggerBeginQuery();
				ExecutorStart(qdesc, 0);
				ExecutorRun(qdesc, ForwardScanDirection, 0);
				AfterTriggerEndQuery(qdesc->estate);
				ExecutorEnd(qdesc);
				FreeQueryDesc(qdesc);
			}
			else
			{
				ProcessUtility(stmt, src,
							   NULL,	/* no params */
							   false,	/* not top level */
							   dest,	/* DestNone */
							   NULL);
			}

			PopActiveSnapshot();
		}
	}

	error_context_stack = errccb.previous;
	return n;
}
