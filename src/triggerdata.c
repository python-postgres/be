/*
 * Postgres.TriggerData
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <compile.h>
#include <structmember.h>

#include "postgres.h"
#include "commands/trigger.h"
#include "mb/pg_wchar.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/extension.h"
#include "pypg/pl.h"
#include "pypg/triggerdata.h"
#include "pypg/type/type.h"

static PyObj
trigdata_get_rel_nspname(PyObj td, void *arg)
{
	PyObj tp;
	tp = PyPgTriggerData_GetRelationType(td);
	tp = PyPgTypeInfo(tp)->nspname_PyUnicode;
	Py_INCREF(tp);
	return(tp);
}

static PyObj
trigdata_get_typname(PyObj td, void *arg)
{
	PyObj tp, rob;
	tp = PyPgTriggerData_GetRelationType(td);
	rob = PyPgTypeInfo(tp)->typname_PyUnicode;
	Py_INCREF(rob);
	return(rob);
}

static PyObj
trigdata_get_relation_id(PyObj td, void *arg)
{
	PyObj tp, rob;
	tp = PyPgTriggerData_GetRelationType(td);
	rob = PyLong_FromOid(
		PyPgTypeInfo(tp)->array.x_no.composite.typrelid);
	return(rob);
}

static PyObj
trigdata_get_dbname(PyObj td, void *arg)
{
	Py_INCREF(py_my_datname_str_ob);
	return(py_my_datname_str_ob);
}

static PyGetSetDef PyPgTriggerData_GetSet[] = {
	{"table_name", trigdata_get_typname,
		NULL, PyDoc_STR("name of table object(event object)"), NULL},
	{"table_schema", trigdata_get_rel_nspname,
		NULL, PyDoc_STR("schema of table object(event object)"), NULL},
	{"trigger_schema", trigdata_get_rel_nspname,
		NULL, PyDoc_STR("schema of trigger object"), NULL},

	{"relation_id", trigdata_get_relation_id, NULL, PyDoc_STR("Oid of the table(event object)"), NULL},

/* Constant */
	{"table_catalog", trigdata_get_dbname, NULL, PyDoc_STR("catalog of table object(event object)"), NULL},
	{"trigger_catalog", trigdata_get_dbname, NULL, PyDoc_STR("catalog of trigger object"), NULL},
	{NULL,}
};

static PyMemberDef PyPgTriggerData_Members[] = {
	{"type", T_OBJECT, offsetof(struct PyPgTriggerData, td_reltype), READONLY,
		PyDoc_STR("relation type of table")},
	{"trigger_name", T_OBJECT, offsetof(struct PyPgTriggerData, td_name), READONLY,
		PyDoc_STR("name of trigger")},
	{"manipulation", T_OBJECT, offsetof(struct PyPgTriggerData, td_manipulation), READONLY,
		PyDoc_STR("event manipulation(INSERT or DELETE or UPDATE or TRUNCATE")},
	{"orientation", T_OBJECT, offsetof(struct PyPgTriggerData, td_orientation), READONLY,
		PyDoc_STR("action orientation(STATEMENT or ROW)")},
	{"timing", T_OBJECT, offsetof(struct PyPgTriggerData, td_timing), READONLY,
		PyDoc_STR("condition timing(BEFORE or AFTER)")},

	{"args", T_OBJECT, offsetof(struct PyPgTriggerData, td_args), READONLY,
		PyDoc_STR("args of trigger")},
	{NULL}
};

static void
trigdata_dealloc(PyObj self)
{
	PyPgTriggerData td = (PyPgTriggerData) self;

	Py_XDECREF(td->td_reltype);
	td->td_reltype = NULL;

	Py_XDECREF(td->td_name);
	td->td_name = NULL;

	Py_XDECREF(td->td_args);
	td->td_args = NULL;

	/*
	 * The rest are borrowed from globals.
	 */
}

const char PyPgTriggerData_Doc[] = "Postgres TriggerData interface";

PyTypeObject PyPgTriggerData_Type = {
	PyVarObject_HEAD_INIT(NULL, 0)
	"Postgres.TriggerData",				/* tp_name */
	sizeof(struct PyPgTriggerData),	/* tp_basicsize */
	0,											/* tp_itemsize */
	trigdata_dealloc,						/* tp_dealloc */
	NULL,										/* tp_print */
	NULL,										/* tp_getattr */
	NULL,										/* tp_setattr */
	NULL,										/* tp_compare */
	NULL,										/* tp_repr */
	NULL,										/* tp_as_number */
	NULL,										/* tp_as_sequence */
	NULL,										/* tp_as_mapping */
	NULL,										/* tp_hash */
	NULL,										/* tp_call */
	NULL,										/* tp_str */
	NULL,										/* tp_getattro */
	NULL,										/* tp_setattro */
	NULL,										/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,			  		/* tp_flags */
	PyPgTriggerData_Doc,					/* tp_doc */
	NULL,										/* tp_traverse */
	NULL,										/* tp_clear */
	NULL,										/* tp_richcompare */
	0,											/* tp_weaklistoffset */
	NULL,										/* tp_iter */
	NULL,										/* tp_iternext */
	NULL,										/* tp_methods */
	PyPgTriggerData_Members,			/* tp_members */
	PyPgTriggerData_GetSet,				/* tp_getset */
	NULL,										/* tp_base */
	NULL,										/* tp_dict */
	NULL,										/* tp_descr_get */
	NULL,										/* tp_descr_set */
	0,											/* tp_dictoffset */
	NULL,										/* tp_init */
	NULL,										/* tp_alloc */
	NULL,										/* tp_new */
};

PyObj
PyPgTriggerData_Initialize(PyObj self, TriggerData *td)
{
	int i;
	Trigger *tg = td->tg_trigger;
	struct PyPgTriggerData *pytd = (PyPgTriggerData) self;

	pytd->td_reltype = PyPgType_FromOid(td->tg_relation->rd_rel->reltype);
	if (pytd->td_reltype == NULL)
		goto fail;

	/*
	 * Build out trigger arguments.
	 */
	pytd->td_args = PyTuple_New(tg->tgnargs);
	for (i = 0; i < tg->tgnargs; ++i)
	{
		PyObj ob = PyUnicode_FromCString(tg->tgargs[i]);
		if (ob == NULL)
			goto fail;
		PyTuple_SET_ITEM(pytd->td_args, i, ob);
	}

	pytd->td_name = PyUnicode_FromCString(tg->tgname);
	if (pytd->td_name == NULL)
		goto fail;

	if (TRIGGER_FIRED_BEFORE(td->tg_event))
		pytd->td_timing = BEFORE_str_ob;
	else
		pytd->td_timing = AFTER_str_ob;

	if (TRIGGER_FIRED_FOR_ROW(td->tg_event))
		pytd->td_orientation = ROW_str_ob;
	else
		pytd->td_orientation = STATEMENT_str_ob;

	switch ((TriggerEvent) (td->tg_event) & TRIGGER_EVENT_OPMASK)
	{
		case TRIGGER_EVENT_INSERT:
			pytd->td_manipulation = INSERT_str_ob;
		break;
		case TRIGGER_EVENT_DELETE:
			pytd->td_manipulation = DELETE_str_ob;
		break;
		case TRIGGER_EVENT_UPDATE:
			pytd->td_manipulation = UPDATE_str_ob;
		break;
		case TRIGGER_EVENT_TRUNCATE:
			pytd->td_manipulation = TRUNCATE_str_ob;
		break;
	}

	return(self);
fail:
	Py_DECREF(self);
	return(NULL);
}
