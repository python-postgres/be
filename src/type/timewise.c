/*
 * time types
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

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
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/relcache.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"

#include "pypg/environment.h"
#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/pl.h"
#include "pypg/error.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/timewise.h"

/*
 * epoch's should be float objects, not long.
 * so parameterize the function used to build the result
 * of the *_part calls.
 */
typedef PyObj (*typgetpart_mkrob)(double);

static PyObj
date_get_part(PyObj self, const char *part_str, typgetpart_mkrob mkrob)
{
	Datum part, tzd, fd;
	double r;
	volatile PyObj rob = NULL;

	if (PyPg_date_Require(self))
		return(NULL);

	PG_TRY();
	{
		tzd = DirectFunctionCall1(date_timestamp, PyPgObject_GetDatum(self));
		part = DirectFunctionCall1(textin, PointerGetDatum(part_str));

		fd = DirectFunctionCall2(timestamp_part, part, tzd);
		r = (double) DatumGetFloat8(fd);

		pfree(DatumGetPointer(part));

#ifndef USE_FLOAT8_BYVAL
		pfree(DatumGetPointer(tzd));
		pfree(DatumGetPointer(fd));
#endif

		rob = mkrob(r);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		Py_XDECREF(rob);
		rob = NULL;
	}
	PG_END_TRY();

	return(rob);
}

static PyObj
date_get_millennium(PyObj self, void *closure)
{
	return(date_get_part(self, "millennium", PyLong_FromDouble));
}

static PyObj
date_get_century(PyObj self, void *closure)
{
	return(date_get_part(self, "century", PyLong_FromDouble));
}

static PyObj
date_get_decade(PyObj self, void *closure)
{
	return(date_get_part(self, "decade", PyLong_FromDouble));
}

static PyObj
date_get_year(PyObj self, void *closure)
{
	return(date_get_part(self, "year", PyLong_FromDouble));
}

static PyObj
date_get_quarter(PyObj self, void *closure)
{
	return(date_get_part(self, "quarter", PyLong_FromDouble));
}

static PyObj
date_get_week(PyObj self, void *closure)
{
	return(date_get_part(self, "week", PyLong_FromDouble));
}

static PyObj
date_get_month(PyObj self, void *closure)
{
	return(date_get_part(self, "month", PyLong_FromDouble));
}

static PyObj
date_get_day(PyObj self, void *closure)
{
	return(date_get_part(self, "day", PyLong_FromDouble));
}

static PyObj
date_get_dayofweek(PyObj self, void *closure)
{
	return(date_get_part(self, "dow", PyLong_FromDouble));
}

static PyObj
date_get_dayofyear(PyObj self, void *closure)
{
	return(date_get_part(self, "doy", PyLong_FromDouble));
}

static PyObj
date_get_epoch(PyObj self, void *closure)
{
	return(date_get_part(self, "epoch", PyFloat_FromDouble));
}

static PyGetSetDef date_getset[] = {
	{"millennium", date_get_millennium, NULL, PyDoc_STR("get the date's millennium")},
	{"century", date_get_century, NULL, PyDoc_STR("get the date's century")},
	{"decade", date_get_decade, NULL, PyDoc_STR("get the date's decade")},
	{"year", date_get_year, NULL, PyDoc_STR("get the date's year")},
	{"quarter", date_get_quarter, NULL, PyDoc_STR("get the date's quarter")},
	{"week", date_get_week, NULL, PyDoc_STR("get the date's week")},
	{"month", date_get_month, NULL, PyDoc_STR("get the date's month")},
	{"day", date_get_day, NULL, PyDoc_STR("get the date's day-of-month")},
	{"epoch", date_get_epoch, NULL, PyDoc_STR("get the date's epoch(seconds since epoch)")},
	{"dayofyear", date_get_dayofyear, NULL, PyDoc_STR("get the date's day of year")},
	{"dayofweek", date_get_dayofweek, NULL, PyDoc_STR("get the date's day of week")},
	{NULL,},
};

#define date_new_datum NULL
PyDoc_STRVAR(PyPg_date_Type_Doc, "calendar date");

PyPgTypeObject PyPg_date_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.date",							/* tp_name */
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
	PyPg_date_Type_Doc,								/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	date_getset,									/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(date)
};

static PyObj
time_get_part(PyObj self, const char *part_str, typgetpart_mkrob mkrob)
{
	Datum part, fd;
	double r;
	volatile PyObj rob = NULL;

	if (PyPg_time_Require(self))
		return(NULL);

	PG_TRY();
	{
		part = DirectFunctionCall1(textin, PointerGetDatum(part_str));

		fd = DirectFunctionCall2(time_part, part, PyPgObject_GetDatum(self));
		r = (double) DatumGetFloat8(fd);

		pfree(DatumGetPointer(part));

#ifndef USE_FLOAT8_BYVAL
		pfree(DatumGetPointer(fd));
#endif

		rob = mkrob(r);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		Py_XDECREF(rob);
		rob = NULL;
	}
	PG_END_TRY();

	return(rob);
}

static PyObj
time_get_hour(PyObj self, void *closure)
{
	return(time_get_part(self, "hour", PyLong_FromDouble));
}

static PyObj
time_get_minute(PyObj self, void *closure)
{
	return(time_get_part(self, "minute", PyLong_FromDouble));
}

static PyObj
time_get_second(PyObj self, void *closure)
{
	return(time_get_part(self, "second", PyLong_FromDouble));
}

static PyObj
time_get_millisecond(PyObj self, void *closure)
{
	return(time_get_part(self, "millisecond", PyLong_FromDouble));
}

static PyObj
time_get_microsecond(PyObj self, void *closure)
{
	return(time_get_part(self, "microsecond", PyLong_FromDouble));
}

static PyObj
time_get_epoch(PyObj self, void *closure)
{
	return(time_get_part(self, "epoch", PyFloat_FromDouble));
}

static PyGetSetDef time_getset[] = {
	{"hour", time_get_hour, NULL, PyDoc_STR("get the time's hour")},
	{"minute", time_get_minute, NULL, PyDoc_STR("get the time's minute")},
	{"second", time_get_second, NULL, PyDoc_STR("get the time's second")},
	{"millisecond", time_get_millisecond, NULL, PyDoc_STR("get the time's millisecond")},
	{"microsecond", time_get_microsecond, NULL, PyDoc_STR("get the time's microsecond")},
	{"epoch", time_get_epoch, NULL, PyDoc_STR("get the time's epoch")},
	{NULL,},
};

#define time_new_datum NULL
PyDoc_STRVAR(PyPg_time_Type_Doc, "Time of day");
PyPgTypeObject PyPg_time_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.time",							/* tp_name */
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
	Py_TPFLAGS_BASETYPE,						/* tp_flags */
	PyPg_time_Type_Doc,						/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	time_getset,								/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,	/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(time)
};

static PyObj
timetz_get_part(PyObj self, const char *part_str, typgetpart_mkrob mkrob)
{
	Datum part, fd;
	double r;
	volatile PyObj rob = NULL;

	if (PyPg_timetz_Require(self))
		return(NULL);

	PG_TRY();
	{
		part = DirectFunctionCall1(textin, PointerGetDatum(part_str));

		fd = DirectFunctionCall2(timetz_part, part, PyPgObject_GetDatum(self));
		r = (double) DatumGetFloat8(fd);

		pfree(DatumGetPointer(part));

#ifndef USE_FLOAT8_BYVAL
		pfree(DatumGetPointer(fd));
#endif

		rob = mkrob(r);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		Py_XDECREF(rob);
		rob = NULL;
	}
	PG_END_TRY();

	return(rob);
}

static PyObj
timetz_get_timezone(PyObj self, void *closure)
{
	return(timetz_get_part(self, "timezone", PyLong_FromDouble));
}

static PyObj
timetz_get_hour(PyObj self, void *closure)
{
	return(timetz_get_part(self, "hour", PyLong_FromDouble));
}

static PyObj
timetz_get_minute(PyObj self, void *closure)
{
	return(timetz_get_part(self, "minute", PyLong_FromDouble));
}

static PyObj
timetz_get_second(PyObj self, void *closure)
{
	return(timetz_get_part(self, "second", PyLong_FromDouble));
}

static PyObj
timetz_get_microsecond(PyObj self, void *closure)
{
	return(timetz_get_part(self, "microsecond", PyLong_FromDouble));
}

static PyObj
timetz_get_millisecond(PyObj self, void *closure)
{
	return(timetz_get_part(self, "millisecond", PyLong_FromDouble));
}

static PyObj
timetz_get_epoch(PyObj self, void *closure)
{
	return(timetz_get_part(self, "epoch", PyFloat_FromDouble));
}

static PyGetSetDef timetz_getset[] = {
	{"timezone", timetz_get_timezone, NULL, PyDoc_STR("get the timetz's timezone")},
	{"hour", timetz_get_hour, NULL, PyDoc_STR("get the time's hour")},
	{"minute", timetz_get_minute, NULL, PyDoc_STR("get the time's minute")},
	{"second", timetz_get_second, NULL, PyDoc_STR("get the time's second")},
	{"millisecond", timetz_get_millisecond, NULL, PyDoc_STR("get the time's millisecond")},
	{"microsecond", timetz_get_microsecond, NULL, PyDoc_STR("get the time's microsecond")},
	{"epoch", timetz_get_epoch, NULL, PyDoc_STR("get the time's epoch")},
	{NULL,},
};

#define timetz_new_datum NULL
PyDoc_STRVAR(PyPg_timetz_Type_Doc, "Time of day with time zone");
PyPgTypeObject PyPg_timetz_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.timetz",				/* tp_name */
	sizeof(struct PyPgObject),				/* tp_basicsize */
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
	Py_TPFLAGS_BASETYPE,						/* tp_flags */
	PyPg_timetz_Type_Doc,					/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	timetz_getset,								/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,	/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(timetz)
};

static PyObj
timestamp_get_part(PyObj self, const char *part_str, typgetpart_mkrob mkrob)
{
	Datum part, fd;
	double r;
	volatile PyObj rob = NULL;

	if (PyPg_timestamp_Require(self))
		return(NULL);

	PG_TRY();
	{
		part = DirectFunctionCall1(textin, PointerGetDatum(part_str));

		fd = DirectFunctionCall2(timestamp_part, part, PyPgObject_GetDatum(self));
		r = (double) DatumGetFloat8(fd);

		pfree(DatumGetPointer(part));

#ifndef USE_FLOAT8_BYVAL
		pfree(DatumGetPointer(fd));
#endif

		rob = mkrob(r);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		Py_XDECREF(rob);
		rob = NULL;
	}
	PG_END_TRY();

	return(rob);
}

static PyObj
timestamp_get_hour(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "hour", PyLong_FromDouble));
}

static PyObj
timestamp_get_minute(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "minute", PyLong_FromDouble));
}

static PyObj
timestamp_get_second(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "second", PyLong_FromDouble));
}

static PyObj
timestamp_get_microsecond(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "microsecond", PyLong_FromDouble));
}

static PyObj
timestamp_get_millisecond(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "millisecond", PyLong_FromDouble));
}

static PyObj
timestamp_get_millennium(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "millennium", PyLong_FromDouble));
}

static PyObj
timestamp_get_century(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "century", PyLong_FromDouble));
}

static PyObj
timestamp_get_decade(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "decade", PyLong_FromDouble));
}

static PyObj
timestamp_get_year(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "year", PyLong_FromDouble));
}

static PyObj
timestamp_get_quarter(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "quarter", PyLong_FromDouble));
}

static PyObj
timestamp_get_week(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "week", PyLong_FromDouble));
}

static PyObj
timestamp_get_month(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "month", PyLong_FromDouble));
}

static PyObj
timestamp_get_day(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "day", PyLong_FromDouble));
}

static PyObj
timestamp_get_epoch(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "epoch", PyFloat_FromDouble));
}

static PyObj
timestamp_get_dayofweek(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "dow", PyLong_FromDouble));
}

static PyObj
timestamp_get_dayofyear(PyObj self, void *closure)
{
	return(timestamp_get_part(self, "doy", PyLong_FromDouble));
}

static PyGetSetDef timestamp_getset[] = {
	{"hour", timestamp_get_hour, NULL, PyDoc_STR("get the timestamp's hour")},
	{"minute", timestamp_get_minute, NULL, PyDoc_STR("get the timestamp's minute")},
	{"second", timestamp_get_second, NULL, PyDoc_STR("get the timestamp's second")},
	{"millisecond", timestamp_get_millisecond, NULL, PyDoc_STR("get the timestamp's millisecond")},
	{"microsecond", timestamp_get_microsecond, NULL, PyDoc_STR("get the timestamp's microsecond")},

	{"millennium", timestamp_get_millennium, NULL, PyDoc_STR("get the timestamp's millennium")},
	{"century", timestamp_get_century, NULL, PyDoc_STR("get the timestamp's decade")},
	{"decade", timestamp_get_decade, NULL, PyDoc_STR("get the timestamp's decade")},
	{"year", timestamp_get_year, NULL, PyDoc_STR("get the timestamp's year")},
	{"quarter", timestamp_get_quarter, NULL, PyDoc_STR("get the timestamp's quarter")},
	{"week", timestamp_get_week, NULL, PyDoc_STR("get the timestamp's week")},
	{"month", timestamp_get_month, NULL, PyDoc_STR("get the timestamp's month")},
	{"day", timestamp_get_day, NULL, PyDoc_STR("get the timestamp's day-of-month")},
	{"epoch", timestamp_get_epoch, NULL, PyDoc_STR("get the timestamp's epoch(seconds since epoch)")},

	{"dayofweek", timestamp_get_dayofweek, NULL, PyDoc_STR("get the timestamp's day of week")},
	{"dayofyear", timestamp_get_dayofyear, NULL, PyDoc_STR("get the timestamp's day of year")},
	{NULL,},
};

#define timestamp_new_datum NULL
PyDoc_STRVAR(PyPg_timestamp_Type_Doc, "timestamp without time zone");
PyPgTypeObject PyPg_timestamp_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.timestamp",						/* tp_name */
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
	PyPg_timestamp_Type_Doc,						/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	timestamp_getset,								/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(timestamp)
};

static PyObj
timestamptz_get_part(PyObj self, const char *part_str, typgetpart_mkrob mkrob)
{
	Datum part, fd;
	double r;
	volatile PyObj rob = NULL;

	if (PyPg_timestamptz_Require(self))
		return(NULL);

	PG_TRY();
	{
		part = DirectFunctionCall1(textin, PointerGetDatum(part_str));

		fd = DirectFunctionCall2(timestamptz_part, part, PyPgObject_GetDatum(self));
		r = (double) DatumGetFloat8(fd);

		pfree(DatumGetPointer(part));

#ifndef USE_FLOAT8_BYVAL
		pfree(DatumGetPointer(fd));
#endif

		rob = mkrob(r);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		Py_XDECREF(rob);
		rob = NULL;
	}
	PG_END_TRY();

	return(rob);
}

static PyObj
timestamptz_get_timezone(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "timezone", PyLong_FromDouble));
}

static PyObj
timestamptz_get_hour(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "hour", PyLong_FromDouble));
}

static PyObj
timestamptz_get_minute(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "minute", PyLong_FromDouble));
}

static PyObj
timestamptz_get_second(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "second", PyLong_FromDouble));
}

static PyObj
timestamptz_get_microsecond(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "microsecond", PyLong_FromDouble));
}

static PyObj
timestamptz_get_millisecond(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "millisecond", PyLong_FromDouble));
}

static PyObj
timestamptz_get_millennium(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "millennium", PyLong_FromDouble));
}

static PyObj
timestamptz_get_century(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "century", PyLong_FromDouble));
}

static PyObj
timestamptz_get_decade(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "decade", PyLong_FromDouble));
}

static PyObj
timestamptz_get_year(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "year", PyLong_FromDouble));
}

static PyObj
timestamptz_get_quarter(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "quarter", PyLong_FromDouble));
}

static PyObj
timestamptz_get_week(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "week", PyLong_FromDouble));
}

static PyObj
timestamptz_get_month(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "month", PyLong_FromDouble));
}

static PyObj
timestamptz_get_day(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "day", PyLong_FromDouble));
}

static PyObj
timestamptz_get_epoch(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "epoch", PyFloat_FromDouble));
}

static PyObj
timestamptz_get_dayofweek(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "dow", PyLong_FromDouble));
}

static PyObj
timestamptz_get_dayofyear(PyObj self, void *closure)
{
	return(timestamptz_get_part(self, "doy", PyLong_FromDouble));
}

static PyGetSetDef timestamptz_getset[] = {
	{"timezone", timestamptz_get_timezone, NULL, PyDoc_STR("get the timestamptz's hour")},
	{"hour", timestamptz_get_hour, NULL, PyDoc_STR("get the timestamptz's hour")},
	{"minute", timestamptz_get_minute, NULL, PyDoc_STR("get the timestamptz's minute")},
	{"second", timestamptz_get_second, NULL, PyDoc_STR("get the timestamptz's second")},
	{"millisecond", timestamptz_get_millisecond, NULL, PyDoc_STR("get the timestamptz's millisecond")},
	{"microsecond", timestamptz_get_microsecond, NULL, PyDoc_STR("get the timestamptz's microsecond")},

	{"millennium", timestamptz_get_millennium, NULL, PyDoc_STR("get the timestamptz's millennium")},
	{"century", timestamptz_get_century, NULL, PyDoc_STR("get the timestamptz's century")},
	{"decade", timestamptz_get_decade, NULL, PyDoc_STR("get the timestamptz's decade")},
	{"year", timestamptz_get_year, NULL, PyDoc_STR("get the timestamptz's year")},
	{"quarter", timestamptz_get_quarter, NULL, PyDoc_STR("get the timestamptz's quarter")},
	{"week", timestamptz_get_week, NULL, PyDoc_STR("get the timestamptz's week")},
	{"month", timestamptz_get_month, NULL, PyDoc_STR("get the timestamptz's month")},
	{"day", timestamptz_get_day, NULL, PyDoc_STR("get the timestamptz's day-of-month")},
	{"epoch", timestamptz_get_epoch, NULL, PyDoc_STR("get the timestamptz's epoch(seconds since epoch)")},

	{"dayofweek", timestamptz_get_dayofweek, NULL, PyDoc_STR("get the timestamptz's day of week")},
	{"dayofyear", timestamptz_get_dayofyear, NULL, PyDoc_STR("get the timestamptz's day of year")},
	{NULL,},
};

#define timestamptz_new_datum NULL
PyDoc_STRVAR(PyPg_timestamptz_Type_Doc, "timestamp with time zone");
PyPgTypeObject PyPg_timestamptz_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.timestamptz",					/* tp_name */
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
	PyPg_timestamptz_Type_Doc,						/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	timestamptz_getset,								/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(timestamptz)
};

static PyObj
interval_get_part(PyObj self, const char *part_str, typgetpart_mkrob mkrob)
{
	Datum part, fd;
	double r;
	volatile PyObj rob = NULL;

	if (PyPg_interval_Require(self))
		return(NULL);

	PG_TRY();
	{
		part = DirectFunctionCall1(textin, PointerGetDatum(part_str));

		fd = DirectFunctionCall2(interval_part, part, PyPgObject_GetDatum(self));
		r = (double) DatumGetFloat8(fd);

		pfree(DatumGetPointer(part));

#ifndef USE_FLOAT8_BYVAL
		pfree(DatumGetPointer(fd));
#endif

		rob = mkrob(r);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		Py_XDECREF(rob);
		rob = NULL;
	}
	PG_END_TRY();

	return(rob);
}

static PyObj
interval_get_hour(PyObj self, void *closure)
{
	return(interval_get_part(self, "hour", PyLong_FromDouble));
}

static PyObj
interval_get_minute(PyObj self, void *closure)
{
	return(interval_get_part(self, "minute", PyLong_FromDouble));
}

static PyObj
interval_get_second(PyObj self, void *closure)
{
	return(interval_get_part(self, "second", PyLong_FromDouble));
}

static PyObj
interval_get_microsecond(PyObj self, void *closure)
{
	return(interval_get_part(self, "microsecond", PyLong_FromDouble));
}

static PyObj
interval_get_millisecond(PyObj self, void *closure)
{
	return(interval_get_part(self, "millisecond", PyLong_FromDouble));
}

static PyObj
interval_get_millennium(PyObj self, void *closure)
{
	return(interval_get_part(self, "millennium", PyLong_FromDouble));
}

static PyObj
interval_get_century(PyObj self, void *closure)
{
	return(interval_get_part(self, "century", PyLong_FromDouble));
}

static PyObj
interval_get_decade(PyObj self, void *closure)
{
	return(interval_get_part(self, "decade", PyLong_FromDouble));
}

static PyObj
interval_get_year(PyObj self, void *closure)
{
	return(interval_get_part(self, "year", PyLong_FromDouble));
}

static PyObj
interval_get_quarter(PyObj self, void *closure)
{
	return(interval_get_part(self, "quarter", PyLong_FromDouble));
}

static PyObj
interval_get_week(PyObj self, void *closure)
{
	return(interval_get_part(self, "week", PyLong_FromDouble));
}

static PyObj
interval_get_month(PyObj self, void *closure)
{
	return(interval_get_part(self, "month", PyLong_FromDouble));
}

static PyObj
interval_get_day(PyObj self, void *closure)
{
	return(interval_get_part(self, "day", PyLong_FromDouble));
}

static PyObj
interval_get_epoch(PyObj self, void *closure)
{
	return(interval_get_part(self, "epoch", PyFloat_FromDouble));
}

static PyGetSetDef interval_getset[] = {
	{"hour", interval_get_hour, NULL, PyDoc_STR("get the interval's hour")},
	{"minute", interval_get_minute, NULL, PyDoc_STR("get the interval's minute")},
	{"second", interval_get_second, NULL, PyDoc_STR("get the interval's second")},
	{"millisecond", interval_get_millisecond, NULL, PyDoc_STR("get the interval's millisecond")},
	{"microsecond", interval_get_microsecond, NULL, PyDoc_STR("get the interval's microsecond")},

	{"millennium", interval_get_millennium, NULL, PyDoc_STR("get the interval's millennium")},
	{"century", interval_get_century, NULL, PyDoc_STR("get the interval's century")},
	{"decade", interval_get_decade, NULL, PyDoc_STR("get the interval's decade")},
	{"year", interval_get_year, NULL, PyDoc_STR("get the interval's year")},
	{"quarter", interval_get_quarter, NULL, PyDoc_STR("get the interval's quarter")},
	{"week", interval_get_week, NULL, PyDoc_STR("get the interval's week")},
	{"month", interval_get_month, NULL, PyDoc_STR("get the interval's month")},
	{"day", interval_get_day, NULL, PyDoc_STR("get the interval's day-of-month")},
	{"epoch", interval_get_epoch, NULL, PyDoc_STR("get the interval's epoch(seconds since epoch)")},

	{NULL,},
};

#define interval_new_datum NULL
PyDoc_STRVAR(PyPg_interval_Type_Doc, "interval type interface");
PyPgTypeObject PyPg_interval_Type = {{
	PyVarObject_HEAD_INIT(&PyPgType_Type, 0)
	"Postgres.types.interval",						/* tp_name */
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
	PyPg_interval_Type_Doc,							/* tp_doc */
	NULL,											/* tp_traverse */
	NULL,											/* tp_clear */
	NULL,											/* tp_richcompare */
	0,												/* tp_weaklistoffset */
	NULL,											/* tp_iter */
	NULL,											/* tp_iternext */
	NULL,											/* tp_methods */
	NULL,											/* tp_members */
	interval_getset,								/* tp_getset */
	(PyTypeObject *) &PyPgObject_Type,				/* tp_base */
	NULL,											/* tp_dict */
	NULL,											/* tp_descr_get */
	NULL,											/* tp_descr_set */
	0,												/* tp_dictoffset */
	NULL,											/* tp_init */
	NULL,											/* tp_alloc */
	NULL,											/* tp_new */
},
	PYPG_INIT_TYPINFO(interval)
};
