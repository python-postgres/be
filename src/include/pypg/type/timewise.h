/*
 * All time related types
 */
#ifndef PyPg_type_timewise_H
#define PyPg_type_timewise_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern PyPgTypeObject PyPg_date_Type;
#define PyPg_date_Type_oid DATEOID
extern PyPgTypeObject PyPg_time_Type;
#define PyPg_time_Type_oid TIMEOID
extern PyPgTypeObject PyPg_timetz_Type;
#define PyPg_timetz_Type_oid TIMETZOID
extern PyPgTypeObject PyPg_timestamp_Type;
#define PyPg_timestamp_Type_oid TIMESTAMPOID
extern PyPgTypeObject PyPg_timestamptz_Type;
#define PyPg_timestamptz_Type_oid TIMESTAMPTZOID
extern PyPgTypeObject PyPg_interval_Type;
#define PyPg_interval_Type_oid INTERVALOID

#define PyPg_date_Check(SELF) \
	(PyObject_TypeCheck((SELF), &PyPg_date_Type))
#define PyPg_date_CheckExact(SELF) (Py_TYPE(SELF) == &PyPg_date_Type)
#define PyPg_date_Require(SELF) \
	Py_Require_Type((PyObj) &PyPg_date_Type, Py_TYPE(SELF))

#define PyPg_time_Check(SELF) \
	(PyObject_TypeCheck((SELF), &PyPg_time_Type))
#define PyPg_time_CheckExact(SELF) \
	(Py_TYPE(SELF) == &PyPg_time_Type)
#define PyPg_time_Require(SELF) \
	Py_Require_Type((PyObj) &PyPg_time_Type, Py_TYPE(SELF))

#define PyPg_timetz_Check(SELF) \
	(PyObject_TypeCheck((SELF), &PyPg_timetz_Type))
#define PyPg_timetz_CheckExact(SELF) \
	(Py_TYPE(SELF) == &PyPg_timetz_Type)
#define PyPg_timetz_Require(SELF) \
	Py_Require_Type((PyObj) &PyPg_timetz_Type, Py_TYPE(SELF))

#define PyPg_timestamp_Check(SELF) \
	(PyObject_TypeCheck((SELF), &PyPg_timestamp_Type))
#define PyPg_timestamp_CheckExact(SELF) \
	(Py_TYPE(SELF) == &PyPg_timestamp_Type)
#define PyPg_timestamp_Require(SELF) \
	Py_Require_Type((PyObj) &PyPg_timestamp_Type, Py_TYPE(SELF))

#define PyPg_timestamptz_Check(SELF) \
	(PyObject_TypeCheck((SELF), &PyPg_timestamptz_Type))
#define PyPg_timestamptz_CheckExact(SELF) \
	(Py_TYPE(SELF) == &PyPg_timestamptz_Type)
#define PyPg_timestamptz_Require(SELF) \
	Py_Require_Type((PyObj) &PyPg_timestamptz_Type, Py_TYPE(SELF))

#define PyPg_interval_Check(SELF) \
	(PyObject_TypeCheck((SELF), &PyPg_interval_Type))
#define PyPg_interval_CheckExact(SELF) \
	(Py_TYPE(SELF) == &PyPg_interval_Type)
#define PyPg_interval_Require(SELF) \
	Py_Require_Type((PyObj) &PyPg_interval_Type, Py_TYPE(SELF))

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_type_timewise_H */
