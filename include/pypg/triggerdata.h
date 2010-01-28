/*
 * Postgres TriggerData interface
 *
 * This isn't actually a full interface to TriggerData;
 * rather it's a collection of Python objects that represent the source
 * TriggerData.
 */
#ifndef PyPg_triggerdata_H
#define PyPg_triggerdata_H 0
#ifdef __cplusplus
extern "C" {
#endif

struct PyPgTriggerData {
	PyObject_HEAD
	PyObj td_reltype;			/* contains all of the table information */
	PyObj td_name;				/* trigger name */
	PyObj td_args;				/* tuple of PyUnicode */
	PyObj td_manipulation;	/* INSERT | UPDATE | DELETE | TRUNCATE */
	PyObj td_timing;			/* BEFORE | AFTER */
	PyObj td_orientation;	/* STATEMENT | ROW */
};
typedef struct PyPgTriggerData *PyPgTriggerData;
extern PyTypeObject PyPgTriggerData_Type;

PyObj PyPgTriggerData_Initialize(PyObj self, TriggerData *ed);
PyObj PyPgTriggerData_New(TriggerData *td);

#define PyPgTriggerData_NEW(TYP) \
	((PyObj) TYP.tp_alloc(&TYP, 0))
#define PyPgTriggerData_New(TD) PyPgTriggerData_Initialize(PyPgTriggerData_NEW(PyPgTriggerData_Type), TD)

#define PyPgTriggerData_Check(SELF) (PyObject_TypeCheck((SELF), &PyPgTriggerData_Type))
#define PyPgTriggerData_CheckExact(SELF) (Py_TYPE(SELF) == &PyPgTriggerData_Type)

#define PyPgTriggerData_GetRelationType(TD) \
	(((PyPgTriggerData) TD)->td_reltype)
#define PyPgTriggerData_GetTiming(TD) \
	(((PyPgTriggerData) TD)->td_timing)
#define PyPgTriggerData_GetManipulation(TD) \
	(((PyPgTriggerData) TD)->td_manipulation)

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_triggerdata_H */
