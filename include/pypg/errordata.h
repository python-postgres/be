/*
 * Postgres ErrorData interfaces
 */
#ifndef PyPg_errordata_H
#define PyPg_errordata_H 0
#ifdef __cplusplus
extern "C" {
#endif

typedef struct PyPgErrorData {
	PyObject_HEAD
	ErrorData *errordata;
} * PyPgErrorData;
extern PyTypeObject PyPgErrorData_Type;

PyObj PyPgErrorData_Initialize(PyObj self, ErrorData *ed);
PyObj PyPgErrorData_FromCurrent(void); /* CopyErrorData(); FlushErrorData(); */

#define PyPgErrorData_NEW()  \
	((PyObj) PyPgErrorData_Type.tp_alloc(&PyPgErrorData_Type, 0))
#define PyPgErrorData_New(ED) PyPgErrorData_Initialize(PyPgErrorData_NEW(&PyPgErrorData_Type), ED)

#define PyPgErrorData_Check(SELF) (PyObject_TypeCheck((SELF), &PyPgErrorData_Type))
#define PyPgErrorData_CheckExact(SELF) (Py_TYPE(SELF) == &PyPgErrorData_Type)

#define PyPgErrorData_GetErrorData(SELF) \
	(((PyPgErrorData)(SELF))->errordata)

#define PyPgErrorData_SetErrorData(SELF, ED) \
	(((PyPgErrorData)(SELF))->errordata) = ED

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_errordata_H */
