/*
 * Postgres object interface for Python
 */
#ifndef PyPg_type_object_H
#define PyPg_type_object_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern PyPgTypeObject PyPgObject_Type;
extern PyPgTypeObject PyPgPseudo_Type;

extern PyPgTypeObject PyPg_void_Type;
#define PyPg_void_Type_oid VOIDOID
extern PyPgTypeObject PyPg_trigger_Type;
#define PyPg_trigger_Type_oid TRIGGEROID

PyObj PyPgObject_FromTypeOidAndDatum(Oid, Datum);
PyObj PyPgObject_FromTypeOidAndPyObject(Oid typeoid, PyObj ob);

PyObj PyPgObject_Initialize(PyObj self, Datum);
PyObj PyPgObject_Operate(const char *, PyObj, PyObj);

#define PyPgObject_BODY	\
	Datum pg_datum;

#define PyPgObject_HEAD	\
	PyObject_HEAD			\
	PyPgObject_BODY

typedef struct PyPgObject {
	PyPgObject_HEAD
} * PyPgObject;

#define PyPgObject(SELF) ((PyPgObject) SELF)

#define PyPgObject_New(typ, datum) \
	PyPgObject_Initialize(PyPgObject_NEW(typ), datum)
#define PyPgObject_NEW(typ) \
	((PyTypeObject *) typ)->tp_alloc(((PyTypeObject *) typ), 0)

#define PyPgObject_Check(SELF) \
	(PyObject_TypeCheck((SELF), ((PyTypeObject *) &PyPgObject_Type)))
#define PyPgObject_CheckExact(SELF) \
	(Py_TYPE(SELF) == ((PyTypeObject *) &PyPgObject_Type))
#define PyPgObjectType_Require(TYP) Py_Require_Type(&PyPgObject_Type, TYP)

#define PyPgObject_GetDatum(SELF)	(PyPgObject(SELF)->pg_datum)
#define PyPgObject_SetDatum(SELF, D) (PyPgObject(SELF)->pg_datum = D)

#define PyPgObject_datumCopy(SELF) \
	(Py_datumCopy( \
		PyPgObject_GetDatum(rob), \
		PyPgTypeInfo(Py_TYPE(rob))->typbyval, \
		PyPgTypeInfo(Py_TYPE(rob))->typlen \
	))

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_type_object_H */
