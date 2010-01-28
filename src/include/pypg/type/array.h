/*
 * Postgres ArrayType
 */
#ifndef PyPg_type_array_H
#define PyPg_type_array_H 0
#ifdef __cplusplus
extern "C" {
#endif

#define PyPg_ARRAY_Type_oid InvalidOid
extern PyPgTypeObject PyPgArray_Type;
PyObj PyPgArray_Initialize(PyObj, ArrayType *);

#define PyPgArray_NEW() \
	(PyObj) PyPgArray_Type.tp_alloc(&PyPgArray_Type, 0)
#define PyPgArray_New(R) \
	PyPgArray_Initialize(PyPgArray_NEW(), R)

#define PyPgArray_Check(SELF) (PyObject_TypeCheck(SELF, &PyPgArray_Type))
#define PyPgArray_CheckExact(SELF) (Py_TYPE(SELF) == &PyPgArray_Type)

#define PyPgArray_GetArrayType(SELF) \
	((ArrayType *) (DatumGetPointer(PyPgObject_GetDatum(SELF))))

#define PyPgArray_SetArrayType(SELF, R) \
	PyPgObject_SetDatum(SELF, PointerGetDatum(R))

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_type_array_H */
