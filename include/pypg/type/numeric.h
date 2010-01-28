/*
 * numeric types
 */
#ifndef PyPg_type_numeric_H
#define PyPg_type_numeric_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern PyPgTypeObject PyPg_int2_Type;
#define PyPg_int2_Type_oid INT2OID
extern PyPgTypeObject PyPg_int4_Type;
#define PyPg_int4_Type_oid INT4OID
extern PyPgTypeObject PyPg_int8_Type;
#define PyPg_int8_Type_oid INT8OID
extern PyPgTypeObject PyPg_float4_Type;
#define PyPg_float4_Type_oid FLOAT4OID
extern PyPgTypeObject PyPg_float8_Type;
#define PyPg_float8_Type_oid FLOAT8OID
extern PyPgTypeObject PyPg_numeric_Type;
#define PyPg_numeric_Type_oid NUMERICOID
extern PyPgTypeObject PyPg_cash_Type;
#define PyPg_cash_Type_oid CASHOID

#define PyPg_int2_Check(SELF) \
	(PyObject_TypeCheck((SELF), &PyPg_int2_Type))
#define PyPg_int2_CheckExact(SELF) (Py_TYPE(SELF) == &PyPg_int2_Type)

#define PyPg_int4_Check(SELF) \
	(PyObject_TypeCheck((SELF), &PyPg_int4_Type))
#define PyPg_int4_CheckExact(SELF) (Py_TYPE(SELF) == &PyPg_int4_Type)

#define PyPg_int8_Check(SELF) \
	(PyObject_TypeCheck((SELF), &PyPg_int8_Type))
#define PyPg_int8_CheckExact(SELF) (Py_TYPE(SELF) == &PyPg_int8_Type)

#define PyPg_float4_Check(SELF) \
	(PyObject_TypeCheck((SELF), &PyPg_float4_Type))
#define PyPg_float4_CheckExact(SELF) (Py_TYPE(SELF) == &PyPg_float4_Type)

#define PyPg_float8_Check(SELF) \
	(PyObject_TypeCheck((SELF), &PyPg_float8_Type))
#define PyPg_float8_CheckExact(SELF) (Py_TYPE(SELF) == &PyPg_float8_Type)

#define PyPg_numeric_Check(SELF) \
	(PyObject_TypeCheck((SELF), &PyPg_numeric_Type))
#define PyPg_numeric_CheckExact(SELF) (Py_TYPE(SELF) == &PyPg_numeric_Type)

#define PyPg_cash_Check(SELF) \
	(PyObject_TypeCheck((SELF), &PyPg_cash_Type))
#define PyPg_cash_CheckExact(SELF) (Py_TYPE(SELF) == &PyPg_cash_Type)

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_type_numeric_H */
