/*
 * Interface to "bitwise" types
 */
#ifndef PyPg_type_bitwise_H
#define PyPg_type_bitwise_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern PyPgTypeObject PyPg_bool_Type;
#define PyPg_bool_Type_oid BOOLOID
extern PyPgTypeObject PyPg_bit_Type;
#define PyPg_bit_Type_oid BITOID
extern PyPgTypeObject PyPg_varbit_Type;
#define PyPg_varbit_Type_oid VARBITOID
extern PyPgTypeObject PyPg_bytea_Type;
#define PyPg_bytea_Type_oid BYTEAOID

#define PyPg_bool_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_bool_Type))
#define PyPg_bool_CheckExact(SELF) (Py_TYPE(SELF) == &PyPg_bool_Type)

#define PyPg_bit_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_bit_Type))
#define PyPg_bit_CheckExact(SELF) (Py_TYPE(SELF) == &PyPg_bit_Type)

#define PyPg_varbit_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_varbit_Type))
#define PyPg_varbit_CheckExact(SELF) (Py_TYPE(SELF) == &PyPg_varbit_Type)

#define BYTES_RETURNS_OBJECT \
	XMETH(isspace) \
	XMETH(isalpha) \
	XMETH(isalnum) \
	XMETH(isdigit) \
	XMETH(islower) \
	XMETH(isupper) \
	XMETH(istitle)

#define BYTES_FILLS_OBJECT \
	XMETH(lower) \
	XMETH(upper) \
	XMETH(title) \
	XMETH(capitalize) \
	XMETH(swapcase)

#define PyPg_bytea_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_bytea_Type))
#define PyPg_bytea_CheckExact(SELF) (Py_TYPE(SELF) == &PyPg_bytea_Type)

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_type_bitwise_H */
