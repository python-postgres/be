/*
 * Postgres string types
 */
#ifndef PyPg_type_string_H
#define PyPg_type_string_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern PyPgTypeObject PyPgString_Type;
#define PyPg_STRING_Type_oid InvalidOid

#define PyPgString_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPgString_Type))
#define PyPgString_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPgString_Type)


extern PyPgTypeObject PyPg_name_Type;
#define PyPg_name_Type_oid NAMEOID
extern PyPgTypeObject PyPg_text_Type;
#define PyPg_text_Type_oid TEXTOID
extern PyPgTypeObject PyPg_char_Type;
#define PyPg_char_Type_oid CHAROID
extern PyPgTypeObject PyPg_bpchar_Type;
#define PyPg_bpchar_Type_oid BPCHAROID
extern PyPgTypeObject PyPg_varchar_Type;
#define PyPg_varchar_Type_oid VARCHAROID
extern PyPgTypeObject PyPg_unknown_Type;
#define PyPg_unknown_Type_oid UNKNOWNOID
extern PyPgTypeObject PyPg_cstring_Type;
#define PyPg_cstring_Type_oid CSTRINGOID

/* High-level check for arbitrary string objects */
#define Py_String_Check(SELF) \
	(PyUnicode_Check(SELF) || PyPgString_Check(SELF))

#define PyPg_name_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_name_Type))
#define PyPg_name_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_name_Type)

#define PyPg_text_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_text_Type))
#define PyPg_text_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_text_Type)

#define PyPg_char_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_char_Type))
#define PyPg_char_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_char_Type)

#define PyPg_bpchar_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_bpchar_Type))
#define PyPg_bpchar_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_bpchar_Type)

#define PyPg_varchar_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_varchar_Type))
#define PyPg_varchar_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_varchar_Type)

#define PyPg_unknown_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_unknown_Type))
#define PyPg_unknown_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_unknown_Type)

#define PyPg_cstring_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_cstring_Type))
#define PyPg_cstring_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_cstring_Type)

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_type_string_H */
