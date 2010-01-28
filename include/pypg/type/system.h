/*
 * system types
 */
#ifndef PyPg_type_system_H
#define PyPg_type_system_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern PyPgTypeObject PyPg_oid_Type;
#define PyPg_oid_Type_oid OIDOID
extern PyPgTypeObject PyPg_cid_Type;
#define PyPg_cid_Type_oid CIDOID
extern PyPgTypeObject PyPg_xid_Type;
#define PyPg_xid_Type_oid XIDOID
extern PyPgTypeObject PyPg_tid_Type;
#define PyPg_tid_Type_oid TIDOID
extern PyPgTypeObject PyPg_aclitem_Type;
#define PyPg_aclitem_Type_oid ACLITEMOID
extern PyPgTypeObject PyPg_refcursor_Type;
#define PyPg_refcursor_Type_oid REFCURSOROID
extern PyPgTypeObject PyPg_regprocedure_Type;
#define PyPg_regprocedure_Type_oid REGPROCEDUREOID
extern PyPgTypeObject PyPg_regproc_Type;
#define PyPg_regproc_Type_oid REGPROCOID
extern PyPgTypeObject PyPg_regclass_Type;
#define PyPg_regclass_Type_oid REGCLASSOID
extern PyPgTypeObject PyPg_regtype_Type;
#define PyPg_regtype_Type_oid REGTYPEOID
extern PyPgTypeObject PyPg_regoper_Type;
#define PyPg_regoper_Type_oid REGOPEROID
extern PyPgTypeObject PyPg_regoperator_Type;
#define PyPg_regoperator_Type_oid REGOPERATOROID
extern PyPgTypeObject PyPg_oidvector_Type;
#define PyPg_oidvector_Type_oid OIDVECTOROID
extern PyPgTypeObject PyPg_int2vector_Type;
#define PyPg_int2vector_Type_oid INT2VECTOROID

#define PyPg_oid_FromObjectId(oid) \
	PyPgObject_New(&PyPg_oid_Type, ObjectIdGetDatum(oid))
#define PyPg_oid_FromOid(oid) PyPg_oid_FromObjectId(oid)

#define PyPg_cid_FromCommandId(cid) \
	PyPgObject_New(&PyPg_cid_Type, CommandIdGetDatum(cid))
#define PyPg_xid_FromTransactionId(xid) \
	PyPgObject_New(&PyPg_xid_Type, TransactionIdGetDatum(xid))

#define PyPg_oid_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_oid_Type))
#define PyPg_oid_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_oid_Type)

#define PyPg_cid_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_cid_Type))
#define PyPg_cid_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_cid_Type)

#define PyPg_xid_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_xid_Type))
#define PyPg_xid_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_xid_Type)

#define PyPg_tid_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_tid_Type))
#define PyPg_tid_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_tid_Type)

#define PyPg_aclitem_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_aclitem_Type))
#define PyPg_aclitem_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_aclitem_Type)

#define PyPg_refcursor_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_refcursor_Type))
#define PyPg_refcursor_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_refcursor_Type)

#define PyPg_regprocedure_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_regprocedure_Type))
#define PyPg_regprocedure_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_regprocedure_Type)

#define PyPg_regproc_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_regproc_Type))
#define PyPg_regproc_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_regproc_Type)

#define PyPg_regclass_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_regclass_Type))
#define PyPg_regclass_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_regclass_Type)

#define PyPg_regtype_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_regtype_Type))
#define PyPg_regtype_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_regtype_Type)

#define PyPg_regoper_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_regoper_Type))
#define PyPg_regoper_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_regoper_Type)

#define PyPg_regoperator_Check(SELF) \
	(PyObject_TypeCheck((SELF), (PyTypeObject *) &PyPg_regoperator_Type))
#define PyPg_regoperator_CheckExact(SELF) \
	(Py_TYPE(SELF) == (PyTypeObject *) &PyPg_regoperator_Type)

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_type_system_H */
