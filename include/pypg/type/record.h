/*
 * record type
 */
#ifndef PyPg_type_record_H
#define PyPg_type_record_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern PyPgTypeObject PyPg_record_Type;
#define PyPg_record_Type_oid RECORDOID

/*
 * Create a PyPgObject (PyPg_record_Type instance) using the given
 * HeapTuple. (Expects OidIsValid(ht->t_tableOid))
 */
PyObj PyPgObject_FromHeapTuple(HeapTuple ht);
PyObj PyPgObject_FromPyPgTypeAndHeapTuple(PyObj subtype, HeapTuple ht);

#define PyPg_record_Check(SELF) \
	(PyObject_TypeCheck((PyObj) SELF, (PyTypeObject *) &PyPg_record_Type))
#define PyPg_record_CheckExact(SELF) (Py_TYPE(SELF) == (PyTypeObject *) &PyPg_record_Type)

/*
 * Anonymous records are handled with subclasses, deny the operation on the base
 * record type.
 */
#define PyPg_record_Require(TYP) (\
	(!PyPg_record_Check(TYP) || TYP == (PyObj) &PyPg_record_Type) ? ( \
		PyErr_Format(PyExc_TypeError, \
			"%s requires a subclass of Postgres.types.record, given '%.200s'", \
			PG_FUNCNAME_MACRO, Py_TYPE(TYP)->tp_name) ? -1 : -1 \
	) : 0)

#define PyPg_record_GetHeapTupleHeader(SELF) \
	((HeapTupleHeader) (DatumGetPointer(PyPgObject_GetDatum(SELF))))

#define PyPg_record_SetHeapTupleHeader(SELF, R) \
	PyPgObject_SetDatum(SELF, PointerGetDatum(R))

/*
 * PyPgObject's are exclusively Datums, so a HeapTupleData structure must be
 * filled in when there is a need to use HeapTuple-dependent routines.
 */
#define PyPgObject_InitHeapTuple(HT, OB) do { \
	(HT)->t_len = (HeapTupleHeaderGetDatumSize(PyPgObject_GetHeapTupleHeader(OB))); \
	(HT)->t_tableOid = PyPgObject_GetTableOid(OB) \
	(HT)->t_data = PyPgObject_GetHeapTupleHeader(OB); \
} while(0)

/*
 * Remove the source composite type to the given target composite type, subtype.
 *
 * On Error: elog/ereport.
 */
void PyPg_record_Reform(PyObj subtype, PyObj src,
	int mod, Datum *outdatum, bool *outnull);

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_type_record_H */
