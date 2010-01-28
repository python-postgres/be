/*
 * Postgres Tuple Descriptor interface type.
 */
#ifndef PyPg_tupledesc_H
#define PyPg_tupledesc_H 0
#ifdef __cplusplus
extern "C" {
#endif

/*
 * Return the TupleDesc's types as a PyTupleObject of PyPgType's
 */
PyObj TupleDesc_GetPyPgTypes(TupleDesc td);
PyObj TupleDesc_BuildNames(TupleDesc td, int active_natts);

TupleDesc TupleDesc_FromNamesAndOids(
	int natts, const char **attnames, Oid *atttypes);
PyObj PyTuple_FromTupleDescAndParameters(TupleDesc td, PyObj args, PyObj kw);

/*
 * Resolve the given object, ob, into an attribute offset using
 * the PyPgTupleDesc object, tdo.
 *
 * If the given object is a string, use the PyPgTupleDesc's NameMap to identify
 * the attribute offset and return it.
 *
 * If the given object is a number, return it as a Py_ssize_t.
 */
Py_ssize_t Offset_FromPyPgTupleDescAndPyObject(PyObj tdo, PyObj ob);

/*
 * Get a tuple of PyPgType's from the PyPgTupleDesc object
 * (effectively filters the None objects from td_types if any)
 */
PyObj PyPgTupleDesc_GetTypes(PyObj pypg_tupledesc_ob);

/*
 * Make a Python tuple of type Oid's that make up the descriptor's types.
 */
PyObj PyPgTupleDesc_GetTypeOids(PyObj pypg_tupledesc_ob);

/*
 * Validate that all the TupleDesc's type objects are current.
 */
bool PyPgTupleDesc_IsCurrent(PyObj pypg_tupledesc_ob);

/*
 * TupleDesc interface type
 *
 * td_idxmap:
 *  This field is used to give quick access to the real attribute
 *  index to use with the td_desc. When this field is NULL, it means
 *  that (td->natts == td_natts), and subsequently that no attributes
 *  have been dropped from the td_desc. The PyPgTupleDesc_GetAttribute(),
 *  PyPgTupleDesc_GetAttributeIndex() accessors abstracts its usage, so 
 *  direct access should not be necessary.
 */
typedef struct PyPgTupleDesc {
	PyObject_HEAD
	TupleDesc td_desc;
	int td_natts;			/* number of atts where !attisdropped */
	int td_polymorphic;		/* first attribute index that is polymorphic -1 if none */
	int *td_idxmap;			/* tuple index to td->attrs index, terminated by -1 */
	int *td_freemap;		/* indexes of !typbyval attrs, terminated by -1 */
	PyObj td_names;			/* tuple() of !attisdropped attnames */
	PyObj td_namemap;		/* dict(): attname -> attindex */
	PyObj td_types;			/* PyTupleObject of PyPgType's or Py_None(dropped) */
} * PyPgTupleDesc;

extern PyTypeObject PyPgTupleDesc_Type;
extern PyObj EmptyPyPgTupleDesc;

void EmptyPyPgTupleDesc_Initialize(void);

PyObj PyPgTupleDesc_NEW(PyTypeObject *subtype, TupleDesc tupd);
/*
 * Create a polymorphed PyPgTupleDesc.
 *
 * pypg_tupledesc_ob must be polymorphic and target_regular_type must not be
 * polymorphic.
 */
PyObj PyPgTupleDesc_Polymorph(PyObj pypg_tupledesc_ob, PyObj target_pypgtype);

/*
 * Create a new PyPgTupleDesc; this assumes that the TupleDesc is in a permanent
 * memory context. (Usually, PythonMemoryContext)
 */
#define PyPgTupleDesc_New(TD) \
	((PyObj) PyPgTupleDesc_NEW(&PyPgTupleDesc_Type, TD))
/*
 * PyPgTupleDesc_New with a copy of the tuple descriptor.
 */
PyObj PyPgTupleDesc_FromCopy(TupleDesc tupd);

#define PyPgTupleDesc(SELF) ((PyPgTupleDesc) SELF)

#define PyPgTupleDesc_Check(SELF) \
	(PyObject_TypeCheck(SELF, &PyPgTupleDesc_Type))
#define PyPgTupleDesc_CheckExact(SELF) \
	(Py_TYPE(SELF) == &PyPgTupleDesc_Type)

#define PyPgTupleDesc_GetTupleDesc(SELF) \
	(PyPgTupleDesc(SELF)->td_desc)
#define PyPgTupleDesc_GetTypesTuple(SELF) \
	(PyPgTupleDesc(SELF)->td_types)
#define PyPgTupleDesc_GetNameMap(SELF) \
	(PyPgTupleDesc(SELF)->td_namemap)
#define PyPgTupleDesc_GetNames(SELF) \
	(PyPgTupleDesc(SELF)->td_names)
#define PyPgTupleDesc_GetNatts(SELF) \
	(PyPgTupleDesc(SELF)->td_natts)
#define PyPgTupleDesc_GetIndexMap(SELF) \
	(PyPgTupleDesc(SELF)->td_idxmap)
#define PyPgTupleDesc_GetFreeMap(SELF) \
	(PyPgTupleDesc(SELF)->td_freemap)

#define PyPgTupleDesc_GetPolymorphic(SELF) \
	(PyPgTupleDesc(SELF)->td_polymorphic)
#define PyPgTupleDesc_IsPolymorphic(SELF) \
	(PyPgTupleDesc_GetPolymorphic(SELF) != -1)

#define PyPgTupleDesc_SetTupleDesc(SELF, TD) \
	(PyPgTupleDesc(SELF)->td_desc = TD)
#define PyPgTupleDesc_SetTypesTuple(SELF, OB) \
	(PyPgTupleDesc(SELF)->td_types = OB)
#define PyPgTupleDesc_SetNameMap(SELF, OB) \
	(PyPgTupleDesc(SELF)->td_namemap = OB)
#define PyPgTupleDesc_SetNames(SELF, OB) \
	(PyPgTupleDesc(SELF)->td_names = OB)
#define PyPgTupleDesc_SetNatts(SELF, NATTS) \
	(PyPgTupleDesc(SELF)->td_natts = NATTS)
#define PyPgTupleDesc_SetIndexMap(SELF, IA) \
	(PyPgTupleDesc(SELF)->td_idxmap = IA)
#define PyPgTupleDesc_SetFreeMap(SELF, FM) \
	(PyPgTupleDesc(SELF)->td_freemap = FM)

#define PyPgTupleDesc_SetPolymorphic(SELF, FIRST_POLY_IDX) \
	(PyPgTupleDesc(SELF)->td_polymorphic = FIRST_POLY_IDX)

#define PyPgTupleDesc_GetAttributeIndex(SELF, AI) \
	((PyPgTupleDesc) SELF)->td_idxmap == NULL ? AI : \
	((PyPgTupleDesc) SELF)->td_idxmap[AI]

#define PyPgTupleDesc_GetAttribute(SELF, AI) \
	PyPgTupleDesc_GetTupleDesc(SELF)->attrs[ \
		PyPgTupleDesc_GetAttributeIndex(SELF, AI) \
	]

#define PyPgTupleDesc_GetAttributeTypeOid(SELF, AN) \
	(PyPgTupleDesc_GetAttribute(SELF, AN)->atttypid)
#define PyPgTupleDesc_GetAttributeType(SELF, AN) \
	PyTuple_GET_ITEM(PyPgTupleDesc_GetTypesTuple(SELF), AN)

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_tupledesc_H */
