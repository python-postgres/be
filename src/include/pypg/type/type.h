/*
 * The type type.
 */
#ifndef PyPg_type_type_H
#define PyPg_type_type_H 0
#ifdef __cplusplus
extern "C" {
#endif

/* Used by typmodin */
extern PyObj PyPg_cstring_Array_Type;

/* Used by tupledesc.c */
extern PyObj PyPg_pg_attribute_Type;

/*
 * Use the contrived typtype field to distinguish arrays.
 * [issubclass could be used, but this is quick]
 */
#define TYPTYPE_ARRAY 'a'
#define TYPTYPE_ABSTRACT '*'

#define PyPg_invalid_Type_oid InvalidOid
#define invalid_new_datum NULL

/*
 * Compensate for older versions..
 */
#ifndef AttributeRelation_Rowtype_Id
#define AttributeRelation_Rowtype_Id PG_ATTRIBUTE_RELTYPE_OID
#endif

/*
 * Direct interface to Datum creation from a Python object.
 *
 * In essence, this factors functionality out of tp_new implementations.
 * tp_new(obj_new) will, in turn, call the type's tp_new_datum from
 * PyPgTypeInfo.
 *
 * Functions of this type must throw a Postgres error on failure and
 * should be *very* careful about not acquiring Python references.
 */
typedef void (*pypg_new_datum)(
	PyObj subtype, PyObj ob, int32 typmod,
	Datum *rdatum, bool *is_null);

/*
 * Provides information for composites.
 */
struct pypg_composite_data {
	Oid typrelid;
	PyObj typrel_TupleDesc;
};

/*
 * Provides the array type of this non-array, and
 * the composite data from above.
 *
 * Don't be tempted to add a typarray_Type here. Circular references and
 * consistency issues make it undesirable.
 */
struct pypg_element_data {
	Oid typarray; /* No array type if !OidIsValid() */
	struct pypg_composite_data composite; /* TYPTYPE_COMPOSITE */
};

/*
 * Array typelem information.
 */
struct pypg_array_data {
	Oid typelem;
	PyObj typelem_Type;
};

struct pypg_type_func {
	FmgrInfo typinput, typoutput;
	FmgrInfo typreceive, typsend;
	FmgrInfo typmodin, typmodout;
};

/*
 * Domain constraint data.
 */
struct pypg_domain_data {
	Oid typbasetype;
	int32 typtypmod;
	int32 typndims;
	bool typnotnull;
	List *constraint_list;
	PyObj typubase_Type; /* ultimate base type; BORROWED reference */
};

/*
 * PyPgTypeInfo - structure appended to PyTypeObject and PyHeapTypeObject
 *
 * Combination of select Form_pg_type fields and derived Python objects.
 *
 * This structure effectively acts as pl/python3's type cache.
 */
typedef struct PyPgTypeInfo {
	Oid typoid, typnamespace, typioparam;
	pypg_new_datum tp_new_datum;

	/*
	 * InvalidTransactionId indicates uninitialized
	 */
	TransactionId	typ_xmin;		/* Stored xmin of the pg_type row */
	ItemPointerData	typ_tid;		/* The pointer to the pg_type row */

	char typtype, typcategory, typalign;
	bool typbyval;
	int2 typlen;

	PyObj typoid_PyLong, typnamespace_PyLong;
	PyObj typname_PyUnicode, nspname_PyUnicode;

	/*
	 * This context is always PythonMemoryContext for builtins, but
	 * temporary child contexts for heap types.
	 */
	MemoryContext typmemory;

	/*
	 * 'x_' prefix in case 'yes' and 'no' are defined.
	 */
	union {
		struct pypg_array_data x_yes; /* TYPTYPE_ARRAY */
		struct pypg_element_data x_no; /* !TYPTYPE_ARRAY */
	} array;

	/*
	 * Is a domain when OidIsValid(typbasetype)
	 */
	union {
		struct pypg_domain_data x_yes;
		struct pypg_type_func x_no;
	} domain;

	/*
	 * Points to domain.x_no, or the domain's
	 * ultimate base type domain.x_no.
	 *
	 * It's safe to point out of the structure because the
	 * domain will always have a reference to the ultimate
	 * base type.
	 */
	struct pypg_type_func *typfunc;
} * PyPgTypeInfo;

#define PYPG_INIT_TYPINFO(TYPNAME) \
 {PyPg_##TYPNAME##_Type_oid, PG_CATALOG_NAMESPACE, \
 InvalidOid, TYPNAME##_new_datum, \
 InvalidTransactionId, InvalidItemPointerDATA, \
 TYPTYPE_ABSTRACT, '\0', '\0', true, 0, NULL, \
 }

/* For statically allocated types (int2, int4, etc) */
typedef struct PyPgTypeObject {
	PyTypeObject type;
	struct PyPgTypeInfo typinfo;
} PyPgTypeObject;

/*
 * For dynamically allocated types (arrays, composites, UDTs, etc)
 * This means that the PyPgTypeInfo accessor needs to inspect TPFLAGS to
 * identify where the PyPgTypeInfo is located.
 */
typedef struct PyPgHeapTypeObject {
	PyHeapTypeObject type;
	struct PyPgTypeInfo typinfo;
} PyPgHeapTypeObject;

extern PyTypeObject PyPgType_Type;

/*
 * Initialize all the builtins and create the cache
 */
int PyPgType_Init(void);

/*
 * Clear the type cache dictionary.
 */
void PyPgClearTypeCache(void);

/*
 * Get the PyPgTypeObject * for the Postgres built-in. (Borrowed Reference)
 *
 * WARNING: This does not check if the type is initialized.
 */
PyObj PyPgType_LookupBuiltin(Oid typoid);

/*
 * The PyPgTypeObject * for the Postgres type identified by the given Oid.
 */
PyObj PyPgType_FromOid(Oid typoid);

/*
 * The PyPgTypeObject * for the Postgres type identified by the given Table Oid.
 */
PyObj PyPgType_FromTableOid(Oid typoid);

/*
 * Create an anonymous record type from a PyPgTupleDesc and a custom
 * typname(PyUnicode object).
 */
PyObj PyPgType_FromPyPgTupleDesc(PyObj td_ob, PyObj typname);

/*
 * Create an anonymous record type from the given TupleDesc.
 */
PyObj PyPgType_FromTupleDesc(TupleDesc td);

/*
 * Convert the given PyObject to an Oid and lookup a type using it.
 *
 * [PyPgType_FromOid(Oid_FromPyObject(ob))]
 */
PyObj PyPgType_FromPyObject(PyObj);

/*
 * Given a Polymorphic PyPgType and a base, target PyPgType,
 * return the appropriate PyPgType for the generic type.
 * 
 * So, if the target base type is an int2, and the 'poly_type' is an anyarray,
 * return a int2[] PyPgType.
 */
PyObj PyPgType_Polymorph(PyObj poly_type, PyObj target_base_type);

/*
 * Validate that the type does not need to be refreshed.
 */
bool PyPgType_IsCurrent(PyObj pypg_type_object);

/*
 * Run the pg_type's typmodin function using the given
 * PyPgObject(mod). mod must be a cstring[].
 *
 * On failure, throws a Postgres error.
 */
int32 PyPgType_modin(PyObj subtype, PyObj mod);

/*
 * Protect abstract types from unexpected attempts
 * to create a Datum from them.
 *
 * This unconditionally elog(ERROR)'s out.
 */
void PyPgType_x_new_datum(
	PyObj subtype, PyObj ob, int32 mod,
	Datum *rdatum, bool *isnull);

/*
 * Run the typinfo's typoutput. ereport/elog on failure.
 */
char * typinfo_typoutput(PyPgTypeInfo typinfo, Datum d, bool isnull);
/*
 * Run the typinfo's typinput. ereport/elog on failure.
 */
void typinfo_typinput(PyPgTypeInfo typinfo, char *cstr, int32 mod, Datum *rdatum, bool *isnull);

/*
 * Create a Python str from the type's typoutput and the given object.
 *
 * 'ob' must be an exact instance of 'subtype'.
 */
PyObj PyPgType_typoutput(PyObj subtype, PyObj ob);

/*
 * Create a Python bytes from the type's typsend and the given object.
 *
 * 'ob' must be an exact instance of 'subtype'.
 */
PyObj PyPgType_typsend(PyObj subtype, PyObj ob);

/*
 * Run the pg_type's typinput function.
 * The 'buf' object must be a buffer protocol supporting object
 * whose contents are server encoded character sequence.
 *
 * On failure, throws a Postgres error.
 */
void PyPgType_typinput(
	PyObj subtype, PyObj buf, int32 mod,
	Datum *rdatum, bool *isnull);

/*
 * Run the pg_type's typreceive function.
 *
 * On failure, throws a Postgres error.
 */
PyObj PyPgType_typreceive(PyObj subtype, PyObj args);

/*
 * Cast the PyPgObject 'ob', to the PyPgType 'subtype'.
 *
 * On failure, throws a Postgres error.
 * On success, rdatum and is_null are set and 0 is returned.
 * When no CAST can be found, 1 is returned and rdatum and is_null are not
 * touched.
 */
int PyPgType_typcast(
	PyObj subtype, PyObj ob, int32 mod,
	Datum *rdatum, bool *isnull);

/*
 * Build a new Datum of the type 'subtype' using the given
 * Python object 'ob'.
 *
 * The type of 'ob' determines what is called. If the object
 * is a PyUnicode object, it will invoke the subtype's typinput.
 * If the object is a PyPgObject, it will attempt to cast the object
 * to the subtype. Otherwise, it will invoke the PyPgType's tp_datum_new
 * function pointer if it's available(not null). If it's not available,
 * the object will be cast to a PyUnicode object and typinput will be used.
 */
void PyPgType_DatumNew(
	PyObj subtype, PyObj ob, int32 mod,
	Datum *rdatum, bool *is_null);

#define PyPgType(SELF) ((PyPgTypeObject *) SELF)
#define PyPgHeapType(SELF) ((PyPgHeapTypeObject *) SELF)

#define PyPgType_Check(SELF) (PyObject_TypeCheck(SELF, &PyPgType_Type))
#define PyPgType_CheckExact(SELF) (Py_TYPE(SELF) == &PyPgType_Type)

#define PyPgType_IsHeapType(SELF) \
	PyType_HasFeature(((PyTypeObject *) SELF), Py_TPFLAGS_HEAPTYPE)	

#define PyPgTypeInfo(SELF) \
	((PyPgType_IsHeapType(SELF)) ? \
		(&(PyPgHeapType(SELF)->typinfo)) : \
	 	(&(PyPgType(SELF)->typinfo)))

#define PyPgType_GetTableOid(SELF) (PyPgTypeInfo(SELF)->array.x_no.composite.typrelid)
#define PyPgType_GetPyPgTupleDesc(SELF) \
	(PyPgTypeInfo(SELF)->array.x_no.composite.typrel_TupleDesc)
#define PyPgType_GetTupleDesc(SELF) \
	PyPgTupleDesc_GetTupleDesc(PyPgType_GetPyPgTupleDesc(SELF))
#define PyPgType_GetOid(SELF) (PyPgTypeInfo(SELF)->typoid)
#define PyPgType_GetTypeName(SELF) (PyPgTypeInfo(SELF)->typname_PyUnicode)
#define PyPgType_GetNamespaceOid(SELF) (PyPgTypeInfo(SELF)->typnamespace)
#define PyPgType_GetOid_PyLong(SELF) (PyPgTypeInfo(SELF)->typoid_PyLong)
#define PyPgType_GetNamespaceOid_PyLong(SELF) (PyPgTypeInfo(SELF)->typnamespace_PyLong)

#define PyPgType_GetArrayType(SELF) \
	(PyPgTypeInfo(SELF)->array.x_no.typarray_Type)
#define PyPgType_GetElementTypeOid(SELF) \
	(PyPgTypeInfo(SELF)->array.x_yes.typelem)
#define PyPgType_GetElementType(SELF) \
	(PyPgTypeInfo(SELF)->array.x_yes.typelem_Type)
#define PyPgType_GetDatumNew(SELF) (PyPgTypeInfo(SELF)->tp_new_datum)
#define PyPgType_GetXMin(SELF) (PyPgTypeInfo(SELF)->typ_xmin)
#define PyPgType_GetItemPointer(SELF) &(PyPgTypeInfo(SELF)->typ_tid)

#define PyPgType_IsComposite(SELF) \
	(PyPgTypeInfo(SELF)->typtype == TYPTYPE_COMPOSITE)
#define PyPgType_IsEnum(SELF) \
	(PyPgTypeInfo(SELF)->typtype == TYPTYPE_ENUM)
#define PyPgType_IsArray(SELF) \
	(PyPgTypeInfo(SELF)->typtype == TYPTYPE_ARRAY)
/*
 * If typinfo->typfunc points to &(typinfo->domain.x_no) inside the
 * structure, it's *not* a domain.
 */
#define typinfo_is_domain(TI) \
	((TI->typfunc) != (&(TI->domain.x_no)))
#define PyPgType_IsDomain(SELF) \
	typinfo_is_domain(PyPgTypeInfo(SELF))

#define PyPgType_Get_typbyval(SELF) \
	(PyPgTypeInfo(SELF)->typbyval)
#define PyPgType_Get_typlen(SELF) \
	(PyPgTypeInfo(SELF)->typlen)

#define PyPgType_ShouldFree(SELF) \
	!(PyPgType_Get_typbyval(SELF))

#define PyPgType_IsPolymorphic(SELF) \
	(PyPgType_IsComposite(SELF) ? \
		(PyPgTupleDesc_GetPolymorphic(PyPgType_GetPyPgTupleDesc(SELF)) != -1) : \
		IsPolymorphicType(PyPgType_GetOid(SELF)))

/*
 * cstring[] and pg_attribute are not listed here, but they are partial
 * built-ins. Type initialization will lookup their types and set them to
 * globals in type.c. Once they've been initialized, they will never be
 * initialized again.
 */
#define PYPG_DB_TYPES(END) \
	TYP(void) \
	TYP(trigger) \
\
	TYP(oid) \
	TYP(xid) \
	TYP(cid) \
	TYP(tid) \
\
	TYP(record) \
	TYP(bool) \
\
	TYP(refcursor) \
	TYP(regprocedure) \
	TYP(regproc) \
	TYP(regoper) \
	TYP(regoperator) \
	TYP(regclass) \
	TYP(regtype) \
\
	TYP(bit) \
	TYP(varbit) \
\
	TYP(aclitem) \
\
	TYP(int2) \
	TYP(int4) \
	TYP(int8) \
	TYP(numeric) \
	TYP(cash) \
\
	TYP(oidvector) \
	TYP(int2vector) \
\
	TYP(float4) \
	TYP(float8) \
\
	TYP(date) \
	TYP(time) \
	TYP(timetz) \
	TYP(timestamp) \
	TYP(timestamptz) \
	TYP(interval) \
\
	TYP(bytea) \
	TYP(char) \
	TYP(name) \
	TYP(bpchar) \
	TYP(varchar) \
	TYP(text) \
	TYP(unknown) \
	TYP(cstring) \
\
	END

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_type_type_H */
