/*
 * Postgres function lookup and invocation.
 */
#ifndef PyPg_function_H
#define PyPg_function_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern PyObj PYSTR(exec);
extern PyObj Py_builtins_module;
extern PyObj Py_compile_ob;
extern PyObj Py_linecache_updatecache_ob;

typedef struct PyPgFunction {
	PyObject_HEAD
	Oid					fn_oid;				/* pg_proc entry Oid */
	Oid					fn_lang;				/* pg_proc.prolang */
	Oid					fn_namespace;		/* pg_proc.pronamespace */
	TransactionId		fn_xmin;				/* Stored xmin of the pg_proc row */
	ItemPointerData		fn_tid;			/* The pointer to the pg_proc row */
	PyObj				fn_oid_str;				/* PyUnicode version of fn_oid */
	PyObj				fn_oid_int;				/* PyLong version fn_oid */
	bool				fn_retset;				/* Set Returning Function */
	bool				fn_stateful;			/* Function returns send'able iterator */
	char            	fn_volatile;		/* provolatile field */
	PyObj				fn_input;				/* PyPgTupleDesc */
	PyObj				fn_output;				/* PyPgType */
	PGFunction 			fn_pointer;			/* PGFunction */
	PyObj				fn_nspname_str;		/* namespace string object */
	PyObj				fn_filename_str;		/* regprocedure-like identity of the function */
	PyObj				fn_prosrc_str;			/* the function's source */
} * PyPgFunction;
extern PyTypeObject PyPgFunction_Type;

PyObj PyPgFunction_FromOid(Oid fn_oid);

/*
 * Check if the PyPgFunction is the current version(pg_proc comparison).
 *
 * Returns true if the Function object is up-to-date.
 * Returns false if the Function object need to be replaced.
 *
 * ereport() on failure.
 */
bool PyPgFunction_IsCurrent(PyObj func);

/*
 * Remove the Function's module object from sys.modules.
 * Usually used after the above returns false.
 *
 * Returns -1 on error with a Python exception set.
 */
int PyPgFunction_RemoveModule(PyObj func);

PyObj PyPgFunction_get_source(PyObj func);
PyObj PyPgFunction_get_code(PyObj func);
PyObj PyPgFunction_load_module(PyObj func);

TupleDesc TupleDesc_From_proargtypes_proargnames(
	int16_t nargs, Datum argtypes, Datum argnames
);
TupleDesc TupleDesc_From_pg_proc_arginfo(HeapTuple);

#define PyPgFunction_NEW()  \
	((PyObj) PyPgFunction_Type.tp_alloc(&PyPgFunction_Type, 0))

#define PyPgFunction_Check(SELF) (PyObject_TypeCheck((SELF), &PyPgFunction_Type))
#define PyPgFunction_CheckExact(SELF) (Py_TYPE(SELF) == &PyPgFunction_Type)


#define PyPgFunction_GetPGFunction(SELF) \
	(((PyPgFunction) SELF)->fn_pointer)
#define PyPgFunction_GetOid(SELF) \
	(((PyPgFunction) SELF)->fn_oid)
#define PyPgFunction_GetLanguage(SELF) \
	(((PyPgFunction) SELF)->fn_lang)
#define PyPgFunction_GetNamespace(SELF) \
	(((PyPgFunction) SELF)->fn_namespace)
#define PyPgFunction_GetXMin(SELF) \
	(((PyPgFunction) SELF)->fn_xmin)
#define PyPgFunction_GetItemPointer(SELF) \
	(&(((PyPgFunction) SELF)->fn_tid))
#define PyPgFunction_GetReturnsSet(SELF) \
	(((PyPgFunction) SELF)->fn_retset)
#define PyPgFunction_GetVolatile(SELF) \
	(((PyPgFunction) SELF)->fn_volatile)
#define PyPgFunction_IsSRF(SELF) \
	PyPgFunction_GetReturnsSet(SELF)
#define PyPgFunction_IsTrigger(SELF) \
	(PyPgFunction_GetOutput(SELF) == ((PyObj) &PyPg_trigger_Type))
#define PyPgFunction_IsStateful(SELF) \
	(((PyPgFunction) (SELF))->fn_stateful)
#define PyPgFunction_GetCode(SELF) \
	(((PyPgFunction) SELF)->fn_code)
#define PyPgFunction_GetInput(SELF) \
	(((PyPgFunction) SELF)->fn_input)
#define PyPgFunction_GetOutput(SELF) \
	(((PyPgFunction) SELF)->fn_output)
#define PyPgFunction_GetPyLongOid(SELF) \
	(((PyPgFunction) SELF)->fn_oid_int)
#define PyPgFunction_GetPyUnicodeOid(SELF) \
	(((PyPgFunction) SELF)->fn_oid_str)
#define PyPgFunction_GetNamespaceName(SELF) \
	(((PyPgFunction) SELF)->fn_nspname_str)
#define PyPgFunction_GetFilename(SELF) \
	(((PyPgFunction) SELF)->fn_filename_str)
#define PyPgFunction_GetSource(SELF) \
	(((PyPgFunction) SELF)->fn_prosrc_str)

#define PyPgFunction_SetPGFunction(SELF, PGF) \
	(((PyPgFunction) SELF)->fn_pointer = PGF)
#define PyPgFunction_SetOid(SELF, OB) \
	(((PyPgFunction) SELF)->fn_oid = OB)
#define PyPgFunction_SetLanguage(SELF, LANOID) \
	(((PyPgFunction) SELF)->fn_lang = LANOID)
#define PyPgFunction_SetNamespace(SELF, NSOID) \
	(((PyPgFunction) SELF)->fn_namespace = NSOID)
#define PyPgFunction_SetXMin(SELF, XID) \
	(((PyPgFunction) SELF)->fn_xmin = XID)
#define PyPgFunction_SetItemPointer(SELF, TID) \
	(((PyPgFunction) SELF)->fn_tid = *TID)
#define PyPgFunction_SetStateful(SELF, S) \
	(((PyPgFunction) (SELF))->fn_stateful = S)
#define PyPgFunction_SetReturnsSet(SELF, OB) \
	(((PyPgFunction) SELF)->fn_retset = OB)
#define PyPgFunction_SetVolatile(SELF, V) \
	(((PyPgFunction) SELF)->fn_volatile = V)
#define PyPgFunction_SetInput(SELF, OB) \
	(((PyPgFunction) SELF)->fn_input = OB)
#define PyPgFunction_SetOutput(SELF, OB) \
	(((PyPgFunction) SELF)->fn_output = OB)
#define PyPgFunction_SetPyLongOid(SELF, OB) \
	(((PyPgFunction) SELF)->fn_oid_int = OB)
#define PyPgFunction_SetPyUnicodeOid(SELF, OB) \
	(((PyPgFunction) SELF)->fn_oid_str = OB)
#define PyPgFunction_SetNamespaceName(SELF, OB) \
	(((PyPgFunction) SELF)->fn_nspname_str = OB)
#define PyPgFunction_SetFilename(SELF, OB) \
	(((PyPgFunction) SELF)->fn_filename_str = OB)
#define PyPgFunction_SetSource(SELF, OB) \
	(((PyPgFunction) SELF)->fn_prosrc_str = OB)

#define PyPgFunction_IsPython(SELF) \
	(PyPgFunction_GetPGFunction(SELF) == plpython3_handler)

#define PyPgFunction_IsPolymorphic(SELF) \
	(PyPgTupleDesc_GetPolymorphic(PyPgFunction_GetInput(SELF)) != -1 || \
	PyPgType_IsPolymorphic(PyPgFunction_GetOutput(SELF)))

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_function_H */
