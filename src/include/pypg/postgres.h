/*
 * Postgres utilities
 */
#ifndef PyPg_postgres_H
#define PyPg_postgres_H 0
#ifdef __cplusplus
extern "C" {
#endif

#if !(PG_VERSION_NUM < 80500)
#define PG_HAS_INLINE_CODE_BLOCK
#endif

/*
 * Developer tool used to cause errors in strange places.
 */
#ifdef PLPY_STRANGE_THINGS
#warning PLPY_STRANGE_THINGS is enabled.
#define RaiseAStrangeError elog(ERROR, "if you are seeing this error, the build has the 'PLPY_STRANGE_THINGS' developer option enabled.");
#else
#define RaiseAStrangeError
#endif

/*
 * Defines for Python object members.
 */
#define T_OID T_ULONG

#if SIZEOF_DATUM == 8
#define T_DATUM T_ULONGLONG
#endif

#if SIZEOF_DATUM == 4
#define T_DATUM T_ULONG
#endif

#ifndef T_DATUM
#error unsupported Datum size
#endif

extern PyObj py_my_datname_str_ob;

extern MemoryContext PythonMemoryContext, PythonWorkMemoryContext;

#undef TEXTDOMAIN
#define TEXTDOMAIN PG_TEXTDOMAIN("python")

/*
 * Used by type/ to initialize statically allocated PyPgType's.
 */
#define InvalidItemPointerDATA \
	{{InvalidBlockNumber >> 16, InvalidBlockNumber & 0xffff}, InvalidOffsetNumber}
#define EmptyFmgrInfo \
	{NULL, InvalidOid,}

/*
 * Call the mentioned function, but yields a Python error on failure.
 */
void * Py_palloc(Size memsize);
Datum Py_datumCopy(Datum d, bool typbyval, int typlen);
TupleDesc Py_CreateTupleDescCopy(TupleDesc);
TupleDesc Py_CreateTupleDescCopyConstr(TupleDesc);
void Py_FreeTupleDesc(TupleDesc td);

/*
 * Given a PyObject, intelligently convert it to an Oid.
 * On failure, -1 is returned. 0 on success.
 */
int Oid_FromPyObject(PyObj src, Oid *out);

/*
 * Given an Oid, convert it to a PyLong.
 * [This abstraction should be used in case the size of Oid's is increased]
 */
#define PyLong_FromOid(OID) PyLong_FromUnsignedLong(OID)

/*
 * Create a PyTupleObject from the 'row' object using the T
 */
PyObj Py_NormalizeRow(
	int rnatts,
	TupleDesc tupleDescriptor,
	PyObj attname_map, PyObj row);

/*
 * Build Nulls and Datums from a TupleDesc, PyPgType's, and PyObject's.
 *
 * This function expects the `values` and `nulls` to be allocated with
 * the enough space for tupleDescriptor->natts attributes.
 *
 * Normally, this is used before heap_form_tuple().
 *
 * On failure, throws a Postgres error.
 */
void Py_BuildDatumsAndNulls(
	TupleDesc tupleDescriptor,
	PyObj pypg_types, PyObj row,
	Datum *values, bool *nulls);

/*
 * Release (pfree) the *values that are by-reference and are not-NULL.
 */
void FreeReferences(int *byrefmap, Datum *values, bool *nulls);

/*
 * Free the references, and then pfree the values and nulls array.
 * Free the values and nulls array using the given TupleDesc.
 */
void FreeDatumsAndNulls(int *byrefmap, Datum *values, bool *nulls);

/*
 * Raise an appropriate ERROR for the given SPI error code
 */
void raise_spi_error(int spi_error);

/*
 * Some FunctionCallInfo utility macros
 */

#define CALLED_AS_SRF(FCINFO) \
	(FCINFO->resultinfo && IsA(FCINFO->resultinfo, ReturnSetInfo))

#if (PG_VERSION_NUM < 80400)
/*
 * In 8.3, there is no "materialized preferred", so be sure to select
 * materialization in order to guarantee the materialization path for supporting
 * the optimized route with returned Postgres.Cursor objects.
 */
#define SRF_SHOULD_MATERIALIZE(FCINFO) \
	(((ReturnSetInfo *) FCINFO->resultinfo)->allowedModes & SFRM_Materialize)
#else
/*
 * Iff materialization is available and (VPC is not available or VPC is
 * available, but materialization is preferrred).
 */
#define SRF_SHOULD_MATERIALIZE(FCINFO) \
	((((ReturnSetInfo *) \
	 FCINFO->resultinfo)->allowedModes & SFRM_Materialize) && ( \
		!(((ReturnSetInfo *) \
		  FCINFO->resultinfo)->allowedModes & SFRM_ValuePerCall) || ( \
			(((ReturnSetInfo *) \
			  FCINFO->resultinfo)->allowedModes & SFRM_Materialize_Preferred) \
		) \
	))
#endif

#define SRF_VPC_REQUEST(FCINFO) \
	(((ReturnSetInfo *) FCINFO->resultinfo)->allowedModes & SFRM_ValuePerCall)

/*
 * Release cache entry, but set the pointer to NULL
 * before doing so.
 *
 * This allows PG_CATCH() sections to identify whether or
 * not they should try to release it. (Did RSC raise?)
 *
 * Usage: RELEASESYSCACHE(&ht);
 */
#define RELEASESYSCACHE(HT) \
	do { \
		HeapTuple _local_ht = *HT; \
		*HT = NULL; \
		ReleaseSysCache(_local_ht); \
	} while(0)

/*
 * Mapping of PG encoding identifiers to Python codec identifiers.
 *
 * See http://docs.python.org/library/codecs.html
 */
#define PG_SERVER_ENCODINGS() \
	IDSTR(PG_SQL_ASCII, "ascii") \
	IDSTR(PG_EUC_JP, "euc_jp") \
	IDSTR(PG_EUC_CN, "euc_cn") \
	IDSTR(PG_EUC_KR, "euc_kr") \
	IDSTR(PG_EUC_JIS_2004, "euc_jis_2004") \
	IDSTR(PG_UTF8, "utf_8") \
	IDSTR(PG_LATIN1, "latin_1") \
	IDSTR(PG_LATIN2, "iso8859_2") \
	IDSTR(PG_LATIN3, "iso8859_3") \
	IDSTR(PG_LATIN4, "iso8859_4") \
	IDSTR(PG_LATIN5, "iso8859_9") \
	IDSTR(PG_LATIN6, "iso8859_10") \
	IDSTR(PG_LATIN7, "iso8859_13") \
	IDSTR(PG_LATIN8, "iso8859_14") \
	IDSTR(PG_LATIN9, "iso8859_15") \
	IDSTR(PG_LATIN10, "iso8859_16") \
	IDSTR(PG_WIN866, "cp866") \
	IDSTR(PG_WIN874, "cp874") \
	IDSTR(PG_WIN1251, "cp1251") \
	IDSTR(PG_WIN1252, "cp1252") \
	IDSTR(PG_WIN1250, "cp1250") \
	IDSTR(PG_WIN1253, "cp1253") \
	IDSTR(PG_WIN1254, "cp1254") \
	IDSTR(PG_WIN1255, "cp1255") \
	IDSTR(PG_WIN1256, "cp1256") \
	IDSTR(PG_WIN1257, "cp1257") \
	IDSTR(PG_WIN1258, "cp1258") \
	IDSTR(PG_KOI8R, "koi8_r") \
	IDSTR(PG_KOI8U, "koi8_u") \
	IDSTR(PG_ISO_8859_5, "iso8859_5") \
	IDSTR(PG_ISO_8859_6, "iso8859_6") \
	IDSTR(PG_ISO_8859_7, "iso8859_7") \
	IDSTR(PG_ISO_8859_8, "iso8859_8")

/*
 * pg_8_3 compatibility. Looking forward to 9.1 and pg-python 2.0
 */
#if (PG_VERSION_NUM < 80400)
#define PG_KOI8U (-1)
#define SFRM_Materialize_Preferred 0
#define SFRM_Materialize_Random 0
#define errstart(xLEVEL, xFILE, xLINE, xFUNC, ...) errstart(xLEVEL, xFILE, xLINE, xFUNC)
#define TEXTARRAYOID 0
#define RECORDARRAYOID 0
#define TRIGGER_EVENT_TRUNCATE 0xDEADBEEF
#define PushActiveSnapshot(...)
#define PopActiveSnapshot(...)
#define CreateQueryDesc(A1, A2, A3, ...) CreateQueryDesc(A1, A3, __VA_ARGS__)
#define CreateDestReceiver(A) CreateDestReceiver(A, NULL)
#define pg_plan_queries(...) pg_plan_queries(__VA_ARGS__, true)
#define GetActiveSnapshot() GetLatestSnapshot()
#define FreeExprContext(X,Y) FreeExprContext(X)
#define TYPCATEGORY_STRING '-'
#define _PG_GET_TYPCATEGORY(X) '\0'
#define _PG_ERROR_IS_RELAY() (PyErr_Occurred())
#else
/*
 * Relays can be detected by code on modern systems (8.4 and greater).
 */
#define _PG_ERROR_IS_RELAY() (geterrcode() == ERRCODE_PYTHON_RELAY)
#define _PG_GET_TYPCATEGORY(X) (X->typcategory)
#endif

/*
 * Unsupported:
 *
	IDSTR(PG_EUC_TW, NULL) \
	IDSTR(PG_MULE_INTERNAL, NULL) \
*/

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_postgres_H */
