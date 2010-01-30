/*
 * Flexible source meant to be #include'd into a main
 * file that #define's TARGET and APFUNC.
 *
 * This is used to build the Postgres.CONST dictionary.
 *
 * TARGET is the dictionary object.
 * APFUNC is usually PyDict_SetItemString.
 */
#define OBJECTIFY(ARG) (PyLong_FromLong((long) ARG))
#define C(V) APFUNC(TARGET, #V, OBJECTIFY(V));
/* utils/elog.h */
	C(DEBUG1)
	C(DEBUG2)
	C(DEBUG3)
	C(DEBUG4)
	C(DEBUG5)
	C(LOG)
	C(INFO)
	C(NOTICE)
	C(WARNING)
	C(ERROR)
	C(COMMERROR)
	C(FATAL)
	C(PANIC)

/* access/htup.h */
#define CC(V) C(V##AttributeNumber)
	CC(SelfItemPointer)
	CC(ObjectId)
	CC(MinTransactionId)
	CC(MinCommandId)
	CC(MaxTransactionId)
	CC(MaxCommandId)
	CC(TableOid)
	CC(FirstLowInvalidHeap)

	CC(MaxTuple)
	CC(MaxHeap)
#undef CC

	C(MaxAttrSize)
	C(HEAPTUPLESIZE)

#define CC(V) C(SizeOfHeap##V)
	CC(Tid)
	CC(Clean)
	CC(Insert)
	CC(Delete)
	CC(Update)
	CC(Header)
#undef CC

#define CC(V) C(XLOG_HEAP_##V)
	CC(INSERT)
	CC(DELETE)
	CC(UPDATE)
	CC(MOVE)
	CC(OPMASK)
	CC(INIT_PAGE)
#undef CC

#define CC(V) C(HEAP_##V)
	CC(HASNULL)
	CC(HASVARWIDTH)
	CC(HASEXTERNAL)
	CC(HASOID)
	CC(XMIN_COMMITTED)
	CC(XMIN_INVALID)
	CC(XMAX_COMMITTED)
	CC(XMAX_INVALID)
	CC(UPDATED)
	CC(MOVED_OFF)
	CC(MOVED_IN)
	CC(MOVED)
	CC(XACT_MASK)
#undef CC
	C(NAMEDATALEN)
	C(INDEX_MAX_KEYS)
	C(MAXDIM)
	C(VARHDRSZ)
	C(MAXTZLEN)

#define CC(V) C(STATUS_##V)
	CC(OK)
	CC(ERROR)
	CC(EOF)
	CC(FOUND)
#undef CC

#define CC(V) C(USE_##V##_DATES)
	CC(POSTGRES)
	CC(ISO)
	CC(SQL)
	CC(GERMAN)
#undef CC

/* commands/trigger.h */
#define CC(V) C(TRIGGER_EVENT_##V)
	CC(INSERT)
	CC(DELETE)
	CC(UPDATE)
	CC(OPMASK)
	CC(ROW)
	CC(BEFORE)
#undef CC

/* utils/syscache.h */
	C(PROCNAMEARGSNSP)
	C(AGGFNOID)
	C(AMNAME)
	C(AMOID)
	C(AMOPOPID)
	C(AMOPSTRATEGY)
	C(AMPROCNUM)
	C(ATTNAME)
	C(ATTNUM)
	C(CASTSOURCETARGET)
	C(CLAAMNAMENSP)
	C(CLAOID)
	C(CONDEFAULT)
	C(CONNAMENSP)
	C(INDEXRELID)
	C(LANGNAME)
	C(LANGOID)
	C(NAMESPACENAME)
	C(NAMESPACEOID)
	C(OPERNAMENSP)
	C(OPEROID)
	C(PROCOID)
	C(RELNAMENSP)
	C(RELOID)
	C(RULERELNAME)
#if PG_VERSION_NUM < 80500
	C(STATRELATT)
#endif
	C(TYPENAMENSP)
	C(TYPEOID)

/* catalog/pg_type.h */
	C(BOOLOID)
	C(BYTEAOID)
	C(CHAROID)
	C(NAMEOID)
	C(INT8OID)
	C(INT2OID)
	C(INT2VECTOROID)
	C(INT4OID)
	C(INT4ARRAYOID)
	C(TEXTOID)
	C(XMLOID)
	C(TEXTARRAYOID)
	C(OIDOID)
	C(OIDVECTOROID)
	C(TIDOID)
	C(XIDOID)
	C(CIDOID)
	C(POINTOID)
	C(LSEGOID)
	C(PATHOID)
	C(BOXOID)
	C(POLYGONOID)
	C(LINEOID)
	C(FLOAT4OID)
	C(FLOAT4ARRAYOID)
	C(FLOAT8OID)
	C(ABSTIMEOID)
	C(RELTIMEOID)
	C(TINTERVALOID)
	C(UNKNOWNOID)
	C(CIRCLEOID)
	C(CASHOID)
	C(MACADDROID)
	C(INETOID)
	C(CIDROID)
	C(ACLITEMOID)
	C(BPCHAROID)
	C(VARCHAROID)
	C(DATEOID)
	C(TIMEOID)
	C(TIMESTAMPOID)
	C(TIMESTAMPTZOID)
	C(INTERVALOID)
	C(TIMETZOID)
	C(BITOID)
	C(VARBITOID)
	C(NUMERICOID)
	C(REFCURSOROID)
	C(REGPROCEDUREOID)
	C(REGOPEROID)
	C(REGOPERATOROID)
	C(REGTYPEOID)
	C(REGTYPEARRAYOID)
	C(RECORDOID)
	C(RECORDARRAYOID)
	C(CSTRINGOID)
	C(CSTRINGARRAYOID)
	C(ANYOID)
	C(ANYARRAYOID)
	C(ANYELEMENTOID)
	C(ANYENUMOID)
	C(ANYNONARRAYOID)
	C(VOIDOID)
	C(TRIGGEROID)
	C(LANGUAGE_HANDLEROID)
	C(INTERNALOID)
	C(OPAQUEOID)
	C(TSVECTOROID)
	C(GTSVECTOROID)
	C(TSQUERYOID)
	C(REGCONFIGOID)
	C(REGDICTIONARYOID)

/* catalog/pg_namespace.h */
	C(PG_CATALOG_NAMESPACE)
	C(PG_TOAST_NAMESPACE)
	C(PG_PUBLIC_NAMESPACE)

/* storage/off.h */
	C(InvalidOffsetNumber)
	C(FirstOffsetNumber)
	C(MaxOffsetNumber)
	C(OffsetNumberMask)

/* storage/lock.h */
	C(MAX_LOCKMODES)
	C(DEFAULT_LOCKMETHOD)
	C(USER_LOCKMETHOD)

/* storage/lmgr.h */
#define CC(V) C(V##Lock)
	CC(No)
	CC(Exclusive)
	CC(AccessShare)
	CC(AccessExclusive)
	CC(RowShare)
	CC(RowExclusive)
	CC(Share)
	CC(ShareRowExclusive)
	CC(ShareUpdateExclusive)
#undef CC

/* libpq/libpq-fs.h */
	C(INV_WRITE)
	C(INV_READ)

/* storage/buf.h */
	C(InvalidBuffer)

/* storage/backendid.h */
	C(InvalidBackendId)

/* storage/large_object.h */
	C(LOBLKSIZE)

/* End Of ints */
#undef OBJECTIFY

#define OBJECTIFY(ARG) (PyLong_FromUnsignedLong(ARG))
/* storage/block.h */
	C(InvalidBlockNumber)
	C(MaxBlockNumber)
/* End of unsigned long */
#undef OBJECTIFY

#define OBJECTIFY(ARG) (PyUnicode_FromString(ARG))
	C(PG_VERSION)
	C(PG_VERSION_STR)

#undef C
#undef OBJECTIFY
