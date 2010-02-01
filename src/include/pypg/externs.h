/*
 * extern them together
 */

/* initialized by _PG_init() */
extern PyObj TransactionScope;
extern PyObj FormatTraceback;
extern PyObj PyExc_PostgresException;
extern PyObj Py_Postgres_module;
extern PyObj Py_anonymous_composites;

/* initialized by PyPgType_Init() */
extern PyObj PyPg_cstring_Array_Type;
extern PyObj PyPg_pg_attribute_Type;

/*
 * volatility of the current function.
 * Cannot go from stable or immutable to volatile.
 */
extern char fn_volatile;

/*
 * Number of transactions seen by the PL.
 */
extern unsigned long pl_xact_count;
extern unsigned long pl_subxact_rollback_count;

#define IDSTR(NAME) extern PyObj NAME##_str_ob;
PYPG_REQUISITE_NAMES()
PYPG_ENTRY_POINTS()
MANIPULATIONS()
TRIGGER_ORIENTATIONS()
TRIGGER_TIMINGS()
#undef IDSTR
