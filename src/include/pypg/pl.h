/*
 * pl prototypes
 */
#ifndef PyPg_pl_H
#define PyPg_pl_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern PyObj Py_ReturnArgs;
extern PyObj PyExc_PostgresStopEvent;

Datum pl_validator(PG_FUNCTION_ARGS);
Datum pl_handler(PG_FUNCTION_ARGS);

/*
 * volatility of the current function.
 * Cannot go from stable or immutable to volatile.
 */
extern char fn_volatile;

#define PL_INTERNAL() \
	IDSTR(__func__)

#define PL_ENTRY_POINTS() \
	IDSTR(main) \
	IDSTR(send) \
	IDSTR(before_insert) \
	IDSTR(before_update) \
	IDSTR(before_delete) \
	IDSTR(after_insert) \
	IDSTR(after_update) \
	IDSTR(after_delete) \
	IDSTR(before_insert_statement) \
	IDSTR(before_update_statement) \
	IDSTR(before_delete_statement) \
	IDSTR(before_truncate_statement) \
	IDSTR(after_insert_statement) \
	IDSTR(after_update_statement) \
	IDSTR(after_delete_statement) \
	IDSTR(after_truncate_statement)

#define PL_TRIGGER_ORIENTATIONS() \
	IDSTR(ROW) \
	IDSTR(STATEMENT)

#define PL_TRIGGER_TIMINGS() \
	IDSTR(BEFORE) \
	IDSTR(AFTER)

#define PL_MANIPULATIONS() \
	IDSTR(INSERT) \
	IDSTR(UPDATE) \
	IDSTR(DELETE) \
	IDSTR(TRUNCATE)

#define IDSTR(NAME) extern PyObj NAME##_str_ob;
PL_INTERNAL()
PL_ENTRY_POINTS()
PL_MANIPULATIONS()
PL_TRIGGER_ORIENTATIONS()
PL_TRIGGER_TIMINGS()
#undef IDSTR

PyObj pl_call_state_get(void);
void pl_call_state_save(PyObj);

/*
 * Structure used to hold information about the execution state of the
 * procedure. This is used by a context callback to provide information
 * about what the function was doing when the error occurred.
 *
 * "[funcname() while materializing]", "[funcname() while building arguments]"
 */
struct pl_exec_state {
	struct pl_fn_info *fn_info;
	MemoryContext return_memory_context;
	const char *description; /* what was the PL doing? */
};

extern struct pl_exec_state *pl_execution_context;
#define PL_CONTEXT() \
	(pl_execution_context)
#define PL_FN_INFO() \
	(pl_execution_context->fn_info)
#define PL_GET_LANGUAGE() ( \
	!pl_execution_context ? InvalidOid : \
	PyPgFunction_GetLanguageOid(PL_FN_INFO()->fi_func) \
)
#define PL_FN_FILENAME() ( \
	!pl_execution_context ? NULL : \
	PyPgFunction_GetFilename(PL_FN_INFO()->fi_func) \
)
#define PL_FN_READONLY() ( \
	!pl_execution_context ? true : \
	PyPgFunction_GetVolatile(PL_FN_INFO()->fi_func) != PROVOLATILE_VOLATILE \
)

/*
 * structure for fn_extra
 *
 * input and output point to the same objects in 'func' iff
 * the function is not polymorphic.
 */
struct pl_fn_info {
	/*
	 * fi_xid used to determine if the fn_info is still valid in situations
	 * where a caller accidently holds onto fn_extra across transactions.
	 *
	 * That is, the references to all the PyObject's stored here are owned by
	 * the TransactionScope set object. At the end of the transaction, that
	 * set object is cleared, and the objects referenced here may no longer
	 * exist. (Py_XACTREF(ob) and Py_DEXTREF(ob) manage add/remove)
	 */
	unsigned long fi_xid;		/* pl_xid that the cache was created in */
	PyObject *fi_func;			/* PyPgFunction */
	PyObject *fi_module;		/* PyPgFunction's module object */
	/*
	 * VPC-SRF state.
	 */
	PyObject *fi_state;			/* call state; see src/stateful.c - Postgres.Stateful */
	PyObject *fi_state_owner;	/* the object (Postgres.Stateful) that owns the state */
								/* Protects against multiple uses of @Stateful in a single call. */
	PyObject *fi_internal_state;/* internal call state; td for triggers, iterator for SRFs */
	PyObject *fi_input;			/* func->fn_input or polymorphed variant */
	PyObject *fi_output;		/* func->fn_output or polymorphed variant */
};

/* Feed it function call info, returns the fi_state from fn_info */
#define FN_INFO_HAS_STATE(FCINFO) \
	((struct pl_fn_info *) fcinfo->flinfo->fn_extra)->fi_internal_state

/*
 * If the fi_xid is 1, that means that the function never needs to be refreshed.
 * [currently only used by the inline executor]
 */
#define FN_INFO_NEEDS_REFRESH(FN_INFO) \
	(fn_info == NULL || (fn_info->fi_xid != 1 && fn_info->fi_xid != ext_xact_count))

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_pl_H */
