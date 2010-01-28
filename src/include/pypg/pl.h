/*
 * pl prototypes
 */
#ifndef PyPg_pl_H
#define PyPg_pl_H 0
#ifdef __cplusplus
extern "C" {
#endif

void _PG_init(void);
void pl_first_call(void);

PyObj pl_call_state_get(void);
void pl_call_state_save(PyObj);

Datum pl_validator(PG_FUNCTION_ARGS);
Datum pl_handler(PG_FUNCTION_ARGS);

/*
 * Allows DB interface points to identify if it can proceed.
 */
typedef enum {
	pl_outside_transaction = -2,
	pl_not_initialized = -1,
	pl_ready_for_access = 0,
	pl_in_failed_transaction = 1,
} pl_state_t;
extern pl_state_t pl_state;

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
	 * exist.
	 */
	unsigned long fi_xid;		/* pl_xid that the cache was created in */
	PyObject *fi_func;			/* PyPgFunction */
	PyObject *fi_module;		/* PyPgFunction's module object */
	/*
	 * VPC-SRF state.
	 */
	PyObject *fi_state;			/* call state; see src/stateful.c - Postgres.Stateful */
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
	(fn_info == NULL || (fn_info->fi_xid != 1 && fn_info->fi_xid != pl_xact_count))

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_pl_H */
