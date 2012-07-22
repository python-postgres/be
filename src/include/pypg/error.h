/*
 * Python exception tools and
 * Postgres elog/ereport support
 */
#ifndef PyPg_error_H
#define PyPg_error_H 0
#ifdef __cplusplus
extern "C" {
#endif

void error_init_tracebacks(void);

extern PyObj PYSTR(pg_errordata);
extern PyObj PYSTR(pg_inhibit_pl_context);

extern PyObj PyExc_PostgresException;

/*
 * Should appear in a condition before most PG_TRY() blocks accessible from
 * Python.
 */
#define DB_IS_NOT_READY() \
	(ext_state == xact_failed ? PyErr_SetInFailedTransaction() : ( \
	(ext_state != ext_ready ? PyErr_SetDatabaseAccessDenied() : ext_state \
)))

/*
 * Returns true on success and false when a Python exception is set.
 * (PyExc_ValueError)
 */
bool sqlerrcode_from_PyObject(PyObj errcode_ob, int *errcode_out);

/*
 * Extract the pl state's description and filename, if any.
 * Never fails, desc and filename are NULL when unavailable.
 */
void collect_errcontext_params(
	struct pl_exec_state *pl_ctx,
	const char **desc, PyObj *filename);

/*
 * When a Python exception needs to be raised to the outer try-block,
 * this function should be used.
 */
void PyErr_RelayException(void);

/*
 * ereport the current Python exception with additional context.
 */
void PyErr_ThrowPostgresErrorWithContext(int code, const char *errstr, struct pl_exec_state *pl_ctx);

/*
 * ereport the current Python exception using the given code and errstr.
 */
void PyErr_ThrowPostgresErrorWithCode(int code, const char *errstr);

/*
 * ereport the current Python exception using the given errstr.
 * (code defaults to intenal error 'XX000')
 */
void PyErr_ThrowPostgresError(const char *errmsg);

/*
 * ereport the current Python exception with the warning
 */
void PyErr_EmitPostgresWarning(const char *errmsg);

/*
 * Convert the Postgres error to a Python exception and mark database as being
 * in error(ext_state != pl_ready_for_access).
 */
void PyErr_SetPgError(bool ignore_ext_state); /* PyErr_Occurred() != NULL afterwards. */

/*
 * When a Postgres ERROR is thrown in a function that cannot raise it,
 * this function should be used to notify the user of the ERROR.
 *
 * Normally, this is used exclusively in dealloc situations where we would be
 * longjmp'ing out of the GC. (right, that's bad)
 */
void PyErr_EmitPgErrorAsWarning(const char *msg);

/*
 * Sets a RuntimeError indicating that the database was accessed at an
 * inappropriate time.
 *
 * Always returns true.
 */
bool PyErr_SetDatabaseAccessDenied(void);

/*
 * Should be called when Postgres interfaces are accessed via Python while
 * inside a failed transaction.
 *
 * Always returns true.
 */
bool PyErr_SetInFailedTransaction(void);

#define _pg_elevels_() \
	ELEVEL(DEBUG5) \
	ELEVEL(DEBUG4) \
	ELEVEL(DEBUG3) \
	ELEVEL(DEBUG2) \
	ELEVEL(DEBUG1) \
	ELEVEL(LOG) \
	ELEVEL(COMMERROR) \
	ELEVEL(INFO) \
	ELEVEL(NOTICE) \
	ELEVEL(WARNING) \
	ELEVEL(ERROR) \
	ELEVEL(FATAL) \
	ELEVEL(PANIC)

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_error_H */
