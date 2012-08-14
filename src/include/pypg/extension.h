/*
 * extension prototypes and externs
 */
#ifndef pg_extension_H
#define pg_extension_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern unsigned long ext_xact_count;
extern PyObj TransactionScope;
#define Py_XACTREF(OB) PySet_Add(TransactionScope, OB)
#define Py_DEXTREF(OB) PySet_Discard(TransactionScope, OB)

void _PG_init(void);

void ext_entry(void);
void ext_check_state(int errlevel_to_raise, unsigned long previous_ist_count);

/*
 * Allows DB interface points to identify if it can proceed.
 */
typedef enum {
	/*
	 * State used for between transaction work.
	 */
	xact_between = -2,

	/*
	 * _PG_init hasn't finished.
	 */
	init_pending = -1,

	/*
	 * Any entry point is accessible or can access any DB function.
	 */
	ext_ready = 0,

	/*
	 * Interaction with database interfaces are prohibited.
	 */
	xact_failed = 1,

	/*
	 * An unnatural state used to indicate two things:
	 *  1. on_proc_exit handler has been called
	 *  2. the PL's internal state cannot be trusted.
	 */
	ext_term = 3,
} extension_state_t;

extern extension_state_t ext_state;

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_extension_H */
