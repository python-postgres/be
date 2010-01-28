/*
 * pl IST management - routines used by Postgres.Transaction objects.
 */
#ifndef PyPg_ist_H
#define PyPg_ist_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern unsigned long pl_ist_count; /* The current "xid". */

/* IST states */
#define pl_ist_open 'O'
#define pl_ist_committed 'C'
#define pl_ist_aborted 'X'
#define pl_ist_new ' '

/*
 * Aborts the current transaction.
 * Provides appropriate errors when given inappropriate state or xid.
 *
 * ON ERROR: Returns false and sets a Python exception.
 */
bool pl_ist_abort(unsigned long xid, char state);


/*
 * Commits the current transaction iff !pl_db_in_error; otherwise
 * abort the transaction and error.
 * Provides appropriate errors when given inappropriate state or xid.
 *
 * ON ERROR: Returns false and sets a Python exception.
 */
bool pl_ist_commit(unsigned long xid, char state);

/*
 * Start a new IST. Increments pl_ist_count by one.
 * Provides appropriate errors when given inappropriate state.
 *
 * ON ERROR: Returns false and sets a Python exception.
 */
bool pl_ist_begin(char state);

/*
 * Used in cases were the stored_ist_count != pl_ist_count
 *
 * ON ERROR: Postgres elog
 */
void pl_ist_reset(unsigned long n_to_abort);

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_ist_H */
