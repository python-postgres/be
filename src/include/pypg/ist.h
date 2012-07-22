/*
 * pl IST management - routines used by Postgres.Transaction objects.
 */
#ifndef PyPg_ist_H
#define PyPg_ist_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern unsigned long ist_count; /* The current "xid". */

/* IST states */
#define ist_open 'O'
#define ist_committed 'C'
#define ist_aborted 'X'
#define ist_new ' '

/*
 * Aborts the current transaction.
 * Provides appropriate errors when given inappropriate state or xid.
 *
 * ON ERROR: Returns false and sets a Python exception.
 */
bool ist_abort(unsigned long xid, char state);


/*
 * Commits the current transaction iff !pl_db_in_error; otherwise
 * abort the transaction and error.
 * Provides appropriate errors when given inappropriate state or xid.
 *
 * ON ERROR: Returns false and sets a Python exception.
 */
bool ist_commit(unsigned long xid, char state);

/*
 * Start a new IST. Increments ist_count by one.
 * Provides appropriate errors when given inappropriate state.
 *
 * ON ERROR: Returns false and sets a Python exception.
 */
bool ist_begin(char state);

/*
 * Used in cases were the stored_ist_count != ist_count
 *
 * ON ERROR: Postgres elog
 */
void ist_reset(unsigned long n_to_abort);

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_ist_H */
