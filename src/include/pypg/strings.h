/*
 * Collection X-Macro's for building PyUnicode objects.
 */

#define PYPG_REQUISITE_NAMES() \
	IDSTR(exec) \
	IDSTR(__func__) \
	IDSTR(Postgres) \
	IDSTR(pg_errordata) \
	IDSTR(pg_inhibit_pl_context)

#define PYPG_ENTRY_POINTS() \
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

#define TRIGGER_ORIENTATIONS() \
	IDSTR(ROW) \
	IDSTR(STATEMENT)

#define TRIGGER_TIMINGS() \
	IDSTR(BEFORE) \
	IDSTR(AFTER)

/*
 * Initialized into globals by _PG_init:
 *  INSERT_str_ob, UPDATE_str_ob, DELETE_str_ob, and TRUNCATE_str_ob.
 */
#define MANIPULATIONS() \
	IDSTR(INSERT) \
	IDSTR(UPDATE) \
	IDSTR(DELETE) \
	IDSTR(TRUNCATE)
