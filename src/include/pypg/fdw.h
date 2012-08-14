/*
 * fdw prototypes
 */
#ifndef PyPg_fdw_H
#define PyPg_fdw_H 0
#ifdef __cplusplus
extern "C" {
#endif

/*
 * Not allocated or referenced by Python code.
 */
struct FDWR {
	FdwRoutine node;
	/*
	 * The actual FDW implementation.
	 */
	PyObj implementation;
	/* ext_xact when assigned to TransactionScope */
	unsigned long ext_xact;
};

Datum fdw_validator(PG_FUNCTION_ARGS);
Datum fdw_handler(PG_FUNCTION_ARGS);

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_fdw_H */
