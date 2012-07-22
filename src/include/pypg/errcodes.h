/*
 * Postgres error codes for Python
 */
#ifndef PyPg_errcodes_H
#define PyPg_errcodes_H 0
#ifdef __cplusplus
extern "C" {
#endif

/* Class PY - Python Errors */
#define ERRCODE_PYTHON_ERROR 				MAKE_SQLSTATE('P','Y', '0','0','0')
#define ERRCODE_PYTHON_EXCEPTION 			MAKE_SQLSTATE('P','Y', '0','0','1')
#define ERRCODE_PYTHON_PROTOCOL_VIOLATION	MAKE_SQLSTATE('P','Y', '0','0','2')
/*
 * Used internally by the PL.
 */
#define ERRCODE_PYTHON_RELAY				MAKE_SQLSTATE('P','Y', '9','9','9')

#ifdef __cplusplus
}
#endif
#endif
