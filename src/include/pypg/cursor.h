/*
 * Postgres cursors
 */
#ifndef PyPg_cursor_H
#define PyPg_cursor_H 0
#ifdef __cplusplus
extern "C" {
#endif

#define CUR_CHUNKS(N) (N + 1)
#define CUR_ROWS(N) (-(N + 2))
#define CUR_COLUMN (-2)
#define CUR_SCROLL_FORWARD 1
#define CUR_SCROLL_BACKWARD -1
#define CUR_UNINITIALIZED 0

typedef struct PyPgCursor {
	PyObject_HEAD
	PyObj cur_statement;		/* PyPgStatement object */
	PyObj cur_output;			/* PyPgType from cur_statement.ps_output */
	PyObj cur_parameters;		/* Original statement `args` given to */

	/*
	 * 0 means uninitialized
	 * -1 means BACKWARD direction on a SCROLL Portal
	 * 1 means FORWARD direction on a SCROLL Portal
	 * < -1 means a rows() iter
	 * > 1 means a chunks() iter and specifies the size of chunks to read.
	 */
	Py_ssize_t cur_chunksize;

	char *cur_name;

	/*
	 * The pl transaction id that the cursor was created in.
	 * Used to identify the validity of cur_portal.
	 */
	unsigned long cur_xid;

	Portal cur_portal;
	PyObj cur_buffer; /* Iterable used for rows() cursors */
} * PyPgCursor;

extern PyTypeObject PyPgCursor_Type;

PyObj PyPgCursor_NEW(
	PyTypeObject *subtype,
	PyObj statement,
	PyObj args, PyObj kw,
	Py_ssize_t chunksize);

/*
 * Close the cursor.
 */
int PyPgCursor_Close(PyObj self);

#define PyPgCursor_New(STMT, ARGS, KW, CS) PyPgCursor_NEW(&PyPgCursor_Type, STMT, ARGS, KW, CS)

#define PyPgCursor_Check(SELF) (PyObject_TypeCheck(SELF, &PyPgCursor_Type))
#define PyPgCursor_CheckExact(SELF) (Py_TYPE(SELF) == &PyPgCursor_Type)
#define PyPgCursor_Materializable(SELF) (PyPgCursor_CheckExact(SELF) && !PyPgCursor_IsColumn(SELF))
#define PyPgCursor_GetStatement(SELF) (((PyPgCursor) SELF)->cur_statement)
#define PyPgCursor_GetParameters(SELF) (((PyPgCursor) SELF)->cur_parameters)
#define PyPgCursor_GetOutput(SELF) (((PyPgCursor) SELF)->cur_output)
#define PyPgCursor_GetChunksize(SELF) (((PyPgCursor) SELF)->cur_chunksize)
#define PyPgCursor_GetXid(SELF) (((PyPgCursor) SELF)->cur_xid)
#define PyPgCursor_GetPortal(SELF) (((PyPgCursor) SELF)->cur_portal)
#define PyPgCursor_GetBuffer(SELF) (((PyPgCursor) SELF)->cur_buffer)
#define PyPgCursor_GetName(SELF) (((PyPgCursor) SELF)->cur_name)

#define PyPgCursor_SetStatement(SELF, STMT) (((PyPgCursor) SELF)->cur_statement = STMT)
#define PyPgCursor_SetParameters(SELF, PARAMS) (((PyPgCursor) SELF)->cur_parameters = PARAMS)
#define PyPgCursor_SetOutput(SELF, TYP) (((PyPgCursor) SELF)->cur_output = TYP)
#define PyPgCursor_SetChunksize(SELF, CS) (((PyPgCursor) SELF)->cur_chunksize = CS)
#define PyPgCursor_SetXid(SELF, XID) (((PyPgCursor) SELF)->cur_xid = XID)
#define PyPgCursor_SetPortal(SELF, PORT) (((PyPgCursor) SELF)->cur_portal = PORT)
#define PyPgCursor_SetBuffer(SELF, BUF) (((PyPgCursor) SELF)->cur_buffer = BUF)
#define PyPgCursor_SetName(SELF, NAMESTR) (((PyPgCursor) SELF)->cur_name = NAMESTR)

/* Scrollable */
#define PyPgCursor_IsDeclared(SELF) (PyPgCursor_GetChunksize(SELF) == 1 || PyPgCursor_GetChunksize(SELF) == -1)
/* One row per __next__ */
#define PyPgCursor_IsRows(SELF) ((((PyPgCursor) SELF)->cur_chunksize) < -2)
/* N rows per __next__ in a list object */
#define PyPgCursor_IsChunks(SELF) ((((PyPgCursor) SELF)->cur_chunksize) > 1)
/* First column of each row per __next__ */
#define PyPgCursor_IsColumn(SELF) ((((PyPgCursor) SELF)->cur_chunksize) == -2)

#define PyPgCursor_GetChunksReadSize(SELF) (PyPgCursor_GetChunksize(SELF) - 1)
#define PyPgCursor_GetRowsReadSize(SELF) ((-PyPgCursor_GetChunksize(SELF)) - 2)
#define PyPgCursor_GetDirection(SELF) (PyPgCursor_GetChunksize(SELF) == -1 ? false : true)

#define PyPgCursor_IsClosed(SELF) ( \
	(PyPgCursor_GetXid(SELF) != ext_xact_count) || \
	!PortalIsValid(PyPgCursor_GetPortal(SELF)))

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_cursor_H */
