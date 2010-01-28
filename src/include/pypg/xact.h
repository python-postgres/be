/*
 * Postgres IST interfaces
 */
#ifndef PyPg_xact_H
#define PyPg_xact_H 0
#ifdef __cplusplus
extern "C" {
#endif

typedef struct PyPgTransaction {
	PyObject_HEAD
	char state;
	unsigned long id;
} * PyPgTransaction;
extern PyTypeObject PyPgTransaction_Type;

PyObj PyPgTransaction_NEW(PyTypeObject *subtype);

/* Always sets a Python error. */
void pl_ist_reset(unsigned long number_to_abort);

#define PyPgTransaction_New() PyPgTransaction_NEW(&PyPgTransaction_Type)
#define PyPgTransaction(SELF) ((PyPgTransaction) SELF)

#define PyPgTransaction_GetState(SELF) \
	(PyPgTransaction(SELF)->state)
#define PyPgTransaction_GetId(SELF) \
	(PyPgTransaction(SELF)->id)

#define PyPgTransaction_SetState(SELF, NEWSTATE) \
	PyPgTransaction(SELF)->state = NEWSTATE
#define PyPgTransaction_SetId(SELF, ID) \
	PyPgTransaction(SELF)->id = ID

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_xact_H */
