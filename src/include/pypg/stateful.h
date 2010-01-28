/*
 * Postgers.Stateful
 */
#ifndef PyPg_stateful_H
#define PyPg_stateful_H 0
#ifdef __cplusplus
extern "C" {
#endif

typedef struct PyPgStateful {
	PyObject_HEAD
	PyObj sf_source;	/* The object that produces state; normally def main(): */
	PyObj sf_func;		/* The PyPgFunction object that goes with this state */
} * PyPgStateful;
extern PyTypeObject PyPgStateful_Type;

#define PyPgStateful_NEW(TYP) ((PyObj) PyPgStateful_Type.tp_alloc(TYP, 0))
#define PyPgStateful_New() PyPgStateful_NEW(&PyPgStateful_Type)

#define PyPgStateful_GetSource(SELF) \
	((PyPgStateful) SELF)->sf_source
#define PyPgStateful_GetFunction(SELF) \
	((PyPgStateful) SELF)->sf_func

#define PyPgStateful_SetSource(SELF, SOURCE) \
	((PyPgStateful) SELF)->sf_source = SOURCE
#define PyPgStateful_SetFunction(SELF, FUNC) \
	((PyPgStateful) SELF)->sf_func = FUNC

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_stateful_H */
