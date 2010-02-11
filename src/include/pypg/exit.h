#ifndef PyPg_exit_H
#define PyPg_exit_H 0
#ifdef __cplusplus
extern "C" {
#endif

/*
 * on_proc_exit handler; arg is always '0'.
 */
void pl_exit(int code, Datum arg);

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_exit_H */
