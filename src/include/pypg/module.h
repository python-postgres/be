/*
 * Postgres utilities
 */
#ifndef PyPg_module_H
#define PyPg_module_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern PyObj Py_Postgres_module;

/*
 * X-Macro of built-ins, expand with PyPg##X##_Type
 */
#define PYPG_TYPES(END) \
	TYP(Stateful) \
	TYP(ErrorData) \
	TYP(TriggerData) \
	TYP(Function) \
	TYP(TupleDesc) \
	TYP(Transaction) \
	TYP(Type) \
	TYP(Object) \
	TYP(String) \
	TYP(Array) \
	TYP(Pseudo) \
	TYP(Statement) \
	TYP(Cursor) \
	END

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_module_H */
