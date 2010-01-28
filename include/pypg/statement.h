/*
 * Postgres prepared statements
 */
#ifndef PyPg_statement_H
#define PyPg_statement_H 0
#ifdef __cplusplus
extern "C" {
#endif

int execute_statements(const char *src);

typedef struct PyPgStatement {
	PyObject_HEAD
	PyObj ps_string;				/* SQL statement string */
	PyObj ps_input;					/* TupleDesc */
	PyObj ps_output;				/* TupleDesc */
	PyObj ps_command;				/* PyUnicode, CommandTag of the statement */
	PyObj ps_parameters;			/* constant parameters; None if none */

	MemoryContext ps_memory;		/* Context for statement data */
	Oid *ps_parameter_types;		/* Type Oids of the statement's parameters */
	OverrideSearchPath *ps_path;	/* Search path when the statement was created */
	SPIPlanPtr ps_plan;				/* Saved NO SCROLL Plan */
	SPIPlanPtr ps_scroll_plan;		/* Saved SCROLL Plan */
} * PyPgStatement;

extern PyTypeObject PyPgStatement_Type;
PyObj PyPgStatement_NEW(PyTypeObject *subtype,
	PyObj sql_statement_string, PyObj parameters);
SPIPlanPtr PyPgStatement_GetPlan(PyObj self);
SPIPlanPtr PyPgStatement_GetScrollPlan(PyObj self);

#define PyPgPreparedStatment_New(STATEMENT_STRING) PyPgStatement_NEW(&PyPgStatement_Type, STATEMENT_STRING)

#define PyPgStatement(SELF) ((PyPgStatement) SELF)

#define PyPgStatement_Check(SELF) (PyObject_TypeCheck(SELF, &PyPgStatement_Type))
#define PyPgStatement_CheckExact(SELF) (Py_TYPE(SELF) == &PyPgStatement_Type)

#define PyPgStatement_GetString(SELF) (PyPgStatement(SELF)->ps_string)
#define PyPgStatement_GetInput(SELF) (PyPgStatement(SELF)->ps_input)
#define PyPgStatement_GetOutput(SELF) (PyPgStatement(SELF)->ps_output)
#define PyPgStatement_GetParameters(SELF) (PyPgStatement(SELF)->ps_parameters)
#define PyPgStatement_GetParameterTypes(SELF) (PyPgStatement(SELF)->ps_parameter_types)
#define PyPgStatement_GetCommand(SELF) (PyPgStatement(SELF)->ps_command)
#define PyPgStatement_GetMemory(SELF) (PyPgStatement(SELF)->ps_memory)
#define PyPgStatement_GetPath(SELF) (PyPgStatement(SELF)->ps_path)

#define PyPgStatement_SetString(SELF, STR) (PyPgStatement(SELF)->ps_string = STR)
#define PyPgStatement_SetInput(SELF, TD) (PyPgStatement(SELF)->ps_input = TD)
#define PyPgStatement_SetOutput(SELF, TD) (PyPgStatement(SELF)->ps_output = TD)
#define PyPgStatement_SetParameters(SELF, PARAMS) (PyPgStatement(SELF)->ps_parameters = PARAMS)
#define PyPgStatement_SetParameterTypes(SELF, PTYPES) (PyPgStatement(SELF)->ps_parameter_types = PTYPES)
#define PyPgStatement_SetCommand(SELF, CMD) (PyPgStatement(SELF)->ps_command = CMD)
#define PyPgStatement_SetMemory(SELF, MEM) (PyPgStatement(SELF)->ps_memory = MEM)
#define PyPgStatement_SetPath(SELF, PATH) (PyPgStatement(SELF)->ps_path = PATH)

#define PyPgStatement_ReturnsRows(SELF) \
	(PyPgStatement_GetOutput(SELF) != Py_None)

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_statement_H */
