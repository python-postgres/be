/*
 * Python utilities
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <pypg/python.h>

/*
 * The reference owner stack. Used to manage DECREFs on objects.
 */
struct PyObject_ReferenceOwner *_PyObject_OwnerStack = NULL;

/*
 * utility function for working with objects expected to be ASCII characters.
 */
PyObj
PyObject_AsASCIIString(PyObj stringable_ob)
{
	PyObj uni, rob;
	uni = PyObject_Str(stringable_ob);
	if (uni == NULL)
		return(NULL);
	rob = PyUnicode_AsASCIIString(uni);
	Py_DECREF(uni);
	return(rob);
}
