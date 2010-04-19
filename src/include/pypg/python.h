/*
 * Python utility macros and definitions.
 */
#ifndef PyPg_python_H
#define PyPg_python_H 0
#ifdef __cplusplus
extern "C" {
#endif

typedef PyObject * PyObj;

extern PyObj Py_builtins_module, Py_compile_ob, Py_exec_str_ob;
extern const char *python_server_encoding;

PyObj PyObject_AsASCIIString(PyObj stringable_ob);

/* New References */
/* Avoids exception on absence */
#define Py_ATTR(ob,str)	(PyObject_HasAttrString(ob,str)? \
								PyObject_GetAttrString(ob,str):(PyObj) NULL)

#define Py_Call(OB,...) PyObject_CallFunctionObjArgs(OB, __VA_ARGS__, NULL)

/*
 * Create a 'python_server_encoding' encoded bytes object from an arbitrary
 * Python object.
 */
#define PyObject_StrBytes(_OB) \
do{ \
	PyObj _OB_STR = PyObject_Str(*_OB); \
	Assert(_OB != NULL && *_OB != NULL);\
	Py_DECREF(*_OB); \
	*_OB = NULL; \
	if (_OB_STR != NULL) { \
		*_OB = PyUnicode_AsEncodedString(_OB_STR, python_server_encoding, "strict"); \
		Py_DECREF(_OB_STR); \
	} \
}while(0)

#define PyUnicode_FromPointerAndSize(CHAR, SIZE) \
	PyUnicode_Decode((const char *) CHAR, SIZE, python_server_encoding, "replace")
#define PyUnicode_FromCString(CHAR) \
	PyUnicode_FromPointerAndSize(CHAR, strlen(CHAR))
#define PyUnicode_FromTEXT(TEXT) \
	PyUnicode_FromPointerAndSize(VARDATA(TEXT), VARSIZE_ANY_EXHDR(TEXT))

#define Py_Require_Type(TYP, ITYP) \
	((PyObj) ITYP == (PyObj) TYP || PyType_IsSubtype((ITYP), ((PyTypeObject *) TYP))) ? 0 : ( \
		PyErr_Format(PyExc_TypeError, \
			"%s requires a %s, given '%s'", \
			PG_FUNCNAME_MACRO, \
			((PyTypeObject *) TYP)->tp_name, ((PyTypeObject *) ITYP)->tp_name) ? -1 : -1 \
	)

/*
 * Python Reference Owners
 *
 * Py_ALLOCATE_OWNER(), Py_ACQUIRE(), Py_DEALLOCATE_OWNER()
 *
 * Constructs for tracking and releasing references owned by a C block.
 *
 * Usage:
 *
 * Py_ALLOCATE_OWNER();
 * {
 * 	PyObj ob;
 *
 * 	ob = PyLong_FromLong(10);
 * 	Py_ACQUIRE(ob);
 * 	...
 * 	...
 * 	Py_ACQUIRE_SPACE();
 * 	{
 * 		while ((ob = PyIter_Next(iter_ob)) != NULL)
 * 		{
 * 			Py_XREPLACE(ob);
 * 			...
 * 		}
 * 	}
 * 	Py_RELEASE_SPACE();
 * }
 * Py_DEALLOCATE_OWNER();
 *
 * Py_ACQUIRE will "send" the reference to the current owner. When the ownership
 * ends(Py_END_OWNERSHIP()), the acquired references will be released.
 *
 * NOTE: Jumping out of the block will cause reference leaks.
 * If you must 'break', or 'goto', be sure to call _PYRO_DEALLOCATE()
 * before doing so.
 */

extern struct PyObject_ReferenceOwner *_PyObject_OwnerStack;

#define _PYRO_CAPACITY 8
struct PyObject_ReferenceOwner {
	PyObject *acquired[_PYRO_CAPACITY];
	int n_objects; /* Number of acquisitions */
};

#define _PYRO_DECLARE_OWNER() \
	volatile struct PyObject_ReferenceOwner _next_owner = {{NULL,},0}; \
	struct PyObject_ReferenceOwner * volatile _prev_owner; \
	_prev_owner = (struct PyObject_ReferenceOwner * volatile) _PyObject_OwnerStack; \
	_PyObject_OwnerStack = ((struct PyObject_ReferenceOwner *) &_next_owner)

#define _PYRO_GET_ACQUIRED(_py_NN) \
	(_PyObject_OwnerStack->acquired[_py_NN])
#define _PYRO_SET_ACQUIRED(_py_N, _py_OB) \
	(_PYRO_GET_ACQUIRED(_py_N) = _py_OB)
#define _PYRO_N_ACQUIRED (_PyObject_OwnerStack->n_objects)

#define Py_ALLOCATE_OWNER() \
	{ \
		_PYRO_DECLARE_OWNER()

/* cheers, suffix operator. */
#define Py_ACQUIRE(OB) \
	(_PYRO_SET_ACQUIRED(_PYRO_N_ACQUIRED++, OB))
#define Py_XACQUIRE(OB) \
	(OB == NULL ? NULL : Py_ACQUIRE(OB))

#define Py_ACQUIRE_SPACE() \
	{ \
		int _pyro_acquired_offset = _PYRO_N_ACQUIRED++; \
		Assert(_PYRO_GET_ACQUIRED(_pyro_acquired_offset) == NULL);

/*
 * For use with iterators. At the start of a loop, Py_XREPLACE(nextval).
 */
#define Py_XREPLACE(OB) do { \
	PyObj _STORED_pyro = _PYRO_GET_ACQUIRED(_pyro_acquired_offset); \
	_PYRO_SET_ACQUIRED(_pyro_acquired_offset, OB); \
	Py_XDECREF(_STORED_pyro); \
} while (0)

/*
 * If we can reclaim the space for further acquisitions, do so.
 * Otherwise, just ignore the whole thing let owner deallocation take care of
 * the reference.
 */
#define Py_RELEASE_SPACE() \
		if (_pyro_acquired_offset + 1 == _PYRO_N_ACQUIRED) \
		{ \
			PyObj _STORED_pyro = _PYRO_GET_ACQUIRED(_pyro_acquired_offset); \
			_PYRO_SET_ACQUIRED(_pyro_acquired_offset, NULL); \
			_PYRO_N_ACQUIRED = _pyro_acquired_offset; \
			Py_XDECREF(_STORED_pyro); \
		} \
	}

#define _PYRO_RELEASE_ALL() do { \
	int i; \
	for (i = 0; i < _PYRO_N_ACQUIRED; ++i) \
	{ \
		Py_XDECREF(_PYRO_GET_ACQUIRED(i)); \
	} \
	_PYRO_N_ACQUIRED = 0; \
} while(0)

#define _PYRO_DEALLOCATE(...) do { \
	_PYRO_RELEASE_ALL(); \
	_PyObject_OwnerStack = ((struct PyObject_ReferenceOwner *) _prev_owner); \
	__VA_ARGS__; \
} while(0)

#define Py_DEALLOCATE_OWNER() \
		_PYRO_DEALLOCATE(); \
	}

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_python_H */
