/*
 * Postgres type interfaces
 *
 * This file contains the necessary functionality for creating and
 * initializing [PG]type interface types. The PyPgType_Type is a PyType_Type
 * subclass that instantiates into [Python] types that represent PostgreSQL
 * types. The structure of [Postgres] objects, instances of PyPgType_Type
 * instances, is described in type/object.c
 * [pypg/type/object.h]--PyPgType_Type instances instantiate into
 * PyPgObject's.
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#include "postgres.h"
#include "access/heapam.h"
#include "access/transam.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_namespace.h"
#include "commands/typecmds.h"
#include "executor/executor.h"
#include "nodes/params.h"
#include "parser/parse_type.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/tuplestore.h"
#include "mb/pg_wchar.h"

#include "pypg/python.h"
#include "pypg/postgres.h"
#include "pypg/strings.h"
#include "pypg/externs.h"
#include "pypg/pl.h"
#include "pypg/error.h"
#include "pypg/tupledesc.h"
#include "pypg/type/type.h"
#include "pypg/type/object.h"
#include "pypg/type/array.h"
#include "pypg/type/bitwise.h"
#include "pypg/type/record.h"
#include "pypg/type/numeric.h"
#include "pypg/type/string.h"
#include "pypg/type/timewise.h"
#include "pypg/type/system.h"

static PyObj type_cache = NULL;
/*
 * Initialized in PyPgType_Init for tupledesc.c.
 */
PyObj PyPg_pg_attribute_Type = NULL;
/* For pg_attribute */
PyObj PyPg_aclitem_Array_Type = NULL;
/* For PyPgType_modin() */
PyObj PyPg_cstring_Array_Type = NULL;

void
PyPgClearTypeCache(void)
{
	PyDict_Clear(type_cache);
}

static bool
is_builtin(Oid typoid)
{
	switch (typoid)
	{
		case CSTRINGARRAYOID:
		case AttributeRelation_Rowtype_Id:
		case ACLITEMOID + 1: /* ACLITEMARRAYOID? */
#define TYP(name) \
		case PyPg_##name##_Type_oid:
		PYPG_DB_TYPES()
#undef TYP
			return(true);
		break;

		default:
			return(false);
		break;
	}

	Assert(false);
}

/*
 * Decrement all non-NULL PyObject's in the given typinfo
 * and free any palloc'd data.
 */
static void
clear_typinfo(PyPgTypeInfo typinfo)
{
	PyObj ob;

	/*
	 * For built-ins, this is used to identify if the
	 * typinfo has been initialized.
	 */
	typinfo->typ_xmin = InvalidTransactionId;

	/*
	 * DECREF all the Python objects.
	 */
	ob = typinfo->typname_PyUnicode;
	typinfo->typname_PyUnicode = NULL;
	Py_XDECREF(ob);

	ob = typinfo->nspname_PyUnicode;
	typinfo->nspname_PyUnicode = NULL;
	Py_XDECREF(ob);

	ob = typinfo->typoid_PyLong;
	typinfo->typoid_PyLong = NULL;
	Py_XDECREF(ob);

	ob = typinfo->typnamespace_PyLong;
	typinfo->typnamespace_PyLong = NULL;
	Py_XDECREF(ob);

	if (typinfo->typtype == TYPTYPE_ARRAY)
	{
		ob = typinfo->array.x_yes.typelem_Type;
		typinfo->array.x_yes.typelem_Type = NULL;
		Py_XDECREF(ob);
	}
	else
	{
		ob = typinfo->array.x_no.composite.typrel_TupleDesc;
		typinfo->array.x_no.composite.typrel_TupleDesc = NULL;
		Py_XDECREF(ob);
	}

	/*
	 * Free any allocated type data
	 */
	if (typinfo->typmemory != PythonMemoryContext &&
		typinfo->typmemory != NULL)
	{
		PG_TRY();
		{
			MemoryContextDelete(typinfo->typmemory);

			/*
			 * When PLPY_STRANGE_THINGS is defined.
			 */
			RaiseAStrangeError
		}
		PG_CATCH();
		{
			PyErr_EmitPgErrorAsWarning("deleting type memory context caused error");
		}
		PG_END_TRY();

		typinfo->typmemory = NULL;
	}
}

/*
 * inherit_typinfo - fill in the typinfo field using super.
 *
 * This is used for filling in the typinfo for a DOMAIN subtype.
 */
static void
inherit_typinfo(PyPgTypeInfo dst, PyPgTypeInfo src)
{
	dst->typtype = src->typtype;
	dst->typalign = src->typalign;
	dst->typbyval = src->typbyval;
	dst->typlen = src->typlen;

	if (dst->typtype == TYPTYPE_ARRAY)
	{
		dst->array.x_yes.typelem = 
			src->array.x_yes.typelem;

		dst->array.x_yes.typelem_Type = 
			src->array.x_yes.typelem_Type;
		Py_XINCREF(dst->array.x_yes.typelem_Type);
	}
	else
	{
		dst->array.x_no.typarray =
			src->array.x_no.typarray;

		if (dst->typtype == TYPTYPE_COMPOSITE)
		{
			dst->array.x_no.composite.typrelid =
				src->array.x_no.composite.typrelid;

			dst->array.x_no.composite.typrel_TupleDesc =
				src->array.x_no.composite.typrel_TupleDesc;
			Py_XINCREF(dst->array.x_no.composite.typrel_TupleDesc);
		}
	}
}

/*
 * PyPgType_IsCurrent - determine if the current pg_type entry is newer
 * than 'typ'.
 *
 * Note: This can THROW().
 */
bool
PyPgType_IsCurrent(PyObj typ)
{
	HeapTuple ht;
	ItemPointerData typ_tid;
	TransactionId typ_xmin;
	Oid typoid;

	typoid = PyPgType_GetOid(typ);

	if (is_builtin(typoid))
		return(true);

	ht = SearchSysCache(TYPEOID, typoid, 0, 0, 0);
	/* no longer exists? yeah, that's not current. */
	if (!HeapTupleIsValid(ht))
		return(false);

	/*
	 * copy visibility info and release pg_type entry
	 */
	typ_xmin = HeapTupleHeaderGetXmin(ht->t_data);
	typ_tid = ht->t_self;
	ReleaseSysCache(ht);

	/*
	 * If the entry changed, don't bother with worrying about the relation's
	 * natts for reltypes.
	 */
	if (PyPgType_IsComposite(typ) && (
		PyPgType_GetXMin(typ) != typ_xmin ||
		!ItemPointerEquals(PyPgType_GetItemPointer(typ), &typ_tid)))
	{
		return(false);
	}

	/*
	 * Was the structure of the type's relation modified?
	 */
	if (PyPgType_IsComposite(typ))
	{
		Oid typrelid = PyPgType_GetTableOid(typ);
		TupleDesc td = PyPgType_GetTupleDesc(typ);

		/*
		 * Registered composite? Compare natts and attisdropped status.
		 *
		 * If it's an anonymous composite, there is no reference point at
		 * this level. It's up to the caller to make sure things are consistent
		 * with its source.
		 */
		if (OidIsValid(typrelid))
		{
			Relation typrel;

			typrel = RelationIdGetRelation(typrelid);
			/* it doesn't exist? okay, it's not up-to-date */
			if (typrel == NULL)
				return(false);

			if (typrel->rd_att->natts != td->natts)
			{
				RelationClose(typrel);
				return(false);
			}
			else
			{
				/*
				 * Were any attributes dropped?
				 */
				Form_pg_attribute *cur_atts = typrel->rd_att->attrs;
				Form_pg_attribute *typ_atts = td->attrs;
				int i;

				for (i = 0; i < td->natts; ++i)
				{
					if (!(cur_atts[i]->attisdropped) != !(typ_atts[i]->attisdropped))
						break; /* break to close relation */
				}
				RelationClose(typrel);

				if (i < td->natts) /* it break'd out */
					return(false);
			}
		}

		/*
		 * Same attributes, or it was anonymous.
		 * Now analyze the types of the attributes in the composite.
		 *
		 * XXX: recursive types would mean infinite recursion here.
		 */
		if (!PyPgTupleDesc_IsCurrent(PyPgType_GetPyPgTupleDesc(typ)))
			return(false);
	}
	else if (PyPgType_IsArray(typ))
	{
		PyObj elem_Type = PyPgType_GetElementType(typ);

		if (!PyPgType_IsCurrent(elem_Type))
			return(false);
	}

	return(true);
}

/*
 * Fill in the FmgrInfo for:
 *
 * typinput, typoutput,
 * typreceive, typsend,
 * typmodin and typmodout.
 *
 * elog() on failure.
 */
static void
fill_fmgrinfo(struct pypg_type_func *typfunc, MemoryContext typcontext)
{
	FmgrInfo *flinfo;

	flinfo = &typfunc->typinput;
	if (OidIsValid(flinfo->fn_oid))
		fmgr_info_cxt(flinfo->fn_oid, flinfo, typcontext);

	flinfo = &typfunc->typoutput;
	if (OidIsValid(flinfo->fn_oid))
		fmgr_info_cxt(flinfo->fn_oid, flinfo, typcontext);

	flinfo = &typfunc->typreceive;
	if (OidIsValid(flinfo->fn_oid))
		fmgr_info_cxt(flinfo->fn_oid, flinfo, typcontext);

	flinfo = &typfunc->typsend;
	if (OidIsValid(flinfo->fn_oid))
		fmgr_info_cxt(flinfo->fn_oid, flinfo, typcontext);

	flinfo = &typfunc->typmodin;
	if (OidIsValid(flinfo->fn_oid))
		fmgr_info_cxt(flinfo->fn_oid, flinfo, typcontext);

	flinfo = &typfunc->typmodout;
	if (OidIsValid(flinfo->fn_oid))
		fmgr_info_cxt(flinfo->fn_oid, flinfo, typcontext);
}

/*
 * Given a PyPgTypeInfo pointer *with* OidIsValid(typinfo->typoid),
 * fill out the pg_type information.
 */
static int
fill_pg_type(PyPgTypeInfo typinfo)
{
	MemoryContext former = CurrentMemoryContext;
	volatile HeapTuple ht = NULL;
	int r = 0;
	TransactionId xmin;
	ItemPointerData tid;

	Assert(typinfo != NULL);
	Assert(OidIsValid(typinfo->typoid));
	Assert(!pl_state);

	typinfo->typoid_PyLong = NULL;
	typinfo->typnamespace_PyLong = NULL;
	typinfo->typname_PyUnicode = NULL;
	typinfo->nspname_PyUnicode = NULL;

	PG_TRY();
	{
		Form_pg_type ts;
		Form_pg_namespace ns;

		ht = SearchSysCache(TYPEOID,
					ObjectIdGetDatum(typinfo->typoid),
					0, 0, 0);
		if (!HeapTupleIsValid(ht))
		{
			ereport(ERROR, (
				errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("type does not exist")));
		}

		/*
		 * For the is_current checks.
		 */
		xmin = HeapTupleHeaderGetXmin(ht->t_data);
		tid = ht->t_self;

		/*
		 * Get the necessary information from pg_type.
		 */
		ts = (Form_pg_type) GETSTRUCT(ht);

		if (!ts->typisdefined)
		{
			ereport(ERROR, (
				errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("type is not defined")));
		}

		typinfo->typioparam = getTypeIOParam(ht);

		/*
		 * Build a context if it's not a builtin.
		 */
		if (is_builtin(typinfo->typoid) == false)
		{
			typinfo->typmemory = AllocSetContextCreate(PythonMemoryContext,
				NameStr(ts->typname),
				ALLOCSET_SMALL_MINSIZE,
				ALLOCSET_SMALL_INITSIZE,
				ALLOCSET_SMALL_MAXSIZE);
		}
		else
			typinfo->typmemory = PythonMemoryContext;

		if (OidIsValid(ts->typbasetype))
		{
			/*
			 * Domains will inherit all the type info from the basetype.
			 * All that is needed is the constraint information.
			 */
			typinfo->domain.x_yes.typbasetype = ts->typbasetype;
			typinfo->domain.x_yes.typndims = ts->typndims;
			typinfo->domain.x_yes.typnotnull = ts->typnotnull;
			/*
			 * This is used by create_heap_type to identify domains.
			 */
			typinfo->typfunc = NULL;

			MemoryContextSwitchTo(typinfo->typmemory);
			typinfo->domain.x_yes.constraint_list =
				GetDomainConstraints(typinfo->typoid);
			MemoryContextSwitchTo(former);

			/*
			 * Builtin DOMAIN? nah.
			 */
			Assert(!is_builtin(typinfo->typoid));
		}
		else
		{
			typinfo->typbyval = ts->typbyval;
			typinfo->typtype = ts->typtype;
			/*
			 * typfunc points to domain.x_no, so
			 * it will *not* be identified as a domain.
			 */
			typinfo->typfunc = &(typinfo->domain.x_no);

			typinfo->typfunc->typinput.fn_oid = ts->typinput;
			typinfo->typfunc->typoutput.fn_oid = ts->typoutput;
			typinfo->typfunc->typreceive.fn_oid = ts->typreceive;
			typinfo->typfunc->typsend.fn_oid = ts->typsend;
			typinfo->typfunc->typmodin.fn_oid = ts->typmodin;
			typinfo->typfunc->typmodout.fn_oid = ts->typmodout;

			/*
			 * throws on failure
			 */
			fill_fmgrinfo(typinfo->typfunc, typinfo->typmemory);

			if (OidIsValid(ts->typelem))
			{
				/*
				 * It's an ARRAY type.
				 */
				typinfo->array.x_yes.typelem = ts->typelem;
				/*
				 * Use the contrived typtype for array types.
				 */
				typinfo->typtype = TYPTYPE_ARRAY;

				Assert(ts->typelem != typinfo->typoid);
				typinfo->array.x_yes.typelem_Type = PyPgType_FromOid(ts->typelem);

				if (typinfo->array.x_yes.typelem_Type == NULL)
				{
					/*
					 * No element type..?
					 */
					RELEASESYSCACHE(&ht);
					PyErr_RelayException();
				}
			}
			else
			{
				if (OidIsValid(ts->typarray))
					typinfo->array.x_no.typarray = ts->typarray;

				/*
				 * Set the relid and get the PyPgTupleDesc
				 */
				if (OidIsValid(ts->typrelid))
				{
					Relation rd;
					rd = RelationIdGetRelation(ts->typrelid);
					if (rd == NULL)
					{
						RELEASESYSCACHE(&ht);
						elog(ERROR, "relation %u of type %u not found", ts->typrelid, typinfo->typoid);
					}

					typinfo->array.x_no.composite.typrelid = ts->typrelid;
					typinfo->array.x_no.composite.typrel_TupleDesc =
						PyPgTupleDesc_FromCopy(rd->rd_att);

					RelationClose(rd);
				}
			}

			typinfo->typcategory = _PG_GET_TYPCATEGORY(ts);
			typinfo->typalign = ts->typalign;
			typinfo->typlen = ts->typlen;
			typinfo->typnamespace = ts->typnamespace;
		}

		/*
		 * Check for the error later.
		 */
		typinfo->typname_PyUnicode =
			PyUnicode_FromCString(NameStr(ts->typname));

		RELEASESYSCACHE(&ht); /* done with pg_type entry */

		/*
		 * We want the namespace information as well.
		 */
		ht = SearchSysCache(NAMESPACEOID,
					ObjectIdGetDatum(typinfo->typnamespace), 0, 0, 0);
		if (!HeapTupleIsValid(ht))
			elog(ERROR,
				"cache lookup failed for namespace %u", typinfo->typnamespace);

		ns = (Form_pg_namespace) GETSTRUCT(ht);
		typinfo->nspname_PyUnicode =
			PyUnicode_FromCString(NameStr(ns->nspname));

		RELEASESYSCACHE(&ht); /* done with pg_namespace entry */

		/*
		 * Fill in the stored item pointer and xmin.
		 */
		typinfo->typ_xmin = xmin;
		typinfo->typ_tid = tid;

		/*
		 * Create and set; check for failures later.
		 */
		typinfo->typoid_PyLong = PyLong_FromUnsignedLong(typinfo->typoid);
		typinfo->typnamespace_PyLong =
			PyLong_FromUnsignedLong(typinfo->typnamespace);
	}
	PG_CATCH();
	{
		r = -1;
		if (ht != NULL)
			ReleaseSysCache(ht);

		MemoryContextSwitchTo(former);
		clear_typinfo(typinfo);
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	/*
	 * Make sure that all the PyObjects got created.
	 */
	if (r == 0 && (
		typinfo->typoid_PyLong == NULL ||
		typinfo->typname_PyUnicode == NULL ||
		typinfo->nspname_PyUnicode == NULL ||
		typinfo->typnamespace_PyLong == NULL))
	{
		clear_typinfo(typinfo);
		r = -1;
	}

	Assert((r != 0 && PyErr_Occurred()) || (r == 0 && !PyErr_Occurred()));

	return(r);
}

/*
 * Create a PyPgHeapType_Type instance using the given PyPgTypeInfo
 * (Does not initialize PyPgTypeInfo in the new object)
 */
static PyObj
create_heap_type(PyPgTypeInfo typinfo)
{
	PyPgTypeInfo btypinfo;
	PyObj base, bases, nbd, d, rob;

	/*
	 * Always *one* base class.
	 */
	bases = PyTuple_New(1);
	if (bases == NULL)
		return(NULL);

	d = PyDict_New();
	if (d == NULL)
	{
		Py_DECREF(bases);
		return(NULL);
	}

	/*
	 * Set this for consistent tp_name fields.
	 */
	if (PyDict_SetItemString(d, "__module__", typinfo->nspname_PyUnicode))
	{
		Py_DECREF(bases);
		Py_DECREF(d);
		return(NULL);
	}

	/*
	 * The name, bases, dict tuple.
	 */
	nbd = PyTuple_New(3);
	if (nbd == NULL)
	{
		Py_DECREF(bases);
		Py_DECREF(d);
		return(NULL);
	}

	/*
	 * Fill the (name, bases, dict) tuple.
	 */
	PyTuple_SET_ITEM(nbd, 0, typinfo->typname_PyUnicode);
	Py_INCREF(typinfo->typname_PyUnicode);
	PyTuple_SET_ITEM(nbd, 1, bases);
	PyTuple_SET_ITEM(nbd, 2, d);

	/*
	 * If a domain, get the basetype to use as the type's base.
	 */
	if (typinfo_is_domain(typinfo))
	{
		base = PyPgType_FromOid(typinfo->domain.x_yes.typbasetype);
		if (base == NULL)
		{
			Py_DECREF(nbd);
			return(NULL);
		}
		btypinfo = PyPgTypeInfo(base);

		/*
		 * Finish the initialization of the domain's typinfo.
		 */
		if (typinfo_is_domain(btypinfo))
		{
			typinfo->domain.x_yes.typubase_Type =
				btypinfo->domain.x_yes.typubase_Type;
		}
		else
		{
			typinfo->domain.x_yes.typubase_Type = base;
		}

		inherit_typinfo(typinfo, btypinfo);
		typinfo->typfunc = btypinfo->typfunc;

		/* Should be the ultimate base's domain.x_no */
		Assert(typinfo->typfunc == &(PyPgTypeInfo(typinfo->domain.x_yes.typubase_Type)->domain.x_no));
	}
	else
	{
		switch (typinfo->typtype)
		{
			case TYPTYPE_COMPOSITE:
				base = (PyObj) &PyPg_record_Type;
			break;

			case TYPTYPE_PSEUDO:
				base = (PyObj) &PyPgPseudo_Type;
			break;

			/* Note: TYPTYPE_ARRAY is contrived in pypg/type/type.h */
			case TYPTYPE_ARRAY:
				base = (PyObj) &PyPgArray_Type;
			break;

			default:
			{
				switch (typinfo->typcategory)
				{
					case TYPCATEGORY_STRING:
						base = (PyObj) &PyPgString_Type;
					break;
					default:
						base = (PyObj) &PyPgObject_Type;
					break;
				}
			}
			break;
		}
		Py_INCREF(base);
		btypinfo = PyPgTypeInfo(base);
	}

	typinfo->tp_new_datum = btypinfo->tp_new_datum;

	/*
	 * give 'bases' the reference to 'base'.
	 */
	PyTuple_SET_ITEM(bases, 0, base);

	/*
	 * Py_TYPE(base) should be PyPgType_Type.
	 */
	Assert(Py_TYPE(base) == (PyTypeObject *) &PyPgType_Type);

	rob = PyType_Type.tp_new((PyTypeObject *) Py_TYPE(base), nbd, NULL);
	Py_DECREF(nbd);

	/*
	 * If this fails, the user is obligated to clear_typinfo.
	 */
	if (rob != NULL)
	{
		PyPgTypeInfo rtypinfo = PyPgTypeInfo(rob);

		memcpy(rtypinfo, typinfo, sizeof(struct PyPgTypeInfo));
		if (btypinfo->typfunc != rtypinfo->typfunc)
		{
			/*
			 * It's not a domain, so the typfunc pointer needs to be updated.
			 */
			rtypinfo->typfunc = &(rtypinfo->domain.x_no);
		}
	}

	return(rob);
}

PyObj
PyPgType_LookupBuiltin(Oid typoid)
{
	PyObj rob;

	switch (typoid)
	{
#define TYP(name) \
		case PyPg_##name##_Type_oid: rob = ((PyObj) &PyPg_##name##_Type); break;
		PYPG_DB_TYPES()
#undef TYP

/*
 * These are specially initialized.
 */
		case AttributeRelation_Rowtype_Id:
			rob = PyPg_pg_attribute_Type;
		break;

		case CSTRINGARRAYOID:
			rob = PyPg_cstring_Array_Type;
		break;

		/*
		 * XXX: ACLITEMARRAYOID?
		 */
		case ACLITEMOID + 1:
			rob = PyPg_aclitem_Array_Type;
		break;

		default:
			/*
			 * No such builtin.
			 */
			rob = NULL;
		break;
	}

	return(rob);
}

/*
 * PyPgType_FromOid - Get a PyPgType from the given Oid
 *
 * There are four main paths in this function:
 *
 *  1. Uninitialized builtin is referenced and then initialized
 *  2. Initialized builtin is referenced and quickly returned
 *  3. Type exists in type cache and is quickly returned if current
 *  4. Type not in cache or is not current, a new heap type created
 */
PyObj
PyPgType_FromOid(Oid typoid)
{
	PyObj typoid_ob, rob = NULL;

	if (DB_IS_NOT_READY())
		return(NULL);

	rob = PyPgType_LookupBuiltin(typoid);
	if (rob == NULL)
	{
		/*
		 * It's not a statically allocated built-in.
		 * Check the type_cache, return the entry iff
		 * it's up-to-date.
		 * Otherwise, create a new type.
		 */
		struct PyPgTypeInfo typinfo = PYPG_INIT_TYPINFO(invalid);
		int contains;

		typoid_ob = PyLong_FromUnsignedLong(typoid);
		if (typoid_ob == NULL)
			return(NULL);

		contains = PySequence_Contains(type_cache, typoid_ob);
		if (contains == -1)
		{
			Py_DECREF(typoid_ob);
			return(NULL);
		}

		if (contains)
		{
			bool is_current;

			rob = PyDict_GetItem(type_cache, typoid_ob);
			Assert(rob != NULL); /* "typoid_ob in type_cache" said yessir. */

			/*
			 * Make sure it's up-to-date.
			 */
			PG_TRY();
			{
				is_current = PyPgType_IsCurrent(rob);
			}
			PG_CATCH();
			{
				Py_DECREF(typoid_ob);
				PyErr_SetPgError(false);
				return(NULL);
			}
			PG_END_TRY();

			if (is_current)
			{
				/*
				 * It's up-to-date. Return the object.
				 */
				Py_DECREF(typoid_ob);
				Py_INCREF(rob);
				return(rob);
			}

			/*
			 * It's not up-to-date. Remove the cache entry.
			 */
			if (PyDict_DelItem(type_cache, typoid_ob))
			{
				/* not likely, but okay, fail out... */
				Py_DECREF(typoid_ob);
				return(NULL);
			}

			rob = NULL;
		}

		/*
		 * Type was not in cache, or was not up-to-date.
		 * Make a new one.
		 */
		typinfo.typoid = typoid;
		if (fill_pg_type(&typinfo))
		{
			/*
			 * unlikely to happen, but it's probably that the type does not exist
			 */
			Py_DECREF(typoid_ob);
			return(NULL);
		}

		/*
		 * typinfo needed to be filled first because
		 * that's how the new type's base is identified.
		 */
		rob = create_heap_type(&typinfo);
		if (rob == NULL)
		{
			Py_DECREF(typoid_ob);
			clear_typinfo(&typinfo);
			return(NULL);
		}

		if (PyDict_SetItem(type_cache, typoid_ob, rob))
		{
			/*
			 * SetItem failed, DECREF and return failure.
			 */
			Py_DECREF(typoid_ob);
			Py_DECREF(rob);
			return(NULL);
		}
	}
	else
	{
		/*
		 * Built-in type.
		 */
		PyPgTypeInfo typinfo = PyPgTypeInfo(rob);

		if (typinfo->typ_xmin == InvalidTransactionId)
		{
			/*
			 * Hasn't been initialized yet, so do so now.
			 */
			if (fill_pg_type(typinfo))
			{
				/*
				 * fill_pg_type will clear_typinfo on failure.
				 */
				return(NULL);
			}

			/*
			 * Builtins are assumed to *not* be domains.
			 */
			Assert(!typinfo_is_domain(typinfo));
		}

		/*
		 * Everything is good. Grab a reference for the caller and return.
		 */
		Py_INCREF(rob);
	}

	return(rob);
}

/*
 * Create an anonymous record type from a PyPgTupleDesc
 */
PyObj
PyPgType_FromPyPgTupleDesc(PyObj td_ob, PyObj typname)
{
	PyPgTypeInfo btypinfo;
	struct PyPgTypeInfo typinfo;
	PyObj rob;

	bzero(&typinfo, sizeof(struct PyPgTypeInfo));
	btypinfo = PyPgTypeInfo(&PyPg_record_Type);

	typinfo.typoid = RECORDOID;
	typinfo.typtype = TYPTYPE_COMPOSITE;
	typinfo.typnamespace = InvalidOid;
	typinfo.typtype = btypinfo->typtype;
	typinfo.typalign = btypinfo->typalign;
	typinfo.typbyval = btypinfo->typbyval;
	typinfo.typlen = btypinfo->typlen;

	/*
	 * Used to "trick" any IsCurrent checks for RECORD returning
	 * multiple OUT parameter functions.
	 */
	typinfo.typ_xmin = btypinfo->typ_xmin;
	typinfo.typ_tid = btypinfo->typ_tid;
	typinfo.typfunc = &(typinfo.domain.x_no);

	typinfo.domain.x_no.typinput.fn_oid = btypinfo->domain.x_no.typinput.fn_oid;
	typinfo.domain.x_no.typoutput.fn_oid = btypinfo->domain.x_no.typoutput.fn_oid;
	typinfo.domain.x_no.typreceive.fn_oid = btypinfo->domain.x_no.typreceive.fn_oid;
	typinfo.domain.x_no.typsend.fn_oid = btypinfo->domain.x_no.typsend.fn_oid;
	typinfo.domain.x_no.typmodin.fn_oid = btypinfo->domain.x_no.typmodin.fn_oid;
	typinfo.domain.x_no.typmodout.fn_oid = btypinfo->domain.x_no.typmodout.fn_oid;

	PG_TRY();
	{
		typinfo.typmemory = AllocSetContextCreate(PythonMemoryContext,
			"PythonRecordTypeMemoryContext",
			ALLOCSET_SMALL_MINSIZE,
			ALLOCSET_SMALL_INITSIZE,
			ALLOCSET_SMALL_MAXSIZE);
		/*
		 * These should be empty, but refill anyways.
		 */
		fill_fmgrinfo(typinfo.typfunc, typinfo.typmemory);
	}
	PG_CATCH();
	{
		clear_typinfo(&typinfo);
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();

	typinfo.array.x_no.composite.typrel_TupleDesc = td_ob;
	Py_INCREF(td_ob);

	typinfo.typname_PyUnicode = typname;
	Py_INCREF(typname);
	typinfo.nspname_PyUnicode = Py_None;
	Py_INCREF(Py_None);
	typinfo.typoid_PyLong = Py_None;
	Py_INCREF(Py_None);
	typinfo.typnamespace_PyLong = Py_None;
	Py_INCREF(Py_None);

	rob = create_heap_type(&typinfo);

	if (rob == NULL)
	{
		/*
		 * Python exception occurred, clear the typinfo.
		 */
		clear_typinfo(&typinfo);
	}

	return(rob);
}

/*
 * PyPgType_AnonymousComposite - re-use existing anonymous types
 */
static PyObj
PyPgType_AnonymousComposite(PyObj tupdesc, PyObj typname)
{
	int r;
	PyObj rob = NULL, triple, attnames, atttypes;

	/*
	 * Build the composite's identity from:
	 *  (typname, column_names, pg_column_types)
	 */

	attnames = PyObject_GetAttrString(tupdesc, "column_names");
	if (attnames == NULL)
		return(NULL);

	atttypes = PyObject_GetAttrString(tupdesc, "pg_column_types");
	if (atttypes == NULL)
	{
		Py_DECREF(attnames);
		return(NULL);
	}

	triple = PyTuple_New(3);
	if (triple == NULL)
	{
		Py_DECREF(attnames);
		Py_DECREF(atttypes);
		return(NULL);
	}

	Py_INCREF(typname);
	PyTuple_SET_ITEM(triple, 0, typname);
	PyTuple_SET_ITEM(triple, 1, attnames);
	PyTuple_SET_ITEM(triple, 2, atttypes);

	/*
	 * only need to decref triple from here out
	 */

	r = PyDict_Contains(Py_anonymous_composites, triple);
	if (r < 0)
	{
		Py_DECREF(triple);
		return(NULL);
	}
	else if (r > 0)
	{
		PyObj curtd;

		rob = PyDict_GetItem(Py_anonymous_composites, triple);
		if (rob == NULL)
		{
			Py_DECREF(triple);
			return(NULL);
		}

		curtd = PyPgType_GetPyPgTupleDesc(rob);
		PG_TRY();
		{
			/*
			 * Have to trap, IsCurrent hits the catalogs.
			 *
			 * We don't do PyPgType_IsCurrent because it would
			 * just be an extra function call before we get to
			 * PyPgTupleDesc_IsCurrent().
			 */
			if (PyPgTupleDesc_IsCurrent(curtd))
				Py_INCREF(rob);
			else
			{
				/*
				 * The existing entry will get overwritten.
				 */
				rob = NULL; /* reference was borrowed */
			}
		}
		PG_CATCH();
		{
			rob = NULL; /* reference was borrowed */
			PyErr_SetPgError(false);

			/*
			 * Error while checking if it's current?
			 * Let's just assume it's invalid now.
			 */
			PyDict_DelItem(Py_anonymous_composites, triple);
			return(NULL);
		}
		PG_END_TRY();
	}

	/*
	 * Not in cache or was not current.
	 */
	if (rob == NULL)
	{
		/*
		 * Make a new one and put it in the cache.
		 */
		rob = PyPgType_FromPyPgTupleDesc(tupdesc, typname);

		if (rob != NULL && !PyPgType_IsPolymorphic(rob))
		{
			if (PyDict_SetItem(Py_anonymous_composites, triple, rob))
			{
				/*
				 * Try to continue anyways.
				 */
				elog(WARNING, "could not cache anonymous composite type");
				PyErr_Clear();
			}

			PG_TRY();
			{
				BlessTupleDesc(PyPgType_GetTupleDesc(rob));
			}
			PG_CATCH();
			{
				PyErr_SetPgError(false);
				Py_DECREF(rob);
				rob = NULL;
			}
			PG_END_TRY();
		}
	}

	Py_DECREF(triple);
	return(rob);
}

PyObj
PyPgType_FromTupleDesc(TupleDesc td)
{
	PyObj typname, rob, td_ob;

	typname = PyUnicode_FromString("<anonymous_record_type>");
	if (typname == NULL)
		return(NULL);

	td_ob = PyPgTupleDesc_FromCopy(td);
	if (td_ob == NULL)
	{
		Py_DECREF(typname);
		return(NULL);
	}

	rob = PyPgType_AnonymousComposite(td_ob, typname);
	Py_DECREF(td_ob);
	Py_DECREF(typname);

	return(rob);
}

void
PyPgType_x_new_datum(
	PyObj subtype, PyObj ob, int32 mod, Datum *rdatum, bool *isnull)
{
	/*
	 * This is used by abstract types to protect against strange
	 * cases where a type was used in an unexpected manner.
	 */
	elog(ERROR, "cannot create Datum from type '%s'",
		((PyTypeObject *) subtype)->tp_name);
}

void
typinfo_typinput(PyPgTypeInfo typinfo, char *cstr,
	int32 mod, Datum *rdatum, bool *isnull)
{
	FunctionCallInfoData fcinfo = {0,};

	fcinfo.flinfo = &(typinfo->typfunc->typinput);
	fcinfo.isnull = false;
	fcinfo.nargs = 3;
	fcinfo.arg[0] = PointerGetDatum(cstr);

	switch (typinfo->typtype)
	{
		case TYPTYPE_ARRAY:
		{
			fcinfo.arg[1] = ObjectIdGetDatum(typinfo->array.x_yes.typelem);
			fcinfo.arg[2] = -1;
		}
		break;

		case TYPTYPE_COMPOSITE:
		{
			/*
			 * If it's anonymous, record_in will probably throw an error.
			 */
			fcinfo.arg[1] = ObjectIdGetDatum(typinfo->typoid);
			fcinfo.arg[2] = -1;
		}
		break;

		case TYPTYPE_ENUM:
		{
			fcinfo.arg[2] = -1;
			if (typinfo_is_domain(typinfo))
			{
				/*
				 * enum_in needs to know the base enum type Oid
				 */
				fcinfo.arg[1] = ObjectIdGetDatum(
					PyPgType_GetOid(typinfo->domain.x_yes.typubase_Type));
			}
			else
				fcinfo.arg[1] = typinfo->typoid;
		}
		break;

		default:
		{
			/*
			 * Normal Type
			 */
			fcinfo.arg[1] = -1;
			fcinfo.arg[2] = mod;
		}
		break;
	}

	*rdatum = FunctionCallInvoke(&fcinfo);
	*isnull = fcinfo.isnull;
}

/*
 * Run typreceive for the typinfo using the given buffer object.
 */
static void
typinfo_typreceive(PyPgTypeInfo typinfo, PyObj ob, int32 typmod,
	Datum *rdatum, bool *isnull)
{
	StringInfoData buf, *bufp = NULL;

	/*
	 * None is NULL, if the receive function is strict, it's done.
	 */
	if (ob == Py_None)
	{
		if (typinfo->typfunc->typreceive.fn_strict)
		{
			*isnull = true;
			*rdatum = 0;
			return;
		}
	}
	else
	{
		char *data;
		Py_ssize_t size;

		/*
		 * Grab the object's RO buffer and write it into StringInfoData.
		 */
		if (PyObject_AsReadBuffer(ob, (const void **) &data, &size))
		{
			PyErr_RelayException();
		}

		/*
		 * bufp is initialized to NULL above for Py_None cases.
		 */
		bufp = &buf;
		initStringInfo(bufp);
		appendBinaryStringInfo(bufp, data, size);
	}

	/*
	 * This function will elog() if the expectations wrt NULL aren't met.
	 * Notably, bufp is NULL when ob is Py_None and !fn_strict.
	 */
	*rdatum = ReceiveFunctionCall(&(typinfo->typfunc->typreceive), bufp,
                    typinfo->typioparam, typmod);

	/*
	 * No elog from ReceiveFunctionCall and ob is "NULL"?
	 * Then the result is NULL.
	 */
	if (ob == Py_None)
		*isnull = true;
	else if (bufp)
	{
		*isnull = false;

		/*
		 * Only free up the buffer data for !Py_None cases.
		 */
		pfree(bufp->data);
	}
}

char *
typinfo_typoutput(PyPgTypeInfo typinfo, Datum d, bool isnull)
{
	char *str = NULL;
	FunctionCallInfoData fcinfo = {0,};

	fcinfo.isnull = false;
	fcinfo.nargs = 1;
	fcinfo.arg[0] = d;
	fcinfo.argnull[0] = isnull;

	fcinfo.flinfo = &(typinfo->typfunc->typoutput);

	str = DatumGetCString(FunctionCallInvoke(&fcinfo));
	if (fcinfo.isnull)
		str = NULL;

	return(str);
}

void
PyPgType_typinput(
	PyObj subtype, PyObj obstr, int32 mod, Datum *rdatum, bool *isnull)
{
	PyPgTypeInfo typinfo = PyPgTypeInfo(subtype);

	if (PyPgObject_Check(obstr))
	{
		char *str;

		str = typinfo_typoutput(
			PyPgTypeInfo(Py_TYPE(obstr)),
			PyPgObject_GetDatum(obstr), false);

		typinfo_typinput(typinfo, str, mod, rdatum, isnull);
		pfree(str);
		/*
		 * Special case PyPgObject
		 */
		return;
	}

	Py_INCREF(obstr);
	PyObject_StrBytes(&obstr);
	if (obstr == NULL)
		PyErr_ThrowPostgresError("failed to get bytes for typinput");

	PG_TRY();
	{
		typinfo_typinput(
			typinfo, PyBytes_AS_STRING(obstr),
			mod, rdatum, isnull
		);
		Py_DECREF(obstr);
	}
	PG_CATCH();
	{
		Py_DECREF(obstr);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

int
PyPgType_typcast(
	PyObj subtype, PyObj ob, int32 mod,
	Datum *rdatum, bool *isnull)
{
	Oid to, from, castor;
	Datum ind;
	HeapTuple tup;
	PyPgTypeInfo typinfo;

	ind = PyPgObject_GetDatum(ob);
	from = PyPgType_GetOid(Py_TYPE(ob));
	typinfo = PyPgTypeInfo(subtype);
	to = typinfo->typoid;

	tup = SearchSysCache(CASTSOURCETARGET,
		ObjectIdGetDatum(from), ObjectIdGetDatum(to), 0, 0
	);
	/*
	 * No cast available.
	 */
	if (!HeapTupleIsValid(tup))
		return(1);

	castor = ((Form_pg_cast) GETSTRUCT(tup))->castfunc;
	ReleaseSysCache(tup);

	if (!OidIsValid(castor))
		return(1);

	/*
	 * indatum, typmod, and isexplicit.
	 */
	*rdatum = OidFunctionCall3(castor, ind, mod, BoolGetDatum(true));
	*isnull = false;
	if (*rdatum == ind)
	{
		/*
		 * That's another object's Datum.
		 */
		*rdatum = datumCopy(
			PyPgObject_GetDatum(ob),
			typinfo->typbyval,
			typinfo->typlen
		);
	}

	return(0);
}

/*
 * check_constraints - check domain constraints
 *
 * This is extremely similar to domain_check_input in domain.c
 */
static void
check_constraints(PyObj subtype, Datum value, bool isnull)
{
	PyPgTypeInfo typinfo;
	ExprContext *econtext = NULL;
	ListCell   *l;

	typinfo = PyPgTypeInfo(subtype);

	if (isnull && typinfo->domain.x_yes.typnotnull)
		ereport(ERROR,
				(errcode(ERRCODE_NOT_NULL_VIOLATION),
				 errmsg("domain %s does not allow null values",
						format_type_be(typinfo->typoid))));

	foreach(l, typinfo->domain.x_yes.constraint_list)
	{
		DomainConstraintState *con = (DomainConstraintState *) lfirst(l);

		switch (con->constrainttype)
		{
			case DOM_CONSTRAINT_NOTNULL:
				if (isnull)
					ereport(ERROR,
							(errcode(ERRCODE_NOT_NULL_VIOLATION),
							 errmsg("domain %s does not allow null values",
									format_type_be(typinfo->typoid))));
				break;
			case DOM_CONSTRAINT_CHECK:
				{
					Datum		conResult;
					bool		conIsNull;

					/* Make the econtext if we didn't already */
					if (econtext == NULL)
						econtext = CreateStandaloneExprContext();

					/*
					 * Set up value to be returned by CoerceToDomainValue
					 * nodes.  Unlike ExecEvalCoerceToDomain, this econtext
					 * couldn't be shared with anything else, so no need to
					 * save and restore fields.
					 */
					econtext->domainValue_datum = value;
					econtext->domainValue_isNull = isnull;

					conResult = ExecEvalExprSwitchContext(con->check_expr,
														  econtext,
														  &conIsNull, NULL);

					if (!conIsNull && !DatumGetBool(conResult))
						ereport(ERROR,
								(errcode(ERRCODE_CHECK_VIOLATION),
								 errmsg("value for domain %s violates check constraint \"%s\"",
										format_type_be(typinfo->typoid),
										con->name)));
					break;
				}
			default:
				elog(ERROR, "unrecognized constraint type: %d",
					 (int) con->constrainttype);
				break;
		}
	}

	/*
	 * Before exiting, call any shutdown callbacks and reset econtext's
	 * per-tuple memory.  This avoids leaking non-memory resources, if
	 * anything in the expression(s) has any.
	 */
	if (econtext)
		FreeExprContext(econtext, true);
}

PyObj
PyPgType_typoutput(PyObj subtype, PyObj ob)
{
	MemoryContext former = CurrentMemoryContext;
	PyPgTypeInfo typinfo;
	volatile PyObj rob = NULL;

	/*
	 * Require the *exact* type.
	 */
	if (subtype != (PyObj) Py_TYPE(ob))
	{
		PyErr_Format(PyExc_TypeError,
			"typoutput requires %s given %s",
			((PyTypeObject *) subtype)->tp_name,
			Py_TYPE(ob)->tp_name
		);
		return(NULL);
	}

	if (DB_IS_NOT_READY())
		return(NULL);

	typinfo = PyPgTypeInfo(subtype);

	PG_TRY();
	{
		char *str = NULL;
		str = typinfo_typoutput(typinfo, PyPgObject_GetDatum(ob), false);
		rob = PyUnicode_FromCString(str);
		pfree(str);
	}
	PG_CATCH();
	{
		Py_XDECREF(rob);
		rob = NULL;
		PyErr_SetPgError(false);
	}
	PG_END_TRY();
	MemoryContextSwitchTo(former);

	return(rob);
}

PyObj
PyPgType_typsend(PyObj self, PyObj ob)
{
	MemoryContext former = CurrentMemoryContext;
	PyPgTypeInfo typinfo;
	volatile PyObj rob = NULL;

	if (Py_TYPE(ob) != (PyTypeObject *) self)
	{
		PyErr_Format(PyExc_TypeError, "cannot send %s using %s",
			Py_TYPE(ob)->tp_name,
			((PyTypeObject *) self)->tp_name);
		return(NULL);
	}

	if (DB_IS_NOT_READY())
		return(NULL);

	typinfo = PyPgTypeInfo(self);

	PG_TRY();
	{
		Datum rd;
		FunctionCallInfoData fcinfo = {0,};

		fcinfo.flinfo = &(typinfo->typfunc->typsend);
		if (ob == Py_None)
		{
			fcinfo.arg[0] = 0;
			fcinfo.isnull = true;
		}
		else
		{
			fcinfo.arg[0] = PyPgObject_GetDatum(ob);
			fcinfo.isnull = false;
		}

		rd = FunctionCallInvoke(&fcinfo);
		rob = PyBytes_FromStringAndSize(VARDATA(rd), VARSIZE(rd) - VARHDRSZ);
		pfree(DatumGetPointer(rd));
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		Py_XDECREF(rob);
		rob = NULL;
	}
	PG_END_TRY();
	MemoryContextSwitchTo(former);

	return(rob);
}

PyObj
PyPgType_typreceive(PyObj self, PyObj args)
{
	MemoryContext former = CurrentMemoryContext;
	volatile PyObj rob = NULL;
	volatile Datum rdatum;
	volatile bool isnull = true;
	PyObj bufob;
	int32 typmod = -1;

	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyArg_ParseTuple(args, "O|i:typreceive", &bufob, &typmod))
		return(NULL);

	PG_TRY();
	{
		/*
		 * The typioparam is in typinfo--initialized by fill_pg_type.
		 */
		typinfo_typreceive(PyPgTypeInfo(self), bufob, typmod,
					(Datum *) &rdatum, (bool *) &isnull);

		if (isnull)
		{
			rob = Py_None;
			Py_INCREF(rob);
		}
		else
		{
			rob = PyPgObject_New(self, rdatum);
			if (PyPgType_ShouldFree(self))
			{
				Datum fd = rdatum;
				rdatum = 0;
				isnull = true;

				pfree(DatumGetPointer(fd));
			}
		}
	}
	PG_CATCH();
	{
		Py_XDECREF(rob);
		rob = NULL;

		if (!isnull && PyPgType_ShouldFree(self))
			pfree(DatumGetPointer(rdatum));

		PyErr_SetPgError(false);
	}
	PG_END_TRY();
	MemoryContextSwitchTo(former);

	return(rob);
}

static PyObj
PyPgType_typinput_method(PyObj self, PyObj args)
{
	volatile PyObj rob = NULL;
	volatile Datum rdatum;
	volatile bool isnull = true;
	PyObj strob;
	int32 typmod = -1;

	if (DB_IS_NOT_READY())
		return(NULL);

	if (!PyArg_ParseTuple(args, "O|i:typinput", &strob, &typmod))
		return(NULL);

	Py_INCREF(strob);
	PyObject_StrBytes(&strob);
	if (strob == NULL)
		return(NULL);

	PG_TRY();
	{
		typinfo_typinput(PyPgTypeInfo(self), PyBytes_AS_STRING(strob), typmod,
					(Datum *) &rdatum, (bool *) &isnull);

		if (isnull)
		{
			rob = Py_None;
			Py_INCREF(rob);
		}
		else
		{
			rob = PyPgObject_New(self, rdatum);
			if (PyPgType_ShouldFree(self))
			{
				Datum fd = rdatum;
				rdatum = 0;
				isnull = true;

				pfree(DatumGetPointer(fd));
			}
		}
	}
	PG_CATCH();
	{
		Py_XDECREF(rob);
		rob = NULL; /* fall through */

		if (!isnull && PyPgType_ShouldFree(self))
			pfree(DatumGetPointer(rdatum));

		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	Py_DECREF(strob);

	return(rob);
}

static PyObj
type_modin(PyObj self, PyObj modargs)
{
	PyObj rob = NULL;
	int32 mod;
	PyPgTypeInfo typinfo;

	typinfo = PyPgTypeInfo(self);

	PG_TRY();
	{
		Datum datum;
		bool isnull = false;
		FunctionCallInfoData fcinfo = {0,};

		/*
		 * Make it a cstring[]
		 */
		PyPgType_DatumNew(PyPg_cstring_Array_Type, modargs, -1, &datum, &isnull);

		fcinfo.flinfo = &(typinfo->typfunc->typmodin);
		fcinfo.isnull = false;
		fcinfo.nargs = 1;
		fcinfo.arg[0] = datum;
		fcinfo.argnull[0] = isnull;

		mod = DatumGetInt32(FunctionCallInvoke(&fcinfo));
		if (!isnull)
			pfree(DatumGetPointer(datum));

		if (fcinfo.isnull)
		{
			rob = Py_None;
			Py_INCREF(Py_None);
		}
		else
		{
			rob = PyLong_FromLong((long) mod);
		}
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return(rob);
}

static PyObj
type_modout(PyObj self, PyObj mod)
{
	volatile PyObj rob = NULL;
	PyPgTypeInfo typinfo;

	typinfo = PyPgTypeInfo(self);

	PG_TRY();
	{
		Datum datum, rd;
		bool isnull = false;
		FunctionCallInfoData fcinfo = {0,};

		/*
		 * Make it an int32.
		 */
		PyPgType_DatumNew((PyObj) &PyPg_int4_Type, mod, -1, &datum, &isnull);

		fcinfo.flinfo = &(typinfo->typfunc->typmodout);
		fcinfo.isnull = false;
		fcinfo.nargs = 1;
		fcinfo.arg[0] = datum;
		fcinfo.argnull[0] = isnull;

		rd = FunctionCallInvoke(&fcinfo);

		if (fcinfo.isnull)
		{
			rob = Py_None;
			Py_INCREF(Py_None);
		}
		else
		{
			rob = PyPgObject_New(&PyPg_cstring_Type, rd);
			pfree(DatumGetPointer(rd));
		}
	}
	PG_CATCH();
	{
		Py_XDECREF(rob);
		rob = NULL;
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return(rob);
}

static PyObj
type_check_constraints(PyObj self, PyObj ob)
{
	Datum datum;
	bool isnull;

	if (ob != Py_None && (PyTypeObject *) self != Py_TYPE(ob))
	{
		PyErr_SetString(PyExc_TypeError,
			"object must be an exact instance of the domain type");
		return(NULL);
	}

	if (!PyPgType_IsDomain(self))
	{
		/*
		 * Not a domain? Then it's certainly okay..
		 */
		Py_INCREF(Py_None);
		return(Py_None);
	}

	if (DB_IS_NOT_READY())
		return(NULL);

	isnull = (ob == Py_None);
	if (!isnull)
		datum = PyPgObject_GetDatum(ob);
	else
		datum = 0;

	PG_TRY();
	{
		check_constraints(self, datum, isnull);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
		return(NULL);
	}
	PG_END_TRY();

	Py_INCREF(Py_None);
	return(Py_None);
}

static PyMethodDef PyPgType_Methods[] = {
	{"typinput", (PyCFunction) PyPgType_typinput_method, METH_VARARGS,
	PyDoc_STR("create an instance of the type using the given string data")},
	{"typoutput", (PyCFunction) PyPgType_typoutput, METH_O,
	PyDoc_STR("create a cstring using the type's typoutput function")},
	{"typreceive", (PyCFunction) PyPgType_typreceive, METH_VARARGS,
	PyDoc_STR("create an instance of the type using the given binary data")},
	{"typsend", (PyCFunction) PyPgType_typsend, METH_O,
	PyDoc_STR("create a bytea object using the type's typsend function")},
	{"typmodin", (PyCFunction) type_modin, METH_O,
	PyDoc_STR("call the type's modin function")},
	{"typmodout", (PyCFunction) type_modout, METH_O,
	PyDoc_STR("call the type's modout function")},
	{"check", (PyCFunction) type_check_constraints, METH_O,
	PyDoc_STR("validate that the object meets the type's constraints(DOMAINs)")},
	{NULL,}
};

static PyObj
type_get_typname(PyObj self, void *unused)
{
	PyObj rob;
	PyPgTypeInfo typinfo = PyPgTypeInfo(self);
	rob = typinfo->typname_PyUnicode;
	Py_INCREF(rob);
	return(rob);
}

static PyObj
type_get_typnamespace(PyObj self, void *unused)
{
	PyObj rob;
	PyPgTypeInfo typinfo = PyPgTypeInfo(self);
	rob = typinfo->typnamespace_PyLong;
	Py_INCREF(rob);
	return(rob);
}

static PyObj
type_get_nspname(PyObj self, void *unused)
{
	PyObj rob;
	PyPgTypeInfo typinfo = PyPgTypeInfo(self);
	rob = typinfo->nspname_PyUnicode;
	Py_INCREF(rob);
	return(rob);
}

static PyObj
type_get_oid(PyObj self, void *unused)
{
	return(PyLong_FromUnsignedLong(PyPgType_GetOid(self)));
}

static PyObj
type_get_oidstr(PyObj self, void *unused)
{
	PyObj oidlong = PyLong_FromUnsignedLong(PyPgType_GetOid(self));
	PyObj rob;
	if (oidlong == NULL)
		return(NULL);
	rob = PyObject_Str(oidlong);
	Py_DECREF(oidlong);
	return(rob);
}

/*
 * Get the ultimate basetype of the type.
 */
static PyObj
type_get_Base(PyObj self, void *closure)
{
	PyPgTypeInfo typinfo;
	PyObj rob;

	if (PyPgObjectType_Require((PyTypeObject *) self))
		return(NULL);

	typinfo = PyPgTypeInfo(self);
	if (typinfo_is_domain(typinfo))
	{
		rob = typinfo->domain.x_yes.typubase_Type;
		Assert(rob != NULL);
	}
	else
		rob = self;

	Py_INCREF(rob);
	return(rob);
}

static PyObj
type_get_Array(PyObj self, void *closure)
{
	PyObj rob;
	PyPgTypeInfo typinfo = PyPgTypeInfo(self);

	if (typinfo->typtype == TYPTYPE_ARRAY)
		rob = self;
	else
	{
		Oid typarray = typinfo->array.x_no.typarray;

		if (OidIsValid(typarray))
			return(PyPgType_FromOid(typarray));
		else
			rob = Py_None;
	}

	Py_INCREF(rob);
	return(rob);
}

static PyObj
type_get_Element(PyObj self, void *closure)
{
	PyObj rob;
	PyPgTypeInfo typinfo = PyPgTypeInfo(self);

	if (typinfo->typtype != TYPTYPE_ARRAY)
		rob = self;
	else
		rob = typinfo->array.x_yes.typelem_Type;

	Py_INCREF(rob);
	return(rob);
}

static PyObj
type_get_descriptor(PyObj self, void *ignored)
{
	PyObj rob = Py_None;

	if (PyPgType_IsComposite(self))
		rob = PyPgType_GetPyPgTupleDesc(self);

	Py_INCREF(rob);
	return(rob);
}

static PyObj
record_get_column_names(PyObj self, void *unused)
{
	PyObj names, tdo;

	if (!PyPgType_IsComposite(self))
	{
		Py_INCREF(Py_None);
		return(Py_None);
	}

	tdo = PyPgType_GetPyPgTupleDesc(self);
	names = PyPgTupleDesc_GetNames(tdo);
	Py_INCREF(names);
	return(names);
}

static PyObj
record_get_column_types(PyObj self, void *unused)
{
	PyObj tdo;

	if (!PyPgType_IsComposite(self))
	{
		Py_INCREF(Py_None);
		return(Py_None);
	}

	tdo = PyPgType_GetPyPgTupleDesc(self);
	return(PyPgTupleDesc_GetTypes(tdo));
}

static PyObj
record_get_pg_column_types(PyObj self, void *unused)
{
	PyObj tdo;

	if (!PyPgType_IsComposite(self))
	{
		Py_INCREF(Py_None);
		return(Py_None);
	}

	tdo = PyPgType_GetPyPgTupleDesc(self);
	return(PyPgTupleDesc_GetTypeOids(tdo));
}

static PyGetSetDef type_getset[] = {
	{"oid", type_get_oid, NULL,
		PyDoc_STR("Get the type's Oid as a Python int")},
	{"oidstr", type_get_oidstr, NULL,
		PyDoc_STR("Get the type's Oid as a Python str")},
	{"typname", type_get_typname, NULL,
		PyDoc_STR("Get the type's typname as a Python str")},
	{"nspname", type_get_nspname, NULL,
		PyDoc_STR("Get the type's namespace as a Python str")},
	{"typnamespace", type_get_typnamespace, NULL,
		PyDoc_STR("Get the type's namespace *Oid* as a Python int")},
	{"descriptor", type_get_descriptor, NULL,
		PyDoc_STR("Get the type's Postgres.TupleDesc if it's a composite")},
	{"column_names", record_get_column_names, NULL,
		PyDoc_STR("name of the columns of the composite")},
	{"column_types", record_get_column_types, NULL,
		PyDoc_STR("types of the columns of the composite")},
	{"pg_column_types", record_get_pg_column_types, NULL,
		PyDoc_STR("type Oids of the composite's columns")},
	{"Base", type_get_Base, NULL,
		PyDoc_STR("The ultimate base type of the type")},
	{"Array", type_get_Array, NULL,
		PyDoc_STR("The Array type of the type")},
	{"Element", type_get_Element, NULL,
		PyDoc_STR("The Element type Array type")},
	{NULL}
};

static void
type_dealloc(PyObj self)
{
	PyPgTypeInfo typinfo;

	typinfo = &(((PyPgHeapTypeObject *) self)->typinfo);
	clear_typinfo(typinfo);

	PyType_Type.tp_dealloc(self);
}

static PyObj
type_new(PyTypeObject *subtype, PyObj args, PyObj kw)
{
	char *words[] = {"oid", NULL};
	PyObj key = NULL;
	Oid typoid;

	if (!PyArg_ParseTupleAndKeywords(args, kw, "O:Type", words, &key))
		return(NULL);

	typoid = Oid_FromPyObject(key);
	if (PyErr_Occurred())
		return(NULL);

	return(PyPgType_FromOid(typoid));
}

PyDoc_STRVAR(PyPgType_Type_Doc, "Python interface for the Postgres type");
PyTypeObject PyPgType_Type = {
	PyVarObject_HEAD_INIT(&PyType_Type, 0)
	"Postgres.Type",						/* tp_name */
	sizeof(PyPgHeapTypeObject),				/* tp_basicsize */
	0,										/* tp_itemsize */
	type_dealloc,							/* tp_dealloc */
	NULL,									/* tp_print */
	NULL,									/* tp_getattr */
	NULL,									/* tp_setattr */
	NULL,									/* tp_compare */
	NULL,									/* tp_repr */
	NULL,									/* tp_as_number */
	NULL,									/* tp_as_sequence */
	NULL,									/* tp_as_mapping */
	NULL,									/* tp_hash */
	NULL,									/* tp_call */
	NULL,									/* tp_str */
	NULL,									/* tp_getattro */
	NULL,									/* tp_setattro */
	NULL,									/* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,						/* tp_flags */
	PyPgType_Type_Doc,						/* tp_doc */
	NULL,									/* tp_traverse */
	NULL,									/* tp_clear */
	NULL,									/* tp_richcompare */
	0,										/* tp_weaklistoffset */
	NULL,									/* tp_iter */
	NULL,									/* tp_iternext */
	PyPgType_Methods,						/* tp_methods */
	NULL,									/* tp_members */
	type_getset,							/* tp_getset */
	&PyType_Type,							/* tp_base */
	NULL,									/* tp_dict */
	NULL,									/* tp_descr_get */
	NULL,									/* tp_descr_set */
	0,										/* tp_dictoffset */
	NULL,									/* tp_init */
	NULL,									/* tp_alloc */
	type_new,								/* tp_new */
};

/*
 * From the beginning, we need:
 *
 *   - the type_cache dictionary
 *   - a specially initialized record type
 *   - The pg_attribute type.
 *   - The cstring[] type.
 */
int
PyPgType_Init(void)
{
	PyObj rectype;

	Py_XDECREF(type_cache);
	type_cache = PyDict_New();
	if (type_cache == NULL)
		return(-1);

	/*
	 * record is a "psuedo psuedo" type.
	 * Base class for composites.
	 */
	rectype = PyPgType_FromOid(RECORDOID);
	if (rectype == NULL)
		return(-1);
	/*
	 * It's really a pseudo-type, but to plpython3,
	 * it's an abstract composite.
	 *
	 * These fields are inherited by subclasses, so alter
	 * them to appropriate values.
	 */
	PyPgTypeInfo(rectype)->typtype = TYPTYPE_COMPOSITE;
	PyPgTypeInfo(rectype)->typlen = -1; /* varlena */
	Py_DECREF(rectype);

	/*
	 * Used by module.c built-ins.
	 */
	rectype = PyPgType_FromOid(OIDOID);
	Py_DECREF(rectype);
	rectype = PyPgType_FromOid(TIMESTAMPTZOID);
	Py_DECREF(rectype);

	Py_XDECREF(PyPg_aclitem_Array_Type);
	PyPg_aclitem_Array_Type = PyPgType_FromOid(ACLITEMOID+1);
	if (PyPg_aclitem_Array_Type == NULL)
		return(-1);
	Assert(PyPgType_GetElementTypeOid(PyPg_aclitem_Array_Type) == ACLITEMOID);

	Py_XDECREF(PyPg_pg_attribute_Type);
	PyPg_pg_attribute_Type = PyPgType_FromOid(AttributeRelation_Rowtype_Id);
	if (PyPg_pg_attribute_Type == NULL)
		return(-1);

	Py_XDECREF(PyPg_cstring_Array_Type);
	PyPg_cstring_Array_Type = PyPgType_FromOid(CSTRINGARRAYOID);
	if (PyPg_cstring_Array_Type == NULL)
		return(-1);

	return(0);
}

int32
PyPgType_modin(PyObj subtype, PyObj mod)
{
	PyPgTypeInfo typinfo;
	FunctionCallInfoData fcinfo = {0,};
	int32 r = 0;

	typinfo = PyPgTypeInfo(subtype);

	fcinfo.flinfo = &(typinfo->typfunc->typmodin);
	fcinfo.isnull = false;
	fcinfo.nargs = 1;
	fcinfo.arg[0] = PyPgObject_GetDatum(mod);
	fcinfo.argnull[0] = false;

	r = DatumGetInt32(FunctionCallInvoke(&fcinfo));
	if (fcinfo.isnull)
		r = -1;

	return(r);
}

/*
 * This is the primary interface that is used when creating a new Datum from a
 * Postgres.Type and an arbitrary Python object.
 *
 * If `ob` is Py_None, *isnull will be set to `true` and *rdatum will be set to
 * zero.
 *
 * If the Postgres.Type has a custom `pypg_new_datum` field, it will be used if
 * `ob` is not a PyPgObject or a PyUnicode object.
 *
 * If `ob` is a PyPgObject, it will attempt to use Postgres' cast facility.
 */
void
PyPgType_DatumNew(
	PyObj subtype, PyObj ob, int32 mod,
	Datum *rdatum, bool *isnull)
{
	PyPgTypeInfo typinfo = PyPgTypeInfo(subtype);
	pypg_new_datum tp_new_datum;

	Assert(PyPgType_Check(subtype));

	if (ob == Py_None)
	{
		/*
		 * Fast path out of here.
		 * Caller needs to check constraints, if any.
		 */
		*isnull = true;
		*rdatum = 0;
		return;
	}

	tp_new_datum = PyPgType_GetDatumNew(subtype);

	if (PyPgObject_Check(ob))
	{
		if (typinfo->typoid == RECORDOID && PyPgType_IsComposite(Py_TYPE(ob)))
		{
			/*
			 * The target type is an anonymous record, and the source is a
			 * composite. A cast is unlikely as the target type
			 * may not actually exist in pg_type(fully anonymous composites).
			 */
			PyPg_record_Reform(subtype, ob, mod, rdatum, isnull);
		}
		else if (mod == -1 && (PyObject_IsInstance(ob, subtype)))
		{
			/*
			 * It's the same thang. datumCopy away.
			 */
			*rdatum = datumCopy(
				PyPgObject_GetDatum(ob),
				typinfo->typbyval,
				typinfo->typlen
			);
			*isnull = false;
		}
		else
		{
			/*
			 * Not the same type, and the target is not an anonymous record.
			 */
			if (PyPgType_typcast(subtype, ob, mod, rdatum, isnull))
			{
				/*
				 * No cast could be found. Make a CString and give to
				 * typinput.
				 */
				PyPgType_typinput(subtype, ob, mod, rdatum, isnull);
			}
		}
	}
	else if (tp_new_datum == NULL || PyUnicode_Check(ob))
	{
		PyPgType_typinput(subtype, ob, mod, rdatum, isnull);
	}
	else
	{
		tp_new_datum(subtype, ob, mod, rdatum, isnull);
	}
}

PyObj
PyPgType_FromTableOid(Oid tableOid)
{
	PyObj rob = NULL;

	Assert(OidIsValid(tableOid));

	if (DB_IS_NOT_READY())
		return(NULL);

	PG_TRY();
	{
		Relation rd = NULL;

		rd = RelationIdGetRelation(tableOid);
		if (rd != NULL)
			rob = PyPgType_FromOid(rd->rd_rel->reltype);
		RelationClose(rd);
	}
	PG_CATCH();
	{
		PyErr_SetPgError(false);
	}
	PG_END_TRY();

	return(rob);
}

PyObj
PyPgType_Polymorph(PyObj self, PyObj target)
{
	PyObj rob = NULL;

	Assert(PyPgType_Check(self));

	if (!PyPgType_IsPolymorphic(self))
	{
		Py_INCREF(self);
		return(self);
	}

	if (PyPgType_IsComposite(self))
	{
		PyObj tdo = PyPgType_GetPyPgTupleDesc(self);
		PyObj typname, new_tdo;
		
		new_tdo = PyPgTupleDesc_Polymorph(tdo, target);
		if (new_tdo == NULL)
			return(NULL);

		/*
		 * Give a string that identifies that the type was from a
		 * polymorphic type.
		 */
		typname = PyUnicode_FromFormat("%R.polymorph(Postgres.types.%U)",
			self, PyPgTypeInfo(target)->typname_PyUnicode
		);
		if (typname != NULL)
		{
			rob = PyPgType_FromPyPgTupleDesc(new_tdo, typname);
			Py_DECREF(typname);
		}

		Py_DECREF(new_tdo);
	}
	else
	{
		Oid polytyp = PyPgType_GetOid(self);

		/*
		 * It's a regular polymorphic type.
		 */

		switch(polytyp)
		{
			case ANYARRAYOID:
				rob = type_get_Array(target, NULL);
				if (rob == NULL)
				{
					PyErr_Format(PyExc_TypeError,
						"polymorphic target (%s) has no array type",
						((PyTypeObject *) target)->tp_name
					);
				}
			break;

			case ANYENUMOID:
				if (!PyPgType_IsEnum(target))
				{
					PyErr_Format(PyExc_TypeError,
						"polymorphic type (%s) requires an enum type, not %s",
						Py_TYPE(self)->tp_name,
						Py_TYPE(target)->tp_name
					);
					break;
				}
			case ANYNONARRAYOID:
			case ANYELEMENTOID:
				rob = target;
				Py_INCREF(rob);
			break;

			default:
				/*
				 * Postgres added a polymorphic type and it wasn't
				 * implemented here. *snif* :~(
				 */
				PyErr_Format(PyExc_TypeError,
					"unsupported polymorphic type %u", polytyp);
			break;
		}
	}

	return(rob);
}
