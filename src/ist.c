/*
 * Postgres IST (Internal SubTransaction) management interfaces
 */
#include <setjmp.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"

#include "pypg/python.h"
#include "pypg/pl.h"
#include "pypg/error.h"
#include "pypg/ist.h"

/*
 * Count of open ISTs.
 */
unsigned long pl_ist_count = 0;

bool
pl_ist_abort(unsigned long xid, char state)
{
	bool r = true;

	PG_TRY();
	{
		if (pl_ist_count != xid)
		{
			ereport(ERROR,(
				errcode(ERRCODE_SAVEPOINT_EXCEPTION),
				errmsg("out-of-order abort attempt on subtransaction %lu", xid),
				errdetail("Subtransaction %lu was expected to exit next.", pl_ist_count)
			));
		}
		/* Prevent wrap around */
		if (pl_ist_count == 0)
		{
			ereport(ERROR,(
				errcode(ERRCODE_SAVEPOINT_EXCEPTION),
				errmsg("no current internal subtransaction"),
				errhint("Attempt to abort the current IST, when none running.")
			));
		}
		if (state == pl_ist_committed)
		{
			ereport(ERROR,(
				errcode(ERRCODE_SAVEPOINT_EXCEPTION),
				errmsg("cannot abort a committed subtransaction")
			));
		}
		if (state == pl_ist_aborted)
		{
			ereport(ERROR,(
				errcode(ERRCODE_SAVEPOINT_EXCEPTION),
				errmsg("subtransaction was already aborted")
			));
		}
		if (state == pl_ist_new)
		{
			ereport(ERROR,(
				errcode(ERRCODE_SAVEPOINT_EXCEPTION),
				errmsg("cannot abort a subtransaction that has not been started")
			));
		}

		pl_ist_count = pl_ist_count - 1;
		RollbackAndReleaseCurrentSubTransaction();

		pl_state = pl_ready_for_access; /* No longer in an error state. */
		SPI_restore_connection();
	}
	PG_CATCH();
	{
		PyErr_SetPgError(true);
		pl_state = pl_in_failed_transaction;
		r = false;
	}
	PG_END_TRY();

	return(r);
}

bool
pl_ist_commit(unsigned long xid, char state)
{
	bool r = true;

	/*
	 * Attempted commit while in an error state?
	 * Resolve to abort, and raise an exception.
	 */
	if (pl_state)
	{
		r = false; /* Always raises an exception in here */

		if (pl_ist_abort(xid, state))
		{
			/*
			 * The abort was successful, but that fact needs to be
			 * communicated as the original action was a commit.
			 * Raise an exception.
			 */
			PyErr_SetInFailedTransaction();
		}

		Assert(PyErr_Occurred());
	}
	else
	{
		PG_TRY();
		{
			if (pl_ist_count != xid)
			{
				ereport(ERROR,(
					errcode(ERRCODE_SAVEPOINT_EXCEPTION),
					errmsg("out-of-order commit attempted on subtransaction %lu", xid),
					errdetail("Subtransaction %lu was expected to exit next.", pl_ist_count)
				));
			}
			/* Prevent wrap around */
			if (pl_ist_count == 0)
			{
				ereport(ERROR,(
					errcode(ERRCODE_SAVEPOINT_EXCEPTION),
					errmsg("no current internal subtransaction"),
					errhint("Attempt to commit an IST, when none running.")
				));
			}
			if (state == pl_ist_committed)
			{
				ereport(ERROR,(
					errcode(ERRCODE_SAVEPOINT_EXCEPTION),
					errmsg("subtransaction already committed")
				));
			}
			if (state == pl_ist_aborted)
			{
				ereport(ERROR,(
					errcode(ERRCODE_SAVEPOINT_EXCEPTION),
					errmsg("cannot commit aborted subtransaction")
				));
			}
			if (state == pl_ist_new)
			{
				ereport(ERROR,(
					errcode(ERRCODE_SAVEPOINT_EXCEPTION),
					errmsg("cannot commit a subtransaction that has not been started")
				));
			}

			pl_ist_count = pl_ist_count - 1;
			ReleaseCurrentSubTransaction();
		}
		PG_CATCH();
		{
			PyErr_SetPgError(true);
			pl_state = pl_in_failed_transaction;
			r = false;
		}
		PG_END_TRY();
		/* end of commit */
	}

	return(r);
}

bool
pl_ist_begin(char state)
{
	bool r = true;

	PG_TRY();
	{
		/* Prevent wrap around */
		if (pl_ist_count + 1 == 0)
		{
			ereport(ERROR,(
				errcode(ERRCODE_SAVEPOINT_EXCEPTION),
				errmsg("too many internal subtransactions"),
				errdetail("Subtransaction %lu was expected to exit next.", pl_ist_count)
			));
		}
		if (state == pl_ist_committed)
		{
			ereport(ERROR,(
				errcode(ERRCODE_SAVEPOINT_EXCEPTION),
				errmsg("cannot start a committed subtransaction")
			));
		}
		if (state == pl_ist_aborted)
		{
			ereport(ERROR,(
				errcode(ERRCODE_SAVEPOINT_EXCEPTION),
				errmsg("cannot start an aborted subtransaction")
			));
		}
		if (state == pl_ist_open)
		{
			ereport(ERROR,(
				errcode(ERRCODE_SAVEPOINT_EXCEPTION),
				errmsg("subtransaction is already open")
			));
		}

		BeginInternalSubTransaction(NULL);
		pl_ist_count = pl_ist_count + 1;
	}
	PG_CATCH();
	{
		PyErr_SetPgError(true);
		pl_state = pl_in_failed_transaction;
		r = false;
	}
	PG_END_TRY();

	return(r);
}

/*
 * pl_ist_reset - Abort the given number of ISTs and set a Python error.
 *
 * Used to handle cases where a code object fails to resolve open transactions.
 */
void
pl_ist_reset(unsigned long count)
{
	for (; count > 0; --count)
		RollbackAndReleaseCurrentSubTransaction();
}
