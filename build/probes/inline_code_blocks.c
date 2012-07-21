#include "postgres.h"
#include "fmgr.h"
//#include "access/htup.h"
//#include "access/tupdesc.h"
//#include "access/transam.h"
//#include "catalog/pg_proc.h"
//#include "catalog/pg_type.h"
#include "storage/itemptr.h"
#include "nodes/parsenodes.h"

#include <stdio.h>

extern InlineCodeBlock codeblock;
#define feature "inline_code_block"
#define parameterl 1

