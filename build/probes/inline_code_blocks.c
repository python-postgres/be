#include "postgres.h"
#include "fmgr.h"
#include "storage/itemptr.h"
#include "nodes/parsenodes.h"

#include <stdio.h>

extern InlineCodeBlock codeblock;
#define feature "inline_code_block"
#define parameterl 1

