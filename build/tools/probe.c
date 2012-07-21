
#include <stdio.h>
int
main(int argc, char *argv[])
{
#ifdef RUNTIME
	RUNTIME();
#endif

#ifdef parameterl
	printf("#define f_%s %ld\n", feature, (long) parameterl);
#endif

#ifdef parameters
	printf("#define f_%s \"%s\"\n", feature, parameters);
#endif
	return(0);
}
