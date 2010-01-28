/*
 * cpp tools
 */
#ifndef PyPg_environment_H
#define PyPg_environment_H 0
#ifdef __cplusplus
extern "C" {
#endif

#define STR(V) #V
#define XSTR(V) STR(V)
#define CATLIT(ONE,TWO) ONE##TWO

#define __SLINE__ XSTR(__LINE__)

#define LOCATION_ARGS const char *const _file_L, \
							 const int _line_L, \
							 const char *const _func_L
#define LOCATION_VARS _file_L, _line_L, _func_L

#define LOCATION_FM "File \"%s\", line %d, in %s"

#define __LOCATIONA__ __FILE__, __LINE__, PG_FUNCNAME_MACRO

#define __LOCATION__			\
	"File \""	__FILE__		\
	"\", line"	__SLINE__

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_environment_H */
