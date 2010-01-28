/*
 * Interfaces for pseudo types
 */
#ifndef PyPg_type_pseudo_H
#define PyPg_type_pseudo_H 0
#ifdef __cplusplus
extern "C" {
#endif

extern PyPgTypeObject PyPgPseudo_Type;

extern PyPgTypeObject PyPg_void_Type;
#define PyPg_void_Type_oid VOIDOID
extern PyPgTypeObject PyPg_trigger_Type;
#define PyPg_trigger_Type_oid TRIGGEROID

#ifdef __cplusplus
}
#endif
#endif /* !PyPg_type_pseudo_H */
