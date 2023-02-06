/**
 * Public header for the LotMan C Library
*/

#ifdef __cplusplus
extern "C" {
#endif

typedef void * Lot;

/**
 * API for getting LotMan library version.
*/
const char * lotman_version();


/**
 * API for creating a "lot."
 * Returns 0 upon success, nonzero for failure.
*/
//int lotman_initialize_root(const char *name, const char *path, const char *owner, const char *resource_limits, const char *reclamation_policy, char **err_msg);

int lotman_add_sublot(const char *name, const char owners[], int num_owners, const char parents[], int num_parents,  const char children[], int num_children, const char *paths, const char *management_policy_attrs, char **err_msg);

//int lotman_remove_sublot(const char *name, char **err_msg);
// //int lotman_get_sublot_paths(const char *path, char **sublot_paths_arr, char **err_msg);


#ifdef __cplusplus
}
#endif