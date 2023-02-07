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

int lotman_add_lot(const char *lotman_JSON_str, const char *lotman_context, char **err_msg);
int lotman_remove_lot(const char *lot_name, const char *lotman_context, char **err_msg);
//int lotman_remove_sublot(const char *name, char **err_msg);
// //int lotman_get_sublot_paths(const char *path, char **sublot_paths_arr, char **err_msg);


#ifdef __cplusplus
}
#endif