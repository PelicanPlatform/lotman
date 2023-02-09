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
int lotman_lot_exists(const char *lot_name, const char *lotman_context, char **err_msg);

/**
 * API for creating a "lot."
 * Takes in a JSON lot object as a string and attempts to create the lot as defined by the JSON object.
 * Returns 0 upon success, nonzero for failure.
*/

int lotman_add_lot(const char *lotman_JSON_str, const char *lotman_context, char **err_msg);
/**
 * API for deleting a lot. 
 * OPTION assign_default_as_parent_to_orphans: If true, any lots that would be orphaned by removing the lot-to-be-removed (LTBR) will be updated to have default as a parent.
 * OPTION assign_default_as_parent_to_non_orphans: If true, any non-orphans (ie lots who have parents aside from LTBR) will be updated to have default as a parent.
 * OPTION assign_LTBR_as_parent_to_orphans: If true, any orphaned lots will be updated to have LTBR's parents as parents.
 * OPTION assign_LTBR_as_parent_to_non_orphans: If true, any non-orphans will be updated to have LTBR's parents as parents.
 * OPTION assign_policy_to_children: If true, the policy attributes of all of LTBR's children will be overwritten with LTBR's policy attributes.
*/
int lotman_remove_lot(const char *lot_name, bool assign_default_as_parent_to_orphans, bool assign_default_as_parent_to_non_orphans, bool assign_LTBR_as_parent_to_orphans, bool assign_LTBR_as_parent_to_non_orphans, bool assign_policy_to_children, const char *lotman_context, char **err_msg);
int lotman_update_lot(const char *lotman_JSON_str, const char *lotman_context, char **err_msg);
// //int lotman_get_sublot_paths(const char *path, char **sublot_paths_arr, char **err_msg);


#ifdef __cplusplus
}
#endif