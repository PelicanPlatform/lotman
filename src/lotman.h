/**
 * Public header for the LotMan C Library
*/

#ifdef __cplusplus
extern "C" {
#endif


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
 * API for deleting a lot. LTBR = lot-to-be-removed.
 * OPTION assign_LTBR_parent_as_parent_to_orphans: If true, any lots whose only parent is currently LTBR will be reassigned to have LTBR's parents as explicit parents.
 *         If false, any orphans will automatically be reassigned to have the default lot as a parent.
 *         NOTE: If LTBR is a root (ie it has only itself as a parent) and this option is true, the function should fail with an indication that the child could not be 
 *               reassigned because LTBR has no parents. This is to explicity ensure the user is aware the orphans must be reassigned to the default lot.
 * OPTION assign_LTBR_parent_as_parent_to_non_orphans: If true, any non-orphans will be updated to have LTBR's parents as explicit parents. If false, LTBR will be removed as 
 *         a parent from the non-orphaned children and no extra parents will be added. 
 *         NOTE: If a child of LTBR has only LTBR and itself as a parent, setting this option to false will cause that child to become a root.
 *         NOTE: If LTBR is a root and this option is true, the function should fail with an indication that the child could not be reassigned because LTBR has no parents.
 * OPTION assign_policy_to_children: If true, the policy attributes of all of LTBR's children will be overwritten with LTBR's policy attributes.
*/
int lotman_remove_lot(const char *lot_name, bool assign_LTBR_parent_as_parent_to_orphans, bool assign_LTBR_parent_as_parent_to_non_orphans, bool assign_policy_to_children, const char *lotman_context, char **err_msg);
int lotman_update_lot(const char *lotman_JSON_str, const char *lotman_context, char **err_msg);
int lotman_add_to_lot(const char *lot_name, const char *lotman_JSON_str, const char *lotman_context, char **err_msg);
int lotman_lot_exists(const char *lot_name, const char *lotman_context, char **err_msg);
int lotman_is_root(const char *lot_name, char **err_msg);
int lotman_get_owners(const char *lot_name, const bool recursive, char ***output, char **err_msg);
int lotman_get_parent_names(const char *lot_name, const bool recursive, const bool get_self, char ***output, char **err_msg);
int lotman_get_children_names(const char *lot_name, const bool recursive, const bool get_self, char ***output, char **err_msg);
int lotman_get_policy_attributes(const char *lot_name, const char *policy_attributes_JSON, char **output, char **err_msg);
int lotman_get_lot_dirs(const char *lot_name, const bool recursive, char **output, char **err_msg);
int lotman_get_matching_lots(const char *criteria_JSON, char ***output, char **err_msg);
int lotman_get_lot_usage(const char *lot_name, const char *usage_attributes_JSON, char **output, char **err_msg);
int lotman_update_lot_usage(const char *lot_name, const char *update_JSON_str, char **er_msg);
int lotman_check_db_health(char **err_msg);
void lotman_free_string_list(char **str_list);
int lotman_get_lot_obj(const char *lot_JSON_str, char **output, char **err_msg);


#ifdef __cplusplus
}
#endif