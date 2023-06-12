/**
 * Public header for the LotMan C Library
*/

#ifdef __cplusplus
extern "C" {
#endif

/*
API Functions
*/

const char * lotman_version();
/**
    DESCRIPTION: Returns current version of LotMan
*/

int lotman_lot_exists(const char *lot_name, char **err_msg);
/**
    DESCRIPTION: A function for determining whether a lot exists in the lot database. 
    
    RETURNS: Returns 0 if the lot does not exist, 1 if the lot does exist. Any other
        values indicate an error. 

    INPUTS:
    lot_name:
        The name of the lot whose existence is to be determine.
    err_msg:
        A reference to a char array that can store any error messages. 

    OUTPUTS: An error message, when one is generated.
*/


int lotman_add_lot(const char *lotman_JSON_str, char **err_msg);
/**
    DESCRIPTION: Function for adding/creating a lot, which gets stored in the lot database. 
    
    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS: 
    lotman_JSON_str: 
        REQUIRED. A string defining the lot in JSON (see below for lot object specification)
    
    err_msg:
        REQUIRED. A reference to a char array that can store any error messages.
    
    OUTPUTS: An error message, when one is generated. 

    Lot Object specification:
    
{   "lot_name": 
        REQUIRED. A string indicating the name of the lot. 
    "owner": 
        REQUIRED. A string indicating the entity that "owns" the contents of the lot. Ownership of a lot
        indicates ownership of the lots contents, but not the lot itself; like renting a garden -- 
        you own what you grow, but not the dirt you grow it in. A lot owner cannot modify the lot's
        properties unless the lot is a self parent. However, the owner can modify the properties of
        any sub lots. 
    "parents": 
        REQUIRED. An array of strings indicating any lots that are parent to the lot about to be added.
        Any parents specified must already exist in the lot database, unless the specified parent
        is the lot itself. At least one parent must be specified.
    "children": 
        OPTIONAL. An array of strings indicating any lots that are children of the lot about to be added.
        Any children must already exist in the lot database. A lot should not specify itself as
        a child, as this case is already handled by the self parent mechanism.
    "paths": 
        OPTIONAL. An array of `path` objects that are used to associate paths/objects to the lot. Each 
        path object has the following structure:
        {"path": "/path/to/associate", "recursive": bool}
        When "recursive" is true, all subdirectories of the path are also attributed to the lot. When false,
        only non-directory objects below the path should be attributed, but any subdirectories should not be.
    "management_policy_attrs": 
        REQUIRED. A JSON object containing the collection of attributes used for creating policies for the 
        lot. The object has the following structure:

    {   "dedicated_GB":
            REQUIRED. A double indicating the size in GB of dedicated storage a lot possesses. During a lot's 
            lifespan, this is the amount of persistant storage it should be allowed to access.
        "opportunistic_GB":
            REQUIRED. A double indicating the size in GB of opportunistic storage a lot possesses. During a 
            lot's lifespan, this is the amount of extra storage it should be able to access after all 
            dedicated storage has been exhausted if the system determines there is unused capacity. Opp 
            storage should be considered transient, and as such, depending on the management policy, lots 
            that are using some amount of opp storage may be subject to purging in the event that the system 
            needs to create space.
        "max_num_objects": An int64 indicating the maxiximum number of objects a lot may have attributed to it.
        "creation_time":
            REQUIRED. A Unix timestamp in milliseconds indicating when the lot should begin its existence.
        "expiration_time":
            REQUIRED. A Unix timestamp in milliseconds indicating when the lot "expires". In a similar vein
            as opp storage, a lot and its storage become transient after its expiration time has passed. If 
            the system determines there is unused space, the lot may continue to exist, but all storage tied 
            to the lot should be considered opportunistic.
        "deletion_time":
            REQUIRED. A Unix timestamp in milliseconds indicating when the lot and its contents should be deleted.
    }
}
*/

int lotman_remove_lot(const char *lot_name, bool assign_LTBR_parent_as_parent_to_orphans,
                      bool assign_LTBR_parent_as_parent_to_non_orphans, bool assign_policy_to_children, 
                      const bool override_policy, char **err_msg);
/**
    DESCRIPTION: Function for deleting a lot, but preserving any children lots. In general, the function to remove
        lots recursively should be favored over this function. When this func is used, LotMan will attempt to handle 
        the proper policy/parent reassignments for children lots according to the selection of options. These 
        reassignments are necessary to preseve the lot database's structure.
        NOTE: Deleting a lot does not actually affect the objects tied to the lot. It only removes the lot from the
            lot database. 

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS: 
    lot_name: 
        A string indicating the name of the lot to be removed (LTBR)
    
    assign_LTBR_parent_as_parent_to_orphans:
        A bool indicating whether LotMan should attempt to assign LTBR's parents as the new parents to any lots 
        orphaned by LTBR's removal. If true, any lots whose only parent is currently LTBR will be reassigned to 
        have LTBR's parents as explicit parents. If false, any orphans will automatically be reassigned to have 
        the default lot as a parent.
        NOTE: If LTBR is a root (ie it has only itself as a parent) and this option is true, the function should 
            fail with an indication that the child could not be reassigned because LTBR has no parents. This is 
            to explicity ensure the user is aware the orphans must be reassigned to the default lot.
    
    assign_LTBR_parent_as_parent_to_non_orphans:
        A bool indicating whether LotMan should attempt to assign LTBR's parents as the new parents to any lots
        that are not orphaned by LTBR's removal. If true, any non-orphans will be updated to have LTBR's parents 
        as explicit parents. If false, LTBR will be removed as a parent from the non-orphaned children and no extra 
        parents will be added. 
        NOTE: If a child of LTBR has only LTBR and itself as a parent, setting this option to false will cause that 
            child to become a root.
        NOTE: If LTBR is a root and this option is true, the function should fail with an indication that the child 
            could not be reassigned because LTBR has no parents.

    assign_policy_to_children:
        A bool indicating whether LTBR's management policy attributes should be assigned to its children. If true,
        LTBR's children will have their policy attributes overwritten by LTBR's.

    override_policy:
        THIS FEATURE IS NOT YET IMPLEMENTED. A bool indicating whether a lot's stored deletion policy should be 
        overwritten by the other options. If true, the lot will be deleted according to other options. If false,
        any of the other options will be superceded by the stored deletion policy 

    err_msg:
        REQUIRED. A reference to a char array that can store any error messages.
    
    OUTPUTS: An error message, when one is generated.
*/









/**
    DESCRIPTION: 

    RETURNS:

    INPUTS: 
    
    
    OUTPUTS: An error message, when one is generated.

*/








int lotman_remove_lots_recursive(const char *lot_name, char **err_msg);
/**
    DESCRIPTION: A function for deleting a lot and its children, recursively. 

    RETURNS:

    INPUTS: 
    
    
    OUTPUTS: An error message, when one is generated.

*/
int lotman_update_lot(const char *lotman_JSON_str, char **err_msg);






int lotman_rm_parents_from_lot(const char *remove_dirs_JSON_str, char **err_msg);
int lotman_rm_paths_from_lots(const char *remove_dirs_JSON_str, char **err_msg);






int lotman_add_to_lot(const char *lotman_JSON_str, char **err_msg);


int lotman_lot_exists(const char *lot_name, char **err_msg);
int lotman_is_root(const char *lot_name, char **err_msg);
int lotman_get_owners(const char *lot_name, const bool recursive, char ***output, char **err_msg);
int lotman_get_parent_names(const char *lot_name, const bool recursive, const bool get_self, char ***output, char **err_msg);
int lotman_get_children_names(const char *lot_name, const bool recursive, const bool get_self, char ***output, char **err_msg);
int lotman_get_policy_attributes(const char *policy_attributes_JSON, char **output, char **err_msg);
int lotman_get_lot_dirs(const char *lot_name, const bool recursive, char **output, char **err_msg);
int lotman_get_lot_usage(const char *usage_attributes_JSON, char **output, char **err_msg);
int lotman_update_lot_usage(const char *update_JSON_str, bool deltaMode, char **err_msg);
int lotman_update_lot_usage_by_dir(const char *update_JSON_str, bool deltaMode, char **err_msg);



void lotman_free_string_list(char **str_list);
//int lotman_check_db_health(char **err_msg); // Should eventually check that data structure conforms to expectations. If there's a cycle or a non-self-parent root, something is wrong
int lotman_get_lot_from_dir(const char *dir_name, char **output, char **err_msg);
int lotman_get_lots_past_exp(const bool recursive, char ***output, char **err_msg);
int lotman_get_lots_past_del(const bool recursive, char ***output, char **err_msg);
int lotman_get_lots_past_opp(const bool recursive_quota, const bool recursive_children, char ***output, char **err_msg);
int lotman_get_lots_past_ded(const bool recursive_quota, const bool recursive_children, char ***output, char **err_msg);
int lotman_get_lots_past_obj(const bool recursive_quota, const bool recursive_children, char ***output, char **err_msg);
int lotman_list_all_lots(char ***output, char **err_msg);
int lotman_get_lot_as_json(const char *lot_name, const bool recursive, char **output, char **err_msg);
int lotman_get_lots_from_dir(const char *dir, const bool recursive, char ***output, char **err_msg);

//int lotman_get_matching_lots(const char *criteria_JSON, char ***output, char **err_msg);




int lotman_set_context_str(const char *key, const char *value, char **err_msg);


#ifdef __cplusplus
}
#endif
