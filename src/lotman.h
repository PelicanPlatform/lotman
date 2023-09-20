/**
 * Public header for the LotMan C Library
*/

#include <memory>

#ifdef __cplusplus
extern "C" {
#endif

// DB timeout
extern std::shared_ptr<int> lotman_db_timeout;

/*
APIs
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
*/


int lotman_add_lot(const char *lotman_JSON_str, char **err_msg);
/**
    DESCRIPTION: Function for adding/creating a lot, which gets stored in the lot database. 
    
    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS: 
    lotman_JSON_str: 
        A string defining the lot in JSON (see below for lot object specification)
    
    err_msg:
        A reference to a char array that can store any error messages.

    Lot Object specification:
{   "lot_name": 
        REQUIRED: A string indicating the name of the lot. 
    "owner": 
        REQUIRED: A string indicating the entity that "owns" the contents of the lot. Ownership of a lot
        indicates ownership of the lots contents, but not the lot itself; like renting a garden -- 
        you own what you grow, but not the dirt you grow it in. A lot owner cannot modify the lot's
        properties unless the lot is a self parent. However, the owner can modify the properties of
        any sub lots. 
    "parents": 
        REQUIRED: An array of strings indicating any lots that are parent to the lot about to be added.
        Any parents specified must already exist in the lot database, unless the specified parent
        is the lot itself. At least one parent must be specified.
    "children": 
        OPTIONAL: An array of strings indicating any lots that are children of the lot about to be added.
        Any children must already exist in the lot database. A lot should not specify itself as
        a child, as this case is already handled by the self parent mechanism.
    "paths": 
        OPTIONAL: An array of `path` objects that are used to associate paths/objects to the lot. Each 
        path object has the following structure:
        {"path": "/path/to/associate", "recursive": bool}
        When "recursive" is true, all subdirectories of the path are also attributed to the lot. When false,
        only non-directory objects below the path should be attributed, but any subdirectories should not be.
    "management_policy_attrs": 
        REQUIRED: A JSON object containing the collection of attributes used for creating policies for the 
        lot. The object has the following structure:

    {   "dedicated_GB":
            REQUIRED: A double indicating the size in GB of dedicated storage a lot possesses. During a lot's 
            lifespan, this is the amount of persistant storage it should be allowed to access.
        "opportunistic_GB":
            REQUIRED: A double indicating the size in GB of opportunistic storage a lot possesses. During a 
            lot's lifespan, this is the amount of extra storage it should be able to access after all 
            dedicated storage has been exhausted if the system determines there is unused capacity. Opp 
            storage should be considered transient, and as such, depending on the management policy, lots 
            that are using some amount of opp storage may be subject to purging in the event that the system 
            needs to create space.
        "max_num_objects": An int64 indicating the maxiximum number of objects a lot may have attributed to it.
        "creation_time":
            REQUIRED: A Unix timestamp in milliseconds indicating when the lot should begin its existence.
        "expiration_time":
            REQUIRED: A Unix timestamp in milliseconds indicating when the lot "expires". In a similar vein
            as opp storage, a lot and its storage become transient after its expiration time has passed. If 
            the system determines there is unused space, the lot may continue to exist, but all storage tied 
            to the lot should be considered opportunistic.
        "deletion_time":
            REQUIRED: A Unix timestamp in milliseconds indicating when the lot and its contents should be deleted.
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
        NOTE: Lotman must be called with the correct context set for a lot to be successfully deleted. When deleting a
            lot, the caller must be set to one of the lot's owners.

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
        A reference to a char array that can store any error messages.
*/


int lotman_remove_lots_recursive(const char *lot_name, char **err_msg);
/**
    DESCRIPTION: A function for deleting a lot and its children, recursively.
        NOTE: Lotman must be called with the correct context set for a lot to be successfully deleted. When deleting a
            lot, the caller must be set to one of the lot's owners.

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    lot_name: 
        A string indicating the name of the lot to be removed (LTBR)

    err_msg:
        A reference to a char array that can store any error messages.
*/


int lotman_update_lot(const char *lotman_JSON_str, char **err_msg);
/**
    DESCRIPTION: A function for updating parameters of a lot from one value to another, distinct from deleting or 
        adding parameters (such as paths or parents). Things that can be updated include owners, parents, paths, 
        and management policy attributes.
        NOTE: Lotman must be called with the correct context set for a lot to be successfully updated. When updating
        a lot, the caller must be set to one of the lot's owners.

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS: 
    lotman_JSON_str:
        A string defining the update object in JSON (see update object specification below).

    err_msg:
        A reference to a char array that can store any error messages.

    Lot Update Object specification:
{   "lot_name": 
        REQUIRED: A string indicating the name of the lot. 
    "owner": 
        OPTIONAL: A string indicating the intended new owner of the lot. 
    "parents": 
        OPTIONAL: An array of parent update objects, where each parent update object takes the following
        form:
        {"current": "current_parent", "new": "new_parent"}
    "paths": 
        OPTIONAL: An array of path update objects, where each path update object takes the following form:
        {"current": "/foo", "new": "/bar", "recursive": true}
    "management_policy_attrs": 
        OPTIONAL: An object that defines how each/any attribute from the Management Policy Attributes should
        be updated, taking the following form:
        {   "dedicated_GB":
                OPTIONAL: A double indicating the new dedicated_GB value.
            "opportunistic_GB":
                OPTIONAL: A double indicating the new opportunistic_GB value.
            "max_num_objects": 
                OPTIONAL: An int64 indicating the new max object count for the lot.
            "expiration_time":
                OPTIONAL: A Unix timestamp in milliseconds indicating the lot's new expiration.
            "deletion_time":
                OPTIONAL: A Unix timestamp in milliseconds indicating the lot's new deletion time.
        }
}
*/


int lotman_rm_parents_from_lot(const char *remove_parents_JSON_str, char **err_msg);
/**
    DESCRIPTION: A function for deleting parents from the lot. This function will not allow all parents
        to be deleted, as lots must have at least one parent (including self-parents).
        NOTE: Lotman must be called with the correct context set for a parent to be removed from a given
        lot. When removing a parent, the caller must be set to one of the lot's owners.

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    remove_parents_JSON_str:
        A string defining the JSON object for removing parents (see specification below).

    err_msg:
        A reference to a char array that can store any error messages.

    Parent removal object:
{   "lot_name":
        REQUIRED: A string indicating the name of the lot whose parents are to be removed.
    "parents":
        REQUIRED: An array of strings, indicating which parents are to be removed.
}
*/


int lotman_rm_paths_from_lots(const char *remove_dirs_JSON_str, char **err_msg);
/**
    DESCRIPTION: A function for deleting paths from the lot.
        NOTE: Lotman must be called with the correct context set for a path to be removed from a given
        lot. When removing a path, the caller must be set to one of the lot's owners.
        NOTE: This function does not rely on knowing the name of the lot that will be affected, unlike
        many other functions. Because each path can be bound to only one lot, the function is able to
        determine all required information on its own. 

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    remove_dirs_JSON_str:
        A string defining the JSON object for removing paths (see specification below).

    err_msg:
        A reference to a char array that can store any error messages.

    Path removal object:
{
    "paths":
        REQUIRED: An array of strings, indicating which paths are to be removed.
}
*/


int lotman_add_to_lot(const char *additions_JSON_str, char **err_msg);
/**
    DESCRIPTION: A function for adding parameters to a lot. Parameters that can be added to a lot
        include parents & paths.
        NOTE: Lotman must be called with the correct context set for parents or paths to be added
        to a given lot. When using this API, the caller must be set to one of the lot's owners.

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS: 
    additions_JSON_str:
        A string defining the JSON object for adding to the lot (see specification below);

    err_msg:
        A reference to a char array that can store any error messages.

    Additions JSON Object:
{   "lot_name":
        REQUIRED: A string indicating the name of the lot. 
    "paths":
        OPTIONAL: An array of path objects with the following form:
        {"path": "/new/path", "recursive": true}
    "parents":
        OPTIONAL: An array of strings indicating the new parents to be added to the lot. These
        parents must be existing, valid lot names.
}
*/

int lotman_lot_exists(const char *lot_name, char **err_msg);
/**
    DESCRIPTION: A function for determining the existence of a lot

    RETURNS: Returns 0 if the lot does not exist, 1 if it does. Any other values indicate an error.

    INPUTS: 
    lot_name:
        A string indicating the name of the lot whose existence is to be determined.

    err_msg:
        A reference to a char array that can store any error messages.
*/


int lotman_is_root(const char *lot_name, char **err_msg);
/**
    DESCRIPTION: A function for determining whether or not a lot is a root lot.

    RETURNS: Returns 0 if false, 1 if true. Any other values indicate an error.

    INPUTS: 
    lot_name:
        A string indicating the lot whose rootness is to be determined.

    err_msg:
        A reference to a char array that can store any error messages.
*/


int lotman_get_owners(const char *lot_name, const bool recursive, char ***output, char **err_msg);
/**
    DESCRIPTION: A function for obtaining owners of a lot. Either the immediate owner can be obtained,
        or the owner of the lot in addition to the owners of the lot's parents.

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    lot_name:
        A string indicating the name of the lot whose owners are to be determined.

    recursive:
        A boolean indicating whether only the lot's immediate owner (recursive = false) or all owners
        (recursive = true) should be generated.

    output:
        A reference to a char **, into which the owners of the lot will be placed.
        NOTE: Requires the use of lotman_free_string_list to free the memory allocated for this array.

    err_msg:
        A reference to a char array that can store any error messages.
*/


int lotman_get_parent_names(const char *lot_name, const bool recursive, const bool get_self, char ***output, char **err_msg);
/**
    DESCRIPTION: A function for obtaining the parents of a lot. Either the immediate parents can be 
        obtained, or the parents can be obtained recursively up to the lot's root parent(s).

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    lot_name:
        A string indicating the name of the lot whose parents are to be obtained.

    recursive:
        A boolean indicating whether only the lot's immediate parents (recursive = fale) or all parents
        (recursive = true) should be generated.
    
    get_self:
        A boolean indicating whether self-parenthood should be indicated for the lot being queried against.
        When get_self is set to true, the input lot's own name will be added to the output if it is a self 
        parent. Because the lot data structure is a Directed Acyclic Graph (DAG) with the exception of self 
        parents, this option allows some DAG algorithms to be run on the data structure.

    output:
        A reference to a char **, into which the parents of the lot will be placed.
        NOTE: Requires the use of lotman_free_string_list to free the memory allocated for this array.

    err_msg:
        A reference to a char array that can store any error messages.
*/
int lotman_get_children_names(const char *lot_name, const bool recursive, const bool get_self, char ***output, char **err_msg);
/**
    DESCRIPTION: A function for obtaining the children of a lot. Either immediate children can be
        determined, or children can be generated recursively down to any leaf nodes in the lot
        data structure.

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    lot_name:
        A string indicating the name of the lot whose children are to be obtained.

    recursive:
        A boolean indicating whether only the lot's immediate children (recursive = false) or all
        children (recursive = true) should be generated.

    get_self:
        A boolean indicating whether self-parenthood should be indicated for the lot being queried against.
        When get_self is set to true, the input lot's own name will be added to the output if it is a self 
        parent (because in this case it is also a self child). Because the lot data structure is a 
        Directed Acyclic Graph (DAG) with the exception of self parents, this option allows some DAG 
        algorithms to be run on the data structure.

    output:
        A reference to a char **, into which children of the lot will be placed.
        NOTE: Requires the use of lotman_free_string_list to free the memory allocated for this array.

    err_msg:
        A reference to a char array that can store any error messages.
*/


int lotman_get_policy_attributes(const char *policy_attributes_JSON_str, char **output, char **err_msg);
/**
    DESCRIPTION: A function for determining a lot's current restricting management policy attributes (MPAs).
        For example, if a lot is created with 10GB of dedicated storage, while a parent of the lot only has 
        5GB of dedicated storage, the value 5GB will be returned with an indication of which parent lot 
        this restriction comes from.

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    policy_attributes_JSON_str:
        A string indicating the JSON object for determining which MPAs should be generated and in what
        manner (see specification below).

    output:
        A character array for storing the results of the query, which will be formatted as a JSON (see
        specification below)

    err_msg:
        A reference to a char array that can store any error messages.

    Policy Attributes Input Json:
{   "lot_name":
        REQUIRED: A string indicating which lot to query against.
    "dedicated_GB":
        OPTIONAL: A boolean indicating whether the value should be generated.
    "opportunistic_GB":
        OPTIONAL: A boolean indicating whether the value should be generated.
    "max_num_objects":
        OPTIONAL: A boolean indicating whether the value should be generated.
    "creation_time":
        OPTIONAL: A boolean indicating whether the value should be generated.
    "expiration_time":
        OPTIONAL: A boolean indicating whether the value should be generated.
    "deletion_time":
        OPTIONAL: A boolean indicating whether the value should be generated.
}

    Policy Attributes Output Json:
        For each attribute passed in the input, the output value will be designated
    as an object indicating the restricting lot name and the restricting value. 
    For example, if the input options selected opportunistic_GB and deletion_time,
    the following object would be generated:
{   "opportunistic_GB": {"lot_name": "<restricting_lot>", "value": <value>},
    "deletion_time": {"lot_name", "<restricting_lot>", "value": <value>}
}
*/


int lotman_get_lot_dirs(const char *lot_name, const bool recursive, char **output, char **err_msg);
/**
    DESCRIPTION: A function for getting all paths associated with a given lot. The output can be
        generated to contain only paths tied directly to a lot, or to include the paths tied
        to a lot and all of its children, since these objects may ultimately affect quotas of the
        supplied lot.

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    lot_name:
        A string indicating the name of the lot whose paths/dirs/objects should be returned.

    recursive:
        A boolean indicating whether paths tied to children should also be generated 
        (recursive = true) or not (recursive = false). 

    output:
        A reference to a string array that will contain the generated paths.

    err_msg:
        A reference to a char array that can store any error messages.
*/


int lotman_get_lot_usage(const char *usage_attributes_JSON_str, char **output, char **err_msg);
/**
    DESCRIPTION: A function for determining a lot's usage. Usage queries are capable of 
        tracking objects associated directly with a lot, or objects associated with a 
        lot and its children. Usage stats that can be tracked using this function include
        dedicated/opportunistic storage, total disk usage, object counts, and GB/objects
        currently being written. 

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    usage_attributes_JSON_str:
        A string indicating the JSON object for determining what usage metric to generate,
        as well as how the metric should be generated (ie tracking only the input lot, or
        taking parent/child relationships into account -- see specification below).

    output:
        A reference to a character array used to store the output JSON (see specification
        below).

    err_msg:
        A reference to a char array that can store any error messages.

    Usage Attributes Input Json:
{   "lot_name":
        REQUIRED: A string indicating which lot to obtain usage metrics for.
    "dedicated_GB":
        OPTIONAL: A boolean. If included, the output will contain a value for dedicated GB.
        When the boolean is true, the generated value will count children usage against the lot.
    "opportunistic_GB":
        OPTIONAL: A boolean. If included, the output will contain a value for opportunistic GB.
        When the boolean is true, the generated value will count children usage against the lot.
    "total_GB":
        OPTIONAL: A boolean. If included, the output will contain a value for total disk usage.
        When the boolean is true, the generated value will count children usage against the lot.
    "num_objects":
        OPTIONAL: A boolean. If included, the output will contain a value for total object count.
        When the boolean is true, the generated value will count children usage against the lot.
    "GB_being_written":
        OPTIONAL: A boolean. If included, the output will contain a value for GB being written.
        When the boolean is true, the generated value will count children usage against the lot.
    "objects_being_written"
        OPTIONAL: A boolean. If included, the output will contain a value for objects being written.
        When the boolean is true, the generated value will count children usage against the lot.
}

    Usage Attributes Output Json:
        For each specified usage metric in the input object, the resulting output will contain 
    information indicating the breakdown of how that metric was calculated. For example,
    if the input object specifies opportunistic_GB: true (indicating children usage should be 
    counted in the result) and num_objects: false (indicating children usage should NOT be counted 
    in the result), the resulting output JSON will contain:
{   "opportunistic_GB": {"children_contrib": <value 1>, "self_contrib": <value 2>, "total": <value 1 + value 2>},
    "num_objects": {"total": <value>}
}
*/


int lotman_update_lot_usage(const char *update_JSON_str, bool delta_mode, char **err_msg);
/**
    DESCRIPTION: A function for reporting lot usage metrics to LotMan. The function can either interpret
        the input JSON as an absolute accounting of metrics, or as a delta (if deltaMode is set to true).

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    update_JSON_str:
        A string indicating the JSON object that reports usage statistics to LotMan (see specification below).

    delta_mode:
        A boolean indicating whether the update object should be interpreted as an absolute accounting or as a
        delta.

    err_msg:
        A reference to a char array that can store any error messages.

    Update Object Input Json: 
{   "lot_name":
        REQUIRED: A string indicating which lot these metrics should be assigned to.
    "self_GB":
        OPTIONAL: A number indicating how many GB are being used by the paths associated directly
        with the lot.
        NOTE: This value should not contain storage accounting for any objects of the lot's children, 
        as those metrics are populated internally by LotMan.
    "self_objects":
        OPTIONAL: A number indicating how many objects are being used by the paths associated directly
        with the lot.
        NOTE: This value should not contain storage accounting for any objects of the lot's children, 
        as those metrics are populated internally by LotMan.
    "self_GB_being_written":
        OPTIONAL: A number indicating how many GB are being written to the paths associated directly
        with the lot.
        NOTE: This value should not contain storage accounting for any objects of the lot's children, 
        as those metrics are populated internally by LotMan.
    "self_objects_being_written":
        OPTIONAL: A number indicating how many objects are being written to the paths associated directly
        with the lot.
        NOTE: This value should not contain storage accounting for any objects of the lot's children, 
        as those metrics are populated internally by LotMan.
}
*/


int lotman_update_lot_usage_by_dir(const char *update_JSON_str, bool delta_mode, char **err_msg);
/**
    DESCRIPTION: A function for reporting lot usage metrics to LotMan by directory tree. Unlike 
        lotman_update_lot_usage, this function only takes a JSON-encoded directory tree, with no
        requirement that the user is aware of which directories belong to which lots. The function 
        can either interpret the input JSON as an absolute accounting of metrics, or as a delta 
        (if deltaMode is set to true).

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    update_JSON_str:
        A string indicating the JSON object that reports usage statistics to LotMan (see specification below).

    delta_mode:
        A boolean indicating whether the update object should be interpreted as an absolute accounting or as a
        delta.

    err_msg:
        A reference to a char array that can store any error messages.

    Directory Update Json:
        The directory update object is a set of nested JSON objects that encode information about
    usage metrics that LotMan tracks. For LotMan to calculate correct statistics without the 
    risk of over or undercounting, care should be taken that the objects are constructed correctly. 
    The idea is that each level of the JSON stores information about a path, its size, and object
    count, as well as whether those counts aggregate any statistics from sub directories. For example,
    if the paths '/foo' and '/foo/bar' are both tracked by LotMan and it is reported that '/foo' contains
    10 objects totalling 2GB, an additional object is required so that LotMan can decrement whatever
    is being used by '/foo/bar' from '/foo's statistics. If the additional object reports that '/foo/bar'
    contains 3 objects comprising 0.75GB of space, LotMan will add 7 objects and 1.25GB of usage to '/foo's 
    lot, whereas '/foo/bar's lot will be updated with 3 objects and 0.75GB of usage.
[
    {"path":
        REQUIRED: A string indicating the path to the object relative to the outter update JSON without
        a preceeding/succeeding file separator.
    "size_GB":
        OPTIONAL: A number indicating how many GB are contained within the directory.
    "num_obj":
        OPTIONAL: A number indicating how many objects are contained within the directory.
    "includes_subdirs":
        REQUIRED: A bool indicating whether 'size_GB' or 'num_obj' are calculated using objects in a 
        subdirectory of 'path' that might potentially be tracked by LotMan. 
    "subdirs":
        REQUIRED: An array of Directory Update JSON objects that encode information about the usage of 
        any of 'path's subdirectories. 
    },
    { <potentially more Directory Update JSON objects> }
]
*/


void lotman_free_string_list(char **str_list);
/**
    DESCRIPTION: A function for freeing char ** arrays allocated internally by LotMan. Use on the output
        wherever a LotMan function takes as input a char ***. Failure to do so will result in memory leaks!

    RETURNS: Void

    INPUTS:
    str_list:
        The array to be freed.
*/


int lotman_get_lot_from_dir(const char *dir_name, char **output, char **err_msg);
/**
    DESCRIPTION: A function for determining the lot associated with a path. If the input path
        is included recursively in another tracked path, the lot to which the recursive path is
        assigned will be returned. If the input path is not tracked under any other paths, LotMan
        will respond that the path belongs to the default lot.

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS: 
    dir_name:
        A string indicating the name of the path/dir/object whose lot ownership is to be determined.

    output:
        A reference to a char array that can store the associated lot as output.

    err_msg:
        A reference to a char array that can store any error messages.
*/


int lotman_get_lots_past_exp(const bool recursive, char ***output, char **err_msg);
/**
    DESCRIPTION: A function for determining all lots in the database that are past their expiration.
        The function can determine which lots meet this criteria either by looking at the expiration 
        directly associated with each lot, or recursively by looking at the most restricting
        expiration in each lot's parent tree. 

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    recursive:
        A boolean indicating whether the output should contain only lots whose personal expiration is
        passed (recursive = false) or it should contain all lots who may also have an expired parent
        (recursive = true).

    output:
        A reference to a char ** for storing an array of expired lot names.
        NOTE: Requires the use of lotman_free_string_list to free the memory allocated for this array.

    err_msg:
        A reference to a char array that can store any error messages.
*/


int lotman_get_lots_past_del(const bool recursive, char ***output, char **err_msg);
/**
    DESCRIPTION: A function for determining all lots in the database that are past their deletion time.
        The function can determine which lots meet this criteria either by looking at the deletion time 
        directly associated with each lot, or recursively by looking at the most restricting deletion
        time in each lot's parent tree. 

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    recursive:
        A boolean indicating whether the output should contain only lots whose personal deletion time is
        passed (recursive = false) or it should contain all lots who may also have a parent passed its
        deletion time (recursive = true).

    output:
        A reference to a char ** for storing an array of lots passed their deletion time. 
        NOTE: Requires the use of lotman_free_string_list to free the memory allocated for this array.

    err_msg:
        A reference to a char array that can store any error messages.
*/


int lotman_get_lots_past_opp(const bool recursive_quota, const bool recursive_children, char ***output, char **err_msg);
/**
    DESCRIPTION: A function for determining all lots in the database that are past their opportunistic storage.
        The function can determine which lots meet this criteria either by looking at the opportunistic storage
        directly associated with each lot, or recursively by looking at the most restricting opportunistic
        storage attribute in each lot's parent tree. 

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    recursive_quota:
        A boolean indicating whether quotas should be treated recursively, ie whether the usage of a lot's children
        should be counted against its own usage (recursive_quota = true) or whether only a lot's personal usage
        should be counted (recursive_quota = false). 

    recursive_children:
        A boolean indicating whether the children of an offending lot should also be returned. In some cases, it may
        be useful to also treat these children as offending lots.
        NOTE: The children generated when recursive_children is true will include all recursive children of the lot,
        not just immediate children.

    output:
        A reference to a char ** for storing an array of lots passed their opportunistic storage quota.
        NOTE: Requires the use of lotman_free_string_list to free the memory allocated for this array.

    err_msg:
        A reference to a char array that can store any error messages.
*/


int lotman_get_lots_past_ded(const bool recursive_quota, const bool recursive_children, char ***output, char **err_msg);
/**
    DESCRIPTION: A function for determining all lots in the database that are past their dedicated storage.
        The function can determine which lots meet this criteria either by looking at the dedicated storage
        directly associated with each lot, or recursively by looking at the most restricting dedicated
        storage attribute in each lot's parent tree. 

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    recursive_quota:
        A boolean indicating whether quotas should be treated recursively, ie whether the usage of a lot's children
        should be counted against its own usage (recursive_quota = true) or whether only a lot's personal usage
        should be counted (recursive_quota = false). 

    recursive_children:
        A boolean indicating whether the children of an offending lot should also be returned. In some cases, it may
        be useful to also treat these children as offending lots.
        NOTE: The children generated when recursive_children is true will include all recursive children of the lot,
        not just immediate children.

    output:
        A reference to a char ** for storing an array of lots passed their dedicated storage quota.
        NOTE: Requires the use of lotman_free_string_list to free the memory allocated for this array.

    err_msg:
        A reference to a char array that can store any error messages.
*/


int lotman_get_lots_past_obj(const bool recursive_quota, const bool recursive_children, char ***output, char **err_msg);
/**
    DESCRIPTION: A function for determining all lots in the database that are past their object storage quota.
        The function can determine which lots meet this criteria either by looking at the object storage quota
        directly associated with each lot, or recursively by looking at the most restricting object
        storage attribute in each lot's parent tree. 

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    recursive_quota:
        A boolean indicating whether quotas should be treated recursively, ie whether the usage of a lot's children
        should be counted against its own usage (recursive_quota = true) or whether only a lot's personal usage
        should be counted (recursive_quota = false). 

    recursive_children:
        A boolean indicating whether the children of an offending lot should also be returned. In some cases, it may
        be useful to also treat these children as offending lots.
        NOTE: The children generated when recursive_children is true will include all recursive children of the lot,
        not just immediate children.

    output:
        A reference to a char ** for storing an array of lots passed their opportunistic storage quota.
        NOTE: Requires the use of lotman_free_string_list to free the memory allocated for this array.

    err_msg:
        A reference to a char array that can store any error messages. 
*/


int lotman_list_all_lots(char ***output, char **err_msg);
/**
    DESCRIPTION: A function for listing all lots in the LotMan database. 

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    output:
        A reference to a char ** for storing the output.
        NOTE: Requires the use of lotman_free_string_list to free the memory allocated for this array.

    err_msg:
        A reference to a char array that can store any error messages.
*/


int lotman_get_lot_as_json(const char *lot_name, const bool recursive, char **output, char **err_msg);
/**
    DESCRIPTION: A function for getting information about a single lot in JSON format. The function
        can either generate information about the input lot only, or it can get the most restricting
        attributes of the lot based on parent/child relationships.

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    lot_name:
        A string indicating which lot should be queried for.

    recursive:
        A bool indicating whether the reported attributes should come only from what is stored in the 
        database about the lot itself (recursive = false), or whether LotMan should calculate the 
        restricting attributes for the lot as determined by its parent/child relationships 
        (recursive = false).

    output:
        A reference to a char array that can store the encoded JSON string for the lot (see sepcification
        below).

    err_msg:
        A reference to a char array that can store any error messages.
    
    Output JSON Specification:
{   "lot_name":
        A string indicating the lot's name.
    "owners":
        An array of strings indicating the lot's owner(s). If recursive if false, only the lot's direct
        owner will be returned, otherwise all parent owners will be included.
    "parents":
        An array of strings indicating the lot's parents.
    "children":
        An array of strings indicating the lot's children.
    "paths":
        An array of JSON objects encoding information about each path associated with the lot. When recursive is
        true, the object will have the following format:
        {"/a/path": {"lot_name": <name of lot that tracks the path>, "recursive": <bool indicating whether path is recursive>}}
        If recursive is false, the object will omit the "lot_name", as it is assumed to be from the supplied input lot.
    "management_policy_attrs":
        A JSON object containing information about the lot's MPAs, as determined by accounting for the 'recursive'
        flag. When recursive is true, each MPA will contain a JSON object with the following format:
        {"lot_name": "<name of restricting lot>", "value": <restricting value>}
    "usage":
        A JSON object containing each of the lot's usage metrics, as determined by accounting for the 'recursive'
        flag. When recursive is true, each usage metric will contain a JSON object with the following format:
        {"children_contrib": <value>,"self_contrib": <value>}
}
*/


int lotman_get_lots_from_dir(const char *dir, const bool recursive, char ***output, char **err_msg);
/**
    DESCRIPTION: A function for getting any lots associated with the supplied path. The function can
        return either solely the lot tracking the path, or any parent lots who might also be affected
        by the path.

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    dir:
        A string indicating the path to be queried for

    recursive:
        A boolean indicating whether only the lot tracking the path should be returned (recursive = false),
        or whether all parent lots should also be returned (recursive = true).

    output:
        A reference to a char ** for storing the output array.
        NOTE: Requires the use of lotman_free_string_list to free the memory allocated for this array.

    err_msg:
        A reference to a char array that can store any error messages.
*/


int lotman_set_context_str(const char *key, const char *value, char **err_msg);
/**
    DESCRIPTION: Provides access to setting various configuration/context values in LotMan

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    key:
        A string indicating which context key is being set. Currently, possible context keys are the "caller"
        (ie identity of who's calling a LotMan function, needed in some cases when determining whether a
        particular call should be allowed) and "lot_home", which is used for setting the location of the 
        generated LotMan SQLite database.

    value:
        The intended value to be assumed by whichever key is provided

    err_msg:
        A reference to a char array that can store any error messages.
*/

int lotman_get_context_str(const char *key, char **output, char **err_msg);
/**
    DESCRIPTION: Provides access for getting various configuration/context values in LotMan

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    key:
        A string indicating which context key is being set. Currently, possible context keys are the "caller"
        (ie identity of who's calling a LotMan function, needed in some cases when determining whether a
        particular call should be allowed) and "lot_home", which is used for setting the location of the
        generated LotMan SQLite database.

    output:
        A buffer for storing the output from the operation

    err_msg:
        A reference to a char array that can store any error messages.
*/

int lotman_set_context_int(const char *key, const int value, char **err_msg);
/**
    DESCRIPTION: Provides access for setting various configuration/context values in LotMan

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    key:
        A string indicating which context key is being set.

    value:
        The intended value to be assumed by whichever key is provided

    err_msg:
        A reference to a char array that can store any error messages.
*/

int lotman_get_context_int(const char *key, int *output, char **err_msg);
/**
    DESCRIPTION: Provides access for getting various configuration/context values in LotMan

    RETURNS: Returns 0 on success. Any other values indicate an error.

    INPUTS:
    key:
        A string indicating which context key is being set. Currently valid is "db_timeout", a
        max value in milliseconds that the database should wait when trying to establish a lock
        on the database. Can be tuned in multiprocess environments to eliminate any potential
        sqlite error no. 5 complaints (SQLITE_BUSY)

    output:
        A buffer for storing the output from the operation

    err_msg:
        A reference to a char array that can store any error messages.
*/

//int lotman_get_matching_lots(const char *criteria_JSON, char ***output, char **err_msg);
//int lotman_check_db_health(char **err_msg); // Should eventually check that data structure conforms to expectations. If there's a cycle or a non-self-parent root, something is wrong

#ifdef __cplusplus
}
#endif
