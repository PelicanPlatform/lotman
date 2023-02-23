#include <algorithm>
#include <stdio.h>
#include <iostream>
#include <picojson.h>
#include <sqlite3.h>
//#include <string>
//#include <vector>

#include "lotman_internal.h"

using namespace lotman;

// TODO: Go through and make things const where they shoudl be declared as such
// TODO: Go through and handle any cases where lot_exists should be checked!
/**
 * Functions specific to Lot class
*/
bool lotman::Lot::lot_exists(std::string lot_name) {
    std::string lot_exists_query = "SELECT lot_name FROM management_policy_attributes WHERE lot_name = ?;";
    std::map<std::string, std::vector<int>> lot_exists_str_map{{lot_name, {1}}};

    return (lotman::Validator::SQL_get_matches(lot_exists_query, lot_exists_str_map).size()>0);
}

bool lotman::Lot::is_root(std::string lot_name) {
    std::string is_root_query = "SELECT parent FROM parents WHERE lot_name = ?;"; 
    std::map<std::string, std::vector<int>> is_root_query_str_map{{lot_name, {1}}};
    std::vector<std::string> parents_vec = lotman::Validator::SQL_get_matches(is_root_query, is_root_query_str_map);
    if (parents_vec.size()==1 && parents_vec[0]==lot_name) {
        // lot_name has only itself as a parent
        return true;
    }

    return false;
}

bool lotman::Lot::add_lot(std::string lot_name, 
                          std::vector<std::string> owners, 
                          std::vector<std::string> parents, 
                          std::vector<std::string> children, 
                          std::map<std::string, int> paths_map, 
                          std::map<std::string, int> management_policy_attrs_int_map,
                          std::map<std::string, double> management_policy_attrs_double_map) {

    // TODO: Error handling
    // TODO: Check if LTBA is a root, and if yes, ensure it has a well-defined set of policies, OR decide to grab some stuff from default lot
    // TODO: Update children when not an insertion
    // TODO: Handle setup of default lot. Must be self parent, must have well-def'd policy, must be a root, etc. 
    // TODO: If a lot is self-parent, it must have a well-def'd policy
    // TODO: Should we allow attaching a path to more than one lot? If not, we need a check.
    
    // Gaurantee that the default lot is the first lot created
    if (lot_name != "default" && !lot_exists("default")) {
        std::cout << "The default lot, named \"default\" must be established first." << std::endl;
        return false;
    }

    if (lot_exists(lot_name)) {
        return false;
    }

    // Check if the lot is a self parent, and if so, require it has a well-def'd policy
    //if (std::find)

    // Check that any specified parents already exist, unless the lot has itself as parent
    for (auto & parents_iter : parents) {
        if (parents_iter != lot_name && !lot_exists(parents_iter)) {
            return false;
        }
    }

    // Check that any specified children already exist
    if (children.size()>0) {
        for (auto & children_iter : children) {
            if (!lot_exists(children_iter)) {
                return false;
            }
        }
    }

    // Check that the added lot won't introduce any cycles
    bool self_parent; // When checking for cycles, we only care about lots who specify a parent other than themselves
    auto self_parent_iter = std::find(parents.begin(), parents.end(), lot_name);  
    self_parent = (self_parent_iter != parents.end());
    if (!children.empty() &&  ((parents.size()==1 && !self_parent) || (parents.size()>1))) { // If there are children and a non-self parent
        bool cycle_exists = lotman::Validator::cycle_check(lot_name, parents, children);
        if (cycle_exists) {
            return false; // Return false, don't do anything with the lot
            // TODO: pass error messages around to tell user why the call is failing
        }
    
    }

    bool store_lot_status = store_lot(lot_name, owners, parents, paths_map, management_policy_attrs_int_map, management_policy_attrs_double_map);

    // If the lot is stored successfully, it's safe to start updating other lots to have correct info.
    if (store_lot_status) {
        bool parent_updated = true;
        for (auto & parents_iter : parents) {
            for (auto & children_iter : children) {
                if (lotman::Validator::insertion_check(lot_name, parents_iter, children_iter)) {
                    // Update child to have lot_name as a parent instead of parents_iter. Later, save LTBA with all its specified parents.
                    std::string parent_update_dynamic_query = "UPDATE parents SET parent=(?) WHERE lot_name=(?) AND parent=(?);";
                    std::map<std::string, std::vector<int>> parent_update_query_str_map{{children_iter, {2}}, {parents_iter, {3}}, {lot_name, {1}}};
                    parent_updated = store_modifications(parent_update_dynamic_query, parent_update_query_str_map);
                    if (!parent_updated) { // Something went wrong, abort
                        //remove_lot(lot_name);
                        return false;
                    }
                }
            }
        }
    }
    return store_lot_status;
}

bool lotman::Lot::remove_lot(std::string lot_name, 
                             bool assign_LTBR_parent_as_parent_to_orphans, 
                             bool assign_LTBR_parent_as_parent_to_non_orphans, 
                             bool assign_policy_to_children) {

    // TODO: Handle all the weird things that can happen when removing a lot.
    // TODO: Finish update_lot functions to perform updates to children. Right now, returns without error
    // TODO: Consider order of operations and/or thread safety for this. Currently, remove_lot does any updating to children before it deletes the lot,
    //       and in the event that the children are updated but the lot cannot be deleted, the data's structure may become corrupt and unrecoverable.
    //       My current thought is that the function could write a temporary recovery file to disk that backs up any data entries that are to be modified,
    //       and then delete it after everythind finishes successfully. In the event of a crash, a special lotman_recover_db function could check for recovery
    //       files, whose existence indicates there was some kind of crash, and then perform the restoration based on what is in the temp file.
    
    // Check that lot exists. Can't remove what isn't there
    if (!lot_exists(lot_name)) {
        std::cout << "Lot does not exist, and cannot be removed" << std::endl;
        return false;
    }
    if (lot_name == "default") {
        std::cout << "The default lot cannot be deleted" << std::endl;
        return false;
    }

    // If lot has no children, it can be removed easily. If there are children, added care must be taken to prevent orphaning them.
    std::vector<std::string> lot_children;
    std::string children_query = "SELECT lot_name FROM parents WHERE parent = ?;";
    std::map<std::string, std::vector<int>> children_query_str_map{{lot_name, {1}}};
    lot_children = lotman::Validator::SQL_get_matches(children_query, children_query_str_map); // get all lots who specify lot-to-be-removed (LTBR) as a parent.
                
    for (auto & child : lot_children) {
        std::cout << "In remove lot, here's a child: " << child << std::endl;
    }

    if (lot_children.size()==0) {
        // No children to orphan, safe to delete lot
        return delete_lot(lot_name);
    }

    for (auto & child : lot_children) {
        if (lotman::Validator::will_be_orphaned(lot_name, child)) {
            if (assign_LTBR_parent_as_parent_to_orphans) {
                if (is_root(lot_name)) {
                    std::cout << "The lot being removed is a root, and has no parents to assign to its children." << std::endl;
                    return false;
                }
                std::vector<std::string> LTBR_owners_vec = lotman::Lot::get_owners(lot_name);
                std::vector<std::string> LTBR_parents_vec = lotman::Lot::get_parent_names(lot_name);
                add_to_lot(child, LTBR_owners_vec, LTBR_parents_vec);
            }
            else {
                std::cout << "The operation cannot be completed beacuse deleting the lot would create an orphan that requires explicit assignment to the default lot. Set assign_LTBR_parent_as_parent_to_orphans=true" << std::endl;
            }
        }
        else {
            if (assign_LTBR_parent_as_parent_to_non_orphans) {
                if (is_root(lot_name)) {
                    std::cout << "The lot being removed is a root, and has no parents to assign to its children." << std::endl;
                    return false;
                }
                std::vector<std::string> LTBR_owners_vec = lotman::Lot::get_owners(lot_name);
                std::vector<std::string> LTBR_parents_vec = lotman::Lot::get_parent_names(lot_name);
                add_to_lot(child, LTBR_owners_vec, LTBR_parents_vec);
            }

        }
    }

    return delete_lot(lot_name);

}

bool lotman::Lot::update_lot(std::string lot_name, 
                std::map<std::string, std::string> owners_map, 
                std::map<std::string, std::string> parents_map, 
                std::map<std::string, int> paths_map, 
                std::map<std::string, int> management_policy_attrs_int_map, 
                std::map<std::string, double> management_policy_attrs_double_map) {
    if (!lot_exists(lot_name)) {
        std::cout << "Lot does not exist so it cannot be updated" << std::endl;
        return false;
    }

    // For each change in each map, store the change.
    std::string owners_update_dynamic_query = "UPDATE owners SET owner=? WHERE lot_name=? AND owner=?;";
    for (auto & key : owners_map) {
        std::map<std::string, std::vector<int>> owners_update_str_map{{key.second, {1}}, {lot_name, {2}}, {key.first, {3}}};
        bool rv = lotman::Lot::store_modifications(owners_update_dynamic_query, owners_update_str_map);
        if (!rv) {
            std::cout << "call to lotman::Lot::store_modifications unsuccessful when updating an owner." << std::endl;
            return false;
        }
    }

    std::string parents_update_dynamic_query = "UPDATE parents SET parent=? WHERE lot_name=? AND parent=?";
    for (auto & key : parents_map) {
        std::map<std::string, std::vector<int>> parents_update_str_map;
        bool rv;
        if (key.first == lot_name) {
            std::map<std::string, std::vector<int>> parents_update_str_map{{key.second, {1}}, {lot_name, {2,3}}};
            rv = lotman::Lot::store_modifications(parents_update_dynamic_query, parents_update_str_map);
        }
        else {
            std::map<std::string, std::vector<int>> parents_update_str_map{{key.second, {1}}, {lot_name, {2}}, {key.first, {3}}};
            rv = lotman::Lot::store_modifications(parents_update_dynamic_query, parents_update_str_map);
        }
        if (!rv) {
            std::cout << "Call to lotman::Lot::store_modifications unsuccessful when updating a parent" << std::endl;
            return false;
        }
    }

    std::string paths_update_dynamic_query = "UPDATE paths SET recursive=? WHERE lot_name=? and path=?;";
    for (auto & key : paths_map) {
        std::map<std::string, std::vector<int>> paths_update_str_map;
        std::map<int, std::vector<int>> paths_update_int_map{{key.second, {1}}};
        bool rv;
        if (key.first == lot_name) {
            std::map<std::string, std::vector<int>> paths_update_str_map{{lot_name, {2,3}}};
            rv = lotman::Lot::store_modifications(paths_update_dynamic_query, paths_update_str_map, paths_update_int_map);
        }
        else {
            std::map<std::string, std::vector<int>> paths_update_str_map{{lot_name, {2}}, {key.first, {3}}};
            rv = lotman::Lot::store_modifications(paths_update_dynamic_query, paths_update_str_map, paths_update_int_map);
        }
        if (!rv) {
            std::cout << "Call to lotman::Lot::store_modifications unsucessful when updating a path" << key.first << " key.second: " << key.second << std::endl;
            return false;
        }
    }

    std::string management_policy_attrs_dynamic_query_first_half = "UPDATE management_policy_attributes SET ";
    std::string management_policy_attrs_dynamic_query_second_half = "=? WHERE lot_name=?;";
    for (auto & key : management_policy_attrs_int_map) {
        std::map<std::string, std::vector<int>> management_policy_attrs_update_str_map{{lot_name, {2}}};
        std::map<int, std::vector<int>> management_policy_attrs_update_int_map{{key.second, {1}}};
        std::array<std::string, 4> allowed_keys{{"max_num_objects", "creation_time", "expiration_time", "deletion_time"}};
        if (std::find(allowed_keys.begin(), allowed_keys.end(), key.first) != allowed_keys.end()) {
            std::string management_policy_attrs_dynamic_query = management_policy_attrs_dynamic_query_first_half + key.first + management_policy_attrs_dynamic_query_second_half;
            bool rv = lotman::Lot::store_modifications(management_policy_attrs_dynamic_query, management_policy_attrs_update_str_map, management_policy_attrs_update_int_map);
            if (!rv) {
                std::cout << "Call to lotman::Lot::store_modifications unsuccessful when updating management policy attribute" << std::endl;
                return false;
            }
        }
        else {
            std::cout << "The key: " << key.first << " is not a recognized key" << std::endl;
            return false;
        }
    }

    for (auto & key : management_policy_attrs_double_map) {
        std::map<std::string, std::vector<int>> management_policy_attrs_update_str_map{{lot_name, {2}}};
        std::map<double, std::vector<int>> management_policy_attrs_update_double_map{{key.second, {1}}};
        std::array<std::string, 2> allowed_keys{{"dedicated_GB", "opportunistic_GB"}};
        if (std::find(allowed_keys.begin(), allowed_keys.end(), key.first) != allowed_keys.end()) {
            std::string management_policy_attrs_dynamic_query = management_policy_attrs_dynamic_query_first_half + key.first + management_policy_attrs_dynamic_query_second_half;
            
            bool rv = lotman::Lot::store_modifications(management_policy_attrs_dynamic_query, management_policy_attrs_update_str_map, std::map<int, std::vector<int>>(), management_policy_attrs_update_double_map);
            if (!rv) {
                std::cout << "Call to lotman::Lot::store_modifications unsuccessful when updating management policy attribute" << std::endl;
                return false;
            }
        }
        else {
            std::cout << "The key: " << key.first << " is not a recognized key" << std::endl;
            return false;
        }
    }

    return true;
}

bool lotman::Lot::add_to_lot(std::string lot_name,
                             std::vector<std::string> owners,
                             std::vector<std::string> parents,
                            std::map<std::string, int> paths_map) {
    if (!lot_exists(lot_name)) {
        std::cout << "Lot does not exist so it cannot be updated" << std::endl;
        return false;
    }
    if (!store_new_rows(lot_name, owners, parents, paths_map)) {
        return false;
    }
    return true;
}

std::vector<std::string> lotman::Lot::get_parent_names(std::string lot_name,
                                                       bool recursive,
                                                       bool get_self) {
    
    
    std::vector<std::string> lot_parents_vec;
    std::string parents_query;
    std::map<std::string, std::vector<int>> parents_query_str_map;
    if (get_self) {
        parents_query = "SELECT parent FROM parents WHERE lot_name = ?;"; 
        parents_query_str_map = {{lot_name, {1}}};
    }
    else {
        parents_query = "SELECT parent FROM parents WHERE lot_name = ? AND parent != ?;"; 
        parents_query_str_map = {{lot_name, {1,2}}};
    }
    lot_parents_vec = lotman::Validator::SQL_get_matches(parents_query, parents_query_str_map);
    
    if (recursive) {
        std::vector<std::string> current_parents{lot_parents_vec};
        parents_query = "SELECT parent FROM parents WHERE lot_name = ? AND parent != ?;"; 
        while (current_parents.size()>0) {
            std::vector<std::string> grandparents;
            for (const auto &parent : current_parents) {
                parents_query_str_map = {{parent, {1,2}}};
                std::vector<std::string> tmp{lotman::Validator::SQL_get_matches(parents_query, parents_query_str_map)};
                grandparents.insert(grandparents.end(), tmp.begin(), tmp.end());
            }
            current_parents = grandparents;
            lot_parents_vec.insert(lot_parents_vec.end(), grandparents.begin(), grandparents.end());
        }
    }

    // Sort and remove any duplicates
    std::sort(lot_parents_vec.begin(), lot_parents_vec.end());
    auto last = std::unique(lot_parents_vec.begin(), lot_parents_vec.end());
    lot_parents_vec.erase(last, lot_parents_vec.end());
    return lot_parents_vec;
}

std::vector<std::string> lotman::Lot::get_owners(std::string lot_name,
                                                 bool recursive) {
    std::string owners_query = "SELECT owner FROM owners WHERE lot_name = ?;"; 

    std::map<std::string, std::vector<int>> owners_query_str_map{{lot_name, {1}}};
    std::vector<std::string> lot_owners_vec = lotman::Validator::SQL_get_matches(owners_query, owners_query_str_map);

    if (recursive) {
        std::vector<std::string> parents_vec{get_parent_names(lot_name, true, false)};
        for (const auto &parent : parents_vec) {
            std::map<std::string, std::vector<int>> owners_query_str_map{{parent, {1}}};
            std::vector<std::string> tmp{lotman::Validator::SQL_get_matches(owners_query, owners_query_str_map)};
            lot_owners_vec.insert(lot_owners_vec.end(), tmp.begin(), tmp.end());
        }

    }

    // Sort and remove any duplicates
    std::sort(lot_owners_vec.begin(), lot_owners_vec.end());
    auto last = std::unique(lot_owners_vec.begin(), lot_owners_vec.end());
    lot_owners_vec.erase(last, lot_owners_vec.end());
    return lot_owners_vec;
}

std::vector<std::string> lotman::Lot::get_children_names(std::string lot_name,
                                                         const bool recursive,
                                                         const bool get_self) {
    std::vector<std::string> lot_children_vec;
    std::string children_query;
    std::map<std::string, std::vector<int>> children_query_str_map;
    if (get_self) {
        children_query = "SELECT lot_name FROM parents WHERE parent = ?;"; 
        children_query_str_map = {{lot_name, {1}}};
    }
    else {
        children_query = "SELECT lot_name FROM parents WHERE parent = ? and lot_name != ?;"; 
        children_query_str_map = {{lot_name, {1,2}}};
    }
    lot_children_vec = lotman::Validator::SQL_get_matches(children_query, children_query_str_map);

    if (recursive) {
        std::vector<std::string> current_children{lot_children_vec};
        children_query = "SELECT lot_name FROM parents WHERE parent = ? AND lot_name != ?;"; 
        while (current_children.size()>0) {
            std::vector<std::string> grandchildren;
            for (const auto &child : current_children) {
                children_query_str_map = {{child, {1,2}}};
                std::vector<std::string> tmp{lotman::Validator::SQL_get_matches(children_query, children_query_str_map)};
                grandchildren.insert(grandchildren.end(), tmp.begin(), tmp.end());
            }
            current_children = grandchildren;
            lot_children_vec.insert(lot_children_vec.end(), grandchildren.begin(), grandchildren.end());
        }
    }

    // Sort and remove any duplicates
    std::sort(lot_children_vec.begin(), lot_children_vec.end());
    auto last = std::unique(lot_children_vec.begin(), lot_children_vec.end());
    lot_children_vec.erase(last, lot_children_vec.end());
    return lot_children_vec;
}                                              

picojson::object lotman::Lot::get_restricting_attribute(const std::string lot_name,
                                                        const std::string key,
                                                        const bool recursive) {
    picojson::object internal_obj;
    std::vector<std::string> value;
    std::array<std::string, 6> allowed_keys{{"dedicated_GB", "opportunistic_GB", "max_num_objects", "creation_time", "expiration_time", "deletion_time"}};
    if (std::find(allowed_keys.begin(), allowed_keys.end(), key) != allowed_keys.end()) {
        std::string policy_attribute_dynamic_query = "SELECT " + key + " FROM management_policy_attributes WHERE lot_name = ?;";
        std::map<std::string, std::vector<int>> management_policy_attrs_dynamic_query_str_map{{lot_name, {1}}};
        value = lotman::Validator::SQL_get_matches(policy_attribute_dynamic_query, management_policy_attrs_dynamic_query_str_map);
        std::string restricting_parent_name = lot_name;
        if (recursive) {
            std::vector<std::string> parents_vec = get_parent_names(lot_name, true);
            for (const auto &parent : parents_vec) {
                std::map<std::string, std::vector<int>> management_policy_attrs_dynamic_query_parent_str_map{{parent, {1}}};
                std::vector<std::string> compare_value = lotman::Validator::SQL_get_matches(policy_attribute_dynamic_query, management_policy_attrs_dynamic_query_parent_str_map);
                if (std::stod(compare_value[0]) < std::stod(value[0])) {
                    value[0] = compare_value[0];
                    restricting_parent_name = parent;
                }
            }
            
        }
        internal_obj["lot_name"] = picojson::value(restricting_parent_name);
        internal_obj["value"] = picojson::value(value[0]);
    }
    else {
        std::cout << "The key: " << key << " is not a recognized key." << std::endl;
    }
    return internal_obj;
}

picojson::object lotman::Lot::get_lot_dirs(const std::string lot_name,
                                           const bool recursive) {
    /**
     * TODO: Currently the SQL_get_matches function can only grab one column at a time, so any time multiple columns
     * are needed (such as in this case, where both the path and the recursion are sought), multiple calls to the
     * function must be made. This reduces efficiency because the lot DB file must be opened and operated on for each 
     * call to SQL_get_matches. To increase efficiency, SQL_get_matches should be reworked to be made aware of how many
     * columns it should expect to get so that it can return each of those columns in a sensible format on the first call.
     */
    
    if (!lot_exists(lot_name))

    picojson::object path_obj;
    std::string lot_dirs_dynamic_path_query = "SELECT path FROM paths WHERE lot_name = ?;"; 
    std::string lot_dirs_dynamic_recursive_query = "SELECT recursive FROM paths WHERE path = ? AND lot_name = ?;";
    std::map<std::string, std::vector<int>> lot_dirs_dynamic_path_query_str_map{{lot_name, {1}}};
    std::vector<std::string> path_output_vec = lotman::Validator::SQL_get_matches(lot_dirs_dynamic_path_query, lot_dirs_dynamic_path_query_str_map);

    
    for (const auto &path : path_output_vec) {
        picojson::object path_obj_internal;
        std::map<std::string, std::vector<int>> lot_dirs_dynamic_query_str_map{{path, {1}}, {lot_name, {2}}};
        std::vector<std::string> recursive_output = lotman::Validator::SQL_get_matches(lot_dirs_dynamic_recursive_query, lot_dirs_dynamic_query_str_map);
        path_obj_internal["lot_name"] = picojson::value(lot_name);
        path_obj_internal["recursive"] = picojson::value(recursive_output[0]);
        path_obj[path] = picojson::value(path_obj_internal);

    }

    if (recursive) {
        std::vector<std::string> children_vec = get_children_names(lot_name, true);
        for (const auto &child : children_vec) {
            std::map<std::string, std::vector<int>> child_dirs_dynamic_path_query_str_map{{child, {1}}};
            std::vector<std::string> child_path_output_vec = lotman::Validator::SQL_get_matches(lot_dirs_dynamic_path_query, child_dirs_dynamic_path_query_str_map);

            for (const auto &path : child_path_output_vec) {
                picojson::object path_obj_internal;
                std::map<std::string, std::vector<int>> child_dirs_dynamic_query_str_map{{path, {1}}, {child, {2}}};
                std::vector<std::string> child_recursive_output = lotman::Validator::SQL_get_matches(lot_dirs_dynamic_recursive_query, child_dirs_dynamic_query_str_map);
                path_obj_internal["lot_name"] = picojson::value(child);
                path_obj_internal["recursive"] = picojson::value(child_recursive_output[0]);
                path_obj[path] = picojson::value(path_obj_internal);
            }
        }
    }

    
    return path_obj;
                                           }

/**
 * Functions specific to Validator class
*/

bool lotman::Validator::cycle_check(std::string start_node,
                                    std::vector<std::string> start_parents, 
                                    std::vector<std::string> start_children) { // Returns true if invalid cycle is detected, else returns false
    // Basic DFS algorithm to check for cycle creation when adding a lot that has both parents and children.

    // Algorithm initialization
    std::vector<std::string> dfs_nodes_to_visit;
    dfs_nodes_to_visit.insert(dfs_nodes_to_visit.end(), start_parents.begin(), start_parents.end());
    for (auto & children_iter : start_children) {
        auto check_iter = std::find(dfs_nodes_to_visit.begin(), dfs_nodes_to_visit.end(), children_iter);
        if (check_iter != dfs_nodes_to_visit.end()) {
            // Run, Luke! It's a trap! Erm... a cycle!
            return true;
        }
    }

    // Iterate
    while (dfs_nodes_to_visit.size()>0) { // When dfs_nodes_to_visit has size 0, we're done checking
        std::vector<std::string> dfs_node_parents = lotman::Lot::get_parent_names(dfs_nodes_to_visit[0]);
        dfs_nodes_to_visit.insert(dfs_nodes_to_visit.end(), dfs_node_parents.begin(), dfs_node_parents.end());

        for (auto & children_iter : start_children) { // For each specified child of start node ...
            auto check_iter = std::find(dfs_nodes_to_visit.begin(), dfs_nodes_to_visit.end(), children_iter); // ... check if that child is going to be visited ...
            if (check_iter != dfs_nodes_to_visit.end()) {
                // ... If it is going to be visited, cycle found!
                return true;
            }
        }
        dfs_nodes_to_visit.erase(dfs_nodes_to_visit.begin()); // Nothing consequential from this node, remove it from the nodes to visit
    }

    return false;
}

bool lotman::Validator::insertion_check(std::string LTBA, 
                                        std::string parent, 
                                        std::string child) { // Checks whether LTBA is being inserted between a parent and child.
    std::vector<std::string> parents_vec = lotman::Lot::get_parent_names(child);
    auto parent_iter = std::find(parents_vec.begin(), parents_vec.end(), parent); // Check if the specified parent is listed as a parent to the child
    if (!(parent_iter == parents_vec.end())) { 
        // Child has "parent" as a parent
        return true;
    }
    return false;
}

bool lotman::Validator::will_be_orphaned(std::string LTBR, 
                                         std::string child) {
    std::vector<std::string> parents_vec = lotman::Lot::get_parent_names(child);
    if (parents_vec.size()==1 && parents_vec[0]==LTBR) {
        return true;
    }
    return false;
}

    
