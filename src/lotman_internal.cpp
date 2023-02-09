#include <stdio.h>
#include <iostream>
#include <picojson.h>
#include <sqlite3.h>
//#include <vector>

#include "lotman_internal.h"

using namespace lotman;


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
                          picojson::array paths, 
                          picojson::value management_policy_attrs) {

    // TODO: Error handling
    // TODO: Check if LTBA is a root, and if yes, ensure it has a well-defined set of policies, OR decide to grab some stuff from default lot
    // TODO: Update children when not an insertion
    // TODO: Check if LTBA exists. If yes: fail

    if (lot_exists(lot_name)) {
        return false;
    }

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

    int store_lot_status = store_lot(lot_name, owners, parents, children, paths, management_policy_attrs);

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
                    //parent_updated = lotman::Validator::SQL_update_parent(children_iter, parents_iter, lot_name);
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
                             bool assign_default_as_parent_to_orphans, 
                             bool assign_default_as_parent_to_non_orphans, 
                             bool assign_LTBR_as_parent_to_orphans, 
                             bool assign_LTBR_as_parent_to_non_orphans, 
                             bool assign_policy_to_children) {

    // TODO: Handle all the weird things that can happen when removing a lot.
    // TODO: Finish update_lot functions to perform updates to children. Right now, returns without error
    
    // Check that lot exists. Can't remove what isn't there
    if (!lot_exists(lot_name)) {
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


    return delete_lot(lot_name);

}

bool lotman::Lot::update_lot(std::string lot_name, 
                std::map<std::string, std::string> owners_map, 
                std::map<std::string, std::string> parents_map, 
                std::map<std::string, int> paths_map, 
                std::map<std::string, int> management_policy_attrs_int_map, 
                std::map<std::string, double> management_policy_attrs_double_map) {
    
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
            std::cout << "The key: " << key.first << " is not a valid key" << std::endl;
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
            std::cout << "The key: " << key.first << " is not a valid key" << std::endl;
            return false;
        }
    }

    return true;
}

std::vector<std::string> lotman::Lot::get_parent_names(std::string lot_name, bool get_root) {
    if (get_root) {
        std::cout << "TODO: Build get_root recursive mode for parents query" << std::endl;
        return std::vector<std::string>();
    }

    std::string parents_query = "SELECT parent FROM parents WHERE lot_name = ? AND parent != ?;"; 
    std::map<std::string, std::vector<int>> parents_query_str_map{{lot_name, {1,2}}};
    std::vector<std::string> lot_parents_vec = lotman::Validator::SQL_get_matches(parents_query, parents_query_str_map);

    return lot_parents_vec;
}

// std::vector<std::string> lotman::Lot::get_sublot_paths(std::string lot_path, bool recursive) {
//     std::vector<std::string> sublot_paths = lotman::Validator::get_child_dirs(lot_path, recursive);
//     return sublot_paths;
// }



/**
 * Functions specific to Validator class
*/

bool lotman::Validator::cycle_check(std::string start_node, std::vector<std::string> start_parents, std::vector<std::string> start_children) { // Returns true if invalid cycle is detected, else returns false
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

bool lotman::Validator::insertion_check(std::string LTBA, std::string parent, std::string child) { // Checks whether LTBA is being inserted between a parent and child.
    std::vector<std::string> parents_vec = lotman::Lot::get_parent_names(child);
    auto parent_iter = std::find(parents_vec.begin(), parents_vec.end(), parent); // Check if the specified parent is listed as a parent to the child
    if (!(parent_iter == parents_vec.end())) { 
        // Child has "parent" as a parent
        return true;
    }
    return false;
}



// bool lotman::Validator::check_for_parent_child_dirs(std::string lot_path) {
//     std::string is_parent_dir_query = "SELECT path FROM lot WHERE path LIKE '" + lot_path + "%';";
//     std::string is_child_dir_query = "SELECT path FROM lot WHERE path=SUBSTRING('" + lot_path + "',1,LENGTH(PATH));";

//     int total_matches = SQL_match_exists(is_parent_dir_query) + SQL_match_exists(is_child_dir_query);
//     if (total_matches > 0) {return false;}

//     return true;
// }


// std::vector<std::string> lotman::Validator::get_parent_names(std::string parent, bool get_root) {
//     std::string parent_query;

//     if (get_root) {
//         // TODO: Finish this part
//         std::cout << "You haven't written the code to get the root parent name!!" << std::endl;
//     }
//     else {
//         parent_query = "SELECT lot_name FROM lot WHERE lot_name=\"" + parent + "\";";

//     }

//     std::vector<std::string> parent_path_vec = lotman::Validator::SQL_get_matches(parent_query);
//     return parent_path_vec;
// }


// std::vector<std::string> lotman::Validator::get_child_dirs(std::string lot_path, bool recursive) {
//     std::string children_query;
//     if (recursive) {
//         // TODO: Set up this recursive mode
//         std::cout << "YOU HAVEN'T BUILT RECURSIVE MODE YET" << std::endl;
//     }
//     else {
//         children_query = "SELECT path FROM lot WHERE path LIKE '"+ lot_path + "/%';";
//     }

//     std::vector<std::string> child_paths = lotman::Validator::SQL_get_matches(children_query);
//     return child_paths;
// }

    
