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

    //std::string lot_exists_query = "SELECT lot_name FROM management_policy_attributes WHERE lot_name='" + lot_name + "';"; // TODO: reformat to prevent injection
    std::string lot_exists_query = "SELECT lot_name FROM management_policy_attributes WHERE lot_name = ?;";
    std::map<std::string, std::vector<int>> lot_exists_str_map{{lot_name, {1}}};

    return (lotman::Validator::SQL_get_matches(lot_exists_query, lot_exists_str_map).size()>0);
}

bool lotman::Lot::add_lot(std::string lot_name, std::vector<std::string> owners, std::vector<std::string> parents, std::vector<std::string> children, picojson::array paths, picojson::value management_policy_attrs) {
    // TODO: Error handling

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
                    parent_updated = lotman::Validator::SQL_update_parent(children_iter, parents_iter, lot_name);
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

bool lotman::Lot::remove_lot(std::string lot_name) {
    // TODO: Handle all the weird things that can happen when removing a lot.
    return delete_lot(lot_name);

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

    
