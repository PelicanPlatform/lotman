#include <algorithm>
#include <stdio.h>
#include <iostream>
#include <picojson.h>
#include <sqlite3.h>
//#include <string>
//#include <vector>

#include "lotman_internal.h"

using namespace lotman;

// TODO: Go through and make things const where they should be declared as such
// TODO: Go through and handle any cases where lot_exists should be checked!
//       --> Starting to feel like this should be done one layer up...

/**
 * Functions specific to Lot class
*/

std::pair<bool, std::string> lotman::Lot2::init_full(const json lot_JSON) {
                
    // try/catch here for error handling

    try {
        lot_name = lot_JSON["lot_name"];

        owner = lot_JSON["owner"];
        parents = lot_JSON["parents"]; 
        paths = lot_JSON["paths"];
        man_policy_attr.dedicated_GB = lot_JSON["management_policy_attrs"]["dedicated_GB"];
        man_policy_attr.opportunistic_GB = lot_JSON["management_policy_attrs"]["opportunistic_GB"];
        man_policy_attr.max_num_objects = lot_JSON["management_policy_attrs"]["max_num_objects"];
        man_policy_attr.creation_time = lot_JSON["management_policy_attrs"]["creation_time"];
        man_policy_attr.expiration_time = lot_JSON["management_policy_attrs"]["expiration_time"];
        man_policy_attr.deletion_time = lot_JSON["management_policy_attrs"]["deletion_time"];
        
        usage.self_GB = 0;
        usage.children_GB = 0;
        usage.self_GB_being_written = 0;
        usage.children_GB_being_written = 0;
        usage.self_objects = 0;
        usage.children_objects = 0;
        usage.self_objects_being_written = 0;
        usage.children_objects_being_written = 0;

        full_lot = true;
        return std::make_pair(true, "");
    }
    catch(std::exception &exc) {
        //std::cerr << exc.what();
        return std::make_pair(false, exc.what());
    }
}


std::pair<bool, std::string> lotman::Lot2::store_lot() {
        if (!full_lot) {
            return std::make_pair(false, "Lot was not fully initialized");
        }

        try {
            auto rp = write_new();
            return std::make_pair(rp.first, rp.second);

        }
        catch (std::exception &exc) {
            return std::make_pair(false, exc.what());
        }
        
}

std::pair<bool, std::string> lotman::Lot2::lot_exists(std::string lot_name) {

    std::string lot_exists_query = "SELECT lot_name FROM management_policy_attributes WHERE lot_name = ?;";
    std::map<std::string, std::vector<int>> lot_exists_str_map{{lot_name, {1}}};

    try {
        return std::make_pair(lotman::Validator::SQL_get_matches(lot_exists_query, lot_exists_str_map).size()>0, "");

    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}

bool lotman::Lot2::destroy_lot() {
    return false;

}





























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
                          std::string owner, 
                          std::vector<std::string> parents, 
                          std::vector<std::string> children, 
                          std::map<std::string, int> paths_map, 
                          std::map<std::string, int> management_policy_attrs_int_map,
                          std::map<std::string, unsigned long long> management_policy_attrs_tmstmp_map,
                          std::map<std::string, double> management_policy_attrs_dbl_map) {

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

    bool store_lot_status = store_lot(lot_name, owner, parents, paths_map, management_policy_attrs_int_map, management_policy_attrs_tmstmp_map, management_policy_attrs_dbl_map);

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

bool lotman::Lot::update_lot_params(std::string lot_name, 
                                    std::string owner, 
                                    std::map<std::string, std::string> parents_map, 
                                    std::map<std::string, int> paths_map, 
                                    std::map<std::string, int> management_policy_attrs_int_map, 
                                    std::map<std::string, double> management_policy_attrs_double_map) {
    if (!lot_exists(lot_name)) {
        std::cout << "Lot does not exist so it cannot be updated" << std::endl;
        return false;
    }

    // For each change in each map, store the change.
    std::string owner_update_dynamic_query = "UPDATE owners SET owner=? WHERE lot_name=?;";
    std::map<std::string, std::vector<int>> owner_update_str_map{{lot_name, {2}}, {owner, {1}}};
    bool rv = lotman::Lot::store_modifications(owner_update_dynamic_query, owner_update_str_map);
    if (!rv) {
        std::cout << "call to lotman::Lot::store_modifications unsuccessful when updating an owner." << std::endl;
        return false;
    }

    std::string parents_update_dynamic_query = "UPDATE parents SET parent=? WHERE lot_name=? AND parent=?";
    for (auto & key : parents_map) {
        std::map<std::string, std::vector<int>> parents_update_str_map;
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
            rv = lotman::Lot::store_modifications(management_policy_attrs_dynamic_query, management_policy_attrs_update_str_map, management_policy_attrs_update_int_map);
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
            
            rv = lotman::Lot::store_modifications(management_policy_attrs_dynamic_query, management_policy_attrs_update_str_map, std::map<int, std::vector<int>>(), management_policy_attrs_update_double_map);
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

bool lotman::Lot::update_lot_usage(std::string lot_name,
                                   std::string key, 
                                   double value) {

    /*
    Function Flow for update_lot_usage:
    * Check that lot proper exists
    * Sanitize inputs by making sure key is allowed/known
    * Get the current usage, used to calculate delta for updating parents' children_* columns
    * Calculate delta
    * Update lot proper
    * For each parent of lot proper, update children_key += delta
    */

    // Check for existence
    if (!lot_exists(lot_name)) {
        std::cout << "Lot does not exist so its usage cannot be updated." << std::endl;
        return false;
    }

    std::array<std::string, 2> allowed_int_keys={"personal_objects", "personal_objects_being_written"};
    std::array<std::string, 2> allowed_double_keys={"personal_GB", "personal_GB_being_written"};

    std::string update_usage_dynamic_query = "UPDATE lot_usage SET " + key + "=? WHERE lot_name=?;";
    std::map<std::string, std::vector<int>> update_usage_str_map = {{lot_name, {2}}};

    // Sanitize inputs
    if (std::find(allowed_int_keys.begin(), allowed_int_keys.end(), key) != allowed_int_keys.end()) {
        // Get the current value for the lot
        std::string get_usage_query = "SELECT " + key + " FROM lot_usage WHERE lot_name = ?;";
        std::map<std::string, std::vector<int>> get_usage_query_str_map{{lot_name, {1}}};
        int current_usage = std::stoi(lotman::Validator::SQL_get_matches(get_usage_query, get_usage_query_str_map)[0]);
        int delta = value - current_usage;

        // Update lot proper
        std::string update_usage_stmt = "UPDATE lot_usage SET " + key + "=? WHERE lot_name=?;";
        std::map<std::string, std::vector<int>> update_usage_str_map = {{lot_name, {2}}};
        std::map<int, std::vector<int>> update_usage_int_map = {{value, {1}}};
        bool rv = lotman::Lot::store_modifications(update_usage_dynamic_query, update_usage_str_map, update_usage_int_map);
        if (!rv) {
            std::cerr << "Call to lotman::Lot::store_modifications unsuccessful when updating lot usage" << std::endl;
            return false;
        }

        /* TODO: need some kind of recovery file if we start updating parents and then fail --> for each parent, store  in temp file current usage for key
                 and delete temp file after done. Use a function "repair_db" that checks for existence of temp file and restores things to those values,
                 deleting the temp file upon completion.
        */

        // Update parents
        std::vector<std::string> parents_vec = get_parent_names(lot_name, true);

        std::string children_key = "children" + key.substr(8);
        std::string parent_usage_query = "SELECT " + children_key + " FROM lot_usage WHERE lot_name = ?;";
        for (const auto &parent : parents_vec) {
            std::map<std::string, std::vector<int>> parent_usage_query_str_map{{parent, {1}}};
            int current_usage = std::stoi(lotman::Validator::SQL_get_matches(parent_usage_query, parent_usage_query_str_map)[0]);
            std::string update_parent_usage_stmt = "UPDATE lot_usage SET " + children_key + "=? WHERE lot_name=?;";
            std::map<std::string, std::vector<int>> update_parent_str_map{{parent, {2}}};
            std::map<int, std::vector<int>> update_parent_int_map{{current_usage+delta, {1}}}; // Update children_key to current_usage + delta
            rv = lotman::Lot::store_modifications(update_parent_usage_stmt, update_parent_str_map, update_parent_int_map);
            if (!rv) {
                std::cerr << "Call to lotman::Lot::store_modifications unsuccessful when updating lot parent usage." << std::endl;
                return false;
            }
        }
    }

    else if (std::find(allowed_double_keys.begin(), allowed_double_keys.end(), key) != allowed_double_keys.end()) {
        // Get the current value for the lot
        std::string get_usage_query = "SELECT " + key + " FROM lot_usage WHERE lot_name = ?;";
        std::map<std::string, std::vector<int>> get_usage_query_str_map{{lot_name, {1}}};
        double current_usage = std::stod(lotman::Validator::SQL_get_matches(get_usage_query, get_usage_query_str_map)[0]);
        double delta = value - current_usage;

        // Update lot proper
        std::string update_usage_stmt = "UPDATE lot_usage SET " + key + "=? WHERE lot_name=?;";
        std::map<std::string, std::vector<int>> update_usage_str_map = {{lot_name, {2}}};
        std::map<double, std::vector<int>> update_usage_double_map = {{value, {1}}};
        std::map<int, std::vector<int>> plc_hldr_int_map;
        bool rv = lotman::Lot::store_modifications(update_usage_dynamic_query, update_usage_str_map, plc_hldr_int_map, update_usage_double_map);
        if (!rv) {
            std::cerr << "Call to lotman::Lot::store_modifications unsuccessful when updating lot usage" << std::endl;
            return false;
        }

        // Update parents
        std::vector<std::string> parents_vec = get_parent_names(lot_name, true);

        std::string children_key = "children" + key.substr(8);
        std::string parent_usage_query = "SELECT " + children_key + " FROM lot_usage WHERE lot_name = ?;";
        for (const auto &parent : parents_vec) {
            std::map<std::string, std::vector<int>> parent_usage_query_str_map{{parent, {1}}};
            double current_usage = std::stod(lotman::Validator::SQL_get_matches(parent_usage_query, parent_usage_query_str_map)[0]);
            std::string update_parent_usage_stmt = "UPDATE lot_usage SET " + children_key + "=? WHERE lot_name=?;";
            std::map<std::string, std::vector<int>> update_parent_str_map{{parent, {2}}};
            std::map<double, std::vector<int>> update_parent_int_map{{current_usage+delta, {1}}}; // Update children_key to current_usage + delta
            std::map<int, std::vector<int>> plc_hldr_int_map;
            rv = lotman::Lot::store_modifications(update_parent_usage_stmt, update_parent_str_map, plc_hldr_int_map, update_parent_int_map);
            if (!rv) {
                std::cerr << "Call to lotman::Lot::store_modifications unsuccessful when updating lot parent usage." << std::endl;
                return false;
            }
        }
    }

    else {
        std::cerr << "Key \"" << key << "\" not recognized" << std::endl;
        return false;
    }

    return true;
}

picojson::object lotman::Lot::get_lot_usage(const std::string lot_name,
                                            const std::string key,
                                            const bool recursive) {

    // TODO: Introduce some notion of verbocity to give options for output, like:
    // {"dedicated_GB" : 10} vs {"dedicated_GB" : {"personal": 5, "children" : 5}} vs {"dedicated_GB" : {"personal" : 5, "child1" : 2.5, "child2" : 2.5}}
    // Think a bit more about whether this makes sense.

    // TODO: Might be worthwhile to join some of these sections that share a common preamble

    picojson::object output_obj;
    std::array<std::string, 4> allowed_keys = {"dedicated_GB", "opportunistic_GB", "total_GB", "num_objects"};

    std::vector<std::string> query_output;
    std::vector<std::vector<std::string>> query_multi_out;

    if (key == "dedicated_GB") {
        if (recursive) {
            std::string rec_ded_usage_query =   "SELECT "
                                                    "CASE "
                                                        "WHEN lot_usage.personal_GB + lot_usage.children_GB <= management_policy_attributes.dedicated_GB THEN lot_usage.personal_GB + lot_usage.children_GB "
                                                        "ELSE management_policy_attributes.dedicated_GB "
                                                    "END AS total, " // For readability, not actually referencing these column names
                                                    "CASE "
                                                        "WHEN lot_usage.personal_GB >= management_policy_attributes.dedicated_GB THEN management_policy_attributes.dedicated_GB "
                                                        "ELSE lot_usage.personal_GB "
                                                    "END AS personal_contrib, "
                                                    "CASE "
                                                        "WHEN lot_usage.personal_GB >= management_policy_attributes.dedicated_GB THEN '0' "
                                                        "WHEN lot_usage.personal_GB + lot_usage.children_GB >= management_policy_attributes.dedicated_GB THEN management_policy_attributes.dedicated_GB - lot_usage.personal_GB "
                                                        "ELSE lot_usage.children_GB "
                                                    "END AS children_contrib "
                                                "FROM lot_usage "
                                                    "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                "WHERE lot_usage.lot_name = ?;";
            std::map<std::string, std::vector<int>> ded_GB_query_str_map{{lot_name, {1}}};
            query_multi_out = lotman::Validator::SQL_get_matches_multi_col(rec_ded_usage_query, 3, ded_GB_query_str_map);
            output_obj["total"] = picojson::value(std::stod(query_multi_out[0][0]));
            output_obj["personal_contrib"] = picojson::value(std::stod(query_multi_out[0][1]));
            output_obj["children_contrib"] = picojson::value(std::stod(query_multi_out[0][2]));
        }
        else {
            std::string ded_GB_query = "SELECT "
                                            "CASE "
                                                "WHEN lot_usage.personal_GB >= management_policy_attributes.dedicated_GB THEN management_policy_attributes.dedicated_GB "
                                                "ELSE lot_usage.personal_GB "
                                            "END AS total "
                                        "FROM "
                                            "lot_usage "
                                            "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                        "WHERE lot_usage.lot_name = ?;";
        
        std::map<std::string, std::vector<int>> ded_GB_query_str_map{{lot_name, {1}}};
        query_output = lotman::Validator::SQL_get_matches(ded_GB_query, ded_GB_query_str_map);
        output_obj["total"] = picojson::value(std::stod(query_output[0]));
        }
    }

    else if (key == "opportunistic_GB") {
        if (recursive) {
            std::string rec_opp_usage_query =   "SELECT "
                                                    "CASE "
                                                        "WHEN lot_usage.personal_GB + lot_usage.children_GB >= management_policy_attributes.opportunistic_GB +management_policy_attributes.dedicated_GB THEN management_policy_attributes.opportunistic_GB "
                                                        "WHEN lot_usage.personal_GB + lot_usage.children_GB >= management_policy_attributes.dedicated_GB THEN lot_usage.personal_GB + lot_usage.children_GB - management_policy_attributes.dedicated_GB "
                                                        "ELSE '0' "
                                                    "END AS total, "
                                                    "CASE "
                                                        "WHEN lot_usage.personal_GB >= management_policy_attributes.opportunistic_GB + management_policy_attributes.dedicated_GB THEN management_policy_attributes.opportunistic_GB "
                                                        "WHEN lot_usage.personal_GB >= management_policy_attributes.dedicated_GB THEN  lot_usage.personal_GB - management_policy_attributes.dedicated_GB "
                                                        "ELSE '0' "
                                                    "END AS personal_contrib, "
                                                    "CASE "
                                                        "WHEN lot_usage.personal_GB >= management_policy_attributes.opportunistic_GB + management_policy_attributes.dedicated_GB THEN '0' "
                                                        "WHEN lot_usage.personal_GB >= management_policy_attributes.dedicated_GB AND lot_usage.personal_GB + lot_usage.children_GB >= management_policy_attributes.opportunistic_GB + management_policy_attributes.dedicated_GB THEN management_policy_attributes.opportunistic_GB + management_policy_attributes.dedicated_GB - lot_usage.personal_GB "
                                                        "WHEN lot_usage.personal_GB >= management_policy_attributes.dedicated_GB AND lot_usage.personal_GB + lot_usage.children_GB < management_policy_attributes.opportunistic_GB + management_policy_attributes.dedicated_GB THEN lot_usage.children_GB "
                                                        "WHEN lot_usage.personal_GB < management_policy_attributes.dedicated_GB AND lot_usage.personal_GB + lot_usage.children_GB >= management_policy_attributes.opportunistic_GB + management_policy_attributes.dedicated_GB THEN management_policy_attributes.opportunistic_GB "
                                                        "WHEN lot_usage.personal_GB < management_policy_attributes.dedicated_GB AND lot_usage.personal_GB + lot_usage.children_GB > management_policy_attributes.dedicated_GB THEN lot_usage.personal_GB + lot_usage.children_GB - management_policy_attributes.dedicated_GB "
                                                        "ELSE '0' "
                                                    "END AS children_contrib "
                                                "FROM "
                                                    "lot_usage "
                                                    "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                "WHERE lot_usage.lot_name = ?;";
            std::map<std::string, std::vector<int>> opp_GB_query_str_map{{lot_name, {1}}};
            query_multi_out = lotman::Validator::SQL_get_matches_multi_col(rec_opp_usage_query, 3, opp_GB_query_str_map);
            output_obj["total"] = picojson::value(std::stod(query_multi_out[0][0]));
            output_obj["personal_contrib"] = picojson::value(std::stod(query_multi_out[0][1]));
            output_obj["children_contrib"] = picojson::value(std::stod(query_multi_out[0][2]));
        }
        else {
            std::string opp_GB_query =  "SELECT "
                                            "CASE "
                                                "WHEN lot_usage.personal_GB >= management_policy_attributes.dedicated_GB + management_policy_attributes.opportunistic_GB THEN management_policy_attributes.opportunistic_GB "
                                                "WHEN lot_usage.personal_GB >= management_policy_attributes.dedicated_GB THEN lot_usage.personal_GB = management_policy_attributes.dedicated_GB "
                                                "ELSE '0' "
                                            "END AS total "
                                        "FROM "
                                            "lot_usage "
                                            "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                        "WHERE lot_usage.lot_name = ?;";

        std::map<std::string, std::vector<int>> opp_GB_query_str_map{{lot_name, {1}}};
        query_output = lotman::Validator::SQL_get_matches(opp_GB_query, opp_GB_query_str_map);
        output_obj["total"] = picojson::value(std::stod(query_output[0]));
        }
    }

    else if (key == "total_GB") {
        // Get the total usage
        if (recursive) {
            // Need to consider usage from children
            std::string child_usage_GB_query = "SELECT personal_GB, children_GB FROM lot_usage WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> child_usage_GB_str_map{{lot_name, {1}}};
            query_multi_out = lotman::Validator::SQL_get_matches_multi_col(child_usage_GB_query, 2, child_usage_GB_str_map);
            output_obj["personal_contrib"] = picojson::value(std::stod(query_multi_out[0][0]));
            output_obj["children_contrib"] = picojson::value(std::stod(query_multi_out[0][1]));
        }
        else {
            std::string usage_GB_query = "SELECT personal_GB FROM lot_usage WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> usage_GB_str_map{{lot_name, {1}}};
            query_output = lotman::Validator::SQL_get_matches(usage_GB_query, usage_GB_str_map);
            output_obj["total"] = picojson::value(std::stod(query_output[0]));
        }
    }
    
    else if (key == "num_objects") {
        if (recursive) {
            std::string rec_num_obj_query = "SELECT personal_objects, children_objects FROM lot_usage WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> rec_num_obj_str_map{{lot_name, {1}}};
            query_multi_out = lotman::Validator::SQL_get_matches_multi_col(rec_num_obj_query, 2, rec_num_obj_str_map);
            output_obj["personal_contrib"] = picojson::value(std::stod(query_multi_out[0][0]));
            output_obj["children_contrib"] = picojson::value(std::stod(query_multi_out[0][1]));
        }
        else {

            std::string num_obj_query = "SELECT personal_objects FROM lot_usage WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> num_obj_str_map{{lot_name, {1}}};
            query_output = lotman::Validator::SQL_get_matches(num_obj_query, num_obj_str_map);
            output_obj["total"] = picojson::value(std::stod(query_output[0]));
        }
    }

    else if (key == "GB_being_written") {
        // TODO: implement this section
    }
    
    else if (key == "objects_being_written") {
        // TODO: implement this section
    }

    else {
        std::cerr << "The key \"" << key << "\" is not recognized." << std::endl;
    }
    return output_obj;
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

std::vector<std::string> lotman::Lot::get_lots_from_owners(picojson::array owners_arr) {
    
    std::vector<std::string> matching_lots_vec{};
    std::string lots_from_owners_dynamic_query = "SELECT lot_name FROM owners WHERE owner = ?;";

    int idx = 0;
    for (const auto &owner_obj : owners_arr) {
        std::vector<std::string> temp_vec;
        if (!owner_obj.is<picojson::object>()) {
            std::cerr << "object in owners array is not recognized as an object." << std::endl;
            return matching_lots_vec;
        }
        picojson::object owner_obj_internal = owner_obj.get<picojson::object>();
        auto iter = owner_obj_internal.begin();
        if (iter == owner_obj_internal.end()) {
            std::cerr << "object in owners array appears empty";
            return matching_lots_vec;
        }

        for(iter; iter != owner_obj_internal.end(); ++iter) {
            auto owner = iter->first;
            auto recursive_val = iter->second;
            if (!recursive_val.is<bool>()) {
                std::cerr << "recursive value for owner object is not recognized as a bool." << std::endl;
                return matching_lots_vec;
            }
            bool recursive = recursive_val.get<bool>();

            std::map<std::string, std::vector<int>> lots_from_owners_dynamic_query_str_map{{owner, {1}}};
            temp_vec = lotman::Validator::SQL_get_matches(lots_from_owners_dynamic_query,lots_from_owners_dynamic_query_str_map);

            if (recursive) {
                std::vector<std::string> internal_temp_vec;
                for (const auto &lot : temp_vec) {
                    std::vector<std::string> children_lots_vec = get_children_names(lot, true);
                    internal_temp_vec.insert(internal_temp_vec.end(), children_lots_vec.begin(), children_lots_vec.end());
                }
                temp_vec.insert(temp_vec.end(), internal_temp_vec.begin(), internal_temp_vec.end());
            }
            
            // Sort temp_vec
            sort(temp_vec.begin(), temp_vec.end());

            // set matching_lots_vec
            if (idx == 0) {
                matching_lots_vec = temp_vec;
            }

            // intersection of matching_lots_vec with temp_vec
            else {
                std::vector<std::string> internal_temp_vec;
                set_intersection(matching_lots_vec.begin(), matching_lots_vec.end(),
                                 temp_vec.begin(), temp_vec.end(),
                                 std::back_inserter(internal_temp_vec));
                matching_lots_vec = internal_temp_vec;
            }
        }
        ++idx;
    }
    auto last = std::unique(matching_lots_vec.begin(), matching_lots_vec.end());
    matching_lots_vec.erase(last, matching_lots_vec.end());
    return matching_lots_vec;
}

std::vector<std::string> lotman::Lot::get_lots_from_parents(picojson::array parents_arr) {

    std::vector<std::string> matching_lots_vec;
    std::string lots_from_parents_dynamic_query = "SELECT lot_name FROM parents WHERE parent = ?;";
    
    int idx = 0;
    for (const auto &parent_obj : parents_arr) {
        std::vector<std::string> temp_vec;
        if (!parent_obj.is<picojson::object>()) {
            std::cerr << "object in parents array is not recognized as an object." << std::endl;
            return matching_lots_vec;
        }
        picojson::object parent_obj_internal = parent_obj.get<picojson::object>();
        auto iter = parent_obj_internal.begin();
        if (iter == parent_obj_internal.end()) {
            std::cerr << "object in parents array appears empty";
            return matching_lots_vec;
        }

        for(iter; iter != parent_obj_internal.end(); ++iter) {
            auto parent = iter->first;
            auto recursive_val = iter->second;
            if (!recursive_val.is<bool>()) {
                std::cerr << "recursive value for parent object is not recognized as a bool." << std::endl;
                return matching_lots_vec;
            }
            bool recursive = recursive_val.get<bool>();

            std::map<std::string, std::vector<int>> lots_from_parents_dynamic_query_str_map{{parent, {1}}};

            temp_vec = lotman::Validator::SQL_get_matches(lots_from_parents_dynamic_query,lots_from_parents_dynamic_query_str_map);


            if (recursive) {
                std::vector<std::string> internal_temp_vec;
                for (const auto &lot : temp_vec) {
                    std::vector<std::string> children_lots_vec = get_children_names(lot, true);
                    internal_temp_vec.insert(internal_temp_vec.end(), children_lots_vec.begin(), children_lots_vec.end());
                }
                temp_vec.insert(temp_vec.end(), internal_temp_vec.begin(), internal_temp_vec.end());
            }

            // Sort temp_vec
            sort(temp_vec.begin(), temp_vec.end());

            // set matching_lots_vec
            if (idx == 0) {
                matching_lots_vec = temp_vec;
            }

            // intersection of matching_lots_vec with temp_vec
            else {
                std::vector<std::string> internal_temp_vec;
                set_intersection(matching_lots_vec.begin(), matching_lots_vec.end(),
                                 temp_vec.begin(), temp_vec.end(),
                                 std::back_inserter(internal_temp_vec));
                matching_lots_vec = internal_temp_vec;
            }
        }
        ++idx;
    }
    auto last = std::unique(matching_lots_vec.begin(), matching_lots_vec.end());
    matching_lots_vec.erase(last, matching_lots_vec.end());
    return matching_lots_vec;
}

std::vector<std::string> lotman::Lot::get_lots_from_children(picojson::array children_arr) {
    std::vector<std::string> matching_lots_vec;
    std::string lots_from_children_dynamic_query = "SELECT parent FROM parents WHERE lot_name = ?;";
    
    int idx = 0;
    for (const auto &children_obj : children_arr) {
        std::vector<std::string> temp_vec;
        if (!children_obj.is<picojson::object>()) {
            std::cerr << "object in children array is not recognized as an object." << std::endl;
            return matching_lots_vec;
        }
        picojson::object children_obj_internal = children_obj.get<picojson::object>();
        auto iter = children_obj_internal.begin();
        if (iter == children_obj_internal.end()) {
            std::cerr << "object in children array appears empty";
            return matching_lots_vec;
        }

        for(iter; iter != children_obj_internal.end(); ++iter) {
            auto children = iter->first;
            auto recursive_val = iter->second;
            if (!recursive_val.is<bool>()) {
                std::cerr << "recursive value for children object is not recognized as a bool." << std::endl;
                return matching_lots_vec;
            }
            bool recursive = recursive_val.get<bool>();

            std::map<std::string, std::vector<int>> lots_from_children_dynamic_query_str_map{{children, {1}}};

            temp_vec = lotman::Validator::SQL_get_matches(lots_from_children_dynamic_query,lots_from_children_dynamic_query_str_map);




            if (recursive) {
                std::vector<std::string> internal_temp_vec;
                for (const auto &lot : temp_vec) {
                    std::vector<std::string> parent_lots_vec = get_parent_names(lot, true);
                    internal_temp_vec.insert(internal_temp_vec.end(), parent_lots_vec.begin(), parent_lots_vec.end());
                }
                temp_vec.insert(temp_vec.end(), internal_temp_vec.begin(), internal_temp_vec.end());
            }

            // Sort temp_vec
            sort(temp_vec.begin(), temp_vec.end());

            // set matching_lots_vec
            if (idx == 0) {
                matching_lots_vec = temp_vec;
            }

            // intersection of matching_lots_vec with temp_vec
            else {
                std::vector<std::string> internal_temp_vec;
                set_intersection(matching_lots_vec.begin(), matching_lots_vec.end(),
                                 temp_vec.begin(), temp_vec.end(),
                                 std::back_inserter(internal_temp_vec));
                matching_lots_vec = internal_temp_vec;
            }
        }
        ++idx;
    }
    auto last = std::unique(matching_lots_vec.begin(), matching_lots_vec.end());
    matching_lots_vec.erase(last, matching_lots_vec.end());
    return matching_lots_vec;
}

std::vector<std::string> lotman::Lot::get_lots_from_paths(picojson::array path_arr) {
    std::vector<std::string> matching_lots_vec{};
    std::string lots_from_path_dynamic_query = "SELECT lot_name FROM paths WHERE path = ?;";

    int idx = 0;
    for (const auto &path_obj : path_arr) {
        std::vector<std::string> temp_vec;
        if (!path_obj.is<picojson::object>()) {
            std::cerr << "object in path array is not recognized as an object." << std::endl;
            return matching_lots_vec;
        }
        picojson::object path_obj_internal = path_obj.get<picojson::object>();
        auto iter = path_obj_internal.begin();
        if (iter == path_obj_internal.end()) {
            std::cerr << "object in owners array appears empty";
            return matching_lots_vec;
        }

        for(iter; iter != path_obj_internal.end(); ++iter) {
            std::string path = iter->first;
            auto recursive_val = iter->second;
            if (!recursive_val.is<bool>()) {
                std::cerr << "recursive value for path object is not recognized as a bool." << std::endl;
                return matching_lots_vec;
            }
            bool recursive = recursive_val.get<bool>();

            int fwd_slash_count = std::count(path.begin(), path.end(), '/');

            
            for (int iter = 0; iter<fwd_slash_count; ++iter) {
                
                std::map<std::string, std::vector<int>> lots_from_path_dynamic_query_str_map{{path, {1}}};
                temp_vec = lotman::Validator::SQL_get_matches(lots_from_path_dynamic_query,lots_from_path_dynamic_query_str_map);

                if (!temp_vec.size()==0) { // a lot with a matching prefix is found: 
                    if (iter==0) { // If a lot has the entire path, the lot can be added without further checks
                        break;
                    }
                    else { // otherwise, the lot may have a prefix of the path and we need to make sure the matching prefix has recursive set to true
                        std::vector<std::string> recursive_temp_vec;
                        std::string recursion_dynamic_query = "SELECT recursive FROM paths WHERE path = ?;";
                        std::map<std::string, std::vector<int>> recursion_dynamic_query_str_map{{path, {1}}};
                        recursive_temp_vec = lotman::Validator::SQL_get_matches(recursion_dynamic_query, recursion_dynamic_query_str_map);
                        if (recursive_temp_vec[0]=="1") { // a hacky way to tell the bool value stored by SQLITE3 as an int and returned to me as a string.
                            // safe to add the lot
                            break;
                        }
                        else { // recursive set to NOT true -- this is not the lot you're looking for
                            // Since each path belongs to only one stem (ie a path belongs explicitly to exactly one lot, and implicitly to that lot's parents)
                            // it can safely be assumed that there was only 1 lot to check in temp_vec

                            // That lot is not needed, so reset temp_vec
                            temp_vec = {};
                        }
                    }
                }
                int prefix_location = path.rfind('/');
                std::string prefix = path.substr(0, prefix_location);
                path = prefix;
            }

            if (recursive) {
                std::vector<std::string> internal_temp_vec;
                for (const auto &lot : temp_vec) {
                    std::vector<std::string> parent_lots_vec = get_parent_names(lot, true);
                    internal_temp_vec.insert(internal_temp_vec.end(), parent_lots_vec.begin(), parent_lots_vec.end());
                }
                temp_vec.insert(temp_vec.end(), internal_temp_vec.begin(), internal_temp_vec.end());

            }
            // Sort temp_vec
            sort(temp_vec.begin(), temp_vec.end());

            // set matching_lots_vec
            if (idx == 0) {
                matching_lots_vec = temp_vec;
            }

            // intersection of matching_lots_vec with temp_vec
            else {
                std::vector<std::string> internal_temp_vec;
                set_intersection(matching_lots_vec.begin(), matching_lots_vec.end(),
                                 temp_vec.begin(), temp_vec.end(),
                                 std::back_inserter(internal_temp_vec));
                matching_lots_vec = internal_temp_vec;
            }
        }
        ++idx;
    }
    auto last = std::unique(matching_lots_vec.begin(), matching_lots_vec.end());
    matching_lots_vec.erase(last, matching_lots_vec.end());
    return matching_lots_vec;
}

std::vector<std::string> lotman::Lot::get_lots_from_int_policy_attr(std::string key, std::string comparator, int comp_value) {

    std::vector<std::string> matching_lots_vec;
    std::array<std::string, 4> policy_attr_int_keys{{"max_num_objects", "creation_time", "expiration_time", "deletion_time"}};
    std::array<std::string, 5> allowed_comparators{{">", "<", "=", "<=", ">="}};

    std::string policy_attr_int_dynamic_query;
    std::map<int, std::vector<int>> policy_attr_int_map;
    if (std::find(policy_attr_int_keys.begin(), policy_attr_int_keys.end(), key) != policy_attr_int_keys.end()) {
        policy_attr_int_dynamic_query = "SELECT lot_name FROM management_policy_attributes WHERE " + key + comparator + "?;";
        policy_attr_int_map = {{comp_value, {1}}};
    }

    std::map<std::string, std::vector<int>> plc_hldr_str_map;
    matching_lots_vec = lotman::Validator::SQL_get_matches(policy_attr_int_dynamic_query, plc_hldr_str_map, policy_attr_int_map);

    // sort the return vector
    std::sort(matching_lots_vec.begin(), matching_lots_vec.end());
    return matching_lots_vec;
}

std::vector<std::string> lotman::Lot::get_lots_from_double_policy_attr(std::string key, std::string comparator, double comp_value) {

    std::vector<std::string> matching_lots_vec;
    std::array<std::string, 4> policy_attr_int_keys{{"dedicated_GB", "opportunistic_GB"}};
    std::array<std::string, 5> allowed_comparators{{">", "<", "=", "<=", ">="}};

    std::string policy_attr_int_dynamic_query;
    std::map<double, std::vector<int>> policy_attr_double_map;
    if (std::find(policy_attr_int_keys.begin(), policy_attr_int_keys.end(), key) != policy_attr_int_keys.end()) {
        policy_attr_int_dynamic_query = "SELECT lot_name FROM management_policy_attributes WHERE " + key + comparator + "?;";
        policy_attr_double_map = {{comp_value, {1}}};
    }

    std::map<std::string, std::vector<int>> plc_hldr_str_map;
    std::map<int, std::vector<int>> plc_hldr_int_map;
    matching_lots_vec = lotman::Validator::SQL_get_matches(policy_attr_int_dynamic_query, plc_hldr_str_map, plc_hldr_int_map, policy_attr_double_map);

    std::sort(matching_lots_vec.begin(), matching_lots_vec.end());
    return matching_lots_vec;
}

std::vector<std::string> lotman::Lot::get_lots_from_usage(std::string key, std::string comparator, double comp_val, bool recursive) {

    std::vector<std::string> matching_lots_vec;
    // TODO: Go back at all levels and add support for GB_being_written and objects_being_written.
    std::array<std::string, 6> usage_keys{{"dedicated_GB_usage", "opportunistic_GB_usage", "total_GB_usage", "num_objects_usage", "GB_being_written", "num_objects_being_written"}}; // TODO: Finish
    std::array<std::string, 5> allowed_comparators{{">", "<", "=", "<=", ">="}};

    if (std::find(allowed_comparators.begin(), allowed_comparators.end(), comparator) != allowed_comparators.end()) {
    
        if (key == "dedicated_GB_usage") {
            if (recursive) {
                if (comp_val <0) {
                    std::string rec_ded_usage_query =   "SELECT "
                                                            "lot_usage.lot_name "
                                                        "FROM lot_usage "
                                                            "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                        "WHERE lot_usage.personal_GB + lot_usage.children_GB " + comparator + " management_policy_attributes.dedicated_GB;";
                    
                    matching_lots_vec = lotman::Validator::SQL_get_matches(rec_ded_usage_query);
                }
                else {
                    std::string rec_ded_usage_query =   "SELECT "
                                                            "lot_usage.lot_name "
                                                        "FROM lot_usage "
                                                            "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                        "WHERE lot_usage.personal_GB + lot_usage.children_GB " + comparator + " ?;";
                    std::map<double, std::vector<int>> dbl_map{{comp_val, {1}}};
                    std::map<std::string, std::vector<int>> plc_hldr_str_map;
                    std::map<int, std::vector<int>> plc_hldr_int_map;
                    matching_lots_vec = lotman::Validator::SQL_get_matches(rec_ded_usage_query, plc_hldr_str_map, plc_hldr_int_map);
                }
            }
            else {
                if (comp_val < 0) {
                    std::string ded_usage_query =   "SELECT "
                                                            "lot_usage.lot_name "
                                                        "FROM lot_usage "
                                                            "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                        "WHERE lot_usage.personal_GB " + comparator + " management_policy_attributes.dedicated_GB;";
                    
                    matching_lots_vec = lotman::Validator::SQL_get_matches(ded_usage_query);
                }
                else {
                    std::string ded_usage_query =   "SELECT "
                                                            "lot_usage.lot_name "
                                                        "FROM lot_usage "
                                                            "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                        "WHERE lot_usage.personal_GB " + comparator + " ?;";
                    std::map<double, std::vector<int>> dbl_map{{comp_val, {1}}};
                    std::map<std::string, std::vector<int>> plc_hldr_str_map;
                    std::map<int, std::vector<int>> plc_hldr_int_map;
                    matching_lots_vec = lotman::Validator::SQL_get_matches(ded_usage_query, plc_hldr_str_map, plc_hldr_int_map);
                }
            }
        }
        else if (key == "opportunistic_GB_usage") {
            if (recursive) {
                if (comp_val <0) {
                    std::string rec_opp_usage_query =   "SELECT "
                                                            "lot_usage.lot_name "
                                                        "FROM lot_usage "
                                                            "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                        "WHERE lot_usage.personal_GB + lot_usage.children_GB " + comparator + " management_policy_attributes.dedicated_GB + management_policy_attributes.opportunistic_GB;";
                    
                    matching_lots_vec = lotman::Validator::SQL_get_matches(rec_opp_usage_query);
                }
                else {
                    std::string rec_opp_usage_query =   "SELECT "
                                                            "lot_usage.lot_name "
                                                        "FROM lot_usage "
                                                            "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                        "WHERE lot_usage.personal_GB + lot_usage.children_GB " + comparator + " management_policy_attributes.dedicated_GB + ?;";
                    std::map<double, std::vector<int>> dbl_map{{comp_val, {1}}};
                    std::map<std::string, std::vector<int>> plc_hldr_str_map;
                    std::map<int, std::vector<int>> plc_hldr_int_map;
                    matching_lots_vec = lotman::Validator::SQL_get_matches(rec_opp_usage_query, plc_hldr_str_map, plc_hldr_int_map);
                }
            }
            else {
                if (comp_val <0) {
                    std::string opp_usage_query =   "SELECT "
                                                            "lot_usage.lot_name "
                                                        "FROM lot_usage "
                                                            "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                        "WHERE lot_usage.personal_GB " + comparator + " management_policy_attributes.dedicated_GB + management_policy_attributes.opportunistic_GB;";
                    
                    matching_lots_vec = lotman::Validator::SQL_get_matches(opp_usage_query);
                }
                else {
                    std::string opp_usage_query =   "SELECT "
                                                            "lot_usage.lot_name "
                                                        "FROM lot_usage "
                                                            "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                        "WHERE lot_usage.personal_GB " + comparator + " management_policy_attributes.dedicated_GB + ?;";
                    std::map<double, std::vector<int>> dbl_map{{comp_val, {1}}};
                    std::map<std::string, std::vector<int>> plc_hldr_str_map;
                    std::map<int, std::vector<int>> plc_hldr_int_map;
                    matching_lots_vec = lotman::Validator::SQL_get_matches(opp_usage_query, plc_hldr_str_map, plc_hldr_int_map);
                }
            }
        }
        else if (key == "total_GB_usage") {
            if (recursive) {
                std::string rec_tot_usage_query =   "SELECT "
                                                        "lot_usage.lot_name "
                                                    "FROM lot_usage "
                                                        "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                    "WHERE lot_usage.personal_GB " + comparator + " ?;";
                std::map<double, std::vector<int>> dbl_map{{comp_val, {1}}};
                std::map<std::string, std::vector<int>> plc_hldr_str_map;
                std::map<int, std::vector<int>> plc_hldr_int_map;
                matching_lots_vec = lotman::Validator::SQL_get_matches(rec_tot_usage_query, plc_hldr_str_map, plc_hldr_int_map);

            }
            else {
                std::string tot_usage_query =   "SELECT "
                                                    "lot_usage.lot_name "
                                                "FROM lot_usage "
                                                    "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                "WHERE lot_usage.personal_GB + lot_usage.children_GB " + comparator + " ?;";
                std::map<double, std::vector<int>> dbl_map{{comp_val, {1}}};
                std::map<std::string, std::vector<int>> plc_hldr_str_map;
                std::map<int, std::vector<int>> plc_hldr_int_map;
                matching_lots_vec = lotman::Validator::SQL_get_matches(tot_usage_query, plc_hldr_str_map, plc_hldr_int_map);
            }
        }

        else if (key == "num_objects") {
            if (recursive) {
                if (comp_val < 0) {
                    std::string rec_num_obj_query = "SELECT "
                                                        "lot_usage.lot_name "
                                                    "FROM lot_usage "
                                                        "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                    "WHERE lot_usage.personal_objects + lot_usage.children_objects " + comparator + " management_policy_attributes.max_num_objects;";
                    matching_lots_vec = lotman::Validator::SQL_get_matches(rec_num_obj_query);
                }
                else {
                    std::string rec_num_obj_query = "SELECT "
                                                        "lot_usage.lot_name "
                                                    "FROM lot_usage "
                                                        "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                    "WHERE lot_usage.personal_objects + lot_usage.children_objects " + comparator + " ?;";
                    std::map<std::string, std::vector<int>> plc_hldr_str_map;
                    std::map<int, std::vector<int>> int_map{{static_cast<int>(comp_val), {1}}};
                    matching_lots_vec = lotman::Validator::SQL_get_matches(rec_num_obj_query, plc_hldr_str_map, int_map);
                }
            }
            else {
                if (comp_val < 0) {
                    std::string num_obj_query = "SELECT "
                                                    "lot_usage.lot_name "
                                                "FROM lot_usage "
                                                    "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                "WHERE lot_usage.personal_objects " + comparator + " management_policy_attributes.max_num_objects;";
                    matching_lots_vec = lotman::Validator::SQL_get_matches(num_obj_query);
                }
                else {
                    std::string num_obj_query = "SELECT "
                                                        "lot_usage.lot_name "
                                                    "FROM lot_usage "
                                                        "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                    "WHERE lot_usage.personal_objects " + comparator + " ?;";
                    std::map<std::string, std::vector<int>> plc_hldr_str_map;
                    std::map<int, std::vector<int>> int_map{{static_cast<int>(comp_val), {1}}};
                    matching_lots_vec = lotman::Validator::SQL_get_matches(num_obj_query, plc_hldr_str_map, int_map);
                }
            }
        }

        // TODO: Come back and add creation_time expiration_time and deletion_time where needed

        else if (key == "GB_being_written") {
            // TODO: implement this section
        }

        else if (key == "num_objects_being_written") {
            // TODO: implement this section

        }
    
        else {
            std::cerr << "The key \"" << key << "\" provided in lotman::Lot::get_lots_from_usage is not recognized." << std::endl;
            return matching_lots_vec;
        }
    }

    std::sort(matching_lots_vec.begin(), matching_lots_vec.end());
    return matching_lots_vec;
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

    
