#include <chrono>
#include <nlohmann/json.hpp>
#include <sqlite3.h>

#include <iostream>

#include "lotman_internal.h"

using json = nlohmann::json;
using namespace lotman;

// TODO: Go through and make things const where they should be declared as such

/**
 * Functions specific to Lot class
*/

std::pair<bool, std::string> lotman::Lot::init_full(json lot_JSON) {
    try {        
        lot_name = lot_JSON["lot_name"];
        owner = lot_JSON["owner"];
        parents = lot_JSON["parents"]; 
        if (!lot_JSON["children"].is_null()) {
            children = lot_JSON["children"];
        }
        if (!lot_JSON["paths"].is_null()) {
            paths = lot_JSON["paths"];
        }
        
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

std::pair<bool, std::string> lotman::Lot::init_reassignment_policy(const bool assign_LTBR_parent_as_parent_to_orphans, 
                                                                    const bool assign_LTBR_parent_as_parent_to_non_orphans,
                                                                    const bool assign_policy_to_children) {
    try {
        reassignment_policy.assign_LTBR_parent_as_parent_to_orphans = assign_LTBR_parent_as_parent_to_orphans;
        reassignment_policy.assign_LTBR_parent_as_parent_to_non_orphans = assign_LTBR_parent_as_parent_to_non_orphans;
        reassignment_policy.assign_policy_to_children = assign_policy_to_children;
        has_reassignment_policy = true;
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }


}

void lotman::Lot::init_self_usage() {
    usage.self_GB = 0;
    usage.self_GB_update_staged = false;
    usage.children_GB = 0;
    usage.self_objects = 0;
    usage.self_objects_update_staged = false;
    usage.children_objects = 0;
    usage.self_GB_being_written = 0;
    usage.self_GB_being_written_update_staged = false;
    usage.children_GB_being_written = 0;
    usage.self_objects_being_written = 0;
    usage.self_objects_being_written_update_staged = false;
    usage.children_objects_being_written = 0;
}

std::pair<bool, std::string> lotman::Lot::store_lot() {        
        if (!full_lot) {
            return std::make_pair(false, "Lot was not fully initialized");
        }

        try {
            // Check that any specified parents already exist, unless the lot has itself as parent
            for (auto & parent : parents) {
                if (parent != lot_name && !lot_exists(parent).first) {
                    return std::make_pair(false, "A parent specified for the lot to be added does not exist in the database.");
                }
            }

            // Check that any specified children already exist
            if (children.size()>0) {
                for (auto & child : children) {
                    if (!lot_exists(child).first) {
                        return std::make_pair(false, "A child specified for the lot to be added does not exist in the database");
                    }
                }
            }

            // Check that the added lot won't introduce any cycles
            bool self_parent; // When checking for cycles, we only care about lots who specify a parent other than themselves
            auto self_parent_iter = std::find(parents.begin(), parents.end(), lot_name);  

            self_parent = (self_parent_iter != parents.end());
            if (!children.empty() &&  ((parents.size()==1 && !self_parent) || (parents.size()>1))) { // If there are children and a non-self parent
                bool cycle_exists = lotman::Checks::cycle_check(lot_name, parents, children);
                if (cycle_exists) {
                    return std::make_pair(false, "The lot cannot be added because the combination of parents/children would introduce a dependency cycle in the data structure."); // Return false, don't do anything with the lot
                }
            
            }
    
            // Store the lot, begin updating other lots after confirming the lot has been successfully stored
            auto rp = this->write_new();
            if (!rp.first) {
                std::string int_err = rp.second;
                std::string ext_err = "Failure to store new lot: ";
                return std::make_pair(false, ext_err + int_err);
            }

            bool parent_updated = true;
            for (auto & parents_iter : parents) {
                for (auto & children_iter : children) {
                    if (lotman::Checks::insertion_check(lot_name, parents_iter, children_iter)) {
                        // Update child to have lot_name as a parent instead of parents_iter. Later, save LTBA with all its specified parents.
                        Lot child(children_iter);

                        json update_arr = json::array();
                        update_arr.push_back({{"current", parents_iter}, {"new", lot_name}});

                        rp = child.update_parents(update_arr);
                        parent_updated = rp.first;
                        if (!parent_updated) {
                            std::string int_err = rp.second;
                            std::string ext_err = "Failure on call to child.update_parents: ";
                            return std::make_pair(false, ext_err + int_err);
                        }
                    }
                }
            }
        return std::make_pair(true, "");

        }
        catch (std::exception &exc) {
            return std::make_pair(false, exc.what());
        }
}

std::pair<bool, std::string> lotman::Lot::lot_exists(std::string lot_name) {
    std::string lot_exists_query = "SELECT lot_name FROM management_policy_attributes WHERE lot_name = ?;";
    std::map<std::string, std::vector<int>> lot_exists_str_map{{lot_name, {1}}};
    try {
        auto rp = lotman::Checks::SQL_get_matches(lot_exists_query, lot_exists_str_map);
        if (!rp.second.empty()) { // There was an error
            std::string int_err = rp.second;
            std::string ext_err = "Failure on call to SQL_get_matches: ";
            return std::make_pair(false, ext_err + int_err);
        }

        return std::make_pair(rp.first.size()>0, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}

std::pair<bool, std::string> lotman::Lot::check_if_root() {
    try {
        std::string is_root_query = "SELECT parent FROM parents WHERE lot_name = ?;"; 
        std::map<std::string, std::vector<int>> is_root_query_str_map{{lot_name, {1}}};

        auto rp = lotman::Checks::SQL_get_matches(is_root_query, is_root_query_str_map);
        if (!rp.second.empty()) {  // There is an error message
            std::string int_err = rp.second;
            std::string ext_err = "Function call to SQL_get_matches failed: ";
            return std::make_pair(false, ext_err + int_err);
        }

        std::vector<std::string> parent_names_vec = rp.first;
        if (parent_names_vec.size()==1 && parent_names_vec[0]==lot_name) {
            // lot_name has only itself as a parent, indicating root
            is_root = true;
            return std::make_pair(true, "");
        }
        else {
            is_root = false;
            return std::make_pair(false, "");
        }
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}

std::pair<bool, std::string> lotman::Lot::destroy_lot() {

    /*
    FUNCTION FLOW
    Prechecks:
    * Check for reassignment policy. Lots MUST have a reassignment policy before deletion
    * Check if lot-to-be-removed (LTBR) is the default lot. The default lot MUST NOT be deleted while other lots exist. LotMan provides no method for deleting the default lot.
    * Check that LTBR actually exists in the database
    
    Meat:
    * Get the LTBR's immediate children, who need to be reassigned
    * Handle children according to policy
    * Delete LTBR
    */

    if (!has_reassignment_policy) {
        return std::make_pair(false, "The lot has no defined reassignment policy.");
    }
    if (lot_name == "default") {
        return std::make_pair(false, "The default lot cannot be deleted.");
    }
    try {
        // Prechecks complete, get the children for LTBR
        auto rp_lotvec_str = this->get_children();
        if (!rp_lotvec_str.second.empty()) { // There is an error message
            std::string int_err = rp_lotvec_str.second;
            std::string ext_err = "Failed to get lot children: ";
            return std::make_pair(false, ext_err + int_err);
        }

        //std::vector<Lot> children = rp_lotvec_str.first;
        // If there are no children, the lot can be deleted without issue
        if (self_children.empty()) { 
            auto rp_bool_str = this->delete_lot_from_db();
            if (!rp_bool_str.first) {
                std::string int_err = rp_bool_str.second;
                std::string ext_err = "Failed to delete the lot from the database: ";
                return std::make_pair(false, ext_err + int_err);
            }
            return std::make_pair(true, "");
        }

        // Reaching this point means there are children --> Reassign them

        for (auto &child : self_children) {
            if (lotman::Checks::will_be_orphaned(lot_name, child.lot_name)) { // Indicates whether LTBR is the only parent to child
                if (reassignment_policy.assign_LTBR_parent_as_parent_to_orphans) {
                    auto rp_bool_str = this->check_if_root();
                    if (!rp_bool_str.second.empty()) {
                        std::string int_err = rp_bool_str.second;
                        std::string ext_err = "Function call to lotman::Lot::check_if_root failed: ";
                        return std::make_pair(false, ext_err + int_err);
                    }
                    if (is_root) {
                        return std::make_pair(false, "The lot being removed is a root, and has no parents to assign to its children.");
                    }

                    // Since lots have only one explicit owner, we cannot reassign ownership.
                    
                    // Generate parents of current lot for assignment to children -- only need immediate parents
                    this->get_parents();
                    // Now assign parents of LTBR to orphan children of LTBR
                    // No need to perform cycle check in this case
                    rp_bool_str = child.add_parents(self_parents);
                    if (!rp_bool_str.first) {
                        std::string int_err = rp_bool_str.second;
                        std::string ext_err = "Failure on call to lotman::Lot::add_parents for child lot: ";
                        return std::make_pair(false, ext_err + int_err);
                    }
                }
                else {
                    return std::make_pair(false, "The operation cannot be completed as requested because deleting the lot would create an orphan that requires explicit assignment to the default lot. Set assign_LTBR_parent_as_parent_to_orphans=true.");
                }
            }
            if (reassignment_policy.assign_LTBR_parent_as_parent_to_non_orphans) {
                auto rp_bool_str = this->check_if_root();
                if (!rp_bool_str.second.empty()) {
                    std::string int_err = rp_bool_str.second;
                    std::string ext_err = "Function call to lotman::Lot::check_if_root failed: ";
                    return std::make_pair(false, ext_err + int_err);
                }
                if (is_root) {
                    return std::make_pair(false, "The lot being removed is a root, and has no parents to assign to its children.");
                }
                this->get_parents();
                // Now assign parents of LTBR to non-orphan children of LTBR
                // No need to perform cycle check in this case
                rp_bool_str = child.add_parents(self_parents);
                if (!rp_bool_str.first) {
                    std::string int_err = rp_bool_str.second;
                    std::string ext_err = "Function call to lotman::Lot::add_parents for child lot failed: ";
                    return std::make_pair(false, ext_err + int_err);
                }
            }
        }
        auto rp_bool_str = delete_lot_from_db();
        if (!rp_bool_str.first) {
            std::string int_err = rp_bool_str.second;
            std::string ext_err = "Function call to lotman::Lot::delete_lot_from_db failed: ";
            return std::make_pair(false, ext_err + int_err);
        }
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}

std::pair<bool, std::string> lotman::Lot::destroy_lot_recursive(){

    if (lot_name == "default") {
        return std::make_pair(false, "The default lot cannot be deleted.");
    }
    try {
        // Prechecks complete, get the children for LTBR
        auto rp_lotvec_str = this->get_children(true);
        if (!rp_lotvec_str.second.empty()) { // There is an error message
            std::string int_err = rp_lotvec_str.second;
            std::string ext_err = "Failed to get lot children: ";
            return std::make_pair(false, ext_err + int_err);
        }

        for (auto &child : recursive_children) {
            auto rp_bool_str = child.delete_lot_from_db();
            if (!rp_bool_str.first) {
                std::string int_err = rp_bool_str.second;
                std::string ext_err = "Failed to delete a lot from the database: ";
                return std::make_pair(false, ext_err + int_err);
            }
        }
        this->delete_lot_from_db();
         return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}

std::pair<std::vector<Lot>, std::string> lotman::Lot::get_parents(const bool recursive, const bool get_self) {
   try {
        std::vector<Lot> parents;
        std::vector<std::string> parent_names_vec;
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
        auto rp = lotman::Checks::SQL_get_matches(parents_query, parents_query_str_map);
        if (!rp.second.empty()) {
            std::string int_err = rp.second;
            std::string ext_err = "Failure on call to SQL_get_matches when getting parents in get_parents: ";
            return std::make_pair(std::vector<Lot>(), ext_err + int_err);
        }

        parent_names_vec = rp.first;
        if (recursive) {
            std::vector<std::string> current_parents{parent_names_vec};
            parents_query = "SELECT parent FROM parents WHERE lot_name = ? AND parent != ?;"; 
            while (current_parents.size()>0) {
                std::vector<std::string> grandparent_names;
                for (const auto &parent : current_parents) {
                    parents_query_str_map = {{parent, {1,2}}};
                    rp = lotman::Checks::SQL_get_matches(parents_query, parents_query_str_map);
                    if (!rp.second.empty()) { //There is an error message
                        std::string int_err = rp.second;
                        std::string ext_err = "Function call to SQL_get_matches failed: ";
                        return std::make_pair(std::vector<Lot>(), ext_err + int_err);
                    }
                    std::vector<std::string> tmp{rp.first};
                    grandparent_names.insert(grandparent_names.end(), tmp.begin(), tmp.end());
                }

                // grandparent_names might have duplicates. Sort and make unique
                std::sort(grandparent_names.begin(), grandparent_names.end());
                auto last = std::unique(grandparent_names.begin(), grandparent_names.end());
                grandparent_names.erase(last, grandparent_names.end());

                current_parents = grandparent_names;
                parent_names_vec.insert(parent_names_vec.end(), grandparent_names.begin(), grandparent_names.end());
            }
        }

        // Final sort
        std::sort(parent_names_vec.begin(), parent_names_vec.end());
        auto last = std::unique(parent_names_vec.begin(), parent_names_vec.end());
        parent_names_vec.erase(last, parent_names_vec.end());

        // children_names now has names of all children according to get_self and recursion.
        // Create lot objects and return vector of lots
        for (const auto &parent_name : parent_names_vec) {
            Lot lot(parent_name);
            parents.push_back(lot);
        }

        // Assign to lot member vars.
        if (recursive) {
            recursive_parents = parents;
        }
        else {
            self_parents = parents;
        }
        return std::make_pair(parents, "");
   }
   catch (std::exception &exc) {
        return std::make_pair(std::vector<Lot>(), exc.what());
   }
}

std::pair<std::vector<Lot>, std::string> lotman::Lot::get_children(const bool recursive, const bool get_self) {
    
    /*
    FUNCTION FLOW
    * Create queries and maps based on get_self
    * Get vector of immediate children
    * If recursive, get children of children
    */
    try {
        std::vector<std::string> children_names;
        std::vector<Lot> children;
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

        // Get first round of children
        auto rp = lotman::Checks::SQL_get_matches(children_query, children_query_str_map); // get all lots who specify lot-to-be-removed (LTBR) as a parent.
        if (!rp.second.empty()) { //There is an error message
            std::string int_err = rp.second;
            std::string ext_err = "Function call to SQL_get_matches failed: ";
            return std::make_pair(std::vector<Lot>(), ext_err + int_err);
        }

        children_names = rp.first;
        if (recursive) {
            std::vector<std::string> current_children_names{children_names};
            children_query = "SELECT lot_name FROM parents WHERE parent = ? AND lot_name != ?;"; 
            while (current_children_names.size()>0) {
                std::vector<std::string> grandchildren_names;
                for (const auto &child_name : current_children_names) {
                    children_query_str_map = {{child_name, {1,2}}};
                    rp = lotman::Checks::SQL_get_matches(children_query, children_query_str_map);
                    if (!rp.second.empty()) { //There is an error message
                        std::string int_err = rp.second;
                        std::string ext_err = "Function call to SQL_get_matches failed: ";
                        return std::make_pair(std::vector<Lot>(), ext_err + int_err);
                    }
                    std::vector<std::string> tmp{rp.first};
                    grandchildren_names.insert(grandchildren_names.end(), tmp.begin(), tmp.end());
                }
                // grandchildren_names might have duplicates. Sort and make unique
                std::sort(grandchildren_names.begin(), grandchildren_names.end());
                auto last = std::unique(grandchildren_names.begin(), grandchildren_names.end());
                grandchildren_names.erase(last, grandchildren_names.end());

                current_children_names = grandchildren_names;
                children_names.insert(children_names.end(), grandchildren_names.begin(), grandchildren_names.end());
            }
        }

        // Final sort
        std::sort(children_names.begin(), children_names.end());
        auto last = std::unique(children_names.begin(), children_names.end());
        children_names.erase(last, children_names.end());
        
        // children_names now has names of all children according to get_self and recursion.
        // Create lot objects and return vector of lots
        for (const auto &child_name : children_names) {
            Lot lot(child_name);
            children.push_back(lot);
        }
            
        // Assign to lot member vars
        if (recursive) {
            recursive_children = children;
        }
        else {
            self_children = children;
        }
        return std::make_pair(children, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(std::vector<Lot>(), exc.what());
    }
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_owners(const bool recursive) {
    try {
        std::vector<std::string> lot_owners_vec;
        std::string owners_query = "SELECT owner FROM owners WHERE lot_name = ?;"; 
        std::map<std::string, std::vector<int>> owners_query_str_map{{lot_name, {1}}};
        auto rp = lotman::Checks::SQL_get_matches(owners_query, owners_query_str_map);
        if (!rp.second.empty()) { // There was an error
            std::string int_err = rp.second;
            std::string ext_err = "Failure on call to SQL_get_matches when getting owners: ";
            return std::make_pair(std::vector<std::string>(), ext_err + int_err);
        }
        lot_owners_vec.push_back(rp.first[0]);

        if (recursive) {
            auto rp2 = this->get_parents(true, false);
            if (!rp2.second.empty()) { // There is an error message
                std::string int_err = rp2.second;
                std::string ext_err = "Failure to get parents: ";
                return std::make_pair(std::vector<std::string>(), ext_err + int_err);
            }
            std::vector<Lot> parents = rp2.first;

            for (const auto &parent : parents) {
                std::map<std::string, std::vector<int>> owners_query_str_map{{parent.lot_name, {1}}};
                rp = lotman::Checks::SQL_get_matches(owners_query, owners_query_str_map);
                if (!rp.second.empty()) { // There is an error message
                    std::string int_err = rp.second;
                    std::string ext_err = "Failure to get owner of parent: ";
                    return std::make_pair(std::vector<std::string>(), ext_err + int_err);
                }

                std::vector<std::string> tmp{rp.first};
                lot_owners_vec.insert(lot_owners_vec.end(), tmp.begin(), tmp.end());
            }
        }

        // Sort and remove any duplicates
        std::sort(lot_owners_vec.begin(), lot_owners_vec.end());
        auto last = std::unique(lot_owners_vec.begin(), lot_owners_vec.end());
        lot_owners_vec.erase(last, lot_owners_vec.end());

        // Assign to lot vars
        if (recursive) {
            recursive_owners = lot_owners_vec;
        }
        else {
            self_owner = lot_owners_vec[0]; // Lots only have one explicit owner, so this is where it must be in the vector.
        }
        return std::make_pair(lot_owners_vec, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(std::vector<std::string>(), exc.what());
    }
}

std::pair<json, std::string> lotman::Lot::get_restricting_attribute(const std::string key,
                                                                     const bool recursive) {
    try {
        json internal_obj;
        std::vector<std::string> value;
        std::array<std::string, 6> allowed_keys{{"dedicated_GB", "opportunistic_GB", "max_num_objects", "creation_time", "expiration_time", "deletion_time"}};
        if (std::find(allowed_keys.begin(), allowed_keys.end(), key) != allowed_keys.end()) {
            std::string policy_attr_query = "SELECT " + key + " FROM management_policy_attributes WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> policy_attr_query_str_map{{lot_name, {1}}};
            auto rp = lotman::Checks::SQL_get_matches(policy_attr_query, policy_attr_query_str_map);
            if (!rp.second.empty()) { // There was an error
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(json(), ext_err + int_err);
            }
            value = rp.first;
            std::string restricting_parent_name = lot_name;

            if (recursive) {
                auto rp2 = this->get_parents(true);
                if (!rp2.second.empty()) { // There was an error
                    std::string int_err = rp2.second;
                    std::string ext_err = "Failure to get lot parents: ";
                    return std::make_pair(json(), ext_err + int_err);
                }

                std::vector<lotman::Lot> parents = rp2.first;
                for (const auto &parent : parents) {
                    std::map<std::string, std::vector<int>> policy_attr_query_parent_str_map{{parent.lot_name, {1}}};
                    rp = lotman::Checks::SQL_get_matches(policy_attr_query, policy_attr_query_parent_str_map);
                    if (!rp.second.empty()) { // There was an error
                        std::string int_err = rp.second;
                        std::string ext_err = "Failure on call to SQL_get_matches: ";
                        return std::make_pair(json(), ext_err + int_err);
                    }

                    std::vector<std::string> compare_value = rp.first;
                    if (std::stod(compare_value[0]) < std::stod(value[0])) {
                        value[0] = compare_value[0];
                        restricting_parent_name = parent.lot_name;
                    }
                }
                internal_obj["lot_name"] = restricting_parent_name;
                internal_obj["value"] = std::stod(value[0]);
            }
            return std::make_pair(internal_obj, "");
        }
        else {
            return std::make_pair(json(), " The key \"" + key + "\" is not recognized.");
        }
    }
    catch (std::exception &exc) {
        return std::make_pair(json(), exc.what());
    }                                                        
}

std::pair<json, std::string> lotman::Lot::get_lot_dirs(const bool recursive) {
    try {
        json path_obj;
        std::string path_query = "SELECT path, recursive FROM paths WHERE lot_name = ?;"; 
        //std::string recursive_query = "SELECT recursive FROM paths WHERE path = ? AND lot_name = ?;";
        std::map<std::string, std::vector<int>> path_query_str_map{{lot_name, {1}}};
        auto rp =  lotman::Checks::SQL_get_matches_multi_col(path_query, 2, path_query_str_map);
        if (!rp.second.empty()) { // There was an error
            std::string int_err = rp.second;
            std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
            return std::make_pair(path_obj, ext_err + int_err);
        }

        for (const auto &path_rec : rp.first) { // the path is at index 0, and the recursion of the path is at index 1
            json path_obj_internal;
            path_obj_internal["lot_name"] = this->lot_name;
            path_obj_internal["recursive"] = static_cast<bool>(std::stoi(path_rec[1]));
            path_obj[path_rec[0]] = path_obj_internal;
        }

        if (recursive) { // Not recursion of path, but recursion of dirs associated to a lot, ie the directories associated with lot proper's children
            auto rp_vec_str = this->get_children(true);
            if (!rp_vec_str.second.empty()) { // There was an error
                std::string int_err = rp_vec_str.second;
                std::string ext_err = "Failure to get children.";
                return std::make_pair(json(), ext_err+int_err);
            }
            for (const auto &child : recursive_children) {
                std::map<std::string, std::vector<int>> child_path_query_str_map{{child.lot_name, {1}}};
                rp = lotman::Checks::SQL_get_matches_multi_col(path_query, 2, child_path_query_str_map);
                if (!rp.second.empty()) { // There was an error
                    std::string int_err = rp.second;
                    std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
                    return std::make_pair(path_obj, ext_err + int_err);
                }

                for (const auto &path_rec : rp.first) {
                    json path_obj_internal;
                    path_obj_internal["lot_name"] = child.lot_name;
                    path_obj_internal["recursive"] = static_cast<bool>(std::stoi(path_rec[1]));
                    path_obj[path_rec[0]] = path_obj_internal;
                }
            }
        }
        
        return std::make_pair(path_obj, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(json(), exc.what());
    }
}

std::pair<json, std::string> lotman::Lot::get_lot_usage(const std::string key, const bool recursive) {

// TODO: Introduce some notion of verbocity to give options for output, like:
    // {"dedicated_GB" : 10} vs {"dedicated_GB" : {"personal": 5, "children" : 5}} vs {"dedicated_GB" : {"personal" : 5, "child1" : 2.5, "child2" : 2.5}}
    // Think a bit more about whether this makes sense.

    // TODO: Might be worthwhile to join some of these sections that share a common preamble

    json output_obj;
    std::array<std::string, 6> allowed_keys = {"dedicated_GB", "opportunistic_GB", "total_GB", "num_objects", "GB_being_written", "objects_being_written"};
    if (std::find(allowed_keys.begin(), allowed_keys.end(), key) == allowed_keys.end()) {
        return std::make_pair(json(), "The key \"" + key + "\" is not recognized.");
    }

    std::vector<std::string> query_output;
    std::vector<std::vector<std::string>> query_multi_out;

    if (key == "dedicated_GB") {
        if (recursive) {
            std::string rec_ded_usage_query =   "SELECT "
                                                    "CASE "
                                                        "WHEN lot_usage.self_GB + lot_usage.children_GB <= management_policy_attributes.dedicated_GB THEN lot_usage.self_GB + lot_usage.children_GB "
                                                        "ELSE management_policy_attributes.dedicated_GB "
                                                    "END AS total, " // For readability, not actually referencing these column names
                                                    "CASE "
                                                        "WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB THEN management_policy_attributes.dedicated_GB "
                                                        "ELSE lot_usage.self_GB "
                                                    "END AS self_contrib, "
                                                    "CASE "
                                                        "WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB THEN '0' "
                                                        "WHEN lot_usage.self_GB + lot_usage.children_GB >= management_policy_attributes.dedicated_GB THEN management_policy_attributes.dedicated_GB - lot_usage.self_GB "
                                                        "ELSE lot_usage.children_GB "
                                                    "END AS children_contrib "
                                                "FROM lot_usage "
                                                    "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                "WHERE lot_usage.lot_name = ?;";
            std::map<std::string, std::vector<int>> ded_GB_query_str_map{{lot_name, {1}}};
            auto rp_multi = lotman::Checks::SQL_get_matches_multi_col(rec_ded_usage_query, 3, ded_GB_query_str_map);
            if (!rp_multi.second.empty()) { // There was an error
                std::string int_err = rp_multi.second;
                std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
                return std::make_pair(json(), ext_err + int_err);
            }
            query_multi_out = rp_multi.first;
            output_obj["total"] = std::stod(query_multi_out[0][0]);
            output_obj["self_contrib"] = std::stod(query_multi_out[0][1]);
            output_obj["children_contrib"] = std::stod(query_multi_out[0][2]);
        }
        else {
            std::string ded_GB_query = "SELECT "
                                            "CASE "
                                                "WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB THEN management_policy_attributes.dedicated_GB "
                                                "ELSE lot_usage.self_GB "
                                            "END AS total "
                                        "FROM "
                                            "lot_usage "
                                            "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                        "WHERE lot_usage.lot_name = ?;";
        
            std::map<std::string, std::vector<int>> ded_GB_query_str_map{{lot_name, {1}}};
            auto rp_single = lotman::Checks::SQL_get_matches(ded_GB_query, ded_GB_query_str_map);
            if (!rp_single.second.empty()) { // There was an error
                std::string int_err = rp_single.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(json(), ext_err + int_err);
            }
            query_output = rp_single.first;
            output_obj["total"] = std::stod(query_output[0]);
        }
    }

    else if (key == "opportunistic_GB") {
        if (recursive) {
            std::string rec_opp_usage_query =   "SELECT "
                                                    "CASE "
                                                        "WHEN lot_usage.self_GB + lot_usage.children_GB >= management_policy_attributes.opportunistic_GB +management_policy_attributes.dedicated_GB THEN management_policy_attributes.opportunistic_GB "
                                                        "WHEN lot_usage.self_GB + lot_usage.children_GB >= management_policy_attributes.dedicated_GB THEN lot_usage.self_GB + lot_usage.children_GB - management_policy_attributes.dedicated_GB "
                                                        "ELSE '0' "
                                                    "END AS total, "
                                                    "CASE "
                                                        "WHEN lot_usage.self_GB >= management_policy_attributes.opportunistic_GB + management_policy_attributes.dedicated_GB THEN management_policy_attributes.opportunistic_GB "
                                                        "WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB THEN  lot_usage.self_GB - management_policy_attributes.dedicated_GB "
                                                        "ELSE '0' "
                                                    "END AS self_contrib, "
                                                    "CASE "
                                                        "WHEN lot_usage.self_GB >= management_policy_attributes.opportunistic_GB + management_policy_attributes.dedicated_GB THEN '0' "
                                                        "WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB AND lot_usage.self_GB + lot_usage.children_GB >= management_policy_attributes.opportunistic_GB + management_policy_attributes.dedicated_GB THEN management_policy_attributes.opportunistic_GB + management_policy_attributes.dedicated_GB - lot_usage.self_GB "
                                                        "WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB AND lot_usage.self_GB + lot_usage.children_GB < management_policy_attributes.opportunistic_GB + management_policy_attributes.dedicated_GB THEN lot_usage.children_GB "
                                                        "WHEN lot_usage.self_GB < management_policy_attributes.dedicated_GB AND lot_usage.self_GB + lot_usage.children_GB >= management_policy_attributes.opportunistic_GB + management_policy_attributes.dedicated_GB THEN management_policy_attributes.opportunistic_GB "
                                                        "WHEN lot_usage.self_GB < management_policy_attributes.dedicated_GB AND lot_usage.self_GB + lot_usage.children_GB > management_policy_attributes.dedicated_GB THEN lot_usage.self_GB + lot_usage.children_GB - management_policy_attributes.dedicated_GB "
                                                        "ELSE '0' "
                                                    "END AS children_contrib "
                                                "FROM "
                                                    "lot_usage "
                                                    "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                "WHERE lot_usage.lot_name = ?;";
            std::map<std::string, std::vector<int>> opp_GB_query_str_map{{lot_name, {1}}};
            auto rp_multi = lotman::Checks::SQL_get_matches_multi_col(rec_opp_usage_query, 3, opp_GB_query_str_map);
            if (!rp_multi.second.empty()) { // There was an error
                std::string int_err = rp_multi.second;
                std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
                return std::make_pair(json(), ext_err + int_err);
            }
            query_multi_out = rp_multi.first;
            output_obj["total"] = std::stod(query_multi_out[0][0]);
            output_obj["self_contrib"] = std::stod(query_multi_out[0][1]);
            output_obj["children_contrib"] = std::stod(query_multi_out[0][2]);
        }
        else {
            std::string opp_GB_query =  "SELECT "
                                            "CASE "
                                                "WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB + management_policy_attributes.opportunistic_GB THEN management_policy_attributes.opportunistic_GB "
                                                "WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB THEN lot_usage.self_GB = management_policy_attributes.dedicated_GB "
                                                "ELSE '0' "
                                            "END AS total "
                                        "FROM "
                                            "lot_usage "
                                            "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                        "WHERE lot_usage.lot_name = ?;";

        std::map<std::string, std::vector<int>> opp_GB_query_str_map{{lot_name, {1}}};
        auto rp_single = lotman::Checks::SQL_get_matches(opp_GB_query, opp_GB_query_str_map);
        if (!rp_single.second.empty()) { // There was an error
            std::string int_err = rp_single.second;
            std::string ext_err = "Failure on call to SQL_get_matches: ";
            return std::make_pair(json(), ext_err + int_err);
        }
        query_output = rp_single.first;
        output_obj["total"] = std::stod(query_output[0]);
        }
    }

    else if (key == "total_GB") {
        // Get the total usage
        if (recursive) {
            // Need to consider usage from children
            std::string child_usage_GB_query = "SELECT self_GB, children_GB FROM lot_usage WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> child_usage_GB_str_map{{lot_name, {1}}};
            auto rp_multi = lotman::Checks::SQL_get_matches_multi_col(child_usage_GB_query, 2, child_usage_GB_str_map);
            if (!rp_multi.second.empty()) { // There was an error
                std::string int_err = rp_multi.second;
                std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
                return std::make_pair(json(), ext_err + int_err);
            }
            query_multi_out = rp_multi.first;
            output_obj["self_contrib"] = std::stod(query_multi_out[0][0]);
            output_obj["children_contrib"] = std::stod(query_multi_out[0][1]);
        }
        else {
            std::string usage_GB_query = "SELECT self_GB FROM lot_usage WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> usage_GB_str_map{{lot_name, {1}}};
            auto rp_single = lotman::Checks::SQL_get_matches(usage_GB_query, usage_GB_str_map);
            if (!rp_single.second.empty()) { // There was an error
                std::string int_err = rp_single.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(json(), ext_err + int_err);
            }
            query_output = rp_single.first;
            output_obj["total"] = std::stod(query_output[0]);
        }
    }
    
    else if (key == "num_objects") {
        if (recursive) {
            std::string rec_num_obj_query = "SELECT self_objects, children_objects FROM lot_usage WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> rec_num_obj_str_map{{lot_name, {1}}};
            auto rp_multi = lotman::Checks::SQL_get_matches_multi_col(rec_num_obj_query, 2, rec_num_obj_str_map);
            if (!rp_multi.second.empty()) { // There was an error
                std::string int_err = rp_multi.second;
                std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
                return std::make_pair(json(), ext_err + int_err);
            }
            query_multi_out = rp_multi.first;
            output_obj["self_contrib"] = std::stod(query_multi_out[0][0]);
            output_obj["children_contrib"] = std::stod(query_multi_out[0][1]);
        }
        else {

            std::string num_obj_query = "SELECT self_objects FROM lot_usage WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> num_obj_str_map{{lot_name, {1}}};
            auto rp_single = lotman::Checks::SQL_get_matches(num_obj_query, num_obj_str_map);
            if (!rp_single.second.empty()) { // There was an error
                std::string int_err = rp_single.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(json(), ext_err + int_err);
            }
            query_output = rp_single.first;
            output_obj["total"] = std::stod(query_output[0]);
        }
    }

    else if (key == "GB_being_written") {
        if (recursive) {
            std::string rec_GB_being_written_query = "SELECT self_GB_being_written, children_GB_being_written FROM lot_usage WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> rec_GB_being_written_str_map{{lot_name, {1}}};
            auto rp_multi = lotman::Checks::SQL_get_matches_multi_col(rec_GB_being_written_query, 2, rec_GB_being_written_str_map);
            if (!rp_multi.second.empty()) { // There was an error
                std::string int_err = rp_multi.second;
                std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
                return std::make_pair(json(), ext_err + int_err);
            }
            query_multi_out = rp_multi.first;
            output_obj["self_contrib"] = std::stod(query_multi_out[0][0]);
            output_obj["children_contrib"] = std::stod(query_multi_out[0][1]);
        }
        else {

            std::string GB_being_written_query = "SELECT self_GB_being_written FROM lot_usage WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> GB_being_written_str_map{{lot_name, {1}}};
            auto rp_single = lotman::Checks::SQL_get_matches(GB_being_written_query, GB_being_written_str_map);
            if (!rp_single.second.empty()) { // There was an error
                std::string int_err = rp_single.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(json(), ext_err + int_err);
            }
            query_output = rp_single.first;
            output_obj["total"] = std::stod(query_output[0]);
        }
    }
    
    else if (key == "objects_being_written") {
        if (recursive) {
            std::string rec_objects_being_written_query = "SELECT self_objects_being_written, children_objects_being_written FROM lot_usage WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> rec_objects_being_written_str_map{{lot_name, {1}}};
            auto rp_multi = lotman::Checks::SQL_get_matches_multi_col(rec_objects_being_written_query, 2, rec_objects_being_written_str_map);
            if (!rp_multi.second.empty()) { // There was an error
                std::string int_err = rp_multi.second;
                std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
                return std::make_pair(json(), ext_err + int_err);
            }
            query_multi_out = rp_multi.first;
            output_obj["self_contrib"] = std::stod(query_multi_out[0][0]);
            output_obj["children_contrib"] = std::stod(query_multi_out[0][1]);
        }
        else {

            std::string objects_being_written_query = "SELECT self_objects_being_written FROM lot_usage WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> objects_being_written_str_map{{lot_name, {1}}};
            auto rp_single = lotman::Checks::SQL_get_matches(objects_being_written_query, objects_being_written_str_map);
            if (!rp_single.second.empty()) { // There was an error
                std::string int_err = rp_single.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(json(), ext_err + int_err);
            }
            query_output = rp_single.first;
            output_obj["total"] = std::stod(query_output[0]);
        }
    }

    return std::make_pair(output_obj, "");
}

std::pair<bool, std::string> lotman::Lot::add_parents(std::vector<Lot> parents) {
    try {
        // Perform a cycle check
        // Build the list of all proposed parents
        std::vector<std::string> parent_names;
        this->get_parents(true, true);
        for (const auto & parent : recursive_parents) {
            parent_names.push_back(parent.lot_name);
        }
        for (const auto & parent_lot : parents) {
            parent_names.push_back(parent_lot.lot_name);
        }

        // Build list of all children, minus self
        std::vector<std::string> children_names;
        this->get_children(true, false);
        for (const auto &child : recursive_children) {
            children_names.push_back(child.lot_name);
        }

        // Perform the cycle check
        if (Checks::cycle_check(lot_name, parent_names, children_names)) {
            std::string err = "The requested parent addition would introduce a dependency cycle.";
            return std::make_pair(false, err);
        }

        // We've passed the cycle check, store the parents
        auto rp = store_new_parents(parents);
        if (!rp.first) {
            std::string int_err = rp.second;
            std::string ext_err = "Call to lotman::Lot::store_new_parents failed: ";
            return std::make_pair(false, ext_err + int_err);
        }
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}

std::pair<bool, std::string> lotman::Lot::add_paths(std::vector<json> paths) {
    try {
        auto rp = store_new_paths(paths);
        if (!rp.first) {
            std::string int_err = rp.second;
            std::string ext_err = "Call to lotman::Lot::store_new_paths failed: ";
            return std::make_pair(false, ext_err + int_err);
        }
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}

std::pair<bool, std::string> lotman::Lot::update_owner(std::string update_val) {
    try {
        std::string owner_update_stmt = "UPDATE owners SET owner=? WHERE lot_name=?;";

        std::map<std::string, std::vector<int>> owner_update_str_map{{lot_name, {2}}, {update_val, {1}}};
        auto rp = store_updates(owner_update_stmt, owner_update_str_map);
        if (!rp.first) {
            std::string int_err = rp.second;
            std::string ext_err = "Failure on call to lotman::Lot::store_updates when storing owner update: ";
            return std::make_pair(false, ext_err + int_err);
        }
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}

std::pair<bool, std::string> lotman::Lot::update_parents(json update_arr) {
    try {    
        // First, perform a cycle check on the whole update arr, and fail if any introduce a cycle
        // Cycle check takes in three arguments -- The start node (in this case, lot_name), and vectors of parents/children of the start node as strings
        
        // Get all the existing parents
        std::vector<std::string> parents;
        this->get_parents(true, true); // get_self is true because either we need get_self to be true for get_parents or get_children.

        for (const auto& parent_lot : recursive_parents) {
            parents.push_back(parent_lot.lot_name);
        }
        // for each existing parent, if it's being updated, swap it out with the new parent.
        for (const auto &update : update_arr) {
            auto parent_iter = std::find(parents.begin(), parents.end(), update["current"]);
            if (parent_iter != parents.end()) {
                *parent_iter = update["new"];
            }
            else {
                return std::make_pair(false, "One of the current parents, " + update["current"].get<std::string>() + ", to be updated is not actually a parent.");
            }
        }

        std::vector<std::string> children;
        this->get_children(true, false);
        for (const auto& child_lot : recursive_children) {
            children.push_back(child_lot.lot_name);
        }

        if (Checks::cycle_check(lot_name, parents, children)) {
            std::string err = "The requested parent update would introduce a dependency cycle.";
            return std::make_pair(false, err);
        }

        std::string parents_update_stmt = "UPDATE parents SET parent=? WHERE lot_name=? AND parent=?";
        // Need to store modifications per map entry
        for (const auto &update_obj : update_arr) {
            std::map<std::string, std::vector<int>> parents_update_str_map;
            if (update_obj["current"] == lot_name) {
                parents_update_str_map = {{update_obj["new"], {1}}, {update_obj["current"], {2,3}}};
            }
            else {
                 parents_update_str_map = {{update_obj["new"], {1}}, {lot_name, {2}}, {update_obj["current"], {3}}};
            }
            auto rp = store_updates(parents_update_stmt, parents_update_str_map);
            if (!rp.first) {
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to lotman::Lot::store_updates when storing parents update: ";
                return std::make_pair(false, ext_err + int_err);
            }
        }
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}

std::pair<bool, std::string> lotman::Lot::update_paths(json update_arr) {
    try {
        // incoming update map looks like {"path1" --> {"path" : "path2", "recursive" : false}}
        std::string paths_update_stmt = "UPDATE paths SET path=? WHERE lot_name=? and path=?;";
        std::string recursive_update_stmt = "UPDATE paths SET recursive=? WHERE lot_name=? and path=?;";

        // Iterate through updates, first perform recursive update THEN path
        for (const auto &update_obj : update_arr/*update_map*/) {
            std::map<int64_t, std::vector<int>> recursive_update_int_map{{update_obj["recursive"].get<int>(), {1}}}; // Unfortunately using int64_t for these bools to write less code
            std::map<std::string, std::vector<int>> recursive_update_str_map{{lot_name, {2}}, {/*pair.first*/ update_obj["current"], {3}}};
            auto rp = store_updates(recursive_update_stmt, recursive_update_str_map, recursive_update_int_map);
            if (!rp.first) {
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to lotman::Lot::store_updates when storing paths recursive update: ";
                return std::make_pair(false, ext_err + int_err);
            }
            
            // Path update
            std::map<std::string, std::vector<int>> paths_update_str_map{{update_obj["new"], {1}}, {lot_name, {2}}, {/*pair.first*/ update_obj["current"], {3}}};
            rp = store_updates(paths_update_stmt, paths_update_str_map);
            if (!rp.first) {
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to lotman::Lot::store_updates when storing paths path update: ";
                return std::make_pair(false, ext_err + int_err);
            }
        }
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }

}

std::pair<bool, std::string> lotman::Lot::update_man_policy_attrs(std::string update_key, double update_val) {
    try {
        std::string man_policy_attr_update_stmt_first_half = "UPDATE management_policy_attributes SET ";
        std::string man_policy_attr_update_stmt_second_half = "=? WHERE lot_name=?;";

        std::array<std::string, 2> dbl_keys = {"dedicated_GB", "opportunistic_GB"};
        std::array<std::string, 4> int_keys = {"max_num_objects", "creation_time", "expiration_time", "deletion_time"};

        if (std::find(dbl_keys.begin(), dbl_keys.end(), update_key) != dbl_keys.end()) {
            std::string man_policy_attr_update_stmt = man_policy_attr_update_stmt_first_half + update_key + man_policy_attr_update_stmt_second_half;
            std::map<std::string, std::vector<int>> man_policy_attr_update_str_map{{lot_name, {2}}};
            std::map<double, std::vector<int>> man_policy_attr_update_dbl_map{{update_val, {1}}};
            auto rp = store_updates(man_policy_attr_update_stmt, man_policy_attr_update_str_map, std::map<int64_t, std::vector<int>>(), man_policy_attr_update_dbl_map);
            if (!rp.first) {
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to lotman::Lot::store_updates when storing management policy attribute update: ";
                return std::make_pair(false, ext_err + int_err);
            }
        }
        else if (std::find(int_keys.begin(), int_keys.end(), update_key) != int_keys.end()) {
            std::string man_policy_attr_update_stmt = man_policy_attr_update_stmt_first_half + update_key + man_policy_attr_update_stmt_second_half;
            std::map<std::string, std::vector<int>> man_policy_attr_update_str_map{{lot_name, {2}}};
            std::map<int64_t, std::vector<int>> man_policy_attr_update_int_map{{update_val, {1}}};
            auto rp = store_updates(man_policy_attr_update_stmt, man_policy_attr_update_str_map, man_policy_attr_update_int_map);
            if (!rp.first) {
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to lotman::Lot::store_updates when storing management policy attribute update: ";
                return std::make_pair(false, ext_err + int_err);
            }
        }
        else {
            return std::make_pair(false, "Update key not found or not recognized.");
        }
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}

 std::pair<bool, std::string> lotman::Lot::update_parent_usage(Lot parent, std::string update_stmt, std::map<std::string, std::vector<int>> update_str_map,
                                                                std::map<int64_t, std::vector<int>> update_int_map, std::map<double, std::vector<int>> update_dbl_map) {
    try {
        auto rp = parent.store_updates(update_stmt, update_str_map, update_int_map, update_dbl_map);
        if (!rp.first) { // There was an error
            std::string int_err = rp.second;
            std::string ext_err = "Failure on call to store_updates for parent: ";
            return std::make_pair(false, ext_err + int_err);
        }
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
 }

std::pair<bool, std::string> lotman::Lot::update_self_usage(const std::string key, const double value) {

    /*
    Function Flow for update_lot_usage:
    * Sanitize inputs by making sure key is allowed/known
    * Get the current usage, used to calculate delta for updating parents' children_* columns
    * Calculate delta
    * Update lot proper
    * For each parent of lot proper, update children_key += delta
    */

    try {
        std::array<std::string, 2> allowed_int_keys={"self_objects", "self_objects_being_written"};
        std::array<std::string, 2> allowed_double_keys={"self_GB", "self_GB_being_written"};
        std::string update_usage_dynamic_query = "UPDATE lot_usage SET " + key + "=? WHERE lot_name=?;";
        std::map<std::string, std::vector<int>> update_usage_str_map = {{lot_name, {2}}};

        // Sanitize inputs
        if (std::find(allowed_int_keys.begin(), allowed_int_keys.end(), key) != allowed_int_keys.end()) {
            // Get the current value for the lot
            std::string get_usage_query = "SELECT " + key + " FROM lot_usage WHERE lot_name = ?;";
            std::map<std::string, std::vector<int>> get_usage_query_str_map{{lot_name, {1}}};
            auto rp = lotman::Checks::SQL_get_matches(get_usage_query, get_usage_query_str_map);

            if (!rp.second.empty()) { // There was an error
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(false, ext_err + int_err);
            }

            int current_usage = std::stoi(rp.first[0]);
            int delta = value - current_usage;

            // Update lot proper
            std::string update_usage_stmt = "UPDATE lot_usage SET " + key + "=? WHERE lot_name=?;";
            std::map<std::string, std::vector<int>> update_usage_str_map = {{lot_name, {2}}};
            std::map<int64_t, std::vector<int>> update_usage_int_map = {{value, {1}}};

            auto rp_bool_str = this->store_updates(update_usage_dynamic_query, update_usage_str_map, update_usage_int_map);
            if (!rp_bool_str.first) {
                std::string int_err = rp_bool_str.second;
                std::string ext_err = "Failure on call to store_updates: ";
                return std::make_pair(false, ext_err + int_err);
            }

            /* TODO: need some kind of recovery file if we start updating parents and then fail --> for each parent, store  in temp file current usage for key
                        and delete temp file after done. Use a function "repair_db" that checks for existence of temp file and restores things to those values,
                        deleting the temp file upon completion.
            */

            // Update parents
            auto rp_lotvec_str = this->get_parents(true);
            if (!rp_lotvec_str.second.empty()) { // There was an error
                std::string int_err = rp_lotvec_str.second;
                std::string ext_err = "Failure on call to get_parents: ";
                return std::make_pair(false, ext_err + int_err);
            }

            std::string children_key = "children" + key.substr(4);
            std::string parent_usage_query = "SELECT " + children_key + " FROM lot_usage WHERE lot_name = ?;";
            for (const auto &parent : recursive_parents) {
                std::map<std::string, std::vector<int>> parent_usage_query_str_map{{parent.lot_name, {1}}};
                auto rp_vec_str = lotman::Checks::SQL_get_matches(parent_usage_query, parent_usage_query_str_map);
                if (!rp_vec_str.second.empty()) { // There was an error
                    std::string int_err = rp_vec_str.second;
                    std::string ext_err = "Failure on call to get_parents: ";
                    return std::make_pair(false, ext_err + int_err);
                }
                int current_usage = std::stoi(rp_vec_str.first[0]);
                std::string update_parent_usage_stmt = "UPDATE lot_usage SET " + children_key + "=? WHERE lot_name=?;";
                std::map<std::string, std::vector<int>> update_parent_str_map{{parent.lot_name, {2}}};
                std::map<int64_t, std::vector<int>> update_parent_int_map{{current_usage+delta, {1}}}; // Update children_key to current_usage + delta
                rp_bool_str = this->update_parent_usage(parent, update_parent_usage_stmt, update_parent_str_map, update_parent_int_map);
                if (!rp_bool_str.first) { // There was an error
                    std::string int_err = rp_bool_str.second;
                    std::string ext_err = "Failure on call to store_updates when updating parent usage: ";
                    return std::make_pair(false, ext_err + int_err);
                }
            }
        }

        else if (std::find(allowed_double_keys.begin(), allowed_double_keys.end(), key) != allowed_double_keys.end()) {
        // Get the current value for the lot
        std::string get_usage_query = "SELECT " + key + " FROM lot_usage WHERE lot_name = ?;";
        std::map<std::string, std::vector<int>> get_usage_query_str_map{{lot_name, {1}}};
        auto rp_vec_str = lotman::Checks::SQL_get_matches(get_usage_query, get_usage_query_str_map);
        if (!rp_vec_str.second.empty()) {
            std::string int_err = rp_vec_str.second;
            std::string ext_err = "Failure on call to SQL_get_matches: ";
            return std::make_pair(false, ext_err + int_err);
        }

        double current_usage = std::stod(rp_vec_str.first[0]);
        double delta = value - current_usage;

        // Update lot proper
        std::string update_usage_stmt = "UPDATE lot_usage SET " + key + "=? WHERE lot_name=?;";
        std::map<std::string, std::vector<int>> update_usage_str_map = {{lot_name, {2}}};
        std::map<double, std::vector<int>> update_usage_double_map = {{value, {1}}};
        std::map<int64_t, std::vector<int>> plc_hldr_int_map;
        auto rp_bool_str = this->store_updates(update_usage_dynamic_query, update_usage_str_map, plc_hldr_int_map, update_usage_double_map);
        if (!rp_bool_str.first) { // There was an error
            std::string int_err = rp_bool_str.second;
            std::string ext_err = "Failure on call to store_updates for lot proper: ";
            return std::make_pair(false, ext_err + int_err);
        }

        // Update parents
        auto rp_lotvec_str = this->get_parents(true);
        if (!rp_lotvec_str.second.empty()) { // There was an error
            std::string int_err = rp_lotvec_str.second;
            std::string ext_err = "Failure on call to get_parents: ";
            return std::make_pair(false, ext_err + int_err);
        }

        std::string children_key = "children" + key.substr(4);
        std::string parent_usage_query = "SELECT " + children_key + " FROM lot_usage WHERE lot_name = ?;";
        for (const auto &parent : recursive_parents) {
            std::map<std::string, std::vector<int>> parent_usage_query_str_map{{parent.lot_name, {1}}};
            rp_vec_str = lotman::Checks::SQL_get_matches(parent_usage_query, parent_usage_query_str_map);
            if (!rp_vec_str.second.empty()) { // There was an error
                std::string int_err = rp_vec_str.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(false, ext_err + int_err);
            }
            double current_usage = std::stod(rp_vec_str.first[0]);
            std::string update_parent_usage_stmt = "UPDATE lot_usage SET " + children_key + "=? WHERE lot_name=?;";
            std::map<std::string, std::vector<int>> update_parent_str_map{{parent.lot_name, {2}}};
            std::map<double, std::vector<int>> update_parent_int_map{{current_usage+delta, {1}}}; // Update children_key to current_usage + delta
            std::map<int64_t, std::vector<int>> plc_hldr_int_map;
            rp_bool_str = this->update_parent_usage(parent, update_parent_usage_stmt, update_parent_str_map, plc_hldr_int_map, update_parent_int_map);
            if (!rp_bool_str.first) { // There was an error
                std::string int_err = rp_bool_str.second;
                std::string ext_err = "Failure on call to store_updates";
                return std::make_pair(false, ext_err + int_err);
            }
        }
    }
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}

std::pair<bool, std::string> lotman::Lot::update_usage_by_dirs(json update_JSON) {
    // TODO: Should lots who don't show up when connecting lots to dirs be reset to have 
    //       0 usage, or should the be kept the way they are? 
    
    try {
        DirUsageUpdate dirUpdate;
        std::vector<Lot> return_lots;
        auto rp = dirUpdate.JSON_math(update_JSON, &return_lots);
        if (!rp.first) {
            std::string int_err = rp.second;
            std::string ext_err = "Failure on call to JSON_math: ";
            return std::make_pair(false, ext_err + int_err);
        }

        for (auto &lot : return_lots) {
            if (lot.usage.self_GB_update_staged) {
                auto rp_bool_str = lot.update_self_usage("self_GB", lot.usage.self_GB);
                if (!rp_bool_str.first) {
                    std::string int_err = rp_bool_str.second;
                    std::string ext_err = "Failure to update lot's self_GB: ";
                    return std::make_pair(false, ext_err + int_err);
                }
            }

            if (lot.usage.self_objects_update_staged) {
                auto rp_bool_str = lot.update_self_usage("self_objects", lot.usage.self_objects);
                if (!rp_bool_str.first) {
                    std::string int_err = rp_bool_str.second;
                    std::string ext_err = "Failure to update lot's self_GB: ";
                    return std::make_pair(false, ext_err + int_err);
                }
            }

            if (lot.usage.self_GB_being_written_update_staged) { 
                auto rp_bool_str = lot.update_self_usage("self_GB_being_written", lot.usage.self_GB_being_written);
                if (!rp_bool_str.first) {
                    std::string int_err = rp_bool_str.second;
                    std::string ext_err = "Failure to update lot's self_GB: ";
                    return std::make_pair(false, ext_err + int_err);
                }
            }

            if (lot.usage.self_objects_being_written_update_staged) { 
                auto rp_bool_str = lot.update_self_usage("self_objects_being_written", lot.usage.self_objects_being_written);
                if (!rp_bool_str.first) {
                    std::string int_err = rp_bool_str.second;
                    std::string ext_err = "Failure to update lot's self_GB: ";
                    return std::make_pair(false, ext_err + int_err);
                }
            }
        }

        return std::make_pair(true, "");
    }

    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_lots_past_exp(const bool recursive) {
    try {
        std::vector<std::string> expired_lots;
        auto now = std::chrono::system_clock::now();
        int64_t ms_since_epoch = std::chrono::time_point_cast<std::chrono::milliseconds>(now).time_since_epoch().count();

        std::string expired_query = "SELECT lot_name FROM management_policy_attributes WHERE expiration_time <= ?;";
        std::map<int64_t, std::vector<int>> expired_map{{ms_since_epoch, {1}}};
        auto rp = lotman::Checks::SQL_get_matches(expired_query, std::map<std::string,std::vector<int>>(), expired_map);
        if (!rp.second.empty()) { // There was an error
            std::string int_err = rp.second;
            std::string ext_err = "Failure on call to SQL_get_matches: ";
            return std::make_pair(std::vector<std::string>(), ext_err + int_err);
        }

        expired_lots = rp.first;
        if (recursive) { // Any child of an expired lot is also expired
            std::vector<std::string> tmp;
            for (auto &lot_name : expired_lots) {
                Lot _lot(lot_name);
                auto rp_lotvec_str = _lot.get_children(true);
                if (!rp_lotvec_str.second.empty()) { // There was an error
                    std::string int_err = rp_lotvec_str.second;
                    std::string ext_err = "Failure on call to get_children.";
                    return std::make_pair(std::vector<std::string>(), ext_err + int_err);
                }
                
                std::vector<std::string> tmp;
                for (const auto &child : _lot.recursive_children) {
                   tmp.push_back(child.lot_name);
                }
            }
            expired_lots.insert(expired_lots.end(), tmp.begin(), tmp.end());

            // Sort and remove any duplicates
            std::sort(expired_lots.begin(), expired_lots.end());
            auto last = std::unique(expired_lots.begin(), expired_lots.end());
            expired_lots.erase(last, expired_lots.end());
        }

        return std::make_pair(expired_lots, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(std::vector<std::string>(), exc.what());
    }
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_lots_past_del(const bool recursive) {
    try {
        auto now = std::chrono::system_clock::now();
        int64_t ms_since_epoch = std::chrono::time_point_cast<std::chrono::milliseconds>(now).time_since_epoch().count();

        std::string deletion_query = "SELECT lot_name FROM management_policy_attributes WHERE deletion_time <= ?;";
        std::map<int64_t, std::vector<int>> deletion_map{{ms_since_epoch, {1}}};
        auto rp = lotman::Checks::SQL_get_matches(deletion_query, std::map<std::string,std::vector<int>>(), deletion_map);
        if (!rp.second.empty()) { // There was an error
            std::string int_err = rp.second;
            std::string ext_err = "Failure on call to SQL_get_matches: ";
            return std::make_pair(std::vector<std::string>(), ext_err + int_err);
        }

        std::vector<std::string> deletion_lots = rp.first;

        if (recursive) { // Any child of an expired lot is also expired
            std::vector<std::string> tmp;
            for (auto &lot_name : deletion_lots) {
                Lot _lot(lot_name);
                auto rp_lotvec_str = _lot.get_children(true);
                if (!rp_lotvec_str.second.empty()) { // There was an error
                    std::string int_err = rp_lotvec_str.second;
                    std::string ext_err = "Failure on call to get_children.";
                    return std::make_pair(std::vector<std::string>(), ext_err + int_err);
                }

                for (const auto &child : _lot.recursive_children){
                    tmp.push_back(child.lot_name);
                }
            }
            deletion_lots.insert(deletion_lots.end(), tmp.begin(), tmp.end());

            // Sort and remove any duplicates
            std::sort(deletion_lots.begin(), deletion_lots.end());
            auto last = std::unique(deletion_lots.begin(), deletion_lots.end());
            deletion_lots.erase(last, deletion_lots.end());
        }

        return std::make_pair(deletion_lots, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(std::vector<std::string>(), exc.what());
    }
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_lots_past_opp(const bool recursive_quota, const bool recursive_children) {
    try {
        std::vector<std::string> lots_past_opp;
        if (recursive_quota) {
            std::string rec_opp_usage_query =   "SELECT "
                                                    "lot_usage.lot_name "
                                                "FROM lot_usage "
                                                    "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                "WHERE lot_usage.self_GB + lot_usage.children_GB >= management_policy_attributes.dedicated_GB + management_policy_attributes.opportunistic_GB;";
            auto rp = lotman::Checks::SQL_get_matches(rec_opp_usage_query);
            if (!rp.second.empty()) { // There was an error
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(std::vector<std::string>(), ext_err + int_err);
            }
            lots_past_opp = rp.first;
        }
        else {
            std::string opp_usage_query =   "SELECT "
                                                "lot_usage.lot_name "
                                            "FROM lot_usage "
                                                "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                            "WHERE lot_usage.self_GB >= management_policy_attributes.dedicated_GB + management_policy_attributes.opportunistic_GB;";
        
            auto rp = lotman::Checks::SQL_get_matches(opp_usage_query);
            if (!rp.second.empty()) { // There was an error
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(std::vector<std::string>(), ext_err + int_err);
            }
            lots_past_opp = rp.first;
        }

        if (recursive_children) { // Get all children of the lots past opp
            std::vector<std::string> tmp;
            for (const auto lot_past_opp : lots_past_opp) {
                Lot _lot(lot_past_opp);
                auto rp_lotvec_str = _lot.get_children(true);
                if (!rp_lotvec_str.second.empty()) { // There was an error
                    std::string int_err = rp_lotvec_str.second;
                    std::string ext_err = "Failure on call to get_children.";
                    return std::make_pair(std::vector<std::string>(), ext_err + int_err);
                }

                for (const auto &child : _lot.recursive_children){
                    tmp.push_back(child.lot_name);
                }
            }
            lots_past_opp.insert(lots_past_opp.end(), tmp.begin(), tmp.end());

            // Sort and remove any duplicates
            std::sort(lots_past_opp.begin(), lots_past_opp.end());
            auto last = std::unique(lots_past_opp.begin(), lots_past_opp.end());
            lots_past_opp.erase(last, lots_past_opp.end());        
        }

        return std::make_pair(lots_past_opp, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(std::vector<std::string>(), exc.what());
    }
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_lots_past_ded(const bool recursive_quota, const bool recursive_children) {
    try {
        std::vector<std::string> lots_past_ded;
        if (recursive_quota) {
            std::string rec_ded_usage_query =   "SELECT "
                                                    "lot_usage.lot_name "
                                                "FROM lot_usage "
                                                    "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                "WHERE lot_usage.self_GB + lot_usage.children_GB >= management_policy_attributes.dedicated_GB;";
            
            auto rp = lotman::Checks::SQL_get_matches(rec_ded_usage_query);
            if (!rp.second.empty()) { // There was an error
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(std::vector<std::string>(), ext_err + int_err);
            }
            lots_past_ded = rp.first;
        }
        else {
            std::string ded_usage_query =   "SELECT "
                                                "lot_usage.lot_name "
                                            "FROM lot_usage "
                                                "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                            "WHERE lot_usage.self_GB >= management_policy_attributes.dedicated_GB;";
            
            auto rp = lotman::Checks::SQL_get_matches(ded_usage_query);
            if (!rp.second.empty()) { // There was an error
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(std::vector<std::string>(), ext_err + int_err);
            }
            lots_past_ded = rp.first;
        }

        if (recursive_children) { // Get all children of the lots past opp
            std::vector<std::string> tmp;
            for (const auto lot_past_ded : lots_past_ded) {
                Lot _lot(lot_past_ded);
                auto rp_lotvec_str = _lot.get_children(true);
                if (!rp_lotvec_str.second.empty()) { // There was an error
                    std::string int_err = rp_lotvec_str.second;
                    std::string ext_err = "Failure on call to get_children.";
                    return std::make_pair(std::vector<std::string>(), ext_err + int_err);
                }

                for (const auto &child : _lot.recursive_children){
                    tmp.push_back(child.lot_name);
                }
            }
            lots_past_ded.insert(lots_past_ded.end(), tmp.begin(), tmp.end());

            // Sort and remove any duplicates
            std::sort(lots_past_ded.begin(), lots_past_ded.end());
            auto last = std::unique(lots_past_ded.begin(), lots_past_ded.end());
            lots_past_ded.erase(last, lots_past_ded.end());        
        }

        return std::make_pair(lots_past_ded, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(std::vector<std::string>(), exc.what());
    }
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_lots_past_obj(const bool recursive_quota, const bool recursive_children) {
    try {
        std::vector<std::string> lots_past_obj;
        if (recursive_quota) {
            std::string rec_obj_usage_query =   "SELECT "
                                                    "lot_usage.lot_name "
                                                "FROM lot_usage "
                                                    "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                                "WHERE lot_usage.self_objects + lot_usage.children_objects >= management_policy_attributes.max_num_objects;";
            
            auto rp = lotman::Checks::SQL_get_matches(rec_obj_usage_query);
            if (!rp.second.empty()) { // There was an error
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(std::vector<std::string>(), ext_err + int_err);
            }
            lots_past_obj = rp.first;
        }
        else {
            std::string obj_usage_query =   "SELECT "
                                                "lot_usage.lot_name "
                                            "FROM lot_usage "
                                                "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
                                            "WHERE lot_usage.self_objects >= management_policy_attributes.max_num_objects;";
            
            auto rp = lotman::Checks::SQL_get_matches(obj_usage_query);
            if (!rp.second.empty()) { // There was an error
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to SQL_get_matches: ";
                return std::make_pair(std::vector<std::string>(), ext_err + int_err);
            }
            lots_past_obj = rp.first;
        }

        if (recursive_children) { // Get all children of the lots past opp
            std::vector<std::string> tmp;
            for (const auto lot_past_obj : lots_past_obj) {
                Lot _lot(lot_past_obj);
                auto rp_lotvec_str = _lot.get_children(true);
                if (!rp_lotvec_str.second.empty()) { // There was an error
                    std::string int_err = rp_lotvec_str.second;
                    std::string ext_err = "Failure on call to get_children.";
                    return std::make_pair(std::vector<std::string>(), ext_err + int_err);
                }

                for (const auto &child : _lot.recursive_children){
                    tmp.push_back(child.lot_name);
                }
            }
            lots_past_obj.insert(lots_past_obj.end(), tmp.begin(), tmp.end());

            // Sort and remove any duplicates
            std::sort(lots_past_obj.begin(), lots_past_obj.end());
            auto last = std::unique(lots_past_obj.begin(), lots_past_obj.end());
            lots_past_obj.erase(last, lots_past_obj.end());        
        }

        return std::make_pair(lots_past_obj, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(std::vector<std::string>(), exc.what());
    }
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::list_all_lots() {
    try {
        std::string lots_query = "SELECT lot_name FROM owners;"; // Surjection between lots and owners means we'll get every lot without duplicates.
        auto rp = lotman::Checks::SQL_get_matches(lots_query);
        if (!rp.second.empty()) { // There was an error
            std::string int_err = rp.second;
            std::string ext_err = "Failure on call to SQL_get_matches: ";
            return std::make_pair(std::vector<std::string>(), ext_err + int_err);
        }
        return std::make_pair(rp.first, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(std::vector<std::string>(), exc.what());
    }
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_lots_from_dir(std::string dir, const bool recursive) {
    try {
        if (dir.back() == '/') { // Remove the character
            dir.pop_back();
        }

        std::string lots_from_dir_query =   "SELECT lot_name FROM paths " 
                                            "WHERE "
                                                "(path = ? OR ? LIKE path || '/%') " // The incoming file path is either the stored file path or a subdirectory of it
                                            "AND "
                                                "(recursive OR path = ?) " // If the stored file path is not recursive, we only match the exact file path
                                            "ORDER BY LENGTH(path) DESC LIMIT 1;"; // We prefer longer matches over shorter ones
        std::map<std::string, std::vector<int>> dir_str_map{{dir, {1,2,3}}};
        auto rp = lotman::Checks::SQL_get_matches(lots_from_dir_query, dir_str_map);
        if (!rp.second.empty()) {
            std::string int_err = rp.second;
            std::string ext_err = "Failure on call to SQL_get_matches: ";
            return std::make_pair(std::vector<std::string>(), ext_err + int_err);
        }

        std::vector<std::string> matching_lots_vec;
        if (rp.first.empty()) { // No associated lots were found, indicating the directory should be associated with the default lot
            matching_lots_vec = {"default"};
        }
        else {
            matching_lots_vec = rp.first;
        }

        if (recursive) { // Indicates we want all of the parent lots.
            Lot lot(matching_lots_vec[0]);
            lot.get_parents(true, false);
            for (auto &parent : lot.recursive_parents) {
                matching_lots_vec.push_back(parent.lot_name);
            }
        }

        return std::make_pair(matching_lots_vec, "");
    }

    catch (std::exception &exc) {
        return std::make_pair(std::vector<std::string>(), exc.what());
    }



}



/*
This commented section of code is what remains of the carcass from the old "get matching lots" function.
Leaving it here for now in case bits and pieces are useful, but it should probably be deleted after awhile.
*/
//     std::vector<std::string> matching_lots_vec;
//     // TODO: Go back at all levels and add support for GB_being_written and objects_being_written.
//     std::array<std::string, 6> usage_keys{{"dedicated_GB_usage", "opportunistic_GB_usage", "total_GB_usage", "num_objects_usage", "GB_being_written", "num_objects_being_written"}}; // TODO: Finish
//     std::array<std::string, 5> allowed_comparators{{">", "<", "=", "<=", ">="}};

//     if (std::find(allowed_comparators.begin(), allowed_comparators.end(), comparator) != allowed_comparators.end()) {
    
//         if (key == "dedicated_GB_usage") {
//             if (recursive) {
//                 if (comp_val <0) {
//                     std::string rec_ded_usage_query =   "SELECT "
//                                                             "lot_usage.lot_name "
//                                                         "FROM lot_usage "
//                                                             "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                         "WHERE lot_usage.personal_GB + lot_usage.children_GB " + comparator + " management_policy_attributes.dedicated_GB;";
                    
//                     matching_lots_vec = lotman::Validator::SQL_get_matches(rec_ded_usage_query);
//                 }
//                 else {
//                     std::string rec_ded_usage_query =   "SELECT "
//                                                             "lot_usage.lot_name "
//                                                         "FROM lot_usage "
//                                                             "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                         "WHERE lot_usage.personal_GB + lot_usage.children_GB " + comparator + " ?;";
//                     std::map<double, std::vector<int>> dbl_map{{comp_val, {1}}};
//                     std::map<std::string, std::vector<int>> plc_hldr_str_map;
//                     std::map<int, std::vector<int>> plc_hldr_int_map;
//                     matching_lots_vec = lotman::Validator::SQL_get_matches(rec_ded_usage_query, plc_hldr_str_map, plc_hldr_int_map);
//                 }
//             }
//             else {
//                 if (comp_val < 0) {
//                     std::string ded_usage_query =   "SELECT "
//                                                             "lot_usage.lot_name "
//                                                         "FROM lot_usage "
//                                                             "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                         "WHERE lot_usage.personal_GB " + comparator + " management_policy_attributes.dedicated_GB;";
                    
//                     matching_lots_vec = lotman::Validator::SQL_get_matches(ded_usage_query);
//                 }
//                 else {
//                     std::string ded_usage_query =   "SELECT "
//                                                             "lot_usage.lot_name "
//                                                         "FROM lot_usage "
//                                                             "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                         "WHERE lot_usage.personal_GB " + comparator + " ?;";
//                     std::map<double, std::vector<int>> dbl_map{{comp_val, {1}}};
//                     std::map<std::string, std::vector<int>> plc_hldr_str_map;
//                     std::map<int, std::vector<int>> plc_hldr_int_map;
//                     matching_lots_vec = lotman::Validator::SQL_get_matches(ded_usage_query, plc_hldr_str_map, plc_hldr_int_map);
//                 }
//             }
//         }
//         else if (key == "opportunistic_GB_usage") {
//             if (recursive) {
//                 if (comp_val <0) {
//                     std::string rec_opp_usage_query =   "SELECT "
//                                                             "lot_usage.lot_name "
//                                                         "FROM lot_usage "
//                                                             "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                         "WHERE lot_usage.personal_GB + lot_usage.children_GB " + comparator + " management_policy_attributes.dedicated_GB + management_policy_attributes.opportunistic_GB;";
                    
//                     matching_lots_vec = lotman::Validator::SQL_get_matches(rec_opp_usage_query);
//                 }
//                 else {
//                     std::string rec_opp_usage_query =   "SELECT "
//                                                             "lot_usage.lot_name "
//                                                         "FROM lot_usage "
//                                                             "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                         "WHERE lot_usage.personal_GB + lot_usage.children_GB " + comparator + " management_policy_attributes.dedicated_GB + ?;";
//                     std::map<double, std::vector<int>> dbl_map{{comp_val, {1}}};
//                     std::map<std::string, std::vector<int>> plc_hldr_str_map;
//                     std::map<int, std::vector<int>> plc_hldr_int_map;
//                     matching_lots_vec = lotman::Validator::SQL_get_matches(rec_opp_usage_query, plc_hldr_str_map, plc_hldr_int_map);
//                 }
//             }
//             else {
//                 if (comp_val <0) {
//                     std::string opp_usage_query =   "SELECT "
//                                                             "lot_usage.lot_name "
//                                                         "FROM lot_usage "
//                                                             "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                         "WHERE lot_usage.personal_GB " + comparator + " management_policy_attributes.dedicated_GB + management_policy_attributes.opportunistic_GB;";
                    
//                     matching_lots_vec = lotman::Validator::SQL_get_matches(opp_usage_query);
//                 }
//                 else {
//                     std::string opp_usage_query =   "SELECT "
//                                                             "lot_usage.lot_name "
//                                                         "FROM lot_usage "
//                                                             "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                         "WHERE lot_usage.personal_GB " + comparator + " management_policy_attributes.dedicated_GB + ?;";
//                     std::map<double, std::vector<int>> dbl_map{{comp_val, {1}}};
//                     std::map<std::string, std::vector<int>> plc_hldr_str_map;
//                     std::map<int, std::vector<int>> plc_hldr_int_map;
//                     matching_lots_vec = lotman::Validator::SQL_get_matches(opp_usage_query, plc_hldr_str_map, plc_hldr_int_map);
//                 }
//             }
//         }
//         else if (key == "total_GB_usage") {
//             if (recursive) {
//                 std::string rec_tot_usage_query =   "SELECT "
//                                                         "lot_usage.lot_name "
//                                                     "FROM lot_usage "
//                                                         "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                     "WHERE lot_usage.personal_GB " + comparator + " ?;";
//                 std::map<double, std::vector<int>> dbl_map{{comp_val, {1}}};
//                 std::map<std::string, std::vector<int>> plc_hldr_str_map;
//                 std::map<int, std::vector<int>> plc_hldr_int_map;
//                 matching_lots_vec = lotman::Validator::SQL_get_matches(rec_tot_usage_query, plc_hldr_str_map, plc_hldr_int_map);

//             }
//             else {
//                 std::string tot_usage_query =   "SELECT "
//                                                     "lot_usage.lot_name "
//                                                 "FROM lot_usage "
//                                                     "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                 "WHERE lot_usage.personal_GB + lot_usage.children_GB " + comparator + " ?;";
//                 std::map<double, std::vector<int>> dbl_map{{comp_val, {1}}};
//                 std::map<std::string, std::vector<int>> plc_hldr_str_map;
//                 std::map<int, std::vector<int>> plc_hldr_int_map;
//                 matching_lots_vec = lotman::Validator::SQL_get_matches(tot_usage_query, plc_hldr_str_map, plc_hldr_int_map);
//             }
//         }

//         else if (key == "num_objects") {
//             if (recursive) {
//                 if (comp_val < 0) {
//                     std::string rec_num_obj_query = "SELECT "
//                                                         "lot_usage.lot_name "
//                                                     "FROM lot_usage "
//                                                         "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                     "WHERE lot_usage.personal_objects + lot_usage.children_objects " + comparator + " management_policy_attributes.max_num_objects;";
//                     matching_lots_vec = lotman::Validator::SQL_get_matches(rec_num_obj_query);
//                 }
//                 else {
//                     std::string rec_num_obj_query = "SELECT "
//                                                         "lot_usage.lot_name "
//                                                     "FROM lot_usage "
//                                                         "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                     "WHERE lot_usage.personal_objects + lot_usage.children_objects " + comparator + " ?;";
//                     std::map<std::string, std::vector<int>> plc_hldr_str_map;
//                     std::map<int, std::vector<int>> int_map{{static_cast<int>(comp_val), {1}}};
//                     matching_lots_vec = lotman::Validator::SQL_get_matches(rec_num_obj_query, plc_hldr_str_map, int_map);
//                 }
//             }
//             else {
//                 if (comp_val < 0) {
//                     std::string num_obj_query = "SELECT "
//                                                     "lot_usage.lot_name "
//                                                 "FROM lot_usage "
//                                                     "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                 "WHERE lot_usage.personal_objects " + comparator + " management_policy_attributes.max_num_objects;";
//                     matching_lots_vec = lotman::Validator::SQL_get_matches(num_obj_query);
//                 }
//                 else {
//                     std::string num_obj_query = "SELECT "
//                                                         "lot_usage.lot_name "
//                                                     "FROM lot_usage "
//                                                         "INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
//                                                     "WHERE lot_usage.personal_objects " + comparator + " ?;";
//                     std::map<std::string, std::vector<int>> plc_hldr_str_map;
//                     std::map<int, std::vector<int>> int_map{{static_cast<int>(comp_val), {1}}};
//                     matching_lots_vec = lotman::Validator::SQL_get_matches(num_obj_query, plc_hldr_str_map, int_map);
//                 }
//             }
//         }

//         // TODO: Come back and add creation_time expiration_time and deletion_time where needed

//         else if (key == "GB_being_written") {
//             // TODO: implement this section
//         }

//         else if (key == "num_objects_being_written") {
//             // TODO: implement this section

//         }
    
//         else {
//             std::cerr << "The key \"" << key << "\" provided in lotman::Lot::get_lots_from_usage is not recognized." << std::endl;
//             return matching_lots_vec;
//         }
//     }

//     std::sort(matching_lots_vec.begin(), matching_lots_vec.end());
//     return matching_lots_vec;


std::pair<bool, std::string> lotman::Lot::check_context_for_parents(std::vector<std::string> parents, bool include_self, bool new_lot) {
    try {
        if (new_lot && parents.size()==1 && parents[0] == lot_name) { // This is a self parent new lot with no other parents. No need to check context.
            return std::make_pair(true, "");
        }

        std::string caller = lotman::Context::get_caller();
        bool allowed = false;
        if (!include_self) {
            for (const auto &parent : parents) {

                if (parent!=lot_name) {
                    Lot parent_lot(parent);
                    auto rp = parent_lot.get_owners(true);
                    if (!rp.second.empty()) { // There was an error
                        std::string int_err = rp.second;
                        std::string ext_err =  "Failed to get parent owners while checking validity of context: ";
                        return std::make_pair(false, ext_err + int_err);
                    }
                    if (std::find(parent_lot.recursive_owners.begin(), parent_lot.recursive_owners.end(), caller) != parent_lot.recursive_owners.end()) { // Caller is an owner
                        allowed = true;
                        break;
                    }
                }
            }
        }
        else {
            for (const auto &parent : parents) {
                Lot parent_lot(parent);
                auto rp = parent_lot.get_owners(true);
                if (!rp.second.empty()) { // There was an error
                    std::string int_err = rp.second;
                    std::string ext_err =  "Failed to get parent owners while checking validity of context: ";
                    return std::make_pair(false, ext_err + int_err);
                }
                if (std::find(parent_lot.recursive_owners.begin(), parent_lot.recursive_owners.end(), caller) != parent_lot.recursive_owners.end()) { // Caller is an owner
                    allowed = true;
                    break;
                }
            }
        }
        if (!allowed) {
            return std::make_pair(false, "Current context prohibits action on lot: Caller does not have proper ownership.");
        }
        
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}
std::pair<bool, std::string> lotman::Lot::check_context_for_parents(std::vector<Lot> parents, bool include_self, bool new_lot) {
    try {
        if (new_lot && parents.size()==1 && parents[0].lot_name == lot_name) { // This is a self parent new lot with no other parents. No need to check context.
            return std::make_pair(true, "");
        }

        std::string caller = lotman::Context::get_caller();
        bool allowed = false;
        if (!include_self) {
            if (parents.size() == 1 && parents[0].lot_name == lot_name) {
                allowed = true;
            }
            for (auto &parent : parents) {
                if (parent.lot_name!=lot_name) {
                    auto rp = parent.get_owners(true);
                    if (!rp.second.empty()) { // There was an error
                        std::string int_err = rp.second;
                        std::string ext_err =  "Failed to get parent owners while checking validity of context: ";
                        return std::make_pair(false, ext_err + int_err);
                    }
                    if (std::find(parent.recursive_owners.begin(), parent.recursive_owners.end(), caller) != parent.recursive_owners.end()) { // Caller is an owner
                        allowed = true;
                        break;
                    }
                }
            }
        }
        else {
            for (auto &parent : parents) {
                auto rp = parent.get_owners(true);
                if (!rp.second.empty()) { // There was an error
                    std::string int_err = rp.second;
                    std::string ext_err =  "Failed to get parent owners while checking validity of context: ";
                    return std::make_pair(false, ext_err + int_err);
                }
                if (std::find(parent.recursive_owners.begin(), parent.recursive_owners.end(), caller) != parent.recursive_owners.end()) { // Caller is an owner
                    allowed = true;
                    break;
                }
            }
        }
        if (!allowed) {
            return std::make_pair(false, "Current context prohibits action on lot: Caller does not have proper ownership.");
        }
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}
std::pair<bool, std::string> lotman::Lot::check_context_for_children(std::vector<std::string> children, bool include_self) {
    try {
        if (children.size() == 0) { // No children means no need to check for context.
            return std::make_pair(true, "");
        }

        std::string caller = lotman::Context::get_caller();
        bool allowed = false;
        if (!include_self) {
            for (const auto &child : children) {
                if (child != lot_name) {
                    Lot child_lot(child);
                    auto rp = child_lot.get_owners(true);
                    if (!rp.second.empty()) { // There was an error
                        std::string int_err = rp.second;
                        std::string ext_err =  "Failed to get child owners while checking validity of context: ";
                        return std::make_pair(false, ext_err + int_err);
                    }
                    if (std::find(child_lot.recursive_owners.begin(), child_lot.recursive_owners.end(), caller) != child_lot.recursive_owners.end()) { // Caller is an owner
                        allowed = true;
                        break;
                    }
                }
            }
        }
        else {
            for (const auto &child : children) {
                Lot child_lot(child);
                auto rp = child_lot.get_owners(true);
                if (!rp.second.empty()) { // There was an error
                    std::string int_err = rp.second;
                    std::string ext_err =  "Failed to get child owners while checking validity of context: ";
                    return std::make_pair(false, ext_err + int_err);
                }
                if (std::find(child_lot.recursive_owners.begin(), child_lot.recursive_owners.end(), caller) != child_lot.recursive_owners.end()) { // Caller is an owner
                    allowed = true;
                    break;
                }
            }
        }
        if (!allowed) {
            return std::make_pair(false, "Current context prohibits action on lot: Caller does not have proper ownership.");
        }
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}
std::pair<bool, std::string> lotman::Lot::check_context_for_children(std::vector<Lot> children, bool include_self) {
    try {
        if (children.size() == 0) { // No children means no need to check for context.
            return std::make_pair(true, "");
        }

        std::string caller = lotman::Context::get_caller();
        bool allowed = false;
        if (!include_self) {
            for (auto &child : children) {
                if (child.lot_name != lot_name) {
                    auto rp = child.get_owners(true);
                    if (!rp.second.empty()) { // There was an error
                        std::string int_err = rp.second;
                        std::string ext_err =  "Failed to get child owners while checking validity of context: ";
                        return std::make_pair(false, ext_err + int_err);
                    }
                    if (std::find(child.recursive_owners.begin(), child.recursive_owners.end(), caller) != child.recursive_owners.end()) { // Caller is an owner
                        allowed = true;
                        break;
                    }
                }
            }
        }
        else {
            for (auto &child : children) {
                auto rp = child.get_owners(true);
                if (!rp.second.empty()) { // There was an error
                    std::string int_err = rp.second;
                    std::string ext_err =  "Failed to get child owners while checking validity of context: ";
                    return std::make_pair(false, ext_err + int_err);
                }
                if (std::find(child.recursive_owners.begin(), child.recursive_owners.end(), caller) != child.recursive_owners.end()) { // Caller is an owner
                    allowed = true;
                    break;
                }
            }
        }
        if (!allowed) {
            return std::make_pair(false, "Current context prohibits action on lot: Caller does not have proper ownership.");
        }
        return std::make_pair(true, "");
    }
    catch (std::exception &exc) {
        return std::make_pair(false, exc.what());
    }
}

/**
 * Functions specific to Checks class
*/

bool lotman::Checks::cycle_check(std::string start_node,
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
        lotman::Lot lot(dfs_nodes_to_visit[0]);
        auto rp = lot.get_parents();

        // TODO: expose errors
        if (!rp.second.empty()) { // There was an error
            std::string int_err = rp.second;
            std::string ext_err = "Failure on call to get_parents(): ";
            return false;
        }

        // convert to vec of strings instead of vec of lots
        std::vector<std::string> dfs_node_parents;
        for (const auto &parent : rp.first) {
            dfs_node_parents.push_back(parent.lot_name);
        }

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

bool lotman::Checks::insertion_check(std::string LTBA, 
                                        std::string parent, 
                                        std::string child) { // Checks whether LTBA is being inserted between a parent and child.

    // TODO: expose errors
    lotman::Lot lot(child);
    auto rp = lot.get_parents();
    if (!rp.second.empty()) { // There was an error
        std::string int_err = rp.second;
        std::string ext_err = "Failure on call to get_parents(): ";
        return false;
    }

    std::vector<std::string> parents_vec;
    for (const auto &parent : rp.first) {
        parents_vec.push_back(parent.lot_name);
    }

    auto parent_iter = std::find(parents_vec.begin(), parents_vec.end(), parent); // Check if the specified parent is listed as a parent to the child
    if (!(parent_iter == parents_vec.end())) { 
        // Child has "parent" as a parent
        return true;
    }
    return false;
}

bool lotman::Checks::will_be_orphaned(std::string LTBR, 
                                         std::string child) {

    // TODO: expose errors
    lotman::Lot lot(child);
    auto rp = lot.get_parents();
    if (!rp.second.empty()) { // There was an error
        std::string int_err = rp.second;
        std::string ext_err = "Failure on call to get_parents(): ";
        return false;
    }

    std::vector<std::string> parents_vec;
    for (const auto &parent : rp.first) {
        parents_vec.push_back(parent.lot_name);
    }

    if (parents_vec.size()==1 && parents_vec[0]==LTBR) {
        return true;
    }
    return false;
}
