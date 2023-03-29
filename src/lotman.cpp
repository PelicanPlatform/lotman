
#include <iostream> //DELETE
#include <string.h>
#include <nlohmann/json.hpp>

#include "lotman.h"
#include "lotman_internal.h"
#include "lotman_version.h"

//ss
/*
Initialize context -- Default is empty
*/

std::string lotman::Context::m_caller{""};

using json = nlohmann::json;

const char * lotman_version() {
    std::string major = std::to_string(Lotman_VERSION_MAJOR);
    std::string minor = std::to_string(Lotman_VERSION_MINOR);
    static std::string version = major+"."+minor;

    return version.c_str();
}

int lotman_add_lot(const char *lotman_JSON_str, 
                   char **err_msg) {
    // TODO: Check for context and figure out what to do with it
    // TODO: Create JSON schema and verify JSON input before calling functions that rely on specific structures.

    try {
        json lot_JSON_obj = json::parse(lotman_JSON_str);

        // Data checks
        auto rp = lotman::Lot2::lot_exists("default");
        if (!rp.first && lot_JSON_obj["lot_name"] != "default") {
            if (err_msg) {
                if (rp.second.empty()) { // function worked, but lot does not exist
                    *err_msg = strdup("The default lot named \"default\" must be created first.");
                }
                else {
                    std::string int_err = rp.second;
                    std::string ext_err = "Function call to lotman::Lot2::lot_exists failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }            
            }
            return -1;
        }

        // Verify lot name is provided
        if (lot_JSON_obj["lot_name"].is_null()) {
            if (err_msg) {
                std::string err = "Could not determine lot_name.";
                *err_msg = strdup(err.c_str());
            }
            return -1; 
        }

        // Make sure lot doesn't already exist
        rp = lotman::Lot2::lot_exists(lot_JSON_obj["lot_name"]);
        if (rp.first) {
            if (err_msg) {
                if (rp.second.empty()) { // function worked, but lot already exists
                    *err_msg = strdup("The lot already exists and cannot be recreated. Maybe you meant to modify it?");
                }
                else {
                    std::string int_err = rp.second;
                    std::string ext_err = "Function call to lotman::Lot2::lot_exists failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }            
            return -1;
            }
        }

        // Make sure there's at least one parent
        if (lot_JSON_obj["parents"].get<std::vector<std::string>>().size() == 0) {
            if (err_msg) {
                *err_msg = strdup("The provided lot has no parents.");
            }
            return -1;
        }

        lotman::Lot2 lot;
        rp = lot.init_full(lot_JSON_obj);
        if (!rp.first){
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failed to initialize the lot from JSON: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        // Check for context and make sure caller is allowed to add the lot as specified
        rp = lot.check_context_for_parents(lot.parents, false, true);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Error while checking context for parents: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }
        rp = lot.check_context_for_children(lot.children);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Error while checking context for children: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        rp = lot.store_lot();
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failed to store lot: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_remove_lot(const char *lot_name, 
                      const bool assign_LTBR_parent_as_parent_to_orphans, 
                      const bool assign_LTBR_parent_as_parent_to_non_orphans, 
                      const bool assign_policy_to_children,
                      const bool override_policy,
                      char **err_msg) {
    // TODO: Check for context and figure out what to do with it
    try {
        auto rp = lotman::Lot2::lot_exists(lot_name);
        if (!rp.first) {
            if (err_msg) {
                if (rp.second.empty()) { // function worked, but lot does not exist
                    *err_msg = strdup("That was easy! The lot does not exist, so it doesn't have to be removed.");
                }
                else {
                    std::string int_err = rp.second;
                    std::string ext_err = "Function call to lotman::Lot2::lot_exists failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }            
            return -1;
            }
        }

        lotman::Lot2 lot;
        rp = lot.init_name(lot_name);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Function call to init_name failed: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        // To destroy a lot, caller must own the lot (not just the contents of the lot)
        // This implies caller owns a parent of the the lot.
        lot.get_parents(true, true); // Lots that are self owners own the lot as well as its content
        rp = lot.check_context_for_parents(lot.recursive_parents, true);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Error while checking context for parents: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        // Use this block when you've created reassignment policies in the database
        // if (override_policy) { // Don't bother to load the assigned policy, just initialize
        //     rp = lot.init_reassignment_policy(assign_LTBR_parent_as_parent_to_orphans, assign_LTBR_parent_as_parent_to_non_orphans, assign_policy_to_children);
        //     if (!rp.first) {
        //         if (err_msg) {
        //             std::string int_err = rp.second;
        //             std::string ext_err = "Function call to init_reassignment_policy failed: ";
        //             *err_msg = strdup((ext_err + int_err).c_str());
        //         }
        //     }
        // }
        // else {
        //     rp = lot.load_reassignment_policy();
        //     if (!rp.first) {
        //         if (rp.second.empty()) { // function worked, but lot has no stored reassignment policy
        //             rp = lot.init_reassignment_policy(assign_LTBR_parent_as_parent_to_orphans, assign_LTBR_parent_as_parent_to_non_orphans, assign_policy_to_children);
        //             if (!rp.first) {
        //                 if (err_msg) {
        //                     std::string int_err = rp.second;
        //                     std::string ext_err = "Function call to init_reassignment_policy failed: ";
        //                     *err_msg = strdup((ext_err + int_err).c_str());
        //                 }
        //             }
        //         }
        //         else { // function did not work
        //             if (err_msg) {
        //                 std::string int_err = rp.second;
        //                 std::string ext_err = "Failed to load reassignment policy: ";
        //                 *err_msg = strdup((ext_err + int_err).c_str());
        //             }
        //             return -1;
        //         }
        //     }
        // }

        rp = lot.init_reassignment_policy(assign_LTBR_parent_as_parent_to_orphans, assign_LTBR_parent_as_parent_to_non_orphans, assign_policy_to_children);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Function call to init_reassignment_policy failed: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        rp = lot.destroy_lot();
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failed to remove lot from database: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}


int lotman_remove_lots_recursive(const char *lot_name, char **err_msg) {
    try {
        auto rp = lotman::Lot2::lot_exists(lot_name);
        if (!rp.first) {
            if (err_msg) {
                if (rp.second.empty()) { // function worked, but lot does not exist
                    *err_msg = strdup("That was easy! The lot does not exist, so it doesn't have to be removed.");
                }
                else {
                    std::string int_err = rp.second;
                    std::string ext_err = "Function call to lotman::Lot2::lot_exists failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }            
            return -1;
            }
        }

        lotman::Lot2 lot;
        rp = lot.init_name(lot_name);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Function call to init_name failed: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        // To destroy a lot, caller must own the lot (not just the contents of the lot)
        // This implies caller owns a parent of the the lot.
        lot.get_parents(true, true); // Lots that are self owners own the lot as well as its content
        rp = lot.check_context_for_parents(lot.recursive_parents, true);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Error while checking context for parents: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        rp = lot.destroy_lot_recursive();
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failed to remove lot from database: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }

}




int lotman_update_lot(const char *lotman_JSON_str, 
                      char **err_msg) {
    try {
        json update_JSON_obj = json::parse(lotman_JSON_str);

        std::array<std::string, 5> known_keys = {"lot_name", "owner", "parents", "paths", "management_policy_attrs"};
        std::array<std::string, 6> known_mpa_keys = {"dedicated_GB", "opportunistic_GB", "max_num_objects", "creation_time", "expiration_time", "deletion_time"};

        // Check that all the update keys are known.
        auto keys_iter = update_JSON_obj.begin();
        for (const auto &key : update_JSON_obj.items()) {
            if (std::find(known_keys.begin(), known_keys.end(), key.key()) == known_keys.end()) {
                if (err_msg) {
                    std::string err = "The key \"" + key.key() + "\" is not recognized. Is there a typo?";
                    *err_msg = strdup(err.c_str());
                }
                return -1;
            }

            if (key.key() == "management_policy_attrs") {
                for (const auto &mpa : key.value().items()) {
                    if (std::find(known_mpa_keys.begin(), known_mpa_keys.end(), mpa.key()) == known_mpa_keys.end()) {
                        if (err_msg) {
                            std::string err = "The management policy attribute key \"" + mpa.key() + "\" is not recognized. Is there a typo?";
                            *err_msg = strdup(err.c_str());
                        }
                        return -1;
                    }
                }
            }
        }

        // Make sure the lot_name is defined
        if (update_JSON_obj["lot_name"].is_null()) {
            if (err_msg) {
                std::string err = "Could not determine lot_name.";
                *err_msg = strdup(err.c_str());
            }
            return -1; 
        }
        // Check that lot exists
        auto rp = lotman::Lot2::lot_exists(update_JSON_obj["lot_name"]);
        if (!rp.first) {
            if (err_msg) {
                if (!rp.second.empty()) { // There was an error
                    std::string int_err = rp.second;
                    std::string ext_err = "Failure on call to lot_exists: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }
                else { // Lot does not exist
                    std::string err = "Lot does not exist";
                    *err_msg = strdup(err.c_str());
                }
            }
            return -1; 
        }

        lotman::Lot2 lot;
        rp = lot.init_name(update_JSON_obj["lot_name"]);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failed to initialize lot name: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1; 
        }

        //Check for context
        lot.get_parents(true, true);
        rp = lot.check_context_for_parents(lot.recursive_parents, true);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Error while checking context for parents: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1; 
        }

        // Start checking which keys to operate on
        if (!update_JSON_obj["owner"].is_null()) {
            rp = lot.update_owner(update_JSON_obj["owner"].get<std::string>());
            if (!rp.first) {
                if (err_msg) {
                    std::string int_err = rp.second;
                    std::string ext_err = "Failed on call to lot.update_owner: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }
            return -1; 
            }
        }

        if (!update_JSON_obj["parents"].is_null()) {
            // Create update map from the update JSON -- Probably a way to improve this
            std::map<std::string, std::string> update_map;
            for (const auto &obj : update_JSON_obj["parents"]) {
                for (const auto &pair : obj.items()) {
                    std::pair<std::string, std::string> map_element = std::make_pair(pair.key(), pair.value());
                    update_map.insert(map_element);
                }
            }

            rp = lot.update_parents(update_map);
            if (!rp.first) {
                if (err_msg) {
                    std::string int_err = rp.second;
                    std::string ext_err = "Failed on call to lot.update_parents";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }
            return -1; 
            }
        }

        if (!update_JSON_obj["paths"].is_null()) {
            // Create update map from the update JSON
            std::map<std::string, json> update_map;
            for (const auto &path_update_obj : update_JSON_obj["paths"]) {
                for (const auto &pair : path_update_obj.items()) {
                    std::pair<std::string, json> map_element = std::make_pair(pair.key(), pair.value());
                    update_map.insert(map_element);
                }
            }

            rp = lot.update_paths(update_map);
            if (!rp.first) {
                if (err_msg) {
                    std::string int_err = rp.second;
                    std::string ext_err = "Failed on call to lot.update_paths";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }
            return -1; 
            }
        }
            
        if (!update_JSON_obj["management_policy_attrs"].is_null()) {
            for (const auto &update_attr : update_JSON_obj["management_policy_attrs"].items()) {
                auto rp = lot.update_man_policy_attrs(update_attr.key(), update_attr.value());
                if (!rp.first) {
                    if (err_msg) {
                        std::string int_err = rp.second;
                        std::string ext_err = "Failed on call to lot.update_paths";
                        *err_msg = strdup((ext_err + int_err).c_str());
                    }
                return -1; 
                }
            }
        }

        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_add_to_lot(const char *lotman_JSON_str, char **err_msg) {
    try {    
        json addition_obj = json::parse(lotman_JSON_str);

        std::array<std::string, 3> known_keys = {"lot_name", "parents", "paths"};

        // Check for known/unknown keys
        auto keys_iter = addition_obj.begin();
        for (const auto &key : addition_obj.items()) {
            if (std::find(known_keys.begin(), known_keys.end(), key.key()) == known_keys.end()) {
                if (err_msg) {
                    std::string err = "The key \"" + key.key() + "\" is not recognized. Is there a typo?";
                    *err_msg = strdup(err.c_str());
                }
                return -1;
            }
        }

        if (addition_obj["lot_name"].is_null()) {
            if (err_msg) {
                std::string err = "Could not determine lot_name.";
                *err_msg = strdup(err.c_str());
            }
            return -1; 
        }

        // Assert lot exists
        auto rp = lotman::Lot2::lot_exists(addition_obj["lot_name"]);
        if (!rp.first) {
            if (err_msg) {
                if (rp.second.empty()) { // function worked, but lot does not exist
                    *err_msg = strdup("That was easy! The lot does not exist, so nothing can be added to it.");
                }
                else {
                    std::string int_err = rp.second;
                    std::string ext_err = "Function call to lotman::Lot2::lot_exists failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }            
            return -1;
            }
        }

        lotman::Lot2 lot;
        rp = lot.init_name(addition_obj["lot_name"]);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failed to initialize lot name: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1; 
        }

        //Check for context
        lot.get_parents(true, true);
        rp = lot.check_context_for_parents(lot.recursive_parents, true);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Error while checking context for parents: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1; 
        }

        // Start checking which keys to operate on
        if (!addition_obj["parents"].is_null()) {
            std::vector<lotman::Lot2> parent_lots;
            for (const auto &parent_name : addition_obj["parents"]) {
                lotman::Lot2 parent_lot;
                parent_lot.init_name(parent_name);
                parent_lots.push_back(parent_lot);
            }

            rp = lot.add_parents(parent_lots);
            if (!rp.first) {
                if (err_msg) {
                    std::string int_err = rp.second;
                    std::string ext_err = "Failure to add parents: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1; 
            }

        }

        if (!addition_obj["paths"].is_null()) {
            rp = lot.add_paths(addition_obj["paths"]);
            if (!rp.first) {
                if (err_msg) {
                    std::string int_err = rp.second;
                    std::string ext_err = "Failure to add paths: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1; 
            }
        }

        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_is_root(const char *lot_name, char **err_msg) {
    if (!lot_name) {
        if (err_msg) {*err_msg = strdup("Name for the lot whose rootness is to be determined must not be nullpointer.");}
        return -1;
    }
    
    try {
        auto rp = lotman::Lot2::lot_exists(lot_name);
        if (!rp.first) {
            if (err_msg) {
                if (rp.second.empty()) { // function worked, but lot does not exist
                    *err_msg = strdup("The lot does not exist");
                }
                else {
                    std::string int_err = rp.second;
                    std::string ext_err = "Function call to lotman::Lot2::lot_exists failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }            
            }
            return -1;
        }

        lotman::Lot2 lot;
        lot.init_name(lot_name);
        rp = lot.check_if_root();
        if (rp.second.empty()) {
            return rp.first;
        }
        else { // There was an error
            if (err_msg) {
                    std::string int_err = rp.second;
                    std::string ext_err = "Function call to lotman::Lot2::check_if_root failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_lot_exists(const char *lot_name, 
                      char **err_msg) {
    if (!lot_name) {
        if (err_msg) {*err_msg = strdup("Name for the lot whose existence is to be determined must not be nullpointer.");}
        return -1;
    }

    try {
        auto rp = lotman::Lot2::lot_exists(lot_name);
        if (rp.second.empty()) { //no error message indicates success --> will change when inner function can handle error propagation
            return rp.first;
        }
        else {
            std::string int_err = rp.second;
            std::string ext_err = "Call to lotman::Lot2::lot_exists failed: ";
            if (err_msg) {
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}
    

int lotman_get_owners(const char *lot_name, 
                      const bool recursive,
                      char ***output,
                      char **err_msg) {
    if (!lot_name) {
        if (err_msg) {*err_msg = strdup("Name for the lot whose owners are to be obtained must not be nullpointer.");}
        return -1;
    }

    try {
        auto rp_bool_str = lotman::Lot2::lot_exists(lot_name);    
        if (!rp_bool_str.first) {
            if (err_msg) {
                if (rp_bool_str.second.empty()) { // function worked, but lot does not exist
                    *err_msg = strdup("The default lot named \"default\" must be created first.");
                }
                else {
                    std::string int_err = rp_bool_str.second;
                    std::string ext_err = "Function call to lotman::Lot2::lot_exists failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }            
            }
            return -1;
        }
        
        lotman::Lot2 lot;
        lot.init_name(lot_name);
        auto rp_vec_str = lot.get_owners(recursive);
        if (!rp_vec_str.second.empty()) { // There was an error
            if (err_msg) {
                std::string int_err = rp_vec_str.second;
                std::string ext_err = "Function call to lotman::Lot2::get_owners failed: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        std::vector<std::string> owners_list = rp_vec_str.first;
        auto owners_list_c = static_cast<char **>(malloc(sizeof(char *) * (owners_list.size() +1))); 
        owners_list_c[owners_list.size()] = nullptr;
        int idx = 0;
        for (const auto &iter : owners_list) {
            owners_list_c[idx] = strdup(iter.c_str());
            if (!owners_list_c[idx]) {
                lotman_free_string_list(owners_list_c);
                if (err_msg) {
                    *err_msg = strdup("Failed to create a copy of string entry in list");
                }
                return -1;
            }
            idx++;
        }
        *output = owners_list_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

void lotman_free_string_list(char **str_list) {
    int idx = 0;
    do {
        free(str_list[idx++]);
    } while (str_list[idx]);
    free(str_list);
}

int lotman_get_parent_names(const char *lot_name, 
                            const bool recursive,
                            const bool get_self,
                            char ***output,
                            char **err_msg){
    if (!lot_name) {
        if (err_msg) {*err_msg = strdup("Name for the lot whose parents are to be obtained must not be nullpointer.");}
        return -1;
    }

    try {
        auto rp_bool_str = lotman::Lot2::lot_exists(lot_name);    
        if (!rp_bool_str.first) {
            if (err_msg) {
                if (rp_bool_str.second.empty()) { // function worked, but lot does not exist
                    *err_msg = strdup("The default lot named \"default\" must be created first.");
                }
                else {
                    std::string int_err = rp_bool_str.second;
                    std::string ext_err = "Function call to lotman::Lot2::lot_exists failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }            
            }
            return -1;
        }

        lotman::Lot2 lot;
        lot.init_name(lot_name);

        auto rp_vec_str = lot.get_parents(recursive, get_self);
        if (!rp_vec_str.second.empty()) { // There was an error
            if (err_msg) {
                std::string int_err = rp_vec_str.second;
                std::string ext_err = "Function call to lotman::Lot2::get_parents failed: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        std::vector<lotman::Lot2> parents = rp_vec_str.first;
        std::vector<std::string> parents_list;
        for (const auto &parent : parents) {
            parents_list.push_back(parent.lot_name);
        }
        auto parents_list_c = static_cast<char **>(malloc(sizeof(char *) * (parents_list.size() +1))); 
        parents_list_c[parents_list.size()] = nullptr;
        int idx = 0;
        for (const auto &iter : parents_list) {
            parents_list_c[idx] = strdup(iter.c_str());
            if (!parents_list_c[idx]) {
                lotman_free_string_list(parents_list_c);
                if (err_msg) {
                    *err_msg = strdup("Failed to create a copy of string entry in list");
                }
                return -1;
            }
            idx++;
        }
        *output = parents_list_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_get_children_names(const char *lot_name, 
                              const bool recursive,
                              const bool get_self,
                              char ***output,
                              char **err_msg) {
    if (!lot_name) {
        if (err_msg) {*err_msg = strdup("Name for the lot whose children are to be obtained must not be nullpointer.");}
        return -1;
    }

    try {
        auto rp_bool_str = lotman::Lot2::lot_exists(lot_name);    
        if (!rp_bool_str.first) {
            if (err_msg) {
                if (rp_bool_str.second.empty()) { // function worked, but lot does not exist
                    *err_msg = strdup("The default lot named \"default\" must be created first.");
                }
                else {
                    std::string int_err = rp_bool_str.second;
                    std::string ext_err = "Function call to lotman::Lot2::lot_exists failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }            
            }
            return -1;
        }

        lotman::Lot2 lot;
        lot.init_name(lot_name);

        auto rp_vec_str = lot.get_children(recursive, get_self);
        if (!rp_vec_str.second.empty()) { // There was an error
            if (err_msg) {
                std::string int_err = rp_vec_str.second;
                std::string ext_err = "Function call to lotman::Lot2::get_children failed: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        std::vector<lotman::Lot2> children = rp_vec_str.first;
        std::vector<std::string> children_list;
        for (const auto &child : children) {
            children_list.push_back(child.lot_name);
        }
        auto children_list_c = static_cast<char **>(malloc(sizeof(char *) * (children_list.size() +1))); 
        children_list_c[children_list.size()] = nullptr;
        int idx = 0;
        for (const auto &iter : children_list) {
            children_list_c[idx] = strdup(iter.c_str());
            if (!children_list_c[idx]) {
                lotman_free_string_list(children_list_c);
                if (err_msg) {
                    *err_msg = strdup("Failed to create a copy of string entry in list");
                }
                return -1;
            }
            idx++;
        }
        *output = children_list_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_get_policy_attributes(const char *policy_attributes_JSON_str,  
                                 char **output,
                                 char **err_msg) {
    try {
        json get_attrs_obj = json::parse(policy_attributes_JSON_str);
        std::array<std::string, 7> known_keys = {"lot_name", "dedicated_GB", "opportunistic_GB", "max_num_objects", "creation_time", "expiration_time", "deletion_time"};

        // Check for known/unknown keys
        auto keys_iter = get_attrs_obj.begin();
        for (const auto &key : get_attrs_obj.items()) {
            if (std::find(known_keys.begin(), known_keys.end(), key.key()) == known_keys.end()) {
                if (err_msg) {
                    std::string err = "The key \"" + key.key() + "\" is not recognized. Is there a typo?";
                    *err_msg = strdup(err.c_str());
                }
                return -1;
            }
        }

        if (get_attrs_obj["lot_name"].is_null()) {
            if (err_msg) {
                std::string err = "Could not determine lot_name.";
                *err_msg = strdup(err.c_str());
            }
            return -1; 
        }

        // Assert lot exists
        auto rp = lotman::Lot2::lot_exists(get_attrs_obj["lot_name"]);
        if (!rp.first) {
            if (err_msg) {
                if (rp.second.empty()) { // function worked, but lot does not exist
                    *err_msg = strdup("That was easy! The lot does not exist, so nothing can be added to it.");
                }
                else {
                    std::string int_err = rp.second;
                    std::string ext_err = "Function call to lotman::Lot2::lot_exists failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }            
            return -1;
            }
        }

        lotman::Lot2 lot;
        rp = lot.init_name(get_attrs_obj["lot_name"]);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failed to initialize lot name: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1; 
        }

        json output_obj;
        for (const auto &pair : get_attrs_obj.items()) {
            if (pair.key() != "lot_name") {
                auto rp_json_bool = lot.get_restricting_attribute(pair.key(), pair.value());
                if (!rp_json_bool.second.empty()) { // There was an error
                    if (err_msg) {
                        std::string int_err = rp.second;
                        std::string ext_err = "Failed to initialize lot name: ";
                        *err_msg = strdup((ext_err + int_err).c_str());
                    }
                    return -1; 
                }
                output_obj[pair.key()] = rp_json_bool.first;
            }
        }

        std::string output_str = output_obj.dump();
        auto output_str_c = static_cast<char *>(malloc(sizeof(char) * (output_str.length() + 1)));
        output_str_c = strdup(output_str.c_str());
        *output = output_str_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_get_lot_dirs(const char *lot_name, 
                        const bool recursive, 
                        char **output,
                        char **err_msg) {
    if (!lot_name) {
        if (err_msg) {*err_msg = strdup("Name for the lot whose directories are to be obtained must not be nullpointer.");}
        return -1;
    }

    try {
        auto rp = lotman::Lot2::lot_exists(lot_name);
        if (!rp.first) {
            if (err_msg) {
                if (rp.second.empty()) { // function worked, but lot does not exist
                    *err_msg = strdup("That was easy! The lot does not exist, so nothing can be added to it.");
                }
                else {
                    std::string int_err = rp.second;
                    std::string ext_err = "Function call to lotman::Lot2::lot_exists failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }            
            return -1;
            }
        }

        lotman::Lot2 lot;
        rp = lot.init_name(lot_name);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failed to initialize lot name: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1; 
        }

        json output_obj;
        auto rp_json_str = lot.get_lot_dirs(recursive);
        if (!rp_json_str.second.empty()) { // There was an error
            if (err_msg) {
                std::string int_err = rp_json_str.second;
                std::string ext_err = "Failure on call to get_lot_dirs: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1; 
        }

        output_obj = rp_json_str.first;
        std::string output_obj_str = output_obj.dump();
        auto output_obj_str_c = static_cast<char *>(malloc(sizeof(char) * (output_obj_str.length() + 1)));
        output_obj_str_c = strdup(output_obj_str.c_str());
        *output = output_obj_str_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_update_lot_usage(const char *update_JSON_str, char **err_msg) {
    try {
        
        json update_usage_JSON = json::parse(update_JSON_str);
        std::array<std::string, 5> known_keys = {"lot_name", "self_GB", "self_GB_being_written", "self_objects", "self_objects_being_written"};

        // Check for known/unknown keys
        auto keys_iter = update_usage_JSON.begin();
        for (const auto &key : update_usage_JSON.items()) {
            if (std::find(known_keys.begin(), known_keys.end(), key.key()) == known_keys.end()) {
                if (err_msg) {
                    std::string err = "The key \"" + key.key() + "\" is not recognized. Is there a typo?";
                    *err_msg = strdup(err.c_str());
                }
                return -1;
            }
        }

        // Verify lot name is provided
        if (update_usage_JSON["lot_name"].is_null()) {
            if (err_msg) {
                std::string err = "Could not determine lot_name.";
                *err_msg = strdup(err.c_str());
            }
            return -1; 
        }

        // Assert lot exists
        auto rp = lotman::Lot2::lot_exists(update_usage_JSON["lot_name"]);
        if (!rp.first) {
            if (err_msg) {
                if (rp.second.empty()) { // function worked, but lot does not exist
                    *err_msg = strdup("That was easy! The lot does not exist, so nothing can be added to it.");
                }
                else {
                    std::string int_err = rp.second;
                    std::string ext_err = "Function call to lotman::Lot2::lot_exists failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }            
            return -1;
            }
        }

        lotman::Lot2 lot;
        rp = lot.init_name(update_usage_JSON["lot_name"]);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failed to initialize lot name: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1; 
        }

        //Check for context
        lot.get_parents(true, true);
        rp = lot.check_context_for_parents(lot.recursive_parents, true);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Error while checking context for parents: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1; 
        }

        for (const auto &pair : update_usage_JSON.items()) {
            if (pair.key() != "lot_name") {
                rp = lot.update_self_usage(pair.key(), pair.value());
                if (!rp.first) { // There was an error
                    if (err_msg) {
                        std::string int_err = rp.second;
                        std::string ext_err = "Failure on call to update_self_usage: ";
                        *err_msg = strdup((ext_err + int_err).c_str());
                    }
                    return -1; 
                }
            }
        }

        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}




int lotman_update_lot_usage_by_dir(const char *update_JSON_str, char **err_msg) {
    try {
        if (!update_JSON_str) {
            if (err_msg) {*err_msg = strdup("Update JSON must not be null.");}
            return -1;
        }

        json update_JSON = json::parse(update_JSON_str);

        auto rp = lotman::Lot2::update_usage_by_dirs(update_JSON);

        std::cout << "In lotman.cpp, rp = " << rp.first << ", " << rp.second;


        return -1;




    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}



















int lotman_get_lot_usage(const char *usage_attributes_JSON_str, char **output, char **err_msg) {  
    try {
        json get_usage_obj = json::parse(usage_attributes_JSON_str);
        std::array<std::string, 7> known_keys = {"lot_name", "dedicated_GB", "opportunistic_GB", "total_GB", "num_objects", "GB_being_written", "obj_being_written"};

        // Check for known/unknown keys
        auto keys_iter = get_usage_obj.begin();
        for (const auto &key : get_usage_obj.items()) {
            if (std::find(known_keys.begin(), known_keys.end(), key.key()) == known_keys.end()) {
                if (err_msg) {
                    std::string err = "The key \"" + key.key() + "\" is not recognized. Is there a typo?";
                    *err_msg = strdup(err.c_str());
                }
                return -1;
            }
        }

        // Verify lot name is provided
        if (get_usage_obj["lot_name"].is_null()) {
            if (err_msg) {
                std::string err = "Could not determine lot_name.";
                *err_msg = strdup(err.c_str());
            }
            return -1; 
        }

        // Assert lot exists
        auto rp = lotman::Lot2::lot_exists(get_usage_obj["lot_name"]);
        if (!rp.first) {
            if (err_msg) {
                if (rp.second.empty()) { // function worked, but lot does not exist
                    *err_msg = strdup("That was easy! The lot does not exist, so nothing can be added to it.");
                }
                else {
                    std::string int_err = rp.second;
                    std::string ext_err = "Function call to lotman::Lot2::lot_exists failed: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }            
            return -1;
            }
        }

        lotman::Lot2 lot;
        rp = lot.init_name(get_usage_obj["lot_name"]);
        if (!rp.first) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failed to initialize lot name: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1; 
        }

        json output_obj;
        for (const auto &pair : get_usage_obj.items()) {
            if (pair.key() != "lot_name") {
                auto rp_json_str = lot.get_lot_usage(pair.key(), pair.value());
                if (!rp_json_str.second.empty()) { // There was an error
                    if (err_msg) {
                        std::string int_err = rp_json_str.second;
                        std::string ext_err = "Failure on call to get_lot_usage: ";
                        *err_msg = strdup((ext_err + int_err).c_str());
                    }
                    return -1; 
                }

                output_obj[pair.key()] = rp_json_str.first;
            }
        }

        std::string output_str = output_obj.dump();
        auto output_str_c = static_cast<char *>(malloc(sizeof(char) * (output_str.length() + 1)));
        output_str_c = strdup(output_str.c_str());
        *output = output_str_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    } 
}

int lotman_check_db_health(char **err_msg) {
    return false;
}


int lotman_set_context(const char *key, const char *value, char **err_msg) {
    try {
        if (!key) {
            if (err_msg) {
                *err_msg = strdup("A key must be provided.");
            }
            return -1;
        }

        if (strcmp(key, "caller")==0) {
            lotman::Context::set_caller(value);
        }

        else {
            if (err_msg) {
                std::string err = "Unrecognized key: " + static_cast<std::string>(key);
                *err_msg = strdup(err.c_str());
            }
            return -1;
            }
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_get_lots_past_exp(const bool recursive, char ***output, char **err_msg) {
    try {
        auto rp = lotman::Lot2::get_lots_past_exp(recursive);
        if (!rp.second.empty()) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to get_lots_past_exp: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }
        std::vector<std::string> expired_lots = rp.first;

        auto expired_lots_c = static_cast<char **>(malloc(sizeof(char *) * (expired_lots.size() +1))); 
        expired_lots_c[expired_lots.size()] = nullptr;
        int idx = 0;
        for (const auto &iter : expired_lots) {
            expired_lots_c[idx] = strdup(iter.c_str());
            if (!expired_lots_c[idx]) {
                lotman_free_string_list(expired_lots_c);
                if (err_msg) {
                    *err_msg = strdup("Failed to create a copy of string entry in list");
                }
                return -1;
            }
            idx++;
        }
        *output = expired_lots_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_get_lots_past_del(const bool recursive, char ***output, char **err_msg) {
    try {
        auto rp = lotman::Lot2::get_lots_past_del(recursive);
        if (!rp.second.empty()) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to get_lots_past_del: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        std::vector<std::string> deletion_lots = rp.first;

        auto deletion_lots_c = static_cast<char **>(malloc(sizeof(char *) * (deletion_lots.size() +1))); 
        deletion_lots_c[deletion_lots.size()] = nullptr;
        int idx = 0;
        for (const auto &iter : deletion_lots) {
            deletion_lots_c[idx] = strdup(iter.c_str());
            if (!deletion_lots_c[idx]) {
                lotman_free_string_list(deletion_lots_c);
                if (err_msg) {
                    *err_msg = strdup("Failed to create a copy of string entry in list");
                }
                return -1;
            }
            idx++;
        }
        *output = deletion_lots_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_get_lots_past_opp(const bool recursive_quota, const bool recursive_children, char ***output, char **err_msg) {
    try {
        auto rp = lotman::Lot2::get_lots_past_opp(recursive_quota, recursive_children);
        if (!rp.second.empty()) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to get_lots_past_del: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        std::vector<std::string> past_opp_lots = rp.first;

        auto past_opp_lots_c = static_cast<char **>(malloc(sizeof(char *) * (past_opp_lots.size() +1))); 
        past_opp_lots_c[past_opp_lots.size()] = nullptr;
        int idx = 0;
        for (const auto &iter : past_opp_lots) {
            past_opp_lots_c[idx] = strdup(iter.c_str());
            if (!past_opp_lots_c[idx]) {
                lotman_free_string_list(past_opp_lots_c);
                if (err_msg) {
                    *err_msg = strdup("Failed to create a copy of string entry in list");
                }
                return -1;
            }
            idx++;
        }
        *output = past_opp_lots_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_get_lots_past_ded(const bool recursive_quota, const bool recursive_children, char ***output, char **err_msg) {
    try {
        auto rp = lotman::Lot2::get_lots_past_ded(recursive_quota, recursive_children);
        if (!rp.second.empty()) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to get_lots_past_del: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        std::vector<std::string> past_ded_lots = rp.first;

        auto past_ded_lots_c = static_cast<char **>(malloc(sizeof(char *) * (past_ded_lots.size() +1))); 
        past_ded_lots_c[past_ded_lots.size()] = nullptr;
        int idx = 0;
        for (const auto &iter : past_ded_lots) {
            past_ded_lots_c[idx] = strdup(iter.c_str());
            if (!past_ded_lots_c[idx]) {
                lotman_free_string_list(past_ded_lots_c);
                if (err_msg) {
                    *err_msg = strdup("Failed to create a copy of string entry in list");
                }
                return -1;
            }
            idx++;
        }
        *output = past_ded_lots_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_get_lots_past_obj(const bool recursive_quota, const bool recursive_children, char ***output, char **err_msg) {
    try {
        auto rp = lotman::Lot2::get_lots_past_obj(recursive_quota, recursive_children);
        if (!rp.second.empty()) {
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to get_lots_past_del: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        std::vector<std::string> past_obj_lots = rp.first;

        auto past_obj_lots_c = static_cast<char **>(malloc(sizeof(char *) * (past_obj_lots.size() +1))); 
        past_obj_lots_c[past_obj_lots.size()] = nullptr;
        int idx = 0;
        for (const auto &iter : past_obj_lots) {
            past_obj_lots_c[idx] = strdup(iter.c_str());
            if (!past_obj_lots_c[idx]) {
                lotman_free_string_list(past_obj_lots_c);
                if (err_msg) {
                    *err_msg = strdup("Failed to create a copy of string entry in list");
                }
                return -1;
            }
            idx++;
        }
        *output = past_obj_lots_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}




int lotman_list_all_lots(char ***output, char **err_msg) {
    try {
        auto rp = lotman::Lot2::list_all_lots();
        if (!rp.second.empty()) { // There was an error
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to list_all_lots: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        std::vector<std::string> all_lots = rp.first;

        auto all_lots_c = static_cast<char **>(malloc(sizeof(char *) * (all_lots.size() +1))); 
        all_lots_c[all_lots.size()] = nullptr;
        int idx = 0;
        for (const auto &iter : all_lots) {
            all_lots_c[idx] = strdup(iter.c_str());
            if (!all_lots_c[idx]) {
                lotman_free_string_list(all_lots_c);
                if (err_msg) {
                    *err_msg = strdup("Failed to create a copy of string entry in list");
                }
                return -1;
            }
            idx++;
        }
        *output = all_lots_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}


int lotman_get_lot_as_json(const char *lot_name, const bool recursive, char **output, char **err_msg) {
    try {
        if (!lot_name) {
            if (err_msg) {*err_msg = strdup("Name for the lot to be returned as JSON must not be nullpointer.");}
            return -1;
        }

        lotman::Lot2 lot;
        lot.init_name(lot_name);

        json output_obj;

        // Start populating fields in output_obj

        // Add name
        output_obj["lot_name"] = lot_name;

        // Add owner(s) according to recursive flag
        std::pair<std::vector<std::string>, std::string> rp_vec_str;
        rp_vec_str = lot.get_owners(recursive);
        if (!rp_vec_str.second.empty()) { // There was an error
            if (err_msg) {
                std::string int_err = rp_vec_str.second;
                std::string ext_err = "Failure on call to get_owners: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }
        if (recursive) {
            output_obj["owners"] = rp_vec_str.first;
        }
        else {
            output_obj["owners"] = rp_vec_str.first[0]; // Only one owner, this is where it will be.
        }
        
        // Add parents according to recursive flag
        std::pair<std::vector<lotman::Lot2>, std::string> rp_lotvec_str;
        rp_lotvec_str = lot.get_parents(recursive, true);
        if (!rp_lotvec_str.second.empty()) { // There was an error
            if (err_msg) {
                std::string int_err = rp_lotvec_str.second;
                std::string ext_err = "Failure on call to get_parents: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }
        std::vector<std::string> tmp;
        for (const auto &parent : rp_lotvec_str.first) {
            tmp.push_back(parent.lot_name);
        }
        output_obj["parents"] = tmp;

        // Add children according to recursive flag
        rp_lotvec_str = lot.get_children(recursive, false);
        if (!rp_lotvec_str.second.empty()) { // There was an error
            if (err_msg) {
                std::string int_err = rp_lotvec_str.second;
                std::string ext_err = "Failure on call to get_children: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }
        tmp = {};
        for (const auto &child : rp_lotvec_str.first) {
            tmp.push_back(child.lot_name);
        }
        output_obj["children"] = tmp;

        // Add paths according to recursive flag
        std::pair<json, std::string> rp_json_str;
        rp_json_str = lot.get_lot_dirs(recursive);
        if (!rp_json_str.second.empty()) { // There was an error
            if (err_msg) {
                std::string int_err = rp_json_str.second;
                std::string ext_err = "Failure on call to get_lot_dirs: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }
        output_obj["paths"] = rp_json_str.first;

        // Add management policy attributes according to recursive flag
        std::array<std::string, 6> man_pol_keys = {"dedicated_GB", "opportunistic_GB", "max_num_objects", "creation_time", "deletion_time", "expiration_time"};
        json internal_man_pol_obj;
        for (const auto &key : man_pol_keys) {
            rp_json_str = lot.get_restricting_attribute(key, recursive);
            if (!rp_json_str.second.empty()) { // There was an error
                if (err_msg) {
                    std::string int_err = rp_json_str.second;
                    std::string ext_err = "Failure on call to get_restricting_attribute: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }
                return -1;
            }

            internal_man_pol_obj[key] = rp_json_str.first;
        }
        output_obj["management_policy_attrs"] = internal_man_pol_obj;

        // Add usage according to recursive flag
        std::array<std::string, 6> usage_keys = {"dedicated_GB", "opportunistic_GB", "total_GB", "num_objects", "GB_being_written", "objects_being_written"};
        json internal_usage_obj;
        for (const auto &key : usage_keys) {
            rp_json_str = lot.get_lot_usage(key, recursive);
            if (!rp_json_str.second.empty()) { // There was an error
                if (err_msg) {
                    std::string int_err = rp_json_str.second;
                    std::string ext_err = "Failure on call to get_lot_usage: ";
                    *err_msg = strdup((ext_err + int_err).c_str());
                }
                return -1;
            }
            internal_usage_obj[key] = rp_json_str.first;
        }
        output_obj["usage"] = internal_usage_obj;

        // Copy the object to output
        std::string output_str = output_obj.dump();
        auto output_str_c = static_cast<char *>(malloc(sizeof(char) * (output_str.length() + 1)));
        output_str_c = strdup(output_str.c_str());
        *output = output_str_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}

int lotman_get_lots_from_dir(const char *dir, const bool recursive, char ***output, char **err_msg) {
    try {

        auto rp = lotman::Lot2::get_lots_from_dir(dir, recursive);
        if (!rp.second.empty()) { // There was an error
            if (err_msg) {
                std::string int_err = rp.second;
                std::string ext_err = "Failure on call to list_all_lots: ";
                *err_msg = strdup((ext_err + int_err).c_str());
            }
            return -1;
        }

        std::vector<std::string> lots_from_dir = rp.first;

        auto lots_from_dir_c = static_cast<char **>(malloc(sizeof(char *) * (lots_from_dir.size() +1))); 
        lots_from_dir_c[lots_from_dir.size()] = nullptr;
        int idx = 0;
        for (const auto &iter : lots_from_dir) {
            lots_from_dir_c[idx] = strdup(iter.c_str());
            if (!lots_from_dir_c[idx]) {
                lotman_free_string_list(lots_from_dir_c);
                if (err_msg) {
                    *err_msg = strdup("Failed to create a copy of string entry in list");
                }
                return -1;
            }
            idx++;
        }
        *output = lots_from_dir_c;
        return 0;
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }
}


//int lotman_get_matching_lots(const char *criteria_JSON_str, 
//                             char ***output, 
//                             char **err_msg) {

//     if (!criteria_JSON_str) {
//         if (err_msg) {*err_msg = strdup("The criteria string must not be nullpointer.");}
//         return -1;
//     }

//     picojson::value criteria_JSON;
//     std::string err = picojson::parse(criteria_JSON, criteria_JSON_str);
//     if (!err.empty()) {
//         std::cerr << "Criteria JSON can't be parsed -- it's probably malformed!";
//         std::cerr << err << std::endl;
//         return -1;
//     }

//     if (!criteria_JSON.is<picojson::object>()) {
//         std::cerr << "Criteria JSON is not recognized as an object -- it's probably malformed!" << std::endl;
//         return -1;
//     }

//     auto criteria_input_obj = criteria_JSON.get<picojson::object>();
//     auto iter = criteria_input_obj.begin();
//     if (iter == criteria_input_obj.end()) {
//         std::cerr << "Something is wrong -- the criteria JSON object appears empty." << std::endl;
//         return -1;
//     }

//     std::vector<std::string> output_vec{};
//     try {
//         std::array<std::string, 4> policy_attr_int_keys{{"max_num_objects", "creation_time", "expiration_time", "deletion_time"}};
//         std::array<std::string, 2> policy_attr_double_keys{{"dedicated_GB", "oppportunistic_GB"}};

//         std::array<std::string, 5> usage_keys{{"dedicated_GB_usage", "opportunistic_GB_usage", "num_objects_usage", "GB_being_written", "objects_being_written"}};

//         std::vector<std::vector<std::string>> intersect_vecs;
//         for (iter; iter != criteria_input_obj.end(); ++iter) {
//             // check if key is one of known keys
//             auto key = iter->first;
//             auto value = iter->second;

//             if (key == "owners") {
//                 if (!value.is<picojson::array>()) {
//                     std::cerr << "Owners key expects an array, but doesn't recognize one -- it's probably malformed!" << std::endl;
//                     return -1;
//                 }
//                 picojson::array owners_arr = value.get<picojson::array>();

//                 // get the sorted vector
//                 std::vector<std::string> lots_from_owners_vec{lotman::Lot::get_lots_from_owners(owners_arr)};
//                 intersect_vecs.push_back(lots_from_owners_vec);  
//             }

//             else if (key == "parents") {
//                 if (!value.is<picojson::array>()) {
//                     std::cerr << "Parents key expects an array, but doesn't recognize one -- it's probably malformed!" << std::endl;
//                     return -1;
//                 }
//                 picojson::array parents_arr = value.get<picojson::array>();

//                 // get the sorted vector
//                 std::vector<std::string> lots_from_parents_vec{lotman::Lot::get_lots_from_parents(parents_arr)};
//                 intersect_vecs.push_back(lots_from_parents_vec);  
                
//             }

//             else if (key == "children") {
//                 if (!value.is<picojson::array>()) {
//                     std::cerr << "Children key expects an array, but doesn't recognize one -- it's probably malformed!" << std::endl;
//                     return -1;
//                 }
//                 picojson::array children_arr = value.get<picojson::array>();

//                 // get the sorted vector
//                 std::vector<std::string> lots_from_children_vec{lotman::Lot::get_lots_from_children(children_arr)};
//                 intersect_vecs.push_back(lots_from_children_vec);  
//             }

//             else if (key == "paths") {
//                 if (!value.is<picojson::array>()) {
//                     std::cerr << "Paths key expects an array value, but doesn't recognize one -- it's probably malformed!" << std::endl;
//                     return -1;
//                 }
//                 picojson::array paths_arr = value.get<picojson::array>();

//                 // get the sorted vector
//                 std::vector<std::string> lots_from_paths_vec{lotman::Lot::get_lots_from_paths(paths_arr)};
//                 intersect_vecs.push_back(lots_from_paths_vec);  
                
//             }

//             else if (std::find(policy_attr_int_keys.begin(), policy_attr_int_keys.end(), key) != policy_attr_int_keys.end()) {
//                 if (!value.is<picojson::object>()) {
//                     std::cerr << "Policy attributes key \"" << key << "\" expects an object, but doesn't recognize one -- it's probably malformed!" << std::endl;
//                     return -1;
//                 }
//                 picojson::object policy_attr_obj = value.get<picojson::object>();
//                 auto policy_attr_iter = policy_attr_obj.begin();
//                 if (policy_attr_iter == policy_attr_obj.end()) {
//                     std::cerr << "Something is wrong -- the policy attribute object for key \"" << key << "\" appears empty." << std::endl;
//                     return -1;
//                 }

//                 std::string comparator;
//                 int comp_value;
//                 for (policy_attr_iter; policy_attr_iter != policy_attr_obj.end(); ++policy_attr_iter) {
//                     auto inner_key = policy_attr_iter->first;
//                     auto inner_value = policy_attr_iter->second;

//                     if (inner_key == "comparator") {
//                         if (!inner_value.is<std::string>()) {
//                             std::cerr << "Bad comparator for key \"" << key << "\"." << std::endl;
//                             return -1;
//                         }
                        
//                         comparator = inner_value.get<std::string>();
//                     }
//                     else if (inner_key == "value") {
//                         if (!inner_value.is<double>()) {
//                             std::cerr << "Bad value for key \"" << key << "\"." << std::endl;
//                             return -1;
//                         }

//                         comp_value = static_cast<int>(inner_value.get<double>());
//                     }
//                     else {
//                         std::cerr << "The inner key \"" << inner_key << "\" for key \"" << key << "\" is not recognized." << std::endl;
//                         return -1;
//                     }

//                 }

//                 // get the sorted vector
//                 std::vector<std::string> lots_from_policy_attr_vec{lotman::Lot::get_lots_from_int_policy_attr(key, comparator, comp_value)};
//                 intersect_vecs.push_back(lots_from_policy_attr_vec);  
//             }

//             else if (std::find(policy_attr_double_keys.begin(), policy_attr_double_keys.end(), key) != policy_attr_double_keys.end()) {
//                 if (!value.is<picojson::object>()) {
//                     std::cerr << "Policy attributes key \"" << key << "\" expects an object, but doesn't recognize one -- it's probably malformed!" << std::endl;
//                     return -1;
//                 }
//                 picojson::object policy_attr_obj = value.get<picojson::object>();
//                 auto policy_attr_iter = policy_attr_obj.begin();
//                 if (policy_attr_iter == policy_attr_obj.end()) {
//                     std::cerr << "Something is wrong -- the policy attribute object for key \"" << key << "\" appears empty." << std::endl;
//                     return -1;
//                 }

//                 std::string comparator;
//                 int comp_value;
//                 for (policy_attr_iter; policy_attr_iter != policy_attr_obj.end(); ++policy_attr_iter) {
//                     auto inner_key = policy_attr_iter->first;
//                     auto inner_value = policy_attr_iter->second;

//                     if (inner_key == "comparator") {
//                         if (!inner_value.is<std::string>()) {
//                             std::cerr << "Bad comparator for key \"" << key << "\"." << std::endl;
//                             return -1;
//                         }
                        
//                         comparator = inner_value.get<std::string>();
//                     }
//                     else if (inner_key == "value") {
//                         if (!inner_value.is<double>()) {
//                             std::cerr << "Bad value for key \"" << key << "\"." << std::endl;
//                             return -1;
//                         }

//                         comp_value = inner_value.get<double>();
//                     }
//                     else {
//                         std::cerr << "The inner key \"" << inner_key << "\" for key \"" << key << "\" is not recognized." << std::endl;
//                         return -1;
//                     }

//                 }
                
//                 // get the sorted vector
//                 std::vector<std::string> lots_from_policy_attr_vec{lotman::Lot::get_lots_from_double_policy_attr(key, comparator, comp_value)};
//                 intersect_vecs.push_back(lots_from_policy_attr_vec);  


//             }

//             else if (std::find(usage_keys.begin(), usage_keys.end(), key) != usage_keys.end()) {
//                 if (!value.is<picojson::object>()) {
//                     std::cerr << "The usage key \"" << key << "\" expects an object value but doesn't recognize one -- it's probably malformed!";
//                     return -1;
//                 }
//                 picojson::object internal_obj = value.get<picojson::object>();
//                 auto internal_iter = internal_obj.begin();
//                 if (internal_iter == internal_obj.end()) {
//                     std::cerr << "Something is wrong -- the usage object for key \"" << key << "\" appears empty.";
//                     return -1;
//                 }
//                 std::string comparator;
//                 double comp_val;
//                 bool recursive;
//                 for (internal_iter; internal_iter != internal_obj.end(); ++internal_iter) {
//                     std::string internal_key = internal_iter->first;
//                     auto internal_value = internal_iter->second;
//                     if (internal_key == "comparator") {
//                         if (!internal_value.is<std::string>()) {
//                             std::cerr << "The comparator for key \"" << key << "\" is not recognized as a string." << std::endl;
//                             return -1;
//                         }
//                         comparator = internal_value.get<std::string>();
//                     }
//                     else if (internal_key == "value") {
//                         std::array<std::string, 3> allowed_allotted_keys{{"dedicated_GB_usage", "opportunistic_GB_usage", "num_objects_usage"}};
//                         if (!internal_value.is<double>()) {       
//                             if (std::find(allowed_allotted_keys.begin(), allowed_allotted_keys.end(), key) != allowed_allotted_keys.end()) {
//                                 if (!internal_value.is<std::string>()) {
//                                     std::cerr << "The comparator value for key \"" << key << "\" must either be a double, or the string 'allotted'." << std::endl;
//                                     return -1;
//                                 }
//                                 if (internal_value.get<std::string>() != "allotted") {
//                                     std::cerr << "The only recognized string comparator value for key \"" << key << "\" 'allotted'." << std::endl;
//                                     return -1;
//                                 }
//                                 comp_val = -1; //set to negative to indicate that "allotted" is the comp val
//                             }
//                             else {
//                                 std::cerr << "The comparator for key \"" << key << "\" is not recognized as a double.";
//                             }
//                         }
//                         else {
//                             comp_val = internal_value.get<double>();
//                         }
//                     }
//                     else if (internal_key == "recursive") {
//                         if (!internal_value.is<bool>()) {
//                             std::cerr << "The recursive value for key \"" << key << "\" is not recognized as a bool." << std::endl;
//                             return -1;
//                         }
//                         recursive = internal_value.get<bool>();
//                     }
//                     else {
//                         std::cerr << "The internal key \"" << internal_key << "\" for key \"" << key << "\" is not recognized." << std::endl;
//                         return -1;
//                     }

//                 }

//                 // get the sorted vector
//                 std::vector<std::string> lots_from_usage_vec{lotman::Lot::get_lots_from_usage(key, comparator, comp_val, recursive)};
//                 intersect_vecs.push_back(lots_from_usage_vec);
//             }

//             else {

//                 std::cerr << "Unrecognized key: " << key << std::endl;
//                 return -1;
//             }
//         } 

//         if (intersect_vecs.size() == 0) {
//             // No criteria were supplied! Strange...
//         }

//         // Only one criterion was supplied
//         else if (intersect_vecs.size() == 1) {
//             output_vec = intersect_vecs[0];
//         }

//         // Multiple criteria supplied 
//         else {
//             std::set_intersection(intersect_vecs[0].begin(), intersect_vecs[0].end(),
//                                   intersect_vecs[1].begin(), intersect_vecs[1].end(),
//                                   std::back_inserter(output_vec));
//             for (int iter = 2; iter < intersect_vecs.size(); ++iter) {
//                 std::vector<std::string> tmp_vec;
//                 set_intersection(output_vec.begin(), output_vec.end(),
//                                  intersect_vecs[iter].begin(), intersect_vecs[iter].end(),
//                                  std::back_inserter(tmp_vec));
//                 output_vec = tmp_vec;
//             }
//         }
//     }
//     catch (std::exception &exc) {
//         if (err_msg) {
//             *err_msg = strdup(exc.what());
//         }
//         std::cerr << *err_msg << std::endl;
//         return -1;
//     }
    
    
//     auto lots_list_c = static_cast<char **>(malloc(sizeof(char *) * (output_vec.size() +1))); 
//     lots_list_c[output_vec.size()] = nullptr;
//     int idx = 0;
//     for (const auto &iter : output_vec) {
//         lots_list_c[idx] = strdup(iter.c_str());
//         if (!lots_list_c[idx]) {
//             lotman_free_string_list(lots_list_c);
//             if (err_msg) {
//                 *err_msg = strdup("Failed to create a copy of string entry in list");
//             }
//             return -1;
//         }
//         idx++;
//     }
//     *output = lots_list_c;
//     return 0;
// }
