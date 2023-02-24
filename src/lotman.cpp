
#include <iostream> //DELETE
#include <string.h>
#include <picojson.h>

#include "lotman.h"
#include "lotman_internal.h"
#include "lotman_version.h"

const char * lotman_version() {
    std::string major = std::to_string(Lotman_VERSION_MAJOR);
    std::string minor = std::to_string(Lotman_VERSION_MINOR);
    static std::string version = major+"."+minor;

    return version.c_str();
}

int lotman_add_lot(const char *lotman_JSON_str, 
                   const char *lotman_context, 
                   char **err_msg) {
    // TODO: Check for context and figure out what to do with it
    
    picojson::value lotman_JSON;
    std::string err = picojson::parse(lotman_JSON, lotman_JSON_str);
    if (!err.empty()) {
        std::cerr << "LotMan JSON can't be parsed -- it's probably malformed!";
        std::cerr << err << std::endl;
    }

    if (!lotman_JSON.is<picojson::object>()) {
        std::cerr << "LotMan JSON is not recognized as an object -- it's probably malformed!" << std::endl;
        return -1;
    }

    auto lot_obj = lotman_JSON.get<picojson::object>();
    auto iter = lot_obj.begin();
    if (iter == lot_obj.end()) {
        std::cerr << "Something is wrong -- LotMan JSON object appears empty." << std::endl;
        return -1;
    }

    std::string lot_name;
    std::vector<std::string> owners, parents, children;
    std::map<std::string, int> management_policy_attrs_int_map, paths_map;
    std::map<std::string, double> management_policy_attrs_double_map;
    //picojson::value management_policy_attrs;
    //picojson::array paths;
    for (iter; iter != lot_obj.end(); ++iter) {
        auto first_item = iter->first; 
        auto second_item = iter->second; 

        if (first_item == "lot_name") {
            if (!second_item.is<std::string>()) {
                std::cerr << "Something is wrong -- lot name is not recognized as a string." << std::endl;
                return -1;
            }
            lot_name = second_item.get<std::string>();
        }
        else if (first_item == "owners") { 
            if (!second_item.is<picojson::array>()) { // TODO: Since second_item is already a vector, no need to create a second owners vec.
                std::cerr << "Something is wrong -- owners array is not array." << std::endl;
                return -1;
            }
            auto owners_array = second_item.get<picojson::array>();
            if (owners_array.empty()) {
                std::cerr << "Something is wrong -- owners array appears empty." << std::endl;
                return -1;
            }
            for (auto & owners_iter : owners_array) {
                if (!owners_iter.is<std::string>()) {
                    std::cerr << "Something is wrong -- one of the owners can't be interpreted as a string." << std::endl;
                    return -1;
                }
                owners.push_back(owners_iter.get<std::string>());
            }
        }
        else if (first_item == "parents") {
            if (!second_item.is<picojson::array>()) {
                std::cerr << "Something is wrong -- parents array is not array." << std::endl;
                return -1; 
            }
            auto parents_array = second_item.get<picojson::array>();
            if (parents_array.empty()) {
                std::cerr << "Something is wrong -- parents array appears empty." << std::endl;
                return -1;
            }
            for (auto & parents_iter : parents_array) {
                if (!parents_iter.is<std::string>()) {
                    std::cerr << "Something is wrong -- one of the parents can't be interpreted as a string." << std::endl;
                    return -1;
                }
                parents.push_back(parents_iter.get<std::string>());
            }
        }
        else if (first_item == "children") {
            if (!second_item.is<picojson::array>()) {
                std::cerr << "Something is wrong -- children array is not array." << std::endl;
                return -1; 
            }
            auto children_array = second_item.get<picojson::array>();
            if (children_array.empty()) {
                std::cerr << "Something is wrong -- it looks like there should be children but none were found. Did you forget to add children?" << std::endl;
                return -1;
            }
            for (auto & children_iter : children_array) {
                if (!children_iter.is<std::string>()) {
                    std::cerr << "Something is wrong -- one of the children can't be interpreted as a string." << std::endl;
                    return -1;
                }
                children.push_back(children_iter.get<std::string>());
            }
        }

        else if (first_item == "management_policy_attrs") {
            if (!second_item.is<picojson::object>()) {
                std::cerr << "Something is wrong -- management_policy_attrs appears to be malformed." << std::endl;
                return -1;
            }

            auto management_policy_attrs = second_item.get<picojson::object>();
            auto management_policy_attrs_iter = management_policy_attrs.begin();
            if (management_policy_attrs_iter == management_policy_attrs.end()) {
                std::cerr << "Management policy attributes object appears empty" << std::endl;
                return -1;
            }
            for (management_policy_attrs_iter; management_policy_attrs_iter != management_policy_attrs.end(); ++management_policy_attrs_iter) {
                std::array<std::string, 2> double_attributes{"dedicated_GB", "opportunistic_GB"};
                std::array<std::string, 4> int_attributes{"max_num_objects", "creation_time", "expiration_time", "deletion_time"};
                if (std::find(double_attributes.begin(), double_attributes.end(), management_policy_attrs_iter->first) !=double_attributes.end()) {
                    if (!management_policy_attrs_iter->second.is<double>()) {
                        std::cerr << "The value provided for: " << management_policy_attrs_iter->first << " is not recognized as a number" << std::endl;
                        return -1;
                    }
                    management_policy_attrs_double_map[management_policy_attrs_iter->first] = management_policy_attrs_iter->second.get<double>();
                }
                else if (std::find(int_attributes.begin(), int_attributes.end(), management_policy_attrs_iter->first) != int_attributes.end()) {
                    if (!management_policy_attrs_iter->second.is<double>()) {
                        std::cerr << "The value provided for: " << management_policy_attrs_iter->first << " is not recognized as a number" << std::endl;
                        return -1;
                    }
                    management_policy_attrs_int_map[management_policy_attrs_iter->first] = management_policy_attrs_iter->second.get<double>();
                }

                else {
                    std::cout << "management policy key not recognized" << management_policy_attrs_iter->first << std::endl;
                    return -1;
                }
            }
        }

        else if (first_item == "paths") {
            if (!second_item.is<picojson::array>()) {
                std::cout << "Second item not recognized as array" << std::endl;
                return -1;
            }

            picojson::array paths_array = second_item.get<picojson::array>();
            if (paths_array.empty()) {
                std::cerr << "second item empty" << std::endl;
                return -1;
            }

            for (auto & paths_array_iter : paths_array) {
                if (!paths_array_iter.is<picojson::object>()) {
                    std::cerr << "in the paths array, entry is not an object" << std::endl;
                    return -1;
                }
                picojson::object path_update_obj = paths_array_iter.get<picojson::object>();
                auto path_update_obj_iter = path_update_obj.begin();
                if (path_update_obj_iter == path_update_obj.end()) {
                    std::cerr << "path update obj appears empty" << std::endl;
                    return -1;
                }
                for (path_update_obj_iter; path_update_obj_iter != path_update_obj.end(); ++path_update_obj_iter) {
                    if (!path_update_obj_iter->second.is<bool>()) {
                        std::cerr << "A recursion flag for the path: " << path_update_obj_iter->first << "is not recognized " << std::endl;
                        return -1;
                    }
                    paths_map[path_update_obj_iter->first] = path_update_obj_iter->second.get<bool>();
                }
            } 
        }
        else {
            std::cout << "An unrecognized key was provdided: " << first_item << std::endl;
        }
    }

    try {
        if (!lotman::Lot::add_lot(lot_name, owners, parents, children, paths_map, management_policy_attrs_int_map, management_policy_attrs_double_map)) {
            if (err_msg) {*err_msg = strdup("Failed to add lot");}
            std::cout << "error: " << *err_msg << std::endl;
            return -1;
        }
    }
    catch(std::exception &exc) {
        if (err_msg) {*err_msg = strdup(exc.what());}
        return -1;
    }

    return 0;
}

int lotman_remove_lot(const char *lot_name, 
                      bool assign_LTBR_parent_as_parent_to_orphans, 
                      bool assign_LTBR_parent_as_parent_to_non_orphans, 
                      bool assign_policy_to_children, 
                      const char *lotman_context, 
                      char **err_msg) {
    // TODO: Check for context and figure out what to do with it

    if (!lot_name) {
        if (err_msg) {*err_msg = strdup("Name for the lot to be removed must not be nullpointer.");}
        return -1;
    }

    try {
        if (!lotman::Lot::remove_lot(lot_name, assign_LTBR_parent_as_parent_to_orphans, assign_LTBR_parent_as_parent_to_non_orphans, assign_policy_to_children)) {
            if (err_msg) {*err_msg = strdup("Failed to remove lot");}
            std::cout << "error: " << *err_msg << std::endl;
            return -1;
        }
    }
    catch(std::exception &exc) {
        if (err_msg) {*err_msg = strdup(exc.what());}
        return -1;
    }

    return 0;
}

int lotman_update_lot(const char *lotman_JSON_str, 
                      const char *lotman_context,
                      char **err_msg) {
    picojson::value lotman_JSON;
    std::string err = picojson::parse(lotman_JSON, lotman_JSON_str);
    if (!err.empty()) {
        std::cerr << "LotMan JSON can't be parsed -- it's probably malformed!";
        std::cerr << err << std::endl;
        return -1;
    }

    if (!lotman_JSON.is<picojson::object>()) {
        std::cerr << "LotMan JSON is not recognized as an object -- it's probably malformed!" << std::endl;
        return -1;
    }

    auto lot_update_obj = lotman_JSON.get<picojson::object>();
    auto iter = lot_update_obj.begin();
    if (iter == lot_update_obj.end()) {
        std::cerr << "Something is wrong -- LotMan update JSON object appears empty." << std::endl;
        return -1;
    }

    std::string lot_name;
    std::map<std::string, std::string> owners_map, parents_map;
    std::map<std::string, int> paths_map, management_policy_attrs_int_map;
    std::map<std::string, double> management_policy_attrs_double_map;
    for (iter; iter != lot_update_obj.end(); ++iter) {
        auto first_item = iter->first; 
        auto second_item = iter->second; 

        if (first_item == "lot_name") {
            if (!second_item.is<std::string>()) {
                std::cerr << "Something is wrong -- lot name is not recognized as a string." << std::endl;
                return -1;
            }
            lot_name = second_item.get<std::string>();
        }
        else if (first_item == "owners") {
            if (!second_item.is<picojson::array>()) {
                std::cout << "Second item not recognized as array" << std::endl;
                return -1;
            }

            picojson::array owners_array = second_item.get<picojson::array>();
            if (owners_array.empty()) {
                std::cerr << "second item empty" << std::endl;
                return -1;
            }

            for (auto & owners_array_iter : owners_array) {
                if (!owners_array_iter.is<picojson::object>()) {
                    std::cerr << "in the owners array, entry is not an object" << std::endl;
                    return -1;
                }
                picojson::object owner_update_obj = owners_array_iter.get<picojson::object>();
                auto owner_update_obj_iter = owner_update_obj.begin();
                if (owner_update_obj_iter == owner_update_obj.end()) {
                    std::cerr << "Owner update obj appears empty" << std::endl;
                    return -1;
                }
                for (owner_update_obj_iter; owner_update_obj_iter != owner_update_obj.end(); ++owner_update_obj_iter) {
                    owners_map[owner_update_obj_iter->first] = owner_update_obj_iter->second.get<std::string>();
                }
            }
            
        }

        else if (first_item == "parents") {
            if (!second_item.is<picojson::array>()) {
                std::cout << "Second item not recognized as array" << std::endl;
                return -1;
            }

            picojson::array parents_array = second_item.get<picojson::array>();
            if (parents_array.empty()) {
                std::cerr << "second item empty" << std::endl;
                return -1;
            }

            for (auto & parents_array_iter : parents_array) {
                if (!parents_array_iter.is<picojson::object>()) {
                    std::cerr << "in the parents array, entry is not an object" << std::endl;
                    return -1;
                }
                picojson::object parent_update_obj = parents_array_iter.get<picojson::object>();
                auto parent_update_obj_iter = parent_update_obj.begin();
                if (parent_update_obj_iter == parent_update_obj.end()) {
                    std::cerr << "Parent update obj appears empty" << std::endl;
                    return -1;
                }
                for (parent_update_obj_iter; parent_update_obj_iter != parent_update_obj.end(); ++parent_update_obj_iter) {
                    parents_map[parent_update_obj_iter->first] = parent_update_obj_iter->second.get<std::string>();
                }
            }
            
        }

        else if (first_item == "management_policy_attrs") {
            if (!second_item.is<picojson::object>()) {
                std::cerr << "management policy object is not an object" << std::endl;
                return -1;
            }
            picojson::object management_policy_attrs = second_item.get<picojson::object>();

            auto management_policy_attrs_iter = management_policy_attrs.begin();
            if (management_policy_attrs_iter == management_policy_attrs.end()) {
                std::cerr << "Management policy attributes object appears empty" << std::endl;
                return -1;
            }
            for (management_policy_attrs_iter; management_policy_attrs_iter != management_policy_attrs.end(); ++management_policy_attrs_iter) {
                std::array<std::string, 2> double_attributes{"dedicated_GB", "opportunistic_GB"};
                std::array<std::string, 4> int_attributes{"max_num_objects", "creation_time", "expiration_time", "deletion_time"};
                if (std::find(double_attributes.begin(), double_attributes.end(), management_policy_attrs_iter->first) !=double_attributes.end()) {
                    management_policy_attrs_double_map[management_policy_attrs_iter->first] = management_policy_attrs_iter->second.get<double>();
                }
                else if (std::find(int_attributes.begin(), int_attributes.end(), management_policy_attrs_iter->first) != int_attributes.end()) {
                    management_policy_attrs_int_map[management_policy_attrs_iter->first] = management_policy_attrs_iter->second.get<double>();
                }

                else {
                    std::cout << "management policy key not recognized" << std::endl;
                    return -1;
                }

            }
        }

        else if (first_item == "paths") {
            if (!second_item.is<picojson::array>()) {
                std::cout << "Second item not recognized as array" << std::endl;
                return -1;
            }

            picojson::array paths_array = second_item.get<picojson::array>();
            if (paths_array.empty()) {
                std::cerr << "second item empty" << std::endl;
                return -1;
            }

            for (auto & paths_array_iter : paths_array) {
                if (!paths_array_iter.is<picojson::object>()) {
                    std::cerr << "in the paths array, entry is not an object" << std::endl;
                    return -1;
                }
                picojson::object path_update_obj = paths_array_iter.get<picojson::object>();
                auto path_update_obj_iter = path_update_obj.begin();
                if (path_update_obj_iter == path_update_obj.end()) {
                    std::cerr << "path update obj appears empty" << std::endl;
                    return -1;
                }
                for (path_update_obj_iter; path_update_obj_iter != path_update_obj.end(); ++path_update_obj_iter) {
                    paths_map[path_update_obj_iter->first] = path_update_obj_iter->second.get<bool>();
                }
            } 
        }
        else {
            std::cout << "An unrecognized key was provdided: " << first_item << std::endl;
        }
    }

    try {
        if (!lotman::Lot::update_lot(lot_name, owners_map, parents_map, paths_map, management_policy_attrs_int_map, management_policy_attrs_double_map)) {
            if (err_msg) {*err_msg = strdup("Failed to modify lot");}
            std::cout << "error: " << *err_msg << std::endl;
            return -1;
        }
    }
    catch(std::exception &exc) {
        if (err_msg) {*err_msg = strdup(exc.what());}
        return -1;
    }

    return 0;



}

int lotman_lot_exists(const char *lot_name, 
                      const char *lotman_context, 
                      char **err_msg) {
    if (!lot_name) {
        if (err_msg) {*err_msg = strdup("Name for the lot whose existence is to be determined must not be nullpointer.");}
        return -1;
    }
    return lotman::Lot::lot_exists(lot_name);
}

int lotman_get_owners(const char *lot_name, 
                      const bool recursive,
                      char ***output,
                      char **err_msg) {
    if (!lot_name) {
        if (err_msg) {*err_msg = strdup("Name for the lot whose owners are to be obtained must not be nullpointer.");}
        return -1;
    }

    std::vector<std::string> owners_list;
    try {
        owners_list = lotman::Lot::get_owners(lot_name, recursive);
    } 
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }

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

    std::vector<std::string> parents_list;
    try {
        parents_list = lotman::Lot::get_parent_names(lot_name, recursive, get_self);
    } 
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
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

int lotman_get_children_names(const char *lot_name, 
                              const bool recursive,
                              const bool get_self,
                              char ***output,
                              char **err_msg) {
    if (!lot_name) {
        if (err_msg) {*err_msg = strdup("Name for the lot whose children are to be obtained must not be nullpointer.");}
        return -1;
    }

    std::vector<std::string> children_list;
    try {
        children_list = lotman::Lot::get_children_names(lot_name, recursive, get_self);
    } 
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
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

int lotman_get_policy_attributes(const char *lot_name, 
                                 const char *policy_attributes_JSON_str,  
                                 char **output,
                                 char **err_msg) {
    if (!lot_name) {
        if (err_msg) {*err_msg = strdup("Name for the lot whose policies are to be obtained must not be nullpointer.");}
        return -1;
    }

    if (!policy_attributes_JSON_str) {
        if (err_msg) {*err_msg = strdup("The policy attributes string must not be nullpointer.");}
        return -1;
    }


    picojson::value policy_attributes_JSON;
    std::string err = picojson::parse(policy_attributes_JSON, policy_attributes_JSON_str);
    if (!err.empty()) {
        std::cerr << "Policy attributes JSON can't be parsed -- it's probably malformed!";
        std::cerr << err << std::endl;
    }

    if (!policy_attributes_JSON.is<picojson::object>()) {
        std::cerr << "Policy attributes JSON is not recognized as an object -- it's probably malformed!" << std::endl;
        return -1;
    }

    auto policy_attributes_input_obj = policy_attributes_JSON.get<picojson::object>();
    auto iter = policy_attributes_input_obj.begin();
    if (iter == policy_attributes_input_obj.end()) {
        std::cerr << "Something is wrong -- the policy attributes JSON object appears empty." << std::endl;
        return -1;
    }

    picojson::object policy_attributes_output_obj;
    try {
        for (iter; iter!=policy_attributes_input_obj.end(); ++iter) {
            std::string key = iter->first;
            int recursive = iter->second.get<bool>();
            policy_attributes_output_obj[key] = picojson::value(lotman::Lot::get_restricting_attribute(lot_name, key, recursive));
        }
        std::string policy_attributes_output_str = picojson::value(policy_attributes_output_obj).serialize();
        auto policy_attributes_output_str_c = static_cast<char *>(malloc(sizeof(char) * (policy_attributes_output_str.length() + 1)));
        policy_attributes_output_str_c = strdup(policy_attributes_output_str.c_str());
        *output = policy_attributes_output_str_c;
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

    picojson::object lot_dirs_output_obj;
    try {
        lot_dirs_output_obj = lotman::Lot::get_lot_dirs(lot_name, recursive);
        


        std::string lot_dirs_output_str = picojson::value(lot_dirs_output_obj).serialize();

        auto lot_dirs_output_str_c = static_cast<char *>(malloc(sizeof(char) * (lot_dirs_output_str.length() + 1)));
        lot_dirs_output_str_c = strdup(lot_dirs_output_str.c_str());
        *output = lot_dirs_output_str_c;
        return 0;
        
    } 
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        return -1;
    }








}

int lotman_get_matching_lots(const char *criteria_JSON_str, 
                            char ***output, 
                            char **err_msg) {
    // std::array<std::string, 13> keys{"owners", "parents", "children", 
    //                                  "paths", "dedicated_GB", "opportunistic_GB", 
    //                                  "max_num_objects", "creation_time", "expiration_time", 
    //                                  "deletion_time", "dedicated_usage_GB", "opportunistic_usage_GB", "num_objects"};
    if (!criteria_JSON_str) {
        if (err_msg) {*err_msg = strdup("The criteria string must not be nullpointer.");}
        return -1;
    }

    picojson::value criteria_JSON;
    std::string err = picojson::parse(criteria_JSON, criteria_JSON_str);
    if (!err.empty()) {
        std::cerr << "Criteria JSON can't be parsed -- it's probably malformed!";
        std::cerr << err << std::endl;
    }

    if (!criteria_JSON.is<picojson::object>()) {
        std::cerr << "Criteria JSON is not recognized as an object -- it's probably malformed!" << std::endl;
        return -1;
    }

    auto criteria_input_obj = criteria_JSON.get<picojson::object>();
    auto iter = criteria_input_obj.begin();
    if (iter == criteria_input_obj.end()) {
        std::cerr << "Something is wrong -- the criteria JSON object appears empty." << std::endl;
        return -1;
    }

    std::vector<std::string> output_vec{};
    try {

        std::vector<std::vector<std::string>> intersect_vecs;
        for (iter; iter != criteria_input_obj.end(); ++iter) {
            // check if key is one of known keys
            auto key = iter->first;
            auto value = iter->second;

            if (key == "owners") {
                if (!value.is<picojson::array>()) {
                    std::cerr << "Owners key expects an array, but doesn't recognize one -- it's probably malformed!" << std::endl;
                    return -1;
                }
                picojson::array owners_arr = value.get<picojson::array>();

                // get the sorted vector
                std::vector<std::string> lots_from_owners_vec{lotman::Lot::get_lots_from_owners(owners_arr)};
                intersect_vecs.push_back(lots_from_owners_vec);  
            }

            else if (key == "parents") {

                
            }

            else if (key == "children") {

                
            }

            else if (key == "paths") {

                
            }

            else if (key == "dedicated_GB") {

                
            }

            else if (key == "oppportunistic_GB") {

                
            }

            else if (key == "max_num_objects") {

                
            }

            else if (key == "creation_time") {

                
            }

            else if (key == "expiration_time") {

                
            }

            else if (key == "deletion_time") {

                
            }

            else if (key == "dedicated_usage_GB") {

                
            }

            else if (key == "opportunistic_usage_GB") {

                
            }

            else if (key == "owners") {

                
            }

            else {

                std::cerr << "Unrecognized key: " << key << std::endl;
                return -1;
            }
        } 

        if (intersect_vecs.size() == 0) {
            // No criteria were supplied! Strange...
        }

        // Only one criterion was supplied
        else if (intersect_vecs.size() == 1) {
            output_vec = intersect_vecs[0];
        }

        // Multiple criteria supplied and multiple
        else {
            std::set_intersection(intersect_vecs[0].begin(), intersect_vecs[0].end(),
                                  intersect_vecs[1].begin(), intersect_vecs[1].end(),
                                  std::back_inserter(output_vec));
            for (int iter = 2; iter < intersect_vecs.size(); ++iter) {
                std::vector<std::string> tmp_vec;
                set_intersection(output_vec.begin(), output_vec.end(),
                                 intersect_vecs[iter].begin(), intersect_vecs[iter].end(),
                                 std::back_inserter(tmp_vec));
                output_vec = tmp_vec;
            }
        }
    }
    catch (std::exception &exc) {
        if (err_msg) {
            *err_msg = strdup(exc.what());
        }
        std::cerr << *err_msg << std::endl;
        return -1;
    }
    
    
    auto lots_list_c = static_cast<char **>(malloc(sizeof(char *) * (output_vec.size() +1))); 
    lots_list_c[output_vec.size()] = nullptr;
    int idx = 0;
    for (const auto &iter : output_vec) {
        lots_list_c[idx] = strdup(iter.c_str());
        if (!lots_list_c[idx]) {
            lotman_free_string_list(lots_list_c);
            if (err_msg) {
                *err_msg = strdup("Failed to create a copy of string entry in list");
            }
            return -1;
        }
        idx++;
    }
    *output = lots_list_c;
    return 0;
}

int lotman_check_db_health(char **err_msg) {
    return false;
}




// int lotman_get_lot_obj(const char *lot_JSON_str, char **output, char **err_msg) {
//     if (!lot_JSON_str) {
//         if (err_msg) {*err_msg = strdup("The lot JSON string must not be nullpointer.");}
//         return -1;
//     }
//     picojson::value lot_JSON;
//     std::string err = picojson::parse(lot_JSON, lot_JSON_str);
//     if (!err.empty()) {
//         std::cerr << "Lot JSON can't be parsed -- it's probably malformed!";
//         std::cerr << err << std::endl;
//     }
//     if (!lot_JSON.is<picojson::object>()) {
//         std::cerr << "Lot JSON is not recognized as an object -- it's probably malformed!" << std::endl;
//         return -1;
//     }
//     auto lot_input_obj = lot_JSON.get<picojson::object>();
//     auto iter = lot_input_obj.begin();
//     if (iter == lot_input_obj.end()) {
//         std::cerr << "Something is wrong -- the lot JSON object appears empty." << std::endl;
//         return -1;
//     }
//     picojson::object lot_output_obj;
//     try {
//         for (iter; iter!=lot_input_obj.end(); ++iter) {
//             std::string key = iter->first;
//             int recursive = static_cast<int>(iter->second.get<double>());
//             policy_attributes_output_obj[key] = picojson::value(lotman::Lot::get_restricting_attribute(lot_name, key, recursive));
//         }
//         std::string policy_attributes_output_str = picojson::value(policy_attributes_output_obj).serialize();
//         auto policy_attributes_output_str_c = static_cast<char *>(malloc(sizeof(char) * (policy_attributes_output_str.length() + 1)));
//         policy_attributes_output_str_c = strdup(policy_attributes_output_str.c_str());
//         *output = policy_attributes_output_str_c;
//         return 0;
//     } 
//     catch (std::exception &exc) {
//         if (err_msg) {
//             *err_msg = strdup(exc.what());
//         }
//         return -1;
//     }
// }
