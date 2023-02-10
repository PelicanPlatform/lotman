
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
                    if (!path_update_obj_iter->second.is<double>()) {
                        std::cerr << "A recursion flag for the path: " << path_update_obj_iter->first << "is not recognized as a number" << std::endl;
                        return -1;
                    }
                    paths_map[path_update_obj_iter->first] = path_update_obj_iter->second.get<double>();
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
        if (err_msg) {*err_msg = strdup("Name for lot to be removed must not be nullpointer.");}
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
                    paths_map[path_update_obj_iter->first] = path_update_obj_iter->second.get<double>();
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
        if (err_msg) {*err_msg = strdup("Name for lot to be removed must not be nullpointer.");}
        return -1;
    }
    return lotman::Lot::lot_exists(lot_name);
}

int lotman_get_owners(const char *lot_name, 
                      bool recursive, 
                      char **err_msg) {
    return false;
}

int lotman_get_parent_names(const char *lot_name, 
                            bool recursive, 
                            char **err_msg){
    return false;
}

int lotman_get_children_names(const char *lot_name, 
                              bool recursive, 
                              char **err_msg) {
    return false;
}

int lotman_get_policy_attributes(const char *lot_name, 
                                 const char *policy_attributes_JSON, 
                                 bool recursive, 
                                 char **err_msg) {
    return false;
}

int lotman_get_lot_dirs(const char *lot_name, 
                        bool recursive, 
                        char **err_msg) {
    return false;
}

int lotman_get_matching_lots(const char *criteria_JSON, 
                            bool recursive, 
                            char **err_msg) {
    return false;
}

int lotman_check_db_health(char **err_msg) {
    return false;
}

