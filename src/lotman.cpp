
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

    
// int lotman_initialize_root(const char *name, const char *path, const char *owner, const char *resource_limits, const char *reclamation_policy, char **err_msg) {
//     if (!path) {
//         if (err_msg) {*err_msg = strdup("The path for the root lot must not be a null pointer");}
//         return -1;
//     }
//     // If the provided name is a nullpointer, use the path as a name
//     const char *real_name;
//     if (!name) {
//         real_name = path;
//     }
//     else if (name) {
//         real_name = name;
//     }
    
//     if (!owner) {
//         if (err_msg) {*err_msg = strdup("The owner for the root lot must not be a null pointer");}
//         return -1;
//     }
//     if (!resource_limits) {
//         if (err_msg) {*err_msg = strdup("The resource_limits pointer must not be null");}
//         return -1;
//     }    
//     if (!reclamation_policy) {
//         if (err_msg) {*err_msg = strdup("The reclamation_policy pointer must not be null");}
//         return -1;
//     }

//     try {
//         if (!lotman::Lot::initialize_root(name, path, owner, resource_limits, reclamation_policy)) {
//             if (err_msg) {*err_msg = strdup("Failed to initialize the root lot.");}
//             return -1;
//         }
//     }
//     catch(std::exception &exc) {
//         if (err_msg) {*err_msg = strdup(exc.what());}
//         return -1;
//     }
//     return 0;
// }


int lotman_add_lot(const char *lotman_JSON_str, const char *lotman_context, char **err_msg) {
    
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
    picojson::value management_policy_attrs;
    picojson::array paths;
    for (iter; iter != lot_obj.end(); ++iter) {
        auto first_item = iter->first; 
        auto second_item = iter->second; 

        // std::cout << "First: " << first_item << std::endl;
        // std::cout << "Second: " << second_item << std::endl;

        if (first_item == "lot_name") {
            if (!second_item.is<std::string>()) {
                std::cerr << "Something is wrong -- lot name is not recognized as a string." << std::endl;
                return -1;
            }
            lot_name = second_item.get<std::string>();
            std::cout << "In lot name loop" << std::endl;
        }
        if (first_item == "owners") { 
            std::cout << "In owners loop" << std::endl;
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
        if (first_item == "parents") {
            std::cout << "In parents loop" << std::endl;
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
        if (first_item == "children") {
            std::cout << "In children loop" << std::endl;
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

        if (first_item == "management_policy_attrs") {
            if (!second_item.is<picojson::object>()) {
                std::cerr << "Something is wrong -- management_policy_attrs appears to be malformed." << std::endl;
                return -1;
            }
            management_policy_attrs = second_item;
        }

        if (first_item == "paths") {
            if (!second_item.is<picojson::array>()) {
                std::cerr << "Something is wrong -- paths array is not recognized as an array." << std::endl;
                return -1; 
            }
            auto paths_array = second_item.get<picojson::array>();
            if (paths_array.empty()) {
                std::cerr << "Something is wrong -- no paths were found in the path array." << std::endl;
                return -1;
            }
            paths = paths_array;
        }

    }

    try {
        if (!lotman::Lot::add_lot(lot_name, owners, parents, children, paths, management_policy_attrs)) {
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


// int lotman_remove_sublot(const char *name, char **err_msg) {
//     if (!name) {
//         if (err_msg) {*err_msg = strdup("Name for lot to be removed must not be nullpointer.");}
//         return -1;
//     }

//     try {
//         if (!lotman::Lot::remove_sublot(name)) {
//             if (err_msg) {*err_msg = strdup("Failed to remove lot");}
//             std::cout << "err message: " << *err_msg << std::endl;
//             return -1;
//         }
//     }
//     catch(std::exception &exc) {
//         if (err_msg) {*err_msg = strdup(exc.what());}
//         return -1;
//     }
//     return 0;
// }


/*

int lotman_get_sublot_paths(const char *path, char **sublot_paths_arr, char **err_msg) {
    if (!path) {
        if (err_msg) {*err_msg = strdup("Path for input lot must not be nullpointer.");}
        return -1;
    }
    if (!sublot_paths_arr) {
        if (err_msg) {*err_msg = strdup("Sublot output pointer must not be null.");}
    }

    try {

        std::vector<std::string> sublot_paths_vec{lotman::Lot::get_sublot_paths(path)};
        *sublot_paths_arr 
    }
}

*/

