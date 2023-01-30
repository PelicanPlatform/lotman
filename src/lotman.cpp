
#include <iostream> //DELETE
#include <string.h>

#include "lotman.h"
#include "lotman_internal.h"
#include "lotman_version.h"

const char * lotman_version() {
    std::string major = std::to_string(Lotman_VERSION_MAJOR);
    std::string minor = std::to_string(Lotman_VERSION_MINOR);
    static std::string version = major+"."+minor;

    return version.c_str();
}

    
int lotman_initialize_root(const char *path, const char *owner, const char *users, const char *resource_limits, const char *reclamation_policy, char **err_msg) {
    if (!path) {
        if (err_msg) {*err_msg = strdup("The path for the root lot must not be a null pointer");}
        return -1;
    }
    if (!owner) {
        if (err_msg) {*err_msg = strdup("The owner for the root lot must not be a null pointer");}
        return -1;
    }
    if (!resource_limits) {
        if (err_msg) {*err_msg = strdup("The resource_limits pointer must not be null");}
        return -1;
    }    
    if (!reclamation_policy) {
        if (err_msg) {*err_msg = strdup("The reclamation_policy pointer must not be null");}
        return -1;
    }

    // DONE: TODO: Assert that lot isn't already initialized at path
    // DONE: TODO: Assert that there is no root lot up/downstream

    try {
        if (!lotman::Lot::initialize_root(path, owner, users, resource_limits, reclamation_policy)) {
            if (err_msg) {*err_msg = strdup("Failed to initialize the root lot.");}
            return -1;
        }
    }
    catch(std::exception &exc) {
        if (err_msg) {*err_msg = strdup(exc.what());}
        return -1;
    }
    return 0;
}


int lotman_add_sublot(const char *path, const char *owner, const char *users, const char *resource_limits, const char *reclamation_policy, char **err_msg) {
    if (!path) {
        if (err_msg) {*err_msg = strdup("The path for the lot must not be a null pointer");}
        return -1;
    }
    if (!owner) {
        if (err_msg) {*err_msg = strdup("The owner for the root lot must not be a null pointer");}
        return -1;
    }
    if (!resource_limits) {
        if (err_msg) {*err_msg = strdup("The resource_limits pointer must not be null");}
        return -1;
    }    
    if (!reclamation_policy) {
        if (err_msg) {*err_msg = strdup("The reclamation_policy pointer must not be null");}
        return -1;
    }

    // DONE: TODO: Generate parent_path from path
    // TODO: Confirm that sublot params don't violate parent lot params

    try {
        if (!lotman::Lot::add_sublot(path, owner, users, resource_limits, reclamation_policy)) {
            if (err_msg) {*err_msg = strdup("Failed to add sublot.");}
            return -1;
        }
    }
    catch(std::exception &exc) {
        if (err_msg) {*err_msg = strdup(exc.what());}
        return -1;
    }
    
    return 0;
}


int lotman_remove_sublot(const char *path, char **err_msg) {
    if (!path) {
        if (err_msg) {*err_msg = strdup("Path for lot to be removed must not be nullpointer.");}
        return -1;
    }

    try {
        if (!lotman::Lot::remove_sublot(path)) {
            if (err_msg) {*err_msg = strdup("Failed to remove lot");}
            std::cout << &err_msg << std::endl;
            return -1;
        }
    }
    catch(std::exception &exc) {
        if (err_msg) {*err_msg = strdup(exc.what());}
        return -1;
    }
    return 0;
}


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

