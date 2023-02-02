
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

    
int lotman_initialize_root(const char *name, const char *path, const char *owner, const char *resource_limits, const char *reclamation_policy, char **err_msg) {
    if (!path) {
        if (err_msg) {*err_msg = strdup("The path for the root lot must not be a null pointer");}
        return -1;
    }
    // If the provided name is a nullpointer, use the path as a name
    const char *real_name;
    if (!name) {
        real_name = path;
    }
    else if (name) {
        real_name = name;
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

    try {
        if (!lotman::Lot::initialize_root(name, path, owner, resource_limits, reclamation_policy)) {
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


int lotman_add_sublot(const char *name, const char *path, const char *parent, const char *owner, const char *resource_limits, const char *reclamation_policy, char **err_msg) {
    if (!path) {
        if (err_msg) {*err_msg = strdup("The path for the lot must not be a null pointer");}
        return -1;
    }
    // If the provided name is a nullpointer, use the path as a name
    const char *real_name;
    if (!name) {
        real_name = path;
    }
    else if (name) {
        real_name = name;
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

    try {
        if (!lotman::Lot::add_sublot(real_name, path, parent, owner, resource_limits, reclamation_policy)) {
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


int lotman_remove_sublot(const char *name, char **err_msg) {
    if (!name) {
        if (err_msg) {*err_msg = strdup("Name for lot to be removed must not be nullpointer.");}
        return -1;
    }

    try {
        if (!lotman::Lot::remove_sublot(name)) {
            if (err_msg) {*err_msg = strdup("Failed to remove lot");}
            std::cout << "err message: " << *err_msg << std::endl;
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

