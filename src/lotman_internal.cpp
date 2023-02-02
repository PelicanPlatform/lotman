#include <stdio.h>
#include <iostream>
#include <picojson.h>
//#include <vector>

#include "lotman_internal.h"

//using namespace lotman;


/**
 * Functions specific to Lot class
*/
    bool lotman::Lot::initialize_root(std::string lot_name, std::string lot_path, std::string owner, std::string resource_limits_str, std::string reclamation_policy_str) {
        std::string parent_path = "NULL";
        picojson::value resource_limits;
        picojson::value reclamation_policy;

        std::string err = picojson::parse(resource_limits, resource_limits_str);
        err =  picojson::parse(reclamation_policy, reclamation_policy_str);

        if (!err.empty()) {
            throw JsonException(err);
        }



        // Disable this for now
        /*
        if (!Validator::check_for_parent_child_dirs(lot_path)) {
            return false;
        }
        */

       return store_lot(lot_name, lot_path, parent_path, owner, resource_limits, reclamation_policy);
    }
    


    bool lotman::Lot::add_sublot(std::string lot_name, std::string lot_path, std::string parent, std::string owner, std::string resource_limits_str, std::string reclamation_policy_str) {
        picojson::value resource_limits;
        picojson::value reclamation_policy;
        
        // Check that the sublot has a parent lot
        std::string parent_lot = lotman::Lot::get_parent_lot_name(parent); // if this returns an empty string, there is no parent lot
        if (parent_lot.length()==0) {return false;} 


        std::string err = picojson::parse(resource_limits, resource_limits_str);
        err =  picojson::parse(reclamation_policy, reclamation_policy_str);

        if (!err.empty()) {
            throw JsonException(err);
        }


        // if (get_sublot_paths(lot_path).size() > 0) {
        //     return false;
        // } // If there are any sublot paths, fail. Prevents inserting sublot between a parent and child.

        return store_lot(lot_name, lot_path, parent, owner, resource_limits, reclamation_policy);
    }
    

    bool lotman::Lot::remove_sublot(std::string lot_path) {
        return remove_lot(lot_path);
    }


    std::string lotman::Lot::get_parent_lot_name(std::string parent, bool get_root) {
        std::vector<std::string> lot_parents_vec = lotman::Validator::get_parent_names(parent, get_root);
        if (!lot_parents_vec.size()>0) {
            return std::string();
        }

        std::string lot_parent = lot_parents_vec[0];
        return lot_parent;
    }

    // std::vector<std::string> lotman::Lot::get_sublot_paths(std::string lot_path, bool recursive) {
    //     std::vector<std::string> sublot_paths = lotman::Validator::get_child_dirs(lot_path, recursive);
    //     return sublot_paths;
    // }



/**
 * Functions specific to Validator class
*/

    // bool lotman::Validator::check_for_parent_child_dirs(std::string lot_path) {
    //     std::string is_parent_dir_query = "SELECT path FROM lot WHERE path LIKE '" + lot_path + "%';";
    //     std::string is_child_dir_query = "SELECT path FROM lot WHERE path=SUBSTRING('" + lot_path + "',1,LENGTH(PATH));";

    //     int total_matches = SQL_match_exists(is_parent_dir_query) + SQL_match_exists(is_child_dir_query);
    //     if (total_matches > 0) {return false;}

    //     return true;
    // }


    std::vector<std::string> lotman::Validator::get_parent_names(std::string parent, bool get_root) {
        std::string parent_query;

        if (get_root) {
            // TODO: Finish this part
            std::cout << "You haven't written the code to get the root parent name!!" << std::endl;
        }
        else {
            parent_query = "SELECT lot_name FROM lot WHERE lot_name=\"" + parent + "\";";

        }

        std::vector<std::string> parent_path_vec = lotman::Validator::SQL_get_matches(parent_query);
        return parent_path_vec;
    }


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

    
