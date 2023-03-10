#include <sqlite3.h>
#include <string>
#include <unistd.h>
#include <cstring>
#include <cstdio>
#include <iostream>
#include <sys/stat.h>

// This shouldn't cause issues, but defining and using int_64 causes some things to fail.
// #ifndef PICOJSON_USE_INT64
// #define PICOJSON_USE_INT64
// #endif
#include <picojson.h>

#include "lotman_internal.h"

/*
Code for initializing the sqlite database that stores important Lot object information

*/

namespace {
void initialize_lotdb( const std::string &lot_file) {
    sqlite3 *db;
    int rc = sqlite3_open(lot_file.c_str(), &db);
    if (rc != SQLITE_OK) {
        std::cerr << "SQLite Lot database creation failed." << std::endl;
        sqlite3_close(db);
        return;
    }

    char *err_msg = nullptr;
    rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS owners ("
                        "lot_name PRIMARY KEY NOT NULL,"
                        "owner NOT NULL)",
                    NULL, 0, &err_msg);
    if (rc) {
        std::cerr << "Sqlite owners table creation failed: " << err_msg << std::endl;
        sqlite3_free(err_msg);
    }

    rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS parents ("
                        "lot_name NOT NULL,"
                        "parent NOT NULL,"
                        "PRIMARY KEY (lot_name, parent))",
                    NULL, 0, &err_msg);
    if (rc) {
        std::cerr << "Sqlite parents table creation failed: " << err_msg << std::endl;
        sqlite3_free(err_msg);
    }

    rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS paths ("
                        "lot_name NOT NULL,"
                        "path NOT NULL,"
                        "recursive NOT_NULL)",
                    NULL, 0, &err_msg);
    if (rc) {
        std::cerr << "Sqlite paths table creation failed: " << err_msg << std::endl;
        sqlite3_free(err_msg);
    }

    rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS management_policy_attributes ("
                        "lot_name PRIMARY KEY NOT NULL,"
                        "dedicated_GB,"
                        "opportunistic_GB,"
                        "max_num_objects,"
                        "creation_time,"
                        "expiration_time,"
                        "deletion_time)",
                    NULL, 0, &err_msg);
    if (rc) {
        std::cerr << "Sqlite management_policy_attrs table creation failed: " << err_msg << std::endl;
        sqlite3_free(err_msg);
    }

    rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS lot_usage ("
                        "lot_name PRIMARY KEY NOT NULL,"
                        "self_GB NOT NULL,"
                        "children_GB NOT NULL,"
                        "self_objects NOT NULL,"
                        "children_objects NOT NULL,"
                        "self_GB_being_written NOT NULL,"
                        "children_GB_being_written NOT NULL,"
                        "self_objects_being_written NOT NULL,"
                        "children_objects_being_written NOT NULL)",
                    NULL, 0, &err_msg);
    if (rc) {
        std::cerr << "Sqlite lot_usage table creation failed: " << err_msg << std::endl;
        sqlite3_free(err_msg);
    }

    sqlite3_close(db);
}

std::string get_lot_file() {
    const char *lot_home_dir = getenv("LOT_HOME"); // Env variable where Lot info is stored

    if (!lot_home_dir) {
        return "";
    }
    // For now, make the assumption that $LOT_HOME is set and not null
    // TODO: Handle getenv("LOT_HOME") returning nullptr
    std::string lot_str(lot_home_dir);
    int r = mkdir(lot_str.c_str(), 0700);
    if ((r < 0) && errno != EEXIST) {
        return "";
    }

    std::string lot_dir = lot_str + "/.lot";
    r = mkdir(lot_dir.c_str(), 0700);
    if ((r < 0) && errno != EEXIST) {
        return "";
    }

    std::string lot_file = lot_dir + "/lotman_cpp.sqlite";
    initialize_lotdb(lot_file);

    return lot_file;
}











std::pair<bool, std::string> initialize_lotdb2(const std::string &lot_file) {
    sqlite3 *db;
    int rc = sqlite3_open(lot_file.c_str(), &db);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return std::make_pair(false, "SQLite Lot database creation failed.");
    }

    char *err_msg = nullptr;
    rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS owners ("
                        "lot_name PRIMARY KEY NOT NULL,"
                        "owner NOT NULL)",
                    NULL, 0, &err_msg);
    if (rc) {
        auto rp = std::make_pair(false, "SQLite owners table creation failed: " + static_cast<std::string>(err_msg));
        sqlite3_free(err_msg);
        return rp;
    }

    rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS parents ("
                        "lot_name NOT NULL,"
                        "parent NOT NULL,"
                        "PRIMARY KEY (lot_name, parent))",
                    NULL, 0, &err_msg);
    if (rc) {
        auto rp = std::make_pair(false, "SQLite parents table creation failed: " + static_cast<std::string>(err_msg));
        sqlite3_free(err_msg);
        return rp;
    }

    rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS paths ("
                        "lot_name NOT NULL,"
                        "path NOT NULL,"
                        "recursive NOT_NULL)",
                    NULL, 0, &err_msg);
    if (rc) {
        auto rp = std::make_pair(false, "SQLite paths table creation failed: " + static_cast<std::string>(err_msg));
        sqlite3_free(err_msg);
        return rp;
    }

    rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS management_policy_attributes ("
                        "lot_name PRIMARY KEY NOT NULL,"
                        "dedicated_GB,"
                        "opportunistic_GB,"
                        "max_num_objects,"
                        "creation_time,"
                        "expiration_time,"
                        "deletion_time)",
                    NULL, 0, &err_msg);
    if (rc) {
        auto rp = std::make_pair(false, "SQLite management_policy_attributes table creation failed: " + static_cast<std::string>(err_msg));
        sqlite3_free(err_msg);
        return rp;
    }

    rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS lot_usage ("
                        "lot_name PRIMARY KEY NOT NULL,"
                        "self_GB NOT NULL,"
                        "children_GB NOT NULL,"
                        "self_objects NOT NULL,"
                        "children_objects NOT NULL,"
                        "self_GB_being_written NOT NULL,"
                        "children_GB_being_written NOT NULL,"
                        "self_objects_being_written NOT NULL,"
                        "children_objects_being_written NOT NULL)",
                    NULL, 0, &err_msg);
    if (rc) {
        auto rp = std::make_pair(false, "SQLite lot_usage table creation failed: " + static_cast<std::string>(err_msg));
        sqlite3_free(err_msg);
        return rp;
    }

    sqlite3_close(db);
    return std::make_pair(true, "");
}



std::pair<bool, std::string> get_lot_file2() {
    const char *lot_home_dir = getenv("LOT_HOME"); // Env variable where Lot info is stored

    if (!lot_home_dir) {
        return std::make_pair(false, "Could not find environment variable $LOT_HOME.");
    }
    // For now, make the assumption that $LOT_HOME is set and not null
    // TODO: Handle getenv("LOT_HOME") returning nullptr
    std::string lot_str(lot_home_dir);
    int r = mkdir(lot_str.c_str(), 0700);
    if ((r < 0) && errno != EEXIST) {
        return std::make_pair(false, "Unable to create directory " + lot_str + ": errno: " + std::to_string(errno));
    }

    std::string lot_dir = lot_str + "/.lot";
    r = mkdir(lot_dir.c_str(), 0700);
    if ((r < 0) && errno != EEXIST) {
        return std::make_pair(false, "Unable to create directory " + lot_dir + ": errno: " + std::to_string(errno));
    }

    std::string lot_file = lot_dir + "/lotman_cpp.sqlite";
    auto rp = initialize_lotdb2(lot_file);
    if (!rp.first) {
        return std::make_pair(false, "Unable to initialize lotdb: " + rp.second);
    }

    return std::make_pair(true, lot_file);
}
} //namespace


std::pair<bool, std::string> lotman::Lot2::write_new() {
    auto lot_fname = get_lot_file2();
    if (!lot_fname.first) {
        return std::make_pair(false, "Could not get lot_file: " + lot_fname.second);
    }
    sqlite3 *db;
    int rc = sqlite3_open(lot_fname.second.c_str(), &db);
    if (rc) {
        sqlite3_close(db);
        return std::make_pair(false, "Unable to open lotdb: sqlite errno: " + std::to_string(rc));
    }

    sqlite3_stmt *owner_stmt;
    rc = sqlite3_prepare_v2(db, "INSERT INTO owners VALUES (?, ?)", -1, &owner_stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_prepare_v2 failed: sqlite errno: " + std::to_string(rc));
    }

    // Bind inputs to sql statement
    rc = sqlite3_bind_text(owner_stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(owner_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_text for lot_name failed: sqlite errno: " + std::to_string(rc));
    }

    rc = sqlite3_bind_text(owner_stmt, 2, owner.c_str(), owner.size(), SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(owner_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_text for owner failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_step(owner_stmt);
    if (rc != SQLITE_DONE) {
        sqlite3_finalize(owner_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_step for owner table failed: sqlite errno: " + std::to_string(rc));
    } 
    sqlite3_exec(db, "COMMIT", 0, 0, 0);
    sqlite3_finalize(owner_stmt);   

    for (const auto& parent : parents) {
        sqlite3_stmt *parent_stmt;
        rc = sqlite3_prepare_v2(db, "INSERT INTO parents VALUES (?, ?)", -1, &parent_stmt, NULL);
        if (rc != SQLITE_OK) {
            sqlite3_close(db);
            return std::make_pair(false, "Call to sqlite3_prepare_v2 for parent_stmt failed: sqlite errno: " + std::to_string(rc));
        }

        // Bind inputs to sql statement
        rc = sqlite3_bind_text(parent_stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC);
        if (rc != SQLITE_OK) {
            sqlite3_finalize(parent_stmt);
            sqlite3_close(db);
            return std::make_pair(false, "Call to sqlite3_bind_text for lot_name failed: sqlite errno: " + std::to_string(rc));
        }
        rc = sqlite3_bind_text(parent_stmt, 2, parent.c_str(), parent.size(), SQLITE_STATIC);
        if (rc != SQLITE_OK) {
            sqlite3_finalize(parent_stmt);
            sqlite3_close(db);
            return std::make_pair(false, "Call to sqlite3_bind_text for parent \"" + parent + "\" failed: sqlite errno: " + std::to_string(rc));
        }
        rc = sqlite3_step(parent_stmt);
        if (rc != SQLITE_DONE) {
            int err = sqlite3_extended_errcode(db);
            sqlite3_finalize(parent_stmt);
            sqlite3_close(db);
            return std::make_pair(false, "Call to sqlite3_step for parent_stmt failed: sqlite errno: " + std::to_string(rc));
        } 
        sqlite3_exec(db, "COMMIT", 0, 0, 0);
        sqlite3_finalize(parent_stmt);   
    }


    // Paths
    for (const auto& path : paths) {
        sqlite3_stmt *path_stmt;

        rc = sqlite3_prepare_v2(db, "INSERT INTO paths VALUES (?, ?, ?)", -1, &path_stmt, NULL);
        if (rc != SQLITE_OK) {
            sqlite3_close(db);
            return std::make_pair(false, "Call to sqlite_prepare_v2 failed for path_stmt: sqlite errno: " + std::to_string(rc));
        }

        // Bind inputs to sql statement
        rc = sqlite3_bind_text(path_stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC);
        if (rc != SQLITE_OK) {
            sqlite3_finalize(path_stmt);
            sqlite3_close(db);
            return std::make_pair(false, "Call to sqlite_bind_text for lot_name failed: sqlite errno: " + std::to_string(rc));
        }

        rc = sqlite3_bind_text(path_stmt, 2, path["path"].get<std::string>().c_str(),  path["path"].get<std::string>().size(), SQLITE_STATIC);
        if (rc != SQLITE_OK) {
            sqlite3_finalize(path_stmt);
            sqlite3_close(db);
            return std::make_pair(false, "Call to sqlite_bind_text for path \"" + path.get<std::string>() + "\" failed: sqlite errno: " + std::to_string(rc));
        }

        rc = sqlite3_bind_int(path_stmt, 3, static_cast<int>(path["recursive"].get<bool>()));
        if (rc != SQLITE_OK) {
            sqlite3_finalize(path_stmt);
            sqlite3_close(db);
            return std::make_pair(false, "Call to sqlite_bind_int for recursive val for path \"" + path.get<std::string>() + "\" failed: sqlite errno: " + std::to_string(rc));
        }

        rc = sqlite3_step(path_stmt);
        if (rc != SQLITE_DONE) {
            sqlite3_finalize(path_stmt);
            sqlite3_close(db);
            return std::make_pair(false, "Call to sqlite_step for path table failed: sqlite errno: " + std::to_string(rc));
        } 
        sqlite3_exec(db, "COMMIT", 0, 0, 0);
        sqlite3_finalize(path_stmt);     
    }

    // Set up management policy attributes
    sqlite3_stmt *man_pol_attr_stmt;
    rc = sqlite3_prepare_v2(db, "INSERT INTO management_policy_attributes VALUES (?, ?, ?, ?, ?, ?, ?)", -1, &man_pol_attr_stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite_prepare_v2 failed for man_pol_attr_stmt: sqlite errno: " + std::to_string(rc));
    }

    // Bind inputs to sql statement
    rc = sqlite3_bind_text(man_pol_attr_stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_text for lot_name failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_double(man_pol_attr_stmt, 2, man_policy_attr.dedicated_GB);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_double for dedicated_GB failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_double(man_pol_attr_stmt, 3, man_policy_attr.opportunistic_GB);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_double for opportunistic_GB failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_int64(man_pol_attr_stmt, 4, man_policy_attr.max_num_objects);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_int64 for max_num_objects failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_int64(man_pol_attr_stmt, 5, man_policy_attr.creation_time);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_int64 for creation_time failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_int64(man_pol_attr_stmt, 6, man_policy_attr.expiration_time);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_int64 for expiration_time failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_int64(man_pol_attr_stmt, 7, man_policy_attr.deletion_time);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_int64 for deletion_time failed: sqlite errno: " + std::to_string(rc));
    }

    rc = sqlite3_step(man_pol_attr_stmt);
    if (rc != SQLITE_DONE) {
        int err = sqlite3_extended_errcode(db);
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_step for man_pol_attr_stmt failed: sqlite errno: " + std::to_string(rc));
    }

    sqlite3_exec(db, "COMMIT", 0, 0, 0);
    sqlite3_finalize(man_pol_attr_stmt);


// Initialize all lot usage parameters to 0
    sqlite3_stmt *init_stmt;
    rc = sqlite3_prepare_v2(db, "INSERT INTO lot_usage VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", -1, &init_stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_prepare_v2 for init_stmt failed: sqlite errno: " + std::to_string(rc));
    }

    // Bind inputs to sql statement
    rc = sqlite3_bind_text(init_stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(init_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_text for lot_name failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_double(init_stmt, 2, usage.self_GB);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(init_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_double for self_GB failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_double(init_stmt, 3, usage.children_GB);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(init_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_double for children_GB failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_int64(init_stmt, 4, usage.self_objects);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(init_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_int64 for self_objects failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_int64(init_stmt, 5, usage.children_objects);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(init_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_int64 for children_objects failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_double(init_stmt, 6, usage.self_GB_being_written);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(init_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_double for self_GB_being_written failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_double(init_stmt, 7, usage.children_GB_being_written);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(init_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_double for children_GB_being_written failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_int64(init_stmt, 8, usage.self_objects_being_written);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(init_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_int64 for self_objects_being_written failed: sqlite errno: " + std::to_string(rc));
    }
    rc = sqlite3_bind_int64(init_stmt, 9, usage.children_objects_being_written);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(init_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_bind_int64 for children_objects_being_written failed: sqlite errno: " + std::to_string(rc));
    }

    rc = sqlite3_step(init_stmt);
    if (rc != SQLITE_DONE) {
        //int err = sqlite3_extended_errcode(db);
        sqlite3_finalize(init_stmt);
        sqlite3_close(db);
        return std::make_pair(false, "Call to sqlite3_step for init_stmt failed: sqlite errno: " + std::to_string(rc));
    }

    sqlite3_exec(db, "COMMIT", 0, 0, 0);
    sqlite3_finalize(init_stmt);
    sqlite3_close(db);

    return std::make_pair(true, "");

}







bool lotman::Lot::store_lot(std::string lot_name, 
                            std::string owner, 
                            std::vector<std::string> parents, 
                            std::map<std::string, int> paths_map, 
                            std::map<std::string, int> management_policy_attrs_int_map, 
                            std::map<std::string, unsigned long long> management_policy_attrs_tmstmp_map,
                            std::map<std::string, double> management_policy_attrs_double_map) {    
    // Get the lot db and open it
    auto lot_fname = get_lot_file();

    sqlite3 *db;
    int rc = sqlite3_open(lot_fname.c_str(), &db);
    if (rc) {
        sqlite3_close(db);
        return false;
    }

    // Insertion of owners into owners table
    if (owner.size() == 0) {
        std::cerr << "Something is wrong, owner string is empty." << std::endl;
        return false;
    }

    sqlite3_stmt *owner_stmt;
    rc = sqlite3_prepare_v2(db, "INSERT INTO owners VALUES (?, ?)", -1, &owner_stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }

    // Bind inputs to sql statement
    if (sqlite3_bind_text(owner_stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(owner_stmt);
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_text(owner_stmt, 2, owner.c_str(), owner.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(owner_stmt);
        sqlite3_close(db);
        return false;
    }
    rc = sqlite3_step(owner_stmt);
    if (rc != SQLITE_DONE) {
        sqlite3_finalize(owner_stmt);
        sqlite3_close(db);
        return false;
    } 
    sqlite3_exec(db, "COMMIT", 0, 0, 0);
    sqlite3_finalize(owner_stmt);   

    // Insertion of values into parents table
    if (parents.empty()) {
        std::cerr << "Something is wrong, owners vector is empty." << std::endl;
        return false;
    }

    for (auto & parents_iter : parents) {
        sqlite3_stmt *stmt;
        rc = sqlite3_prepare_v2(db, "INSERT INTO parents VALUES (?, ?)", -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            sqlite3_close(db);
            return false;
        }

        // Bind inputs to sql statement
        if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }
        if (sqlite3_bind_text(stmt, 2, parents_iter.c_str(), parents_iter.size(), SQLITE_STATIC) != SQLITE_OK) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            int err = sqlite3_extended_errcode(db);
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        } 
        sqlite3_exec(db, "COMMIT", 0, 0, 0);
        sqlite3_finalize(stmt);   
    }

    // Insertion of values into paths table
    if (paths_map.empty()) {
        std::cerr << "Something is wrong, paths map appears empty." << std::endl;
        sqlite3_close(db);
        return false;
    }

    for (auto & path : paths_map) {
        sqlite3_stmt *stmt;

        rc = sqlite3_prepare_v2(db, "INSERT INTO paths VALUES (?, ?, ?)", -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            sqlite3_close(db);
            return false;
        }

        // Bind inputs to sql statement
        if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }
        if (sqlite3_bind_text(stmt, 2, path.first.c_str(), path.first.size(), SQLITE_STATIC) != SQLITE_OK) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }

        if (sqlite3_bind_int(stmt, 3, path.second) != SQLITE_OK) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            int err = sqlite3_extended_errcode(db);
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        } 
        sqlite3_exec(db, "COMMIT", 0, 0, 0);
        sqlite3_finalize(stmt);     
    }

    // Insertion of values into management_policy_attrs table
    double dedicated_GB, opportunistic_GB;
    int max_num_objects;
    unsigned long long creation_time, expiration_time, deletion_time;
    
    if (management_policy_attrs_int_map.empty() || management_policy_attrs_tmstmp_map.empty() || management_policy_attrs_double_map.empty()) {
        std::cerr << "Something is wrong, either the management_policy_attrs_int/timestamp/double map appears empty." << std::endl;
        sqlite3_close(db);
        return false;
    }

    for (auto & management_policy_attr_int : management_policy_attrs_int_map) {
        if ( management_policy_attr_int.first == "max_num_objects") {
            max_num_objects = management_policy_attr_int.second;
        }
    }

    for (auto &management_policy_attrs_tmstmp : management_policy_attrs_tmstmp_map) {
        if (management_policy_attrs_tmstmp.first == "creation_time") {
            creation_time = management_policy_attrs_tmstmp.second;
        }
        if (management_policy_attrs_tmstmp.first == "expiration_time") {
            expiration_time = management_policy_attrs_tmstmp.second;
        }
        if (management_policy_attrs_tmstmp.first == "deletion_time") {
            deletion_time = management_policy_attrs_tmstmp.second;
        }
    }

    for (auto & management_policy_attr_double : management_policy_attrs_double_map) {
        if (management_policy_attr_double.first == "dedicated_GB") {
            dedicated_GB = management_policy_attr_double.second;
        }
        if (management_policy_attr_double.first == "opportunistic_GB") {
            opportunistic_GB = management_policy_attr_double.second;
        }
    }



    sqlite3_stmt *man_pol_attr_stmt;
    rc = sqlite3_prepare_v2(db, "INSERT INTO management_policy_attributes VALUES (?, ?, ?, ?, ?, ?, ?)", -1, &man_pol_attr_stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }

    // Bind inputs to sql statement
    if (sqlite3_bind_text(man_pol_attr_stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_double(man_pol_attr_stmt, 2, dedicated_GB) != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_double(man_pol_attr_stmt, 3, opportunistic_GB) != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_int64(man_pol_attr_stmt, 4, max_num_objects) != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_int64(man_pol_attr_stmt, 5, creation_time) != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_int64(man_pol_attr_stmt, 6, expiration_time) != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_int64(man_pol_attr_stmt, 7, deletion_time) != SQLITE_OK) {
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return false;
    }

    rc = sqlite3_step(man_pol_attr_stmt);
    if (rc != SQLITE_DONE) {
        int err = sqlite3_extended_errcode(db);
        sqlite3_finalize(man_pol_attr_stmt);
        sqlite3_close(db);
        return false;
    }

    sqlite3_exec(db, "COMMIT", 0, 0, 0);
    sqlite3_finalize(man_pol_attr_stmt);

    // Initialize all lot usage parameters to 0
    sqlite3_stmt *init_stmt;
    rc = sqlite3_prepare_v2(db, "INSERT INTO lot_usage VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", -1, &init_stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }

    // Bind inputs to sql statement
    if (sqlite3_bind_text(init_stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(init_stmt);
        sqlite3_close(db);
        return false;
    }


    // Initialize lot_usage values to 0 (lot_usage must be updated later)
    std::array<int, 4> double_bind_arr{{2,3,6,7}};
    std::array<int, 4> int_bind_arr{{4,5,8,9}};
    for (const auto &index : double_bind_arr) {
        if (sqlite3_bind_double(init_stmt, index, 0) != SQLITE_OK) {
            sqlite3_finalize(init_stmt);
            sqlite3_close(db);
            return false;
        }
    }
    for (const auto &index : int_bind_arr) {
        if (sqlite3_bind_int64(init_stmt, index, 0) != SQLITE_OK) {
            sqlite3_finalize(init_stmt);
            sqlite3_close(db);
            return false;
        }
    }

    rc = sqlite3_step(init_stmt);
    if (rc != SQLITE_DONE) {
        int err = sqlite3_extended_errcode(db);
        sqlite3_finalize(init_stmt);
        sqlite3_close(db);
        return false;
    }

    sqlite3_exec(db, "COMMIT", 0, 0, 0);
    sqlite3_finalize(init_stmt);
    sqlite3_close(db);

    return true;
}

bool lotman::Lot::delete_lot(std::string lot_name) {
    auto lot_fname = get_lot_file();
    if (lot_fname.size() == 0) {return false;}

    sqlite3 *db;
    int rc = sqlite3_open(lot_fname.c_str(), &db);
    if (rc) {
        sqlite3_close(db);
        return false;
    }

    // Delete from owners table
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, "DELETE FROM owners WHERE lot_name = ?;", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    sqlite3_exec(db, "COMMIT", 0, 0, 0);
    
    // Delete from parents table
    rc = sqlite3_prepare_v2(db, "DELETE FROM parents WHERE lot_name = ?;", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    sqlite3_exec(db, "COMMIT", 0, 0, 0);

    // Delete from paths
    rc = sqlite3_prepare_v2(db, "DELETE FROM paths WHERE lot_name = ?;", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    sqlite3_exec(db, "COMMIT", 0, 0, 0);

    // Delete from management_policy_attributes
        rc = sqlite3_prepare_v2(db, "DELETE FROM management_policy_attributes WHERE lot_name = ?;", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    sqlite3_exec(db, "COMMIT", 0, 0, 0);

    // Delete from lot_usage
    rc = sqlite3_prepare_v2(db, "DELETE FROM lot_usage WHERE lot_name = ?;", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    sqlite3_exec(db, "COMMIT", 0, 0, 0);

    sqlite3_finalize(stmt);
    sqlite3_close(db);

    return true;
}

std::vector<std::string> lotman::Validator::SQL_get_matches(std::string dynamic_query, std::map<std::string, std::vector<int>> str_map, std::map<int, std::vector<int>> int_map, std::map<double, std::vector<int>> double_map) { 
    std::vector<std::string> data_vec;

    auto lot_fname = get_lot_file();
    if (lot_fname.size() == 0) {return data_vec;}

    sqlite3 *db;
    int rc = sqlite3_open(lot_fname.c_str(), &db);
    if (rc) {
        sqlite3_close(db);
        return data_vec;
    }

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, dynamic_query.c_str(), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        std::cout << "Can't prepare get_matches query" << std::endl;
        sqlite3_close(db);
        return data_vec;
    }


    for (const auto & key : str_map) {
        for (const auto & value : key.second) {
            if (sqlite3_bind_text(stmt, value, key.first.c_str(), key.first.size(), SQLITE_STATIC) != SQLITE_OK) {
                std::cout << "problem with str_map binding" << std::endl;
                sqlite3_finalize(stmt);
                sqlite3_close(db);
                return data_vec;
            }
        }
    }
    

    for (const auto & key : int_map) {
        for (const auto & value : key.second) {
            if (sqlite3_bind_int(stmt, value, key.first) != SQLITE_OK) {
                std::cout << "problem with int_map binding" << std::endl;
                sqlite3_finalize(stmt);
                sqlite3_close(db);
                return data_vec;
            }
        }
    }
    

    for (const auto & key : double_map) {
        for (const auto & value : key.second) {
            if (sqlite3_bind_double(stmt, value, key.first) != SQLITE_OK) {
                std::cout << "problem with double map binding" << std::endl;
                sqlite3_finalize(stmt);
                sqlite3_close(db);
                return data_vec;
            }
        }
    }
    

    rc = sqlite3_step(stmt);
    while (rc == SQLITE_ROW) {
        const unsigned char *_data = sqlite3_column_text(stmt, 0);
        std::string data(reinterpret_cast<const char *>(_data));
        data_vec.push_back(data);
        rc = sqlite3_step(stmt);
    }
    if (rc == SQLITE_DONE) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return data_vec;
    }
    else {
        std::cout << "in rc != sqlite_done" << std::endl;
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return data_vec;
    }

}



std::vector<std::vector<std::string>> lotman::Validator::SQL_get_matches_multi_col(std::string dynamic_query, int num_returns, std::map<std::string, std::vector<int>> str_map, std::map<int, std::vector<int>> int_map, std::map<double, std::vector<int>> double_map) { 
    std::vector<std::vector<std::string>> data_vec;

    auto lot_fname = get_lot_file();
    if (lot_fname.size() == 0) {return data_vec;}

    sqlite3 *db;
    int rc = sqlite3_open(lot_fname.c_str(), &db);
    if (rc) {
        sqlite3_close(db);
        return data_vec;
    }

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, dynamic_query.c_str(), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        std::cout << "Can't prepare get_matches query" << std::endl;

        sqlite3_close(db);
        return data_vec;
    }


    for (const auto & key : str_map) {
        for (const auto & value : key.second) {
            if (sqlite3_bind_text(stmt, value, key.first.c_str(), key.first.size(), SQLITE_STATIC) != SQLITE_OK) {
                std::cout << "problem with str_map binding" << std::endl;
                sqlite3_finalize(stmt);
                sqlite3_close(db);
                return data_vec;
            }
        }
    }
    

    for (const auto & key : int_map) {
        for (const auto & value : key.second) {
            if (sqlite3_bind_int(stmt, value, key.first) != SQLITE_OK) {
                std::cout << "problem with int_map binding" << std::endl;
                sqlite3_finalize(stmt);
                sqlite3_close(db);
                return data_vec;
            }
        }
    }
    

    for (const auto & key : double_map) {
        for (const auto & value : key.second) {
            if (sqlite3_bind_double(stmt, value, key.first) != SQLITE_OK) {
                std::cout << "problem with double map binding" << std::endl;
                sqlite3_finalize(stmt);
                sqlite3_close(db);
                return data_vec;
            }
        }
    }
    

    rc = sqlite3_step(stmt);
    while (rc == SQLITE_ROW) {
        std::vector<std::string> internal_data_vec;
        for (int iter = 0; iter< num_returns; ++iter) {
            const unsigned char *_data = sqlite3_column_text(stmt, iter);
            std::string data(reinterpret_cast<const char *>(_data));
            internal_data_vec.push_back(data);
        }
        data_vec.push_back(internal_data_vec);
        rc = sqlite3_step(stmt);
    }
    if (rc == SQLITE_DONE) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return data_vec;
    }
    else {
        std::cout << "in rc != sqlite_done" << std::endl;
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return data_vec;
    }

}




bool lotman::Lot::store_modifications(std::string dynamic_query,
                                      std::map<std::string, std::vector<int>> str_map,
                                      std::map<int,std::vector<int>> int_map,
                                      std::map<double, std::vector<int>> double_map) {
    auto lot_fname = get_lot_file();
    if (lot_fname.size() == 0) {return false;}

    sqlite3 *db;
    int rc = sqlite3_open(lot_fname.c_str(), &db);
    if (rc) {
        sqlite3_close(db);
        return false;
    }

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, dynamic_query.c_str(), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        std::cout << "Can't prepare modify_lot query!" << std::endl;
        std::cout << "Modify lot query looks like: " << dynamic_query << std::endl;
        std::cout << "rc = " << rc << std::endl;
        sqlite3_close(db);
        return false;
    }

    for (const auto & key : str_map) {
        for (const auto & value : key.second) {
            if (sqlite3_bind_text(stmt, value, key.first.c_str(), key.first.size(), SQLITE_STATIC) != SQLITE_OK) {
                std::cout << "problem with str_map binding" << std::endl;
                sqlite3_finalize(stmt);
                sqlite3_close(db);
                return false;
            }
        }
    }
    

    for (const auto & key : int_map) {
        for (const auto & value : key.second) {
            if (sqlite3_bind_int(stmt, value, key.first) != SQLITE_OK) {
                std::cout << "problem with int_map binding" << std::endl;
                sqlite3_finalize(stmt);
                sqlite3_close(db);
                return false;
            }
        }
    }
    

    for (const auto & key : double_map) {
        for (const auto & value : key.second) {
            if (sqlite3_bind_double(stmt, value, key.first) != SQLITE_OK) {
                std::cout << "problem with double map binding" << std::endl;
                sqlite3_finalize(stmt);
                sqlite3_close(db);
                return false;
            }
        }
    }
    

    // char *prepared_query = sqlite3_expanded_sql(stmt); // Useful to print for debugging
    rc = sqlite3_step(stmt);

    if (rc != SQLITE_DONE) {
        std::cout << "in rc != sqlite_done" << std::endl;
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }

    return true;
}

bool lotman::Lot::store_new_rows(std::string lot_name,
                           std::vector<std::string> owners,
                           std::vector<std::string> parents,
                           std::map<std::string, int> paths_map) {

    auto lot_fname = get_lot_file();
    if (lot_fname.size() == 0) {return false;}

    sqlite3 *db;
    int rc = sqlite3_open(lot_fname.c_str(), &db);
    if (rc) {
        sqlite3_close(db);
        return false;
    }

    for (auto & owner : owners) {
        sqlite3_stmt *stmt;
        rc = sqlite3_prepare_v2(db, "INSERT INTO owners VALUES (?, ?)", -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            sqlite3_close(db);
            return false;
        }

        // Bind inputs to sql statement
        if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }
        if (sqlite3_bind_text(stmt, 2, owner.c_str(), owner.size(), SQLITE_STATIC) != SQLITE_OK) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        } 
        sqlite3_exec(db, "COMMIT", 0, 0, 0);
        sqlite3_finalize(stmt);   
    }

    // Insertion of values into parents table
    for (auto & parent : parents) {
        sqlite3_stmt *stmt;
        rc = sqlite3_prepare_v2(db, "INSERT INTO parents VALUES (?, ?)", -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            sqlite3_close(db);
            return false;
        }

        // Bind inputs to sql statement
        if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }
        if (sqlite3_bind_text(stmt, 2, parent.c_str(), parent.size(), SQLITE_STATIC) != SQLITE_OK) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            int err = sqlite3_extended_errcode(db);
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        } 
        sqlite3_exec(db, "COMMIT", 0, 0, 0);
        sqlite3_finalize(stmt);   
    }

    for (auto & path : paths_map) {
        sqlite3_stmt *stmt;
        rc = sqlite3_prepare_v2(db, "INSERT INTO paths VALUES (?, ?, ?)", -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            sqlite3_close(db);
            return false;
        }

        // Bind inputs to sql statement
        if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }
        if (sqlite3_bind_text(stmt, 2, path.first.c_str(), path.first.size(), SQLITE_STATIC) != SQLITE_OK) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }
        if (sqlite3_bind_int(stmt, 3, path.second) != SQLITE_OK) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            int err = sqlite3_extended_errcode(db);
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }
        sqlite3_exec(db, "COMMIT", 0, 0, 0);
        sqlite3_finalize(stmt);   
    }
    return true;
}
