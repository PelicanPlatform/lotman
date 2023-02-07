#include <sqlite3.h>
#include <string>
#include <unistd.h>
#include <cstring>
#include <cstdio>
#include <iostream>
#include <sys/stat.h>

#ifndef PICOJSON_USE_INT64
#define PICOJSON_USE_INT64
#endif
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
                        "lot_name NOT NULL,"
                        "owner NOT NULL,"
                        "PRIMARY KEY (lot_name, owner))",
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
                        "recursive,"
                        "PRIMARY KEY (lot_name, path))",
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
                        "GB_written,"
                        "objects_written,"
                        "bytes_being_written,"
                        "objects_being_written)",
                    NULL, 0, &err_msg);
    if (rc) {
        std::cerr << "Sqlite lot_usage table creation failed: " << err_msg << std::endl;
        sqlite3_free(err_msg);
    }

    sqlite3_close(db);
}

std::string get_lot_file() {
    const char *lot_home_dir = getenv("LOT_HOME"); // Env variable where Lot info is stored

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
} //namespace

bool lotman::Lot::store_lot(std::string lot_name, std::vector<std::string> owners, std::vector<std::string> parents, std::vector<std::string> children, picojson::array paths, picojson::value management_policy_attrs) {
    // Get the lot db and open it
    auto lot_fname = get_lot_file();

    sqlite3 *db;
    int rc = sqlite3_open(lot_fname.c_str(), &db);
    if (rc) {
        sqlite3_close(db);
        return false;
    }

    // Insertion of owners into owners table
    if (owners.empty()) {
        std::cerr << "Something is wrong, owners vector is empty." << std::endl;
        return false;
    }

    for (auto & owner_iter : owners) {
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
        if (sqlite3_bind_text(stmt, 2, owner_iter.c_str(), owner_iter.size(), SQLITE_STATIC) != SQLITE_OK) {
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

    // Prep JSON objects for insertion into DB
    if (!management_policy_attrs.is<picojson::object>()) {
        std::cerr << "management policy attributes JSON is not an object -- it's probably malformed!" << std::endl;
        return false;
    }

    // Insertion of values into paths table
    //auto paths_obj = paths.get<picojson::array>();
    if (paths.empty()) {
        std::cerr << "Something is wrong, paths object appears empty." << std::endl;
        sqlite3_close(db);
        return false;
    }
    std::string path_local;
    bool recursive_local; 
    for (auto & paths_iter : paths) {
        auto path_obj = paths_iter.get<picojson::object>();
        auto path_obj_iterator = path_obj.begin();
        for (path_obj_iterator; path_obj_iterator != path_obj.end(); ++path_obj_iterator) {
            path_local = path_obj_iterator->first; // grab the path from the key
            recursive_local = path_obj_iterator->second.get<double>(); // grab the recursive flag from the value, cast as bool. Using int64_t instead of double in get method causes undefined error.
        }
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
        if (sqlite3_bind_text(stmt, 2, path_local.c_str(), path_local.size(), SQLITE_STATIC) != SQLITE_OK) {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            return false;
        }

        if (sqlite3_bind_int(stmt, 3, recursive_local) != SQLITE_OK) {
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
    double dedicated_storage_GB, opportunistic_storage_GB;
    int max_num_objects, creation_time, expiration_time, deletion_time;
    auto policy_attrs_obj = management_policy_attrs.get<picojson::object>();
    auto iter = policy_attrs_obj.begin();
    if (iter == policy_attrs_obj.end()) {
        std::cerr << "Something is wrong, policy attribute object appears empty." << std::endl;
        sqlite3_close(db);
        return false;
    }

    for (iter; iter != policy_attrs_obj.end(); ++iter) {
        if (iter->first == "dedicated_storage_GB") {
            dedicated_storage_GB = iter->second.get<double>();
        }
        if (iter->first == "opportunistic_storage_GB") {
            opportunistic_storage_GB = iter->second.get<double>();
        }
        if (iter->first == "max_num_objects") {
            max_num_objects = iter->second.get<double>();
        }
        if (iter->first == "creation_time") {
            creation_time = iter->second.get<double>();
        }
        if (iter->first == "expiration_time") {
            expiration_time = iter->second.get<double>();
        }
        if (iter->first == "deletion_time") {
            deletion_time = iter->second.get<double>();
        }

    }

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, "INSERT INTO management_policy_attributes VALUES (?, ?, ?, ?, ?, ?, ?)", -1, &stmt, NULL);
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
    if (sqlite3_bind_double(stmt, 2, dedicated_storage_GB) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_double(stmt, 3, opportunistic_storage_GB) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_int64(stmt, 4, max_num_objects) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_int64(stmt, 5, creation_time) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_int64(stmt, 6, expiration_time) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_int64(stmt, 7, deletion_time) != SQLITE_OK) {
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

    std::cout << "Hitting 'return true'" << std::endl;
    return true;
}






std::vector<std::string> lotman::Validator::SQL_get_matches(std::string query) { // TODO: Update this to use bind_text for added protection against SQL injection
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
    rc = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        std::cout << "Can't prepare get_matches query" << std::endl;
        sqlite3_close(db);
        return data_vec;
    }

    rc = sqlite3_step(stmt);
    while (rc == SQLITE_ROW) {
        std::cout << "stepping" << std::endl;
        const unsigned char *_data = sqlite3_column_text(stmt, 0);
        std::string data(reinterpret_cast<const char *>(_data));
        data_vec.push_back(data);

        rc = sqlite3_step(stmt);
    }
    if (rc == SQLITE_DONE) {
        std::cout << "rc == SQLITE_DONE" << std::endl;
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

bool lotman::Validator::SQL_update_parent(std::string lot_name, std::string current_parent, std::string parent_set_val) {
    auto lot_fname = get_lot_file();
    if (lot_fname.size() == 0) {return false;}

    sqlite3 *db;
    int rc = sqlite3_open(lot_fname.c_str(), &db);
    if (rc) {
        std::cout << "Can't open DB" << std::endl;
        sqlite3_close(db);
        return false;
    }

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, "UPDATE parents SET parent=(?) WHERE lot_name=(?) AND parent=(?);", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        std::cout << "Can't prepare update parent statement" << std::endl;
        sqlite3_close(db);
        return false;
    }

    if (sqlite3_bind_text(stmt, 2, lot_name.c_str(), lot_name.size(), SQLITE_STATIC) != SQLITE_OK) {
        std::cout << "Can't bind lot name" << std::endl;
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }

    if (sqlite3_bind_text(stmt, 1, parent_set_val.c_str(), parent_set_val.size(), SQLITE_STATIC) != SQLITE_OK) {
        std::cout << "Can't bind parent set val" << std::endl;
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }

    if (sqlite3_bind_text(stmt, 3, current_parent.c_str(), current_parent.size(), SQLITE_STATIC) != SQLITE_OK) {
        std::cout << "Can't bind current parent" << std::endl;
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        int err = sqlite3_extended_errcode(db);
        std::cout << "Can't step statement: err: " << err << std::endl;
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }

    sqlite3_exec(db, "COMMIT", 0, 0, 0);
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    return true;
}