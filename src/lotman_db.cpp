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

    std::cout << "HERE 1, in db" << std::endl;
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
            std::cout << "Here adding to owners, statement not prepped!" << std::endl;
            std::cout << "rc: " << rc << std::endl;
            std::cout << sqlite3_extended_errcode(db) << std::endl;
            std::cout << sqlite3_errstr(sqlite3_extended_errcode(db)) << std::endl;
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
            int err = sqlite3_extended_errcode(db);
            std::cout << "Err code: " << err << std::endl;
            std::cout << "RC: " << rc << std::endl;
            sqlite3_finalize(stmt);
            sqlite3_close(db);
            std::cout << "Can't do this " << std::endl;
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
            std::cout << "Here adding to parents, statement not prepped!" << std::endl;
            std::cout << "rc: " << rc << std::endl;
            std::cout << sqlite3_extended_errcode(db) << std::endl;
            std::cout << sqlite3_errstr(sqlite3_extended_errcode(db)) << std::endl;
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
            recursive_local = path_obj_iterator->second.get<double>(); // grab the recursive flag from the value, cast as bool. Using int64_t instead of double causes undefined error.
        }
        sqlite3_stmt *stmt;

        rc = sqlite3_prepare_v2(db, "INSERT INTO paths VALUES (?, ?, ?)", -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            std::cout << "Here adding to dirs, statement not prepped!" << std::endl;
            std::cout << "rc: " << rc << std::endl;
            std::cout << sqlite3_extended_errcode(db) << std::endl;
            std::cout << sqlite3_errstr(sqlite3_extended_errcode(db)) << std::endl;
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

    std::cout << "HERE 1.5" << std::endl;
    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, "INSERT INTO management_policy_attributes VALUES (?, ?, ?, ?, ?, ?, ?)", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        std::cout << sqlite3_extended_errcode(db) << std::endl;
        std::cout << sqlite3_errstr(sqlite3_extended_errcode(db)) << std::endl;
        sqlite3_close(db);
        return false;
    }

    std::cout << "HERE 2" << std::endl; 
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

    std::cout << "HERE 3" << std::endl;
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


// int main() {

//     std::string lot_name = "Justin's Lot";
//     std::vector<std::string> owners_vec{"Justin", "Brian"};
//     std::vector<std::string> parents_vec{"Justin's Lot", "another_parent"};
//     std::vector<std::string> children_vec{"some_child", "another_child"};
//     const char man_policy_str[] = "{\"dedicated_storage_GB\":10, \"opportunistic_storage_GB\":5, \"max_num_objects\":100, \"creation_time\":123, \"expiration_time\":234, \"deletion_time\":345}";
//     const char paths_str[] = "{\"/a/path\":0, \"/foo/bar\":1}";
//     picojson::value man_policy;
//     picojson::value paths;

//     std::string err = picojson::parse(man_policy, man_policy_str);
//     if (!err.empty()) {
//         std::cerr << err << std::endl;
//     }

//     err = picojson::parse(paths, paths_str);
//     if (!err.empty()) {
//         std::cerr << err << std::endl;
//     }

//     bool rv = store_lot(lot_name, owners_vec, parents_vec, children_vec, paths, man_policy);
//     std::cout << "main rv: " << std::endl;


//     return 0;
// }


















// bool lotman::Lot::store_lot(std::string name, std::string path, std::string parent, std::string owner, picojson::value resource_limits, picojson::value reclamation_policy) {
//     //picojson::object resource_limits_object;
//     //picojson::object reclamation_policy_object;
//     //resource_limits_object["resource_limits"] = resource_limits;
//     //reclamation_policy_object["reclamation_policy"] = reclamation_policy;
//     picojson::value resource_limits_value(resource_limits);
//     picojson::value reclamation_policy_value(reclamation_policy);
//     std::string resource_limits_json_str = resource_limits_value.serialize();
//     std::string reclamation_policy_json_str = reclamation_policy_value.serialize();
    
//     auto lot_fname = get_lot_file();

//     sqlite3 *db;
//     int rc = sqlite3_open(lot_fname.c_str(), &db);
//     if (rc) {
//         sqlite3_close(db);
//         return false;
//     }

//     sqlite3_stmt *stmt;
//     rc = sqlite3_prepare_v2(db, "INSERT INTO lot VALUES (?, ?, ?, ?, ?, ?)", -1, &stmt, NULL);
//     if (rc != SQLITE_OK) {
//         sqlite3_close(db);
//         return false;
//     }

//     // Bind inputs to sql statement
//     if (sqlite3_bind_text(stmt, 1, name.c_str(), name.size(), SQLITE_STATIC) != SQLITE_OK) {
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);
//         return false;
//     }

//     if (sqlite3_bind_text(stmt, 2, path.c_str(), path.size(), SQLITE_STATIC) != SQLITE_OK) {
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);
//         return false;
//     }

//     if (sqlite3_bind_text(stmt, 3, parent.c_str(), parent.size(), SQLITE_STATIC) != SQLITE_OK) {
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);
//         return false;
//     }

//     if (sqlite3_bind_text(stmt, 4, owner.c_str(), owner.size(), SQLITE_STATIC) != SQLITE_OK) {
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);
//         return false;
//     }

//     if (sqlite3_bind_text(stmt, 5, resource_limits_json_str.c_str(), resource_limits_json_str.size(), SQLITE_STATIC) != SQLITE_OK) {
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);
//         return false;
//     }   

//     if (sqlite3_bind_text(stmt, 6, reclamation_policy_json_str.c_str(), reclamation_policy_json_str.size(), SQLITE_STATIC) != SQLITE_OK) {
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);
//         return false;
//     }   

//     rc = sqlite3_step(stmt);
//     if (rc != SQLITE_DONE) {
//         int err = sqlite3_extended_errcode(db);
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);

//         if (err == SQLITE_CONSTRAINT_PRIMARYKEY) {
//             std::cerr << "Lot path already exists" << std::endl;
//         }
//         return false;
//     }

//     sqlite3_exec(db, "COMMIT", 0, 0, 0);
//     sqlite3_finalize(stmt);
//     sqlite3_close(db);

//     return true;
// }


// bool lotman::Lot::remove_lot(std::string name) {
//     auto lot_fname = get_lot_file();
//     if (lot_fname.size() == 0) {return false;}

//     sqlite3 *db;
//     int rc = sqlite3_open(lot_fname.c_str(), &db);
//     if (rc) {
//         sqlite3_close(db);
//         return false;
//     }

//     sqlite3_stmt *stmt;
//     rc = sqlite3_prepare_v2(db, "DELETE FROM lot WHERE lot_name = ?;", -1, &stmt, NULL);
//     if (rc != SQLITE_OK) {
//         sqlite3_close(db);
//         return false;
//     }

//     if (sqlite3_bind_text(stmt, 1, name.c_str(), name.size(), SQLITE_STATIC) != SQLITE_OK) {
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);
//         return false;
//     }

//     rc = sqlite3_step(stmt);
//     if (rc != SQLITE_DONE) {
//         int err = sqlite3_extended_errcode(db);
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);
//         return false;
//     }

//     sqlite3_exec(db, "COMMIT", 0, 0, 0);
//     sqlite3_finalize(stmt);
//     sqlite3_close(db);

//     return true;
// }

// bool lotman::Validator::SQL_match_exists(std::string query) {
//     auto lot_fname = get_lot_file();
//     if (lot_fname.size() == 0) {return false;}

//     sqlite3 *db;
//     int rc = sqlite3_open(lot_fname.c_str(), &db);
//     if (rc) {
//         sqlite3_close(db);
//         return false;
//     }

//     sqlite3_stmt *stmt;
//     rc = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL);
//     if (rc != SQLITE_OK) {
//         sqlite3_close(db);
//         return false;
//     }

//     rc = sqlite3_step(stmt);
//     if (rc == SQLITE_ROW) {
//         //int err = sqlite3_extended_errcode(db);
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);
//         return true;
//     }
//     else if (rc == SQLITE_DONE) {
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);
//         return false;
//     }
//     else {
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);
//         return false;
//     }
// }




// std::vector<std::string> lotman::Validator::SQL_get_matches(std::string query) {
//     std::vector<std::string> data_vec;
//     auto lot_fname = get_lot_file();
//     if (lot_fname.size() == 0) {return data_vec;}

//     sqlite3 *db;
//     int rc = sqlite3_open(lot_fname.c_str(), &db);
//     if (rc) {
//         sqlite3_close(db);
//         return data_vec;
//     }

//     sqlite3_stmt *stmt;
//     rc = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL);
//     if (rc != SQLITE_OK) {
//         sqlite3_close(db);
//         return data_vec;
//     }

//     rc = sqlite3_step(stmt);
//     while (rc == SQLITE_ROW) {
//         const unsigned char *_data = sqlite3_column_text(stmt, 0);
//         std::string data(reinterpret_cast<const char *>(_data));
//         data_vec.push_back(data);

//         rc = sqlite3_step(stmt);
//     }
//     if (rc == SQLITE_DONE) {
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);
//         return data_vec;
//     }
//     else {
//         sqlite3_finalize(stmt);
//         sqlite3_close(db);
//         return data_vec;
//     }

// }