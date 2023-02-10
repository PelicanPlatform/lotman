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

bool lotman::Lot::store_lot(std::string lot_name, 
                            std::vector<std::string> owners, 
                            std::vector<std::string> parents, 
                            std::map<std::string, int> paths_map, 
                            std::map<std::string, int> management_policy_attrs_int_map, 
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
    int max_num_objects, creation_time, expiration_time, deletion_time;
    
    if (management_policy_attrs_int_map.empty() || management_policy_attrs_double_map.empty()) {
        std::cerr << "Something is wrong, either the management_policy_attrs_int/double map appears empty." << std::endl;
        sqlite3_close(db);
        return false;
    }

    for (auto & management_policy_attr_int : management_policy_attrs_int_map) {
        if ( management_policy_attr_int.first == "max_num_objects") {
            max_num_objects = management_policy_attr_int.second;
        }
        if (management_policy_attr_int.first == "creation_time") {
            creation_time = management_policy_attr_int.second;
        }
        if (management_policy_attr_int.first == "expiration_time") {
            expiration_time = management_policy_attr_int.second;
        }
        if (management_policy_attr_int.first == "deletion_time") {
            deletion_time = management_policy_attr_int.second;
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
    if (sqlite3_bind_double(stmt, 2, dedicated_GB) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    if (sqlite3_bind_double(stmt, 3, opportunistic_GB) != SQLITE_OK) {
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
        std::cout << "Can't prepare modify_lot query" << std::endl;
        std::cout << "modify lot query looks like: " << dynamic_query << std::endl;
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
    

    char *prepared_query = sqlite3_expanded_sql(stmt);
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
