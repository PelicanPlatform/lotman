#include <sqlite3.h>
#include <string>
#include <unistd.h>
#include <cstring>
#include <cstdio>
#include <iostream>
#include <sys/stat.h>

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
        rc = sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS lot ("
                            "lot_name PRIMARY KEY NOT NULL,"
                            "path,"
                            "parent,"
                            "owner NOT NULL,"
                            "resource_limits,"
                            "reclamation_policy)",
                        NULL, 0, &err_msg);
        if (rc) {
            std::cerr << "Sqlite table creation failed: " << err_msg << std::endl;
            sqlite3_free(err_msg);
        }
        sqlite3_close(db);
    }


    std::string 
    get_lot_file() {
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
}

bool lotman::Lot::store_lot(std::string path, std::string parent, std::string owner, std::string users, picojson::value resource_limits, picojson::value reclamation_policy) {
    //picojson::object resource_limits_object;
    //picojson::object reclamation_policy_object;
    //resource_limits_object["resource_limits"] = resource_limits;
    //reclamation_policy_object["reclamation_policy"] = reclamation_policy;
    picojson::value resource_limits_value(resource_limits);
    picojson::value reclamation_policy_value(reclamation_policy);
    std::string resource_limits_json_str = resource_limits_value.serialize();
    std::string reclamation_policy_json_str = reclamation_policy_value.serialize();
    
    auto lot_fname = get_lot_file();
    if (lot_fname.size() == 0) {return false;}

    sqlite3 *db;
    int rc = sqlite3_open(lot_fname.c_str(), &db);
    if (rc) {
        sqlite3_close(db);
        return false;
    }

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, "INSERT INTO lot VALUES (?, ?, ?, ?, ?, ?)", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }

    // Bind inputs to sql statement
    if (sqlite3_bind_text(stmt, 1, path.c_str(), path.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }

    if (sqlite3_bind_text(stmt, 2, parent.c_str(), parent.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }

    if (sqlite3_bind_text(stmt, 3, owner.c_str(), owner.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }

    if (sqlite3_bind_text(stmt, 4, users.c_str(), users.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }

    if (sqlite3_bind_text(stmt, 5, resource_limits_json_str.c_str(), resource_limits_json_str.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }   

    if (sqlite3_bind_text(stmt, 6, reclamation_policy_json_str.c_str(), reclamation_policy_json_str.size(), SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }   

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        int err = sqlite3_extended_errcode(db);
        sqlite3_finalize(stmt);
        sqlite3_close(db);

        if (err == SQLITE_CONSTRAINT_PRIMARYKEY) {
            std::cerr << "Lot path already exists" << std::endl;
        }
        return false;
    }

    sqlite3_exec(db, "COMMIT", 0, 0, 0);
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    return true;
}

bool lotman::Lot::remove_lot(std::string path) {
    auto lot_fname = get_lot_file();
    if (lot_fname.size() == 0) {return false;}

    sqlite3 *db;
    int rc = sqlite3_open(lot_fname.c_str(), &db);
    if (rc) {
        sqlite3_close(db);
        return false;
    }

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, "DELETE FROM lot WHERE path = ?;", -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }

    if (sqlite3_bind_text(stmt, 1, path.c_str(), path.size(), SQLITE_STATIC) != SQLITE_OK) {
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

bool lotman::Validator::SQL_match_exists(std::string query) {
    auto lot_fname = get_lot_file();
    if (lot_fname.size() == 0) {return false;}

    sqlite3 *db;
    int rc = sqlite3_open(lot_fname.c_str(), &db);
    if (rc) {
        sqlite3_close(db);
        return false;
    }

    sqlite3_stmt *stmt;
    rc = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        //int err = sqlite3_extended_errcode(db);
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return true;
    }
    else if (rc == SQLITE_DONE) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
    else {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return false;
    }
}

std::vector<std::string> lotman::Validator::SQL_get_matches(std::string query) {
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
        sqlite3_close(db);
        return data_vec;
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
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return data_vec;
    }

}