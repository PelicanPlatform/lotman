#include "lotman.h"
#include "lotman_internal.h"

#include <nlohmann/json.hpp>
#include <pwd.h>
#include <sqlite3.h>
#include <sys/stat.h>
#include <unistd.h>

// We'll enable WAL mode by default because it makes the sqlite db more
// friendly to a multiprocess environment and reduces locks and timeouts
std::shared_ptr<bool> WAL = std::make_shared<bool>(false);

/*
Code for initializing the sqlite database that stores important Lot object information

*/

namespace {

std::pair<bool, std::string> initialize_lotdb(const std::string &lot_file) {
	sqlite3 *db;
	int rc = sqlite3_open(lot_file.c_str(), &db);
	if (rc != SQLITE_OK) {
		sqlite3_close(db);
		return std::make_pair(false, "SQLite Lot database creation failed.");
	}

	// Enable WAL mode by executing a pragma statement
	if (*WAL == false) {
		const char *pragma_sql = "PRAGMA journal_mode = WAL;";
		rc = sqlite3_exec(db, pragma_sql, 0, 0, 0);
		if (rc != SQLITE_OK) {
			fprintf(stderr, "Failed to enable WAL mode: %s\n", sqlite3_errmsg(db));
			sqlite3_close(db);
			return std::make_pair(false, "");
		}

		*WAL = true;
	}

	sqlite3_busy_timeout(db, *lotman_db_timeout);

	char *err_msg = nullptr;
	rc = sqlite3_exec(db,
					  "CREATE TABLE IF NOT EXISTS owners ("
					  "lot_name PRIMARY KEY NOT NULL,"
					  "owner NOT NULL)",
					  NULL, 0, &err_msg);
	if (rc) {
		auto rp = std::make_pair(false, "SQLite owners table creation failed: " + static_cast<std::string>(err_msg));
		sqlite3_free(err_msg);
		return rp;
	}

	rc = sqlite3_exec(db,
					  "CREATE TABLE IF NOT EXISTS parents ("
					  "lot_name NOT NULL,"
					  "parent NOT NULL,"
					  "PRIMARY KEY (lot_name, parent))",
					  NULL, 0, &err_msg);
	if (rc) {
		auto rp = std::make_pair(false, "SQLite parents table creation failed: " + static_cast<std::string>(err_msg));
		sqlite3_free(err_msg);
		return rp;
	}

	rc = sqlite3_exec(db,
					  "CREATE TABLE IF NOT EXISTS paths ("
					  "lot_name NOT NULL,"
					  "path UNIQUE NOT NULL,"
					  "recursive NOT NULL)",
					  NULL, 0, &err_msg);
	if (rc) {
		auto rp = std::make_pair(false, "SQLite paths table creation failed: " + static_cast<std::string>(err_msg));
		sqlite3_free(err_msg);
		return rp;
	}

	rc = sqlite3_exec(db,
					  "CREATE TABLE IF NOT EXISTS management_policy_attributes ("
					  "lot_name PRIMARY KEY NOT NULL,"
					  "dedicated_GB,"
					  "opportunistic_GB,"
					  "max_num_objects,"
					  "creation_time,"
					  "expiration_time,"
					  "deletion_time)",
					  NULL, 0, &err_msg);
	if (rc) {
		auto rp = std::make_pair(false, "SQLite management_policy_attributes table creation failed: " +
											static_cast<std::string>(err_msg));
		sqlite3_free(err_msg);
		return rp;
	}

	rc = sqlite3_exec(db,
					  "CREATE TABLE IF NOT EXISTS lot_usage ("
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

std::pair<bool, std::string> get_lot_file() {
	const char *lot_env_dir = getenv("LOT_HOME"); // Env variable where Lot info is stored

	auto bufsize = sysconf(_SC_GETPW_R_SIZE_MAX);
	bufsize = (bufsize == -1) ? 16384 : bufsize;

	std::unique_ptr<char[]> buf(new char[bufsize]);

	std::string home_dir;
	struct passwd pwd, *result = NULL;
	getpwuid_r(geteuid(), &pwd, buf.get(), bufsize, &result);
	if (result && result->pw_dir) {
		home_dir = result->pw_dir;
	}

	std::string lot_home_dir;
	std::string configured_lot_home_dir = lotman::Context::get_lot_home();
	if (configured_lot_home_dir.length() > 0) { // The variable has been configured
		lot_home_dir = configured_lot_home_dir;
	} else {
		lot_home_dir = lot_env_dir ? lot_env_dir : home_dir.c_str();
	}

	if (lot_home_dir.size() == 0) {
		return std::make_pair(false, "Could not get Lot home");
	}

	int r = mkdir(lot_home_dir.c_str(), 0700);
	if ((r < 0) && errno != EEXIST) {
		return std::make_pair(false,
							  "Unable to create directory " + lot_home_dir + ": errno: " + std::to_string(errno));
	}

	std::string lot_db_dir = lot_home_dir + "/.lot";
	r = mkdir(lot_db_dir.c_str(), 0700);
	if ((r < 0) && errno != EEXIST) {
		return std::make_pair(false, "Unable to create directory " + lot_db_dir + ": errno: " + std::to_string(errno));
	}

	std::string lot_file = lot_db_dir + "/lotman_cpp.sqlite";
	auto rp = initialize_lotdb(lot_file);
	if (!rp.first) {
		return std::make_pair(false, "Unable to initialize lotdb: " + rp.second);
	}

	return std::make_pair(true, lot_file);
}
} // namespace

std::pair<bool, std::string> lotman::Lot::write_new() {
	auto lot_fname = get_lot_file();
	if (!lot_fname.first) {
		return std::make_pair(false, "Could not get lot_file: " + lot_fname.second);
	}
	sqlite3 *db;
	int rc = sqlite3_open(lot_fname.second.c_str(), &db);
	if (rc) {
		sqlite3_close(db);
		return std::make_pair(false, "Unable to open lotdb: sqlite errno: " + std::to_string(rc));
	}

	sqlite3_busy_timeout(db, *lotman_db_timeout);

	sqlite3_stmt *owner_stmt;
	rc = sqlite3_prepare_v2(db, "INSERT INTO owners VALUES (?, ?)", -1, &owner_stmt, NULL);
	if (rc != SQLITE_OK) {
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_prepare_v2 failed: sqlite errno: " + std::to_string(rc));
	}

	// Bind inputs to sql statement
	rc = sqlite3_bind_text(owner_stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(owner_stmt);
		sqlite3_close(db);
		return std::make_pair(false,
							  "Call to sqlite3_bind_text for lot_name failed: sqlite errno: " + std::to_string(rc));
	}

	rc = sqlite3_bind_text(owner_stmt, 2, owner.c_str(), owner.size(), SQLITE_TRANSIENT);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(owner_stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_text for owner failed: sqlite errno: " + std::to_string(rc));
	}
	rc = sqlite3_step(owner_stmt);
	if (rc != SQLITE_DONE) {
		sqlite3_finalize(owner_stmt);
		sqlite3_close(db);
		return std::make_pair(false,
							  "Call to sqlite3_step for owner table failed: sqlite errno: " + std::to_string(rc));
	}
	sqlite3_exec(db, "COMMIT", 0, 0, 0);
	sqlite3_finalize(owner_stmt);

	for (const auto &parent : parents) {
		sqlite3_stmt *parent_stmt;
		rc = sqlite3_prepare_v2(db, "INSERT INTO parents VALUES (?, ?)", -1, &parent_stmt, NULL);
		if (rc != SQLITE_OK) {
			sqlite3_close(db);
			return std::make_pair(false, "Call to sqlite3_prepare_v2 for parent_stmt failed: sqlite errno: " +
											 std::to_string(rc));
		}

		// Bind inputs to sql statement
		rc = sqlite3_bind_text(parent_stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT);
		if (rc != SQLITE_OK) {
			sqlite3_finalize(parent_stmt);
			sqlite3_close(db);
			return std::make_pair(false,
								  "Call to sqlite3_bind_text for lot_name failed: sqlite errno: " + std::to_string(rc));
		}
		rc = sqlite3_bind_text(parent_stmt, 2, parent.c_str(), parent.size(), SQLITE_TRANSIENT);
		if (rc != SQLITE_OK) {
			sqlite3_finalize(parent_stmt);
			sqlite3_close(db);
			return std::make_pair(false, "Call to sqlite3_bind_text for parent \"" + parent +
											 "\" failed: sqlite errno: " + std::to_string(rc));
		}
		rc = sqlite3_step(parent_stmt);
		if (rc != SQLITE_DONE) {
			int err = sqlite3_extended_errcode(db);
			sqlite3_finalize(parent_stmt);
			sqlite3_close(db);
			return std::make_pair(false,
								  "Call to sqlite3_step for parent_stmt failed: sqlite errno: " + std::to_string(rc));
		}
		sqlite3_exec(db, "COMMIT", 0, 0, 0);
		sqlite3_finalize(parent_stmt);
	}

	// Paths
	for (const auto &path : paths) {
		sqlite3_stmt *path_stmt;
		rc = sqlite3_prepare_v2(db, "INSERT INTO paths VALUES (?, ?, ?)", -1, &path_stmt, NULL);
		if (rc != SQLITE_OK) {
			sqlite3_close(db);
			return std::make_pair(false, "Call to sqlite_prepare_v2 failed for path_stmt: sqlite errno: " +
											 std::to_string(rc));
		}

		// Bind inputs to sql statement
		rc = sqlite3_bind_text(path_stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT);
		if (rc != SQLITE_OK) {
			sqlite3_finalize(path_stmt);
			sqlite3_close(db);
			return std::make_pair(false,
								  "Call to sqlite_bind_text for lot_name failed: sqlite errno: " + std::to_string(rc));
		}
		rc = sqlite3_bind_text(path_stmt, 2, path["path"].get<std::string>().c_str(),
							   path["path"].get<std::string>().size(), SQLITE_TRANSIENT);
		if (rc != SQLITE_OK) {
			sqlite3_finalize(path_stmt);
			sqlite3_close(db);
			return std::make_pair(false, "Call to sqlite_bind_text for path \"" + path.get<std::string>() +
											 "\" failed: sqlite errno: " + std::to_string(rc));
		}
		rc = sqlite3_bind_int(path_stmt, 3, static_cast<int>(path["recursive"].get<bool>()));
		if (rc != SQLITE_OK) {
			sqlite3_finalize(path_stmt);
			sqlite3_close(db);
			return std::make_pair(false, "Call to sqlite_bind_int for recursive val for path \"" +
											 path.get<std::string>() +
											 "\" failed: sqlite errno: " + std::to_string(rc));
		}

		rc = sqlite3_step(path_stmt);
		if (rc != SQLITE_DONE) {
			sqlite3_finalize(path_stmt);
			sqlite3_close(db);
			return std::make_pair(false,
								  "Call to sqlite_step for path table failed: sqlite errno: " + std::to_string(rc));
		}
		sqlite3_exec(db, "COMMIT", 0, 0, 0);
		sqlite3_finalize(path_stmt);
	}

	// Set up management policy attributes
	sqlite3_stmt *man_pol_attr_stmt;
	rc = sqlite3_prepare_v2(db, "INSERT INTO management_policy_attributes VALUES (?, ?, ?, ?, ?, ?, ?)", -1,
							&man_pol_attr_stmt, NULL);
	if (rc != SQLITE_OK) {
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite_prepare_v2 failed for man_pol_attr_stmt: sqlite errno: " +
										 std::to_string(rc));
	}

	// Bind inputs to sql statement
	rc = sqlite3_bind_text(man_pol_attr_stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(man_pol_attr_stmt);
		sqlite3_close(db);
		return std::make_pair(false,
							  "Call to sqlite3_bind_text for lot_name failed: sqlite errno: " + std::to_string(rc));
	}
	rc = sqlite3_bind_double(man_pol_attr_stmt, 2, man_policy_attr.dedicated_GB);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(man_pol_attr_stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_double for dedicated_GB failed: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_bind_double(man_pol_attr_stmt, 3, man_policy_attr.opportunistic_GB);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(man_pol_attr_stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_double for opportunistic_GB failed: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_bind_int64(man_pol_attr_stmt, 4, man_policy_attr.max_num_objects);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(man_pol_attr_stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_int64 for max_num_objects failed: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_bind_int64(man_pol_attr_stmt, 5, man_policy_attr.creation_time);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(man_pol_attr_stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_int64 for creation_time failed: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_bind_int64(man_pol_attr_stmt, 6, man_policy_attr.expiration_time);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(man_pol_attr_stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_int64 for expiration_time failed: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_bind_int64(man_pol_attr_stmt, 7, man_policy_attr.deletion_time);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(man_pol_attr_stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_int64 for deletion_time failed: sqlite errno: " +
										 std::to_string(rc));
	}

	rc = sqlite3_step(man_pol_attr_stmt);
	if (rc != SQLITE_DONE) {
		int err = sqlite3_extended_errcode(db);
		sqlite3_finalize(man_pol_attr_stmt);
		sqlite3_close(db);
		return std::make_pair(false,
							  "Call to sqlite3_step for man_pol_attr_stmt failed: sqlite errno: " + std::to_string(rc));
	}

	sqlite3_exec(db, "COMMIT", 0, 0, 0);
	sqlite3_finalize(man_pol_attr_stmt);

	// Initialize all lot usage parameters to 0
	sqlite3_stmt *init_stmt;
	rc = sqlite3_prepare_v2(db, "INSERT INTO lot_usage VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", -1, &init_stmt, NULL);
	if (rc != SQLITE_OK) {
		sqlite3_close(db);
		return std::make_pair(false,
							  "Call to sqlite3_prepare_v2 for init_stmt failed: sqlite errno: " + std::to_string(rc));
	}

	// Bind inputs to sql statement
	rc = sqlite3_bind_text(init_stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(init_stmt);
		sqlite3_close(db);
		return std::make_pair(false,
							  "Call to sqlite3_bind_text for lot_name failed: sqlite errno: " + std::to_string(rc));
	}
	rc = sqlite3_bind_double(init_stmt, 2, usage.self_GB);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(init_stmt);
		sqlite3_close(db);
		return std::make_pair(false,
							  "Call to sqlite3_bind_double for self_GB failed: sqlite errno: " + std::to_string(rc));
	}
	rc = sqlite3_bind_double(init_stmt, 3, usage.children_GB);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(init_stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_double for children_GB failed: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_bind_int64(init_stmt, 4, usage.self_objects);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(init_stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_int64 for self_objects failed: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_bind_int64(init_stmt, 5, usage.children_objects);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(init_stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_int64 for children_objects failed: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_bind_double(init_stmt, 6, usage.self_GB_being_written);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(init_stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_double for self_GB_being_written failed: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_bind_double(init_stmt, 7, usage.children_GB_being_written);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(init_stmt);
		sqlite3_close(db);
		return std::make_pair(false,
							  "Call to sqlite3_bind_double for children_GB_being_written failed: sqlite errno: " +
								  std::to_string(rc));
	}
	rc = sqlite3_bind_int64(init_stmt, 8, usage.self_objects_being_written);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(init_stmt);
		sqlite3_close(db);
		return std::make_pair(false,
							  "Call to sqlite3_bind_int64 for self_objects_being_written failed: sqlite errno: " +
								  std::to_string(rc));
	}
	rc = sqlite3_bind_int64(init_stmt, 9, usage.children_objects_being_written);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(init_stmt);
		sqlite3_close(db);
		return std::make_pair(false,
							  "Call to sqlite3_bind_int64 for children_objects_being_written failed: sqlite errno: " +
								  std::to_string(rc));
	}

	rc = sqlite3_step(init_stmt);
	if (rc != SQLITE_DONE) {
		// int err = sqlite3_extended_errcode(db);
		sqlite3_finalize(init_stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_step for init_stmt failed: sqlite errno: " + std::to_string(rc));
	}

	sqlite3_exec(db, "COMMIT", 0, 0, 0);
	sqlite3_finalize(init_stmt);
	sqlite3_close(db);

	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::delete_lot_from_db() {
	auto lot_fname = get_lot_file();
	if (!lot_fname.first) {
		return std::make_pair(false, "Could not get lot_file: " + lot_fname.second);
	}
	sqlite3 *db;
	int rc = sqlite3_open(lot_fname.second.c_str(), &db);
	if (rc) {
		sqlite3_close(db);
		return std::make_pair(false, "Unable to open lotdb: sqlite errno: " + std::to_string(rc));
	}

	sqlite3_busy_timeout(db, *lotman_db_timeout);

	// Delete from owners table
	sqlite3_stmt *stmt;
	rc = sqlite3_prepare_v2(db, "DELETE FROM owners WHERE lot_name = ?;", -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_prepare_v2 failed when preparing statement to delete lot from "
									 "owners table: sqlite3 errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT);
	if (rc != SQLITE_OK) {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_text for lot_name failed when preparing to delete lot from "
									 "owners table: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE) {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Failed to delete lot from owners table: sqlite3 errno: " + std::to_string(rc));
	}
	sqlite3_finalize(stmt);
	sqlite3_exec(db, "COMMIT", 0, 0, 0);

	// Delete from parents table
	rc = sqlite3_prepare_v2(db, "DELETE FROM parents WHERE lot_name = ?;", -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_prepare_v2 failed when preparing statement to delete lot from "
									 "parents table: sqlite3 errno: " +
										 std::to_string(rc));
	}
	if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT) != SQLITE_OK) {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_text for lot_name failed when preparing to delete lot from "
									 "parents table: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE) {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Failed to delete lot from parents table: sqlite3 errno: " + std::to_string(rc));
	}
	sqlite3_finalize(stmt);
	sqlite3_exec(db, "COMMIT", 0, 0, 0);

	// Delete from paths
	rc = sqlite3_prepare_v2(db, "DELETE FROM paths WHERE lot_name = ?;", -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_prepare_v2 failed when preparing statement to delete lot from "
									 "paths table: sqlite3 errno: " +
										 std::to_string(rc));
	}
	if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT) != SQLITE_OK) {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_text for lot_name failed when preparing to delete lot from "
									 "paths table: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE) {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Failed to delete lot from paths table: sqlite3 errno: " + std::to_string(rc));
	}
	sqlite3_finalize(stmt);
	sqlite3_exec(db, "COMMIT", 0, 0, 0);

	// Delete from management_policy_attributes
	rc = sqlite3_prepare_v2(db, "DELETE FROM management_policy_attributes WHERE lot_name = ?;", -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_prepare_v2 failed when preparing statement to delete lot from "
									 "management_policy_attributes table: sqlite3 errno: " +
										 std::to_string(rc));
	}
	if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT) != SQLITE_OK) {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_text for lot_name failed when preparing to delete lot from "
									 "management_policy_attributes table: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE) {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Failed to delete lot from management_policy_attributes table: sqlite3 errno: " +
										 std::to_string(rc));
	}
	sqlite3_finalize(stmt);
	sqlite3_exec(db, "COMMIT", 0, 0, 0);

	// Delete from lot_usage
	rc = sqlite3_prepare_v2(db, "DELETE FROM lot_usage WHERE lot_name = ?;", -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_prepare_v2 failed when preparing statement to delete lot from "
									 "usage table: sqlite3 errno: " +
										 std::to_string(rc));
	}
	if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT) != SQLITE_OK) {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Call to sqlite3_bind_text for lot_name failed when preparing to delete lot from "
									 "usage table: sqlite errno: " +
										 std::to_string(rc));
	}
	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE) {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Failed to delete lot from usage table: sqlite3 errno: " + std::to_string(rc));
	}

	sqlite3_finalize(stmt);
	sqlite3_exec(db, "COMMIT", 0, 0, 0);
	sqlite3_close(db);

	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::store_updates(std::string update_stmt,
														std::map<std::string, std::vector<int>> update_str_map,
														std::map<int64_t, std::vector<int>> update_int_map,
														std::map<double, std::vector<int>> update_dbl_map) {
	auto lot_fname = get_lot_file();
	if (!lot_fname.first) {
		return std::make_pair(false, "Could not get lot_file: " + lot_fname.second);
	}
	sqlite3 *db;
	int rc = sqlite3_open(lot_fname.second.c_str(), &db);
	if (rc) {
		sqlite3_close(db);
		return std::make_pair(false, "Unable to open lotdb: sqlite errno: " + std::to_string(rc));
	}

	sqlite3_busy_timeout(db, *lotman_db_timeout);

	sqlite3_stmt *stmt;
	rc = sqlite3_prepare_v2(db, update_stmt.c_str(), -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		sqlite3_close(db);
		return std::make_pair(
			false, "Call to sqlite3_prepare_v2 failed when preparing statement to write updates: sqlite3 errno: " +
					   std::to_string(rc));
	}

	// No need to check if maps are empty -- won't enter loop if they are
	for (const auto &key : update_str_map) {
		for (const auto &value : key.second) { // Handles case when a single key needs to be mapped to multiple places
											   // in the statement (hence, the map is to a vector of ints)
			if (sqlite3_bind_text(stmt, value, key.first.c_str(), key.first.size(), SQLITE_TRANSIENT) != SQLITE_OK) {
				sqlite3_finalize(stmt);
				sqlite3_close(db);
				return std::make_pair(false, "Call to sqlite3_bind_text for update_str_map failed when preparing to "
											 "write updates: sqlite errno: " +
												 std::to_string(rc));
			}
		}
	}

	for (const auto &key : update_int_map) {
		for (const auto &value : key.second) {
			if (sqlite3_bind_int64(stmt, value, key.first) != SQLITE_OK) {
				sqlite3_finalize(stmt);
				sqlite3_close(db);
				return std::make_pair(false, "Call to sqlite3_bind_int for update_int_map failed when preparing to "
											 "write updates: sqlite errno: " +
												 std::to_string(rc));
			}
		}
	}

	for (const auto &key : update_dbl_map) {
		for (const auto &value : key.second) {
			if (sqlite3_bind_double(stmt, value, key.first) != SQLITE_OK) {
				sqlite3_finalize(stmt);
				sqlite3_close(db);
				return std::make_pair(false, "Call to sqlite3_bind_double for update_int_map failed when preparing to "
											 "write updates: sqlite errno: " +
												 std::to_string(rc));
			}
		}
	}

	// char *prepared_query = sqlite3_expanded_sql(stmt); // Useful to print for debugging
	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE) {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return std::make_pair(false, "Failed to write updates: sqlite3 errno: " + std::to_string(rc));
	}

	sqlite3_finalize(stmt);
	sqlite3_close(db);

	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::store_new_paths(std::vector<json> new_paths) {
	auto lot_fname = get_lot_file();
	if (!lot_fname.first) {
		return std::make_pair(false, "Could not get lot_file: " + lot_fname.second);
	}
	sqlite3 *db;
	int rc = sqlite3_open(lot_fname.second.c_str(), &db);
	if (rc) {
		sqlite3_close(db);
		return std::make_pair(false, "Unable to open lotdb: sqlite errno: " + std::to_string(rc));
	}

	sqlite3_busy_timeout(db, *lotman_db_timeout);

	for (const auto &path : new_paths) {
		sqlite3_stmt *stmt;
		rc = sqlite3_prepare_v2(db, "INSERT INTO paths VALUES (?, ?, ?)", -1, &stmt, NULL);
		if (rc != SQLITE_OK) {
			sqlite3_close(db);
			return std::make_pair(
				false,
				"Call to sqlite3_prepare_v2 failed when preparing statement to write new paths: sqlite3 errno: " +
					std::to_string(rc));
		}

		// Bind inputs to sql statement
		if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT) != SQLITE_OK) {
			sqlite3_finalize(stmt);
			sqlite3_close(db);
			return std::make_pair(
				false,
				"Call to sqlite3_bind_text for lot_name failed when preparing to write new path: sqlite errno: " +
					std::to_string(rc));
		}
		if (sqlite3_bind_text(stmt, 2, path["path"].get<std::string>().c_str(), path["path"].get<std::string>().size(),
							  SQLITE_TRANSIENT) != SQLITE_OK) {
			sqlite3_finalize(stmt);
			sqlite3_close(db);
			return std::make_pair(
				false, "Call to sqlite3_bind_text for path failed when preparing to write new path: sqlite errno: " +
						   std::to_string(rc));
		}
		if (sqlite3_bind_int(stmt, 3, path["recursive"].get<bool>()) != SQLITE_OK) {
			sqlite3_finalize(stmt);
			sqlite3_close(db);
			return std::make_pair(
				false, "Call to sqlite3_bind_int for path failed when preparing to write new path: sqlite errno: " +
						   std::to_string(rc));
		}
		rc = sqlite3_step(stmt);
		if (rc != SQLITE_DONE) {
			int err = sqlite3_extended_errcode(db);
			sqlite3_finalize(stmt);
			sqlite3_close(db);
			return std::make_pair(false, "Failed to write new paths: sqlite3 errno: " + std::to_string(rc));
		}
		sqlite3_exec(db, "COMMIT", 0, 0, 0);
		sqlite3_finalize(stmt);
	}

	sqlite3_close(db);

	return std::make_pair(true, "");
}
std::pair<bool, std::string> lotman::Lot::store_new_parents(std::vector<Lot> new_parents) {
	auto lot_fname = get_lot_file();
	if (!lot_fname.first) {
		return std::make_pair(false, "Could not get lot_file: " + lot_fname.second);
	}
	sqlite3 *db;
	int rc = sqlite3_open(lot_fname.second.c_str(), &db);
	if (rc) {
		sqlite3_close(db);
		return std::make_pair(false, "Unable to open lotdb: sqlite errno: " + std::to_string(rc));
	}

	sqlite3_busy_timeout(db, *lotman_db_timeout);

	for (const auto &parent : new_parents) {
		sqlite3_stmt *stmt;
		rc = sqlite3_prepare_v2(db, "INSERT INTO parents VALUES (?, ?)", -1, &stmt, NULL);
		if (rc != SQLITE_OK) {
			sqlite3_close(db);
			return std::make_pair(
				false,
				"Call to sqlite3_prepare_v2 failed when preparing statement to write new parents: sqlite3 errno: " +
					std::to_string(rc));
		}

		// Bind inputs to sql statement
		if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT) != SQLITE_OK) {
			sqlite3_finalize(stmt);
			sqlite3_close(db);
			return std::make_pair(
				false,
				"Call to sqlite3_bind_text for lot_name failed when preparing to write new parents: sqlite errno: " +
					std::to_string(rc));
		}
		if (sqlite3_bind_text(stmt, 2, parent.lot_name.c_str(), parent.lot_name.size(), SQLITE_TRANSIENT) !=
			SQLITE_OK) {
			sqlite3_finalize(stmt);
			sqlite3_close(db);
			return std::make_pair(false, "Call to sqlite3_bind_text for parent.lot_name failed when preparing to write "
										 "new parent: sqlite errno: " +
											 std::to_string(rc));
		}
		rc = sqlite3_step(stmt);
		if (rc != SQLITE_DONE) {
			int err = sqlite3_extended_errcode(db);
			sqlite3_finalize(stmt);
			sqlite3_close(db);
			return std::make_pair(false, "Failed to write new parent: sqlite3 errno: " + std::to_string(rc));
		}
		sqlite3_exec(db, "COMMIT", 0, 0, 0);
		sqlite3_finalize(stmt);
	}

	sqlite3_close(db);

	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::remove_parents_from_db(std::vector<std::string> parents) {
	auto lot_fname = get_lot_file();
	if (!lot_fname.first) {
		return std::make_pair(false, "Could not get lot_file: " + lot_fname.second);
	}
	sqlite3 *db;
	int rc = sqlite3_open(lot_fname.second.c_str(), &db);
	if (rc) {
		sqlite3_close(db);
		return std::make_pair(false, "Unable to open lotdb: sqlite errno: " + std::to_string(rc));
	}

	sqlite3_busy_timeout(db, *lotman_db_timeout);

	// Delete from parents table
	for (const auto &parent : parents) {
		sqlite3_stmt *stmt;
		rc = sqlite3_prepare_v2(db, "DELETE FROM parents WHERE lot_name = ? AND parent = ?;", -1, &stmt, NULL);
		if (rc != SQLITE_OK) {
			sqlite3_close(db);
			return std::make_pair(false, "Call to sqlite3_prepare_v2 failed when preparing statement to delete parents "
										 "from the lot: sqlite3 errno: " +
											 std::to_string(rc));
		}

		// Bind the lot
		if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT) != SQLITE_OK) {
			sqlite3_finalize(stmt);
			sqlite3_close(db);
			return std::make_pair(false, "Call to sqlite3_bind_text for lot_name failed when preparing to delete a "
										 "parent from parents table: sqlite errno: " +
											 std::to_string(rc));
		}

		// Bind the parent
		if (sqlite3_bind_text(stmt, 2, parent.c_str(), parent.size(), SQLITE_TRANSIENT) != SQLITE_OK) {
			sqlite3_finalize(stmt);
			sqlite3_close(db);
			return std::make_pair(false, "Call to sqlite3_bind_text for parent.lot_name failed when preparing to "
										 "delete a parent from parents table: sqlite errno: " +
											 std::to_string(rc));
		}
		rc = sqlite3_step(stmt);
		if (rc != SQLITE_DONE) {
			sqlite3_finalize(stmt);
			sqlite3_close(db);
			return std::make_pair(false,
								  "Failed to delete parent from parents table: sqlite3 errno: " + std::to_string(rc));
		}
		sqlite3_exec(db, "COMMIT", 0, 0, 0);
		sqlite3_finalize(stmt);
	}

	sqlite3_close(db);
	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::remove_paths_from_db(std::vector<std::string> paths) {
	auto lot_fname = get_lot_file();
	if (!lot_fname.first) {
		return std::make_pair(false, "Could not get lot_file: " + lot_fname.second);
	}
	sqlite3 *db;
	int rc = sqlite3_open(lot_fname.second.c_str(), &db);
	if (rc) {
		sqlite3_close(db);
		return std::make_pair(false, "Unable to open lotdb: sqlite errno: " + std::to_string(rc));
	}

	sqlite3_busy_timeout(db, *lotman_db_timeout);

	// Delete from paths table
	for (const auto &path : paths) {
		sqlite3_stmt *stmt;
		// rc = sqlite3_prepare_v2(db, "DELETE FROM paths WHERE lot_name = ? AND path = ?;", -1, &stmt, NULL);
		rc = sqlite3_prepare_v2(db, "DELETE FROM paths WHERE path = ?;", -1, &stmt,
								NULL); // Because paths should be unique, we don't need to specify which lot
		if (rc != SQLITE_OK) {
			sqlite3_close(db);
			return std::make_pair(false, "Call to sqlite3_prepare_v2 failed when preparing statement to delete paths "
										 "from the lot: sqlite3 errno: " +
											 std::to_string(rc));
		}

		// // Bind the lot
		// if (sqlite3_bind_text(stmt, 1, lot_name.c_str(), lot_name.size(), SQLITE_TRANSIENT) != SQLITE_OK) {
		//     sqlite3_finalize(stmt);
		//     sqlite3_close(db);
		//     return std::make_pair(false, "Call to sqlite3_bind_text for lot_name failed when preparing to delete a
		//     path from paths table: sqlite errno: " + std::to_string(rc));
		// }

		// Bind the path
		if (sqlite3_bind_text(stmt, 1, path.c_str(), path.size(), SQLITE_TRANSIENT) != SQLITE_OK) {
			sqlite3_finalize(stmt);
			sqlite3_close(db);
			return std::make_pair(false, "Call to sqlite3_bind_text for path failed when preparing to delete a path "
										 "from paths table: sqlite errno: " +
											 std::to_string(rc));
		}
		rc = sqlite3_step(stmt);
		if (rc != SQLITE_DONE) {
			sqlite3_finalize(stmt);
			sqlite3_close(db);
			return std::make_pair(false, "Failed to delete path from path table: sqlite3 errno: " + std::to_string(rc));
		}
		sqlite3_exec(db, "COMMIT", 0, 0, 0);
		sqlite3_finalize(stmt);
	}

	sqlite3_close(db);
	return std::make_pair(true, "");
}

std::pair<std::vector<std::string>, std::string>
lotman::Checks::SQL_get_matches(std::string dynamic_query, std::map<std::string, std::vector<int>> str_map,
								std::map<int64_t, std::vector<int>> int_map,
								std::map<double, std::vector<int>> double_map) {
	std::vector<std::string> return_vec;

	auto lot_fname = get_lot_file();
	if (!lot_fname.first) {
		return std::make_pair(return_vec, "Could not get lot_file: " + lot_fname.second);
	}
	sqlite3 *db;
	int rc = sqlite3_open(lot_fname.second.c_str(), &db);
	if (rc) {
		sqlite3_close(db);
		return std::make_pair(return_vec, "Unable to open lotdb: sqlite errno: " + std::to_string(rc));
	}

	sqlite3_busy_timeout(db, *lotman_db_timeout);

	sqlite3_stmt *stmt;
	rc = sqlite3_prepare_v2(db, dynamic_query.c_str(), -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		sqlite3_close(db);
		return std::make_pair(return_vec, "Call to sqlite3_prepare_v2 failed: sqlite errno: " + std::to_string(rc));
	}

	for (const auto &key : str_map) {
		for (const auto &value : key.second) {
			rc = sqlite3_bind_text(stmt, value, key.first.c_str(), key.first.size(), SQLITE_TRANSIENT);
			if (rc != SQLITE_OK) {
				sqlite3_finalize(stmt);
				sqlite3_close(db);
				return std::make_pair(return_vec,
									  "Call to sqlite3_bind_text failed while binding str_map: sqlite3 errno: " +
										  std::to_string(rc));
			}
		}
	}

	for (const auto &key : int_map) {
		for (const auto &value : key.second) {
			rc = sqlite3_bind_int64(stmt, value, key.first);
			if (rc != SQLITE_OK) {
				sqlite3_finalize(stmt);
				sqlite3_close(db);
				return std::make_pair(return_vec,
									  "Call to sqlite3_bind_int failed while binding int_map: sqlite3 errno: " +
										  std::to_string(rc));
			}
		}
	}

	for (const auto &key : double_map) {
		for (const auto &value : key.second) {
			rc = sqlite3_bind_double(stmt, value, key.first);
			if (rc != SQLITE_OK) {
				sqlite3_finalize(stmt);
				sqlite3_close(db);
				return std::make_pair(return_vec,
									  "Call to sqlite3_bind_double failed while binding double_map: sqlite3 errno: " +
										  std::to_string(rc));
			}
		}
	}

	rc = sqlite3_step(stmt);
	while (rc == SQLITE_ROW) {
		const unsigned char *_data = sqlite3_column_text(stmt, 0);
		std::string data(reinterpret_cast<const char *>(_data));
		return_vec.push_back(data);
		rc = sqlite3_step(stmt);
	}
	if (rc == SQLITE_DONE) {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return std::make_pair(return_vec, "");
	} else {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return make_pair(return_vec, "There was an error while stepping through SQLite results: sqlite3 errno: " +
										 std::to_string(rc));
	}
}

std::pair<std::vector<std::vector<std::string>>, std::string> lotman::Checks::SQL_get_matches_multi_col(
	std::string dynamic_query, int num_returns, std::map<std::string, std::vector<int>> str_map,
	std::map<int64_t, std::vector<int>> int_map, std::map<double, std::vector<int>> double_map) {
	std::vector<std::vector<std::string>> return_vec;
	auto lot_fname = get_lot_file();
	if (!lot_fname.first) {
		return std::make_pair(return_vec, "Could not get lot_file: " + lot_fname.second);
	}
	sqlite3 *db;
	int rc = sqlite3_open(lot_fname.second.c_str(), &db);
	if (rc) {
		sqlite3_close(db);
		return std::make_pair(return_vec, "Unable to open lotdb: sqlite errno: " + std::to_string(rc));
	}

	sqlite3_busy_timeout(db, *lotman_db_timeout);

	sqlite3_stmt *stmt;
	rc = sqlite3_prepare_v2(db, dynamic_query.c_str(), -1, &stmt, NULL);
	if (rc != SQLITE_OK) {
		sqlite3_close(db);
		return std::make_pair(return_vec, "Call to sqlite3_prepare_v2 failed: sqlite errno: " + std::to_string(rc));
	}

	for (const auto &key : str_map) {
		for (const auto &value : key.second) {
			if (sqlite3_bind_text(stmt, value, key.first.c_str(), key.first.size(), SQLITE_TRANSIENT) != SQLITE_OK) {
				sqlite3_finalize(stmt);
				sqlite3_close(db);
				return std::make_pair(return_vec,
									  "Call to sqlite3_bind_text failed while binding str_map: sqlite3 errno: " +
										  std::to_string(rc));
			}
		}
	}

	for (const auto &key : int_map) {
		for (const auto &value : key.second) {
			if (sqlite3_bind_int64(stmt, value, key.first) != SQLITE_OK) {
				sqlite3_finalize(stmt);
				sqlite3_close(db);
				return std::make_pair(return_vec,
									  "Call to sqlite3_bind_int failed while binding int_map: sqlite3 errno: " +
										  std::to_string(rc));
			}
		}
	}

	for (const auto &key : double_map) {
		for (const auto &value : key.second) {
			if (sqlite3_bind_double(stmt, value, key.first) != SQLITE_OK) {
				sqlite3_finalize(stmt);
				sqlite3_close(db);
				return std::make_pair(return_vec,
									  "Call to sqlite3_bind_double failed while binding double_map: sqlite3 errno: " +
										  std::to_string(rc));
			}
		}
	}

	rc = sqlite3_step(stmt);
	while (rc == SQLITE_ROW) {
		std::vector<std::string> internal_data_vec;
		for (int iter = 0; iter < num_returns; ++iter) {
			const unsigned char *_data = sqlite3_column_text(stmt, iter);
			std::string data(reinterpret_cast<const char *>(_data));
			internal_data_vec.push_back(data);
		}
		return_vec.push_back(internal_data_vec);
		rc = sqlite3_step(stmt);
	}
	if (rc == SQLITE_DONE) {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return std::make_pair(return_vec, "");
	} else {
		sqlite3_finalize(stmt);
		sqlite3_close(db);
		return make_pair(return_vec, "There was an error while stepping through SQLite results: sqlite3 errno: " +
										 std::to_string(rc));
	}
}
