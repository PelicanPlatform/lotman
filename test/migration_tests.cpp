#include "../src/lotman.h"
#include "../src/lotman_db.h"

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <sqlite3.h>

// RAII wrapper for C-style memory management (consistent with other test files)
struct CStringDeleterMig {
	void operator()(char *ptr) const {
		if (ptr)
			free(ptr);
	}
};
using UniqueCStringMig = std::unique_ptr<char, CStringDeleterMig>;

// Helper to create temp directory
std::string create_temp_directory_mig() {
	std::string temp_dir_template = "/tmp/lotman_mig_test_XXXXXX";
	char temp_dir_name[temp_dir_template.size() + 1];
	std::strcpy(temp_dir_name, temp_dir_template.c_str());

	char *mkdtemp_result = mkdtemp(temp_dir_name);
	if (mkdtemp_result == nullptr) {
		std::cerr << "Failed to create temporary directory\n";
		exit(1);
	}

	return std::string(mkdtemp_result);
}

class MigrationTest : public ::testing::Test {
  protected:
	std::string tmp_dir;

	void SetUp() override {
		tmp_dir = create_temp_directory_mig();
		// Reset storage manager to ensure clean state
		lotman::db::StorageManager::reset();
	}

	void TearDown() override {
		lotman::db::StorageManager::reset();
		std::filesystem::remove_all(tmp_dir);
	}
};

TEST_F(MigrationTest, TestV0ToV1Migration) {
	// 1. Create a database that looks like a real pre-versioning LotMan database.
	// We'll use the ORM to create it (ensuring schema compatibility), then manually
	// remove the schema_versions table to simulate a v0 database.
	std::string db_dir = tmp_dir + "/.lot";
	std::filesystem::create_directories(db_dir);
	std::string db_path = db_dir + "/lotman_cpp.sqlite";

	// First, let StorageManager create a proper database with all tables
	{
		char *raw_err = nullptr;
		int rv = lotman_set_context_str("lot_home", tmp_dir.c_str(), &raw_err);
		UniqueCStringMig err(raw_err);
		ASSERT_EQ(rv, 0) << "Failed to set lot_home: " << (err.get() ? err.get() : "unknown error");

		auto &storage = lotman::db::StorageManager::get_storage();

		// Insert test data
		storage.replace(lotman::db::Owner{"test_lot", "test_owner"});

		// Reset to close the connection
		lotman::db::StorageManager::reset();
	}

	// Now remove the schema_versions table to simulate a v0 database
	{
		sqlite3 *db;
		int rc = sqlite3_open(db_path.c_str(), &db);
		ASSERT_EQ(rc, SQLITE_OK);

		char *errMsg = nullptr;
		rc = sqlite3_exec(db, "DROP TABLE schema_versions", nullptr, nullptr, &errMsg);
		if (rc != SQLITE_OK) {
			std::cerr << "SQL error: " << errMsg << std::endl;
			sqlite3_free(errMsg);
		}
		ASSERT_EQ(rc, SQLITE_OK);
		sqlite3_close(db);
	}

	// 2. Re-initialize StorageManager (should detect existing DB without version and set to v1)
	{
		char *raw_err = nullptr;
		int rv = lotman_set_context_str("lot_home", tmp_dir.c_str(), &raw_err);
		UniqueCStringMig err(raw_err);
		ASSERT_EQ(rv, 0) << "Failed to set lot_home: " << (err.get() ? err.get() : "unknown error");

		auto &storage = lotman::db::StorageManager::get_storage();

		// 3. Verify schema_versions table was created and database recognized as version 1
		try {
			auto versions = storage.get_all<lotman::db::SchemaVersion>();
			ASSERT_EQ(versions.size(), 1);
			ASSERT_EQ(versions[0].version, 1);
		} catch (const std::exception &e) {
			FAIL() << "Failed to query schema_versions: " << e.what();
		}

		// 4. Verify existing data was preserved
		try {
			auto owners = storage.get_all<lotman::db::Owner>();
			ASSERT_EQ(owners.size(), 1);
			ASSERT_EQ(owners[0].lot_name, "test_lot");
			ASSERT_EQ(owners[0].owner, "test_owner");
		} catch (const std::exception &e) {
			FAIL() << "Failed to query owners (data not preserved?): " << e.what();
		}
	}
}

TEST_F(MigrationTest, TestFreshDBVersion) {
	// 1. Ensure no DB exists (SetUp handles this via unique tmp_dir)

	// 2. Initialize StorageManager
	char *raw_err = nullptr;
	int rv = lotman_set_context_str("lot_home", tmp_dir.c_str(), &raw_err);
	UniqueCStringMig err(raw_err);
	ASSERT_EQ(rv, 0) << "Failed to set lot_home: " << (err.get() ? err.get() : "unknown error");

	auto &storage = lotman::db::StorageManager::get_storage();

	// 3. Verify schema_versions table exists and has version 1 (TARGET_DB_VERSION)
	try {
		auto versions = storage.get_all<lotman::db::SchemaVersion>();
		ASSERT_EQ(versions.size(), 1);
		ASSERT_EQ(versions[0].version, 1);
	} catch (const std::exception &e) {
		FAIL() << "Failed to query schema_versions: " << e.what();
	}
}

TEST_F(MigrationTest, TestCorruptedDBRejected) {
	// This test verifies that corrupted or incompatible databases are rejected
	// with a clear error message, rather than silently losing data.
	std::string db_dir = tmp_dir + "/.lot";
	std::filesystem::create_directories(db_dir);
	std::string db_path = db_dir + "/lotman_cpp.sqlite";

	// Create a database with an incompatible schema (missing columns, wrong types, etc.)
	sqlite3 *db;
	int rc = sqlite3_open(db_path.c_str(), &db);
	ASSERT_EQ(rc, SQLITE_OK);

	// Create an owners table with a different schema (simulating corruption or incompatibility)
	const char *sql = "CREATE TABLE owners (lot_name TEXT PRIMARY KEY, owner TEXT, extra_column INTEGER)";
	char *errMsg = nullptr;
	rc = sqlite3_exec(db, sql, nullptr, nullptr, &errMsg);
	if (rc != SQLITE_OK) {
		std::cerr << "SQL error: " << errMsg << std::endl;
		sqlite3_free(errMsg);
	}
	ASSERT_EQ(rc, SQLITE_OK);
	sqlite3_close(db);

	// Try to initialize StorageManager - should throw an error
	char *raw_err = nullptr;
	int rv = lotman_set_context_str("lot_home", tmp_dir.c_str(), &raw_err);
	UniqueCStringMig err(raw_err);
	ASSERT_EQ(rv, 0) << "Failed to set lot_home: " << (err.get() ? err.get() : "unknown error");

	// This should throw because the schema is incompatible
	EXPECT_THROW(
		{
			try {
				lotman::db::StorageManager::get_storage();
			} catch (const std::runtime_error &e) {
				// Verify the error message mentions data loss protection
				std::string msg = e.what();
				EXPECT_TRUE(msg.find("data loss") != std::string::npos ||
							msg.find("schema mismatch") != std::string::npos)
					<< "Error message should mention data loss or schema mismatch, got: " << msg;
				throw;
			}
		},
		std::runtime_error);
}
