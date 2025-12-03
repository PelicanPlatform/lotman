#include "../src/lotman.h"
#include "../src/lotman_db.h"

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <gtest/gtest.h>
#include <sqlite3.h>

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
	// 1. Create a raw sqlite DB with just 'owners' table (simulating v0)
	sqlite3 *db;
	std::string db_dir = tmp_dir + "/.lot";
	std::filesystem::create_directories(db_dir);
	std::string db_path = db_dir + "/lotman_cpp.sqlite";

	int rc = sqlite3_open(db_path.c_str(), &db);
	ASSERT_EQ(rc, SQLITE_OK);

	// Create owners table manually (as it would exist in v0)
	const char *sql = "CREATE TABLE owners (lot_name TEXT PRIMARY KEY, owner TEXT)";
	char *errMsg = nullptr;
	rc = sqlite3_exec(db, sql, nullptr, nullptr, &errMsg);
	if (rc != SQLITE_OK) {
		std::cerr << "SQL error: " << errMsg << std::endl;
		sqlite3_free(errMsg);
	}
	ASSERT_EQ(rc, SQLITE_OK);
	sqlite3_close(db);

	// 2. Initialize StorageManager (should trigger migration)
	char *raw_err = nullptr;
	lotman_set_context_str("lot_home", tmp_dir.c_str(), &raw_err);
	if (raw_err)
		free(raw_err);

	auto &storage = lotman::db::StorageManager::get_storage();

	// 3. Verify schema_versions table exists and has version 1
	bool version_table_exists = false;
	try {
		auto versions = storage.get_all<lotman::db::SchemaVersion>();
		ASSERT_EQ(versions.size(), 1);
		ASSERT_EQ(versions[0].version, 1);
		version_table_exists = true;
	} catch (const std::exception &e) {
		FAIL() << "Failed to query schema_versions: " << e.what();
	}
	ASSERT_TRUE(version_table_exists);
}

TEST_F(MigrationTest, TestFreshDBVersion) {
	// 1. Ensure no DB exists (SetUp handles this via unique tmp_dir)

	// 2. Initialize StorageManager
	char *raw_err = nullptr;
	lotman_set_context_str("lot_home", tmp_dir.c_str(), &raw_err);
	if (raw_err)
		free(raw_err);

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
