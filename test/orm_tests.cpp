/**
 * ORM Integration Tests for LotMan
 *
 * These tests verify that the sqlite_orm integration works correctly
 * for all basic CRUD operations on the database layer.
 */

#include "../src/lotman.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

// RAII wrappers for C-style memory management
struct CStringDeleter {
	void operator()(char *ptr) const {
		if (ptr)
			free(ptr);
	}
};
using UniqueCString = std::unique_ptr<char, CStringDeleter>;

struct StringListDeleter {
	void operator()(char **ptr) const {
		if (ptr)
			lotman_free_string_list(ptr);
	}
};
using UniqueStringList = std::unique_ptr<char *, StringListDeleter>;

// Test fixture for ORM-specific tests
class ORMTest : public ::testing::Test {
  protected:
	std::string tmp_dir;

	std::string create_temp_directory() {
		std::string temp_dir_template = "/tmp/lotman_orm_test_XXXXXX";
		char temp_dir_name[temp_dir_template.size() + 1];
		std::strcpy(temp_dir_name, temp_dir_template.c_str());

		char *mkdtemp_result = mkdtemp(temp_dir_name);
		if (mkdtemp_result == nullptr) {
			std::cerr << "Failed to create temporary directory\n";
			exit(1);
		}

		return std::string(mkdtemp_result);
	}

	void SetUp() override {
		tmp_dir = create_temp_directory();
		char *raw_err = nullptr;
		auto rv = lotman_set_context_str("lot_home", tmp_dir.c_str(), &raw_err);
		UniqueCString err(raw_err);
		if (rv != 0) {
			std::cerr << "Failed to set lot_home: " << err.get() << std::endl;
			exit(1);
		}
		raw_err = nullptr;
		rv = lotman_set_context_str("caller", "test_owner", &raw_err);
		UniqueCString err2(raw_err);
		if (rv != 0) {
			std::cerr << "Failed to set caller: " << err2.get() << std::endl;
			exit(1);
		}
	}

	void TearDown() override {
		std::filesystem::remove_all(tmp_dir);
	}

	// Helper to add a lot and assert success
	void addLot(const char *lot_json) {
		char *raw_err = nullptr;
		int rv = lotman_add_lot(lot_json, &raw_err);
		UniqueCString err_msg(raw_err);
		ASSERT_EQ(rv, 0) << "Failed to add lot: " << err_msg.get();
	}

	// Helper to set caller context
	void setCallerContext(const char *caller) {
		char *raw_err = nullptr;
		int rv = lotman_set_context_str("caller", caller, &raw_err);
		UniqueCString err_msg(raw_err);
		ASSERT_EQ(rv, 0) << "Failed to set caller context: " << err_msg.get();
	}

	// Helper to add the required default lot
	void addDefaultLot() {
		const char *default_lot = R"({
			"lot_name": "default",
			"owner": "default_owner",
			"parents": ["default"],
			"paths": [{"path": "/default/paths", "recursive": true}],
			"management_policy_attrs": {
				"dedicated_GB": 100,
				"opportunistic_GB": 50,
				"max_num_objects": 1000,
				"creation_time": 1700000000,
				"expiration_time": 1800000000,
				"deletion_time": 1900000000
			}
		})";
		addLot(default_lot);
	}

	// Helper to get string list as vector for easier testing
	std::vector<std::string> stringListToVector(char **list) {
		std::vector<std::string> result;
		if (list) {
			for (int i = 0; list[i] != nullptr; i++) {
				result.push_back(list[i]);
			}
		}
		return result;
	}
};

namespace {

//=============================================================================
// Basic CRUD Operations Tests (ORM Layer)
//=============================================================================

TEST_F(ORMTest, CreateLotWithORM) {
	// Test that basic lot creation works with the ORM layer
	addDefaultLot();

	const char *test_lot = R"({
		"lot_name": "orm_test_lot",
		"owner": "test_owner",
		"parents": ["orm_test_lot"],
		"paths": [{"path": "/orm/test/path", "recursive": true}],
		"management_policy_attrs": {
			"dedicated_GB": 50,
			"opportunistic_GB": 25,
			"max_num_objects": 500,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";

	char *raw_err = nullptr;
	int rv = lotman_add_lot(test_lot, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_EQ(rv, 0) << "Failed to create lot: " << err_msg.get();

	// Verify lot exists
	raw_err = nullptr;
	rv = lotman_lot_exists("orm_test_lot", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 1) << "Lot should exist after creation";
}

TEST_F(ORMTest, ReadLotOwners) {
	// Test that reading lot owners works correctly with ORM
	addDefaultLot();

	const char *test_lot = R"({
		"lot_name": "owner_test_lot",
		"owner": "specific_owner",
		"parents": ["owner_test_lot"],
		"paths": [{"path": "/owner/test", "recursive": false}],
		"management_policy_attrs": {
			"dedicated_GB": 10,
			"opportunistic_GB": 5,
			"max_num_objects": 100,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(test_lot);

	char *raw_err = nullptr;
	char **owners = nullptr;
	int rv = lotman_get_owners("owner_test_lot", false, &owners, &raw_err);
	UniqueCString err_msg(raw_err);
	UniqueStringList owners_guard(owners);

	ASSERT_EQ(rv, 0) << "Failed to get owners: " << err_msg.get();
	ASSERT_NE(owners, nullptr) << "Owners should not be null";

	auto owners_vec = stringListToVector(owners);
	ASSERT_EQ(owners_vec.size(), 1) << "Should have exactly one owner";
	EXPECT_EQ(owners_vec[0], "specific_owner") << "Owner should match";
}

TEST_F(ORMTest, ReadLotPaths) {
	// Test that reading lot paths works correctly with ORM
	addDefaultLot();

	const char *test_lot = R"({
		"lot_name": "path_test_lot",
		"owner": "test_owner",
		"parents": ["path_test_lot"],
		"paths": [
			{"path": "/path/one", "recursive": true},
			{"path": "/path/two", "recursive": false}
		],
		"management_policy_attrs": {
			"dedicated_GB": 10,
			"opportunistic_GB": 5,
			"max_num_objects": 100,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(test_lot);

	char *raw_err = nullptr;
	char *paths_json = nullptr;
	int rv = lotman_get_lot_dirs("path_test_lot", false, &paths_json, &raw_err);
	UniqueCString err_msg(raw_err);
	UniqueCString paths_guard(paths_json);

	ASSERT_EQ(rv, 0) << "Failed to get paths: " << err_msg.get();
	ASSERT_NE(paths_json, nullptr) << "Paths JSON should not be null";

	// Output is JSON, verify it contains our paths
	std::string json_str(paths_json);
	EXPECT_TRUE(json_str.find("/path/one") != std::string::npos) << "Should contain /path/one";
	EXPECT_TRUE(json_str.find("/path/two") != std::string::npos) << "Should contain /path/two";
}

TEST_F(ORMTest, UpdateLotOwner) {
	// Test that updating lot owner works correctly with ORM
	addDefaultLot();

	const char *test_lot = R"({
		"lot_name": "update_test_lot",
		"owner": "original_owner",
		"parents": ["update_test_lot"],
		"paths": [{"path": "/update/test", "recursive": true}],
		"management_policy_attrs": {
			"dedicated_GB": 10,
			"opportunistic_GB": 5,
			"max_num_objects": 100,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(test_lot);

	// Set caller to original owner to perform update
	setCallerContext("original_owner");

	// Update the owner
	const char *update_json = R"({
		"lot_name": "update_test_lot",
		"owner": "new_owner"
	})";

	char *raw_err = nullptr;
	int rv = lotman_update_lot(update_json, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_EQ(rv, 0) << "Failed to update lot: " << err_msg.get();

	// Verify the update
	char **owners = nullptr;
	raw_err = nullptr;
	rv = lotman_get_owners("update_test_lot", false, &owners, &raw_err);
	err_msg.reset(raw_err);
	UniqueStringList owners_guard(owners);

	ASSERT_EQ(rv, 0) << "Failed to get owners after update: " << err_msg.get();

	auto owners_vec = stringListToVector(owners);
	ASSERT_EQ(owners_vec.size(), 1) << "Should have exactly one owner";
	EXPECT_EQ(owners_vec[0], "new_owner") << "Owner should be updated";
}

TEST_F(ORMTest, DeleteLot) {
	// Test that deleting a lot works correctly with ORM
	addDefaultLot();

	const char *test_lot = R"({
		"lot_name": "delete_test_lot",
		"owner": "test_owner",
		"parents": ["delete_test_lot"],
		"paths": [{"path": "/delete/test", "recursive": true}],
		"management_policy_attrs": {
			"dedicated_GB": 10,
			"opportunistic_GB": 5,
			"max_num_objects": 100,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(test_lot);

	// Verify lot exists before deletion
	char *raw_err = nullptr;
	int rv = lotman_lot_exists("delete_test_lot", &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_EQ(rv, 1) << "Lot should exist before deletion";

	// Delete the lot
	raw_err = nullptr;
	rv = lotman_remove_lot("delete_test_lot", false, false, false, false, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << "Failed to delete lot: " << err_msg.get();

	// Verify lot no longer exists
	raw_err = nullptr;
	rv = lotman_lot_exists("delete_test_lot", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << "Lot should not exist after deletion";
}

//=============================================================================
// Parent-Child Relationship Tests (ORM Layer)
//=============================================================================

TEST_F(ORMTest, CreateChildLot) {
	// Test creating a child lot under a parent
	addDefaultLot();

	// Create parent lot
	const char *parent_lot = R"({
		"lot_name": "parent_lot",
		"owner": "parent_owner",
		"parents": ["parent_lot"],
		"paths": [{"path": "/parent/path", "recursive": true}],
		"management_policy_attrs": {
			"dedicated_GB": 100,
			"opportunistic_GB": 50,
			"max_num_objects": 1000,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(parent_lot);

	// Set caller to parent owner to create child
	setCallerContext("parent_owner");

	// Create child lot
	const char *child_lot = R"({
		"lot_name": "child_lot",
		"owner": "child_owner",
		"parents": ["parent_lot"],
		"paths": [{"path": "/parent/path/child", "recursive": false}],
		"management_policy_attrs": {
			"dedicated_GB": 25,
			"opportunistic_GB": 10,
			"max_num_objects": 250,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";

	char *raw_err = nullptr;
	int rv = lotman_add_lot(child_lot, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_EQ(rv, 0) << "Failed to create child lot: " << err_msg.get();

	// Verify child exists
	raw_err = nullptr;
	rv = lotman_lot_exists("child_lot", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 1) << "Child lot should exist";
}

TEST_F(ORMTest, GetChildrenNames) {
	// Test getting children of a parent lot
	addDefaultLot();

	// Create parent lot
	const char *parent_lot = R"({
		"lot_name": "parent_with_children",
		"owner": "parent_owner",
		"parents": ["parent_with_children"],
		"paths": [{"path": "/parent/children/test", "recursive": true}],
		"management_policy_attrs": {
			"dedicated_GB": 100,
			"opportunistic_GB": 50,
			"max_num_objects": 1000,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(parent_lot);

	setCallerContext("parent_owner");

	// Create two child lots
	const char *child1 = R"({
		"lot_name": "child_one",
		"owner": "child_owner",
		"parents": ["parent_with_children"],
		"paths": [{"path": "/parent/children/test/one", "recursive": false}],
		"management_policy_attrs": {
			"dedicated_GB": 20,
			"opportunistic_GB": 10,
			"max_num_objects": 200,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(child1);

	const char *child2 = R"({
		"lot_name": "child_two",
		"owner": "child_owner",
		"parents": ["parent_with_children"],
		"paths": [{"path": "/parent/children/test/two", "recursive": false}],
		"management_policy_attrs": {
			"dedicated_GB": 20,
			"opportunistic_GB": 10,
			"max_num_objects": 200,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(child2);

	// Get children names
	char *raw_err = nullptr;
	char **children = nullptr;
	int rv = lotman_get_children_names("parent_with_children", false, false, &children, &raw_err);
	UniqueCString err_msg(raw_err);
	UniqueStringList children_guard(children);

	ASSERT_EQ(rv, 0) << "Failed to get children: " << err_msg.get();
	ASSERT_NE(children, nullptr) << "Children list should not be null";

	auto children_vec = stringListToVector(children);
	ASSERT_EQ(children_vec.size(), 2) << "Should have exactly two children";

	// Check both children are present (order may vary)
	bool has_child_one = std::find(children_vec.begin(), children_vec.end(), "child_one") != children_vec.end();
	bool has_child_two = std::find(children_vec.begin(), children_vec.end(), "child_two") != children_vec.end();
	EXPECT_TRUE(has_child_one) << "child_one should be in children list";
	EXPECT_TRUE(has_child_two) << "child_two should be in children list";
}

TEST_F(ORMTest, GetParentNames) {
	// Test getting parents of a child lot
	addDefaultLot();

	// Create parent lot
	const char *parent_lot = R"({
		"lot_name": "my_parent",
		"owner": "parent_owner",
		"parents": ["my_parent"],
		"paths": [{"path": "/my/parent/path", "recursive": true}],
		"management_policy_attrs": {
			"dedicated_GB": 100,
			"opportunistic_GB": 50,
			"max_num_objects": 1000,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(parent_lot);

	setCallerContext("parent_owner");

	// Create child lot
	const char *child_lot = R"({
		"lot_name": "my_child",
		"owner": "child_owner",
		"parents": ["my_parent"],
		"paths": [{"path": "/my/parent/path/child", "recursive": false}],
		"management_policy_attrs": {
			"dedicated_GB": 25,
			"opportunistic_GB": 10,
			"max_num_objects": 250,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(child_lot);

	// Get parent names
	char *raw_err = nullptr;
	char **parents = nullptr;
	int rv = lotman_get_parent_names("my_child", false, false, &parents, &raw_err);
	UniqueCString err_msg(raw_err);
	UniqueStringList parents_guard(parents);

	ASSERT_EQ(rv, 0) << "Failed to get parents: " << err_msg.get();
	ASSERT_NE(parents, nullptr) << "Parents list should not be null";

	auto parents_vec = stringListToVector(parents);
	ASSERT_EQ(parents_vec.size(), 1) << "Should have exactly one parent";
	EXPECT_EQ(parents_vec[0], "my_parent") << "Parent should be 'my_parent'";
}

TEST_F(ORMTest, DeleteChildThenParent) {
	// Test that we can delete child first, then parent
	addDefaultLot();

	// Create parent lot
	const char *parent_lot = R"({
		"lot_name": "deletable_parent",
		"owner": "owner",
		"parents": ["deletable_parent"],
		"paths": [{"path": "/deletable/parent", "recursive": true}],
		"management_policy_attrs": {
			"dedicated_GB": 100,
			"opportunistic_GB": 50,
			"max_num_objects": 1000,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(parent_lot);

	setCallerContext("owner");

	// Create child lot
	const char *child_lot = R"({
		"lot_name": "deletable_child",
		"owner": "owner",
		"parents": ["deletable_parent"],
		"paths": [{"path": "/deletable/parent/child", "recursive": false}],
		"management_policy_attrs": {
			"dedicated_GB": 25,
			"opportunistic_GB": 10,
			"max_num_objects": 250,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(child_lot);

	// Delete child first
	char *raw_err = nullptr;
	int rv = lotman_remove_lot("deletable_child", false, false, false, false, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_EQ(rv, 0) << "Failed to delete child: " << err_msg.get();

	// Verify child is deleted
	raw_err = nullptr;
	rv = lotman_lot_exists("deletable_child", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << "Child should not exist after deletion";

	// Now delete parent
	raw_err = nullptr;
	rv = lotman_remove_lot("deletable_parent", false, false, false, false, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << "Failed to delete parent: " << err_msg.get();

	// Verify parent is deleted
	raw_err = nullptr;
	rv = lotman_lot_exists("deletable_parent", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << "Parent should not exist after deletion";
}

//=============================================================================
// Management Policy Attributes Tests (ORM Layer)
//=============================================================================

TEST_F(ORMTest, ReadManagementPolicyAttributes) {
	// Test reading management policy attributes via ORM
	addDefaultLot();

	const char *test_lot = R"({
		"lot_name": "policy_test_lot",
		"owner": "test_owner",
		"parents": ["policy_test_lot"],
		"paths": [{"path": "/policy/test", "recursive": true}],
		"management_policy_attrs": {
			"dedicated_GB": 123.5,
			"opportunistic_GB": 45.25,
			"max_num_objects": 999,
			"creation_time": 1700000001,
			"expiration_time": 1800000002,
			"deletion_time": 1900000003
		}
	})";
	addLot(test_lot);

	// Get policy attributes as JSON - API takes a JSON query string specifying which attributes to return
	const char *query_json = R"({
		"lot_name": "policy_test_lot",
		"dedicated_GB": true,
		"opportunistic_GB": true,
		"max_num_objects": true,
		"creation_time": true,
		"expiration_time": true,
		"deletion_time": true
	})";
	char *raw_err = nullptr;
	char *json_output = nullptr;
	int rv = lotman_get_policy_attributes(query_json, &json_output, &raw_err);
	UniqueCString err_msg(raw_err);
	UniqueCString json_guard(json_output);

	ASSERT_EQ(rv, 0) << "Failed to get policy attributes: " << err_msg.get();
	ASSERT_NE(json_output, nullptr) << "JSON output should not be null";

	// The output should contain our values - they're in nested objects like {"dedicated_GB": {"lot_name": "...",
	// "value": ...}}
	std::string json_str(json_output);
	EXPECT_TRUE(json_str.find("dedicated_GB") != std::string::npos) << "Should contain dedicated_GB key";
	EXPECT_TRUE(json_str.find("max_num_objects") != std::string::npos) << "Should contain max_num_objects key";
}

TEST_F(ORMTest, UpdateManagementPolicyAttributes) {
	// Test updating management policy attributes via ORM
	addDefaultLot();

	const char *test_lot = R"({
		"lot_name": "policy_update_lot",
		"owner": "test_owner",
		"parents": ["policy_update_lot"],
		"paths": [{"path": "/policy/update", "recursive": true}],
		"management_policy_attrs": {
			"dedicated_GB": 100,
			"opportunistic_GB": 50,
			"max_num_objects": 1000,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(test_lot);

	// Update policy attributes
	const char *update_json = R"({
		"lot_name": "policy_update_lot",
		"management_policy_attrs": {
			"dedicated_GB": 200
		}
	})";

	char *raw_err = nullptr;
	int rv = lotman_update_lot(update_json, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_EQ(rv, 0) << "Failed to update lot policy: " << err_msg.get();

	// Verify the update
	const char *query_json = R"({
		"lot_name": "policy_update_lot",
		"dedicated_GB": true
	})";
	char *json_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_policy_attributes(query_json, &json_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueCString json_guard(json_output);

	ASSERT_EQ(rv, 0) << "Failed to get policy attributes after update: " << err_msg.get();

	// Verify output contains dedicated_GB
	std::string json_str(json_output);
	EXPECT_TRUE(json_str.find("dedicated_GB") != std::string::npos) << "Should contain dedicated_GB key";
	EXPECT_TRUE(json_str.find("200") != std::string::npos) << "Should contain updated dedicated_GB value";
}

//=============================================================================
// Path Operations Tests (ORM Layer)
//=============================================================================

TEST_F(ORMTest, AddPathsToLot) {
	// Test adding paths to an existing lot
	addDefaultLot();

	const char *test_lot = R"({
		"lot_name": "path_add_lot",
		"owner": "test_owner",
		"parents": ["path_add_lot"],
		"paths": [{"path": "/initial/path", "recursive": true}],
		"management_policy_attrs": {
			"dedicated_GB": 100,
			"opportunistic_GB": 50,
			"max_num_objects": 1000,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(test_lot);

	// Add more paths
	const char *add_paths_json = R"({
		"lot_name": "path_add_lot",
		"paths": [
			{"path": "/added/path/one", "recursive": false},
			{"path": "/added/path/two", "recursive": true}
		]
	})";

	char *raw_err = nullptr;
	int rv = lotman_add_to_lot(add_paths_json, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_EQ(rv, 0) << "Failed to add paths: " << err_msg.get();

	// Verify paths were added
	char *paths_json = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lot_dirs("path_add_lot", false, &paths_json, &raw_err);
	err_msg.reset(raw_err);
	UniqueCString paths_guard(paths_json);

	ASSERT_EQ(rv, 0) << "Failed to get paths: " << err_msg.get();
	ASSERT_NE(paths_json, nullptr) << "Paths JSON should not be null";

	// Verify JSON contains all three paths
	std::string json_str(paths_json);
	EXPECT_TRUE(json_str.find("/initial/path") != std::string::npos) << "Should contain /initial/path";
	EXPECT_TRUE(json_str.find("/added/path/one") != std::string::npos) << "Should contain /added/path/one";
	EXPECT_TRUE(json_str.find("/added/path/two") != std::string::npos) << "Should contain /added/path/two";
}

TEST_F(ORMTest, LotExistenceCheck) {
	// Test lot existence checking
	addDefaultLot();

	// Check non-existent lot
	char *raw_err = nullptr;
	int rv = lotman_lot_exists("nonexistent_lot", &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_EQ(rv, 0) << "Non-existent lot should return 0";

	// Create a lot
	const char *test_lot = R"({
		"lot_name": "existence_test",
		"owner": "test_owner",
		"parents": ["existence_test"],
		"paths": [{"path": "/existence/test", "recursive": true}],
		"management_policy_attrs": {
			"dedicated_GB": 10,
			"opportunistic_GB": 5,
			"max_num_objects": 100,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	addLot(test_lot);

	// Check existing lot
	raw_err = nullptr;
	rv = lotman_lot_exists("existence_test", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 1) << "Existing lot should return 1";
}

TEST_F(ORMTest, MultipleLotsInDatabase) {
	// Test that multiple lots can coexist properly
	addDefaultLot();

	// Create several lots
	for (int i = 1; i <= 5; i++) {
		std::string lot_json = R"({
			"lot_name": "multi_lot_)" +
							   std::to_string(i) + R"(",
			"owner": "test_owner",
			"parents": ["multi_lot_)" +
							   std::to_string(i) + R"("],
			"paths": [{"path": "/multi/)" +
							   std::to_string(i) + R"(", "recursive": true}],
			"management_policy_attrs": {
				"dedicated_GB": )" +
							   std::to_string(i * 10) + R"(,
				"opportunistic_GB": )" +
							   std::to_string(i * 5) + R"(,
				"max_num_objects": )" +
							   std::to_string(i * 100) + R"(,
				"creation_time": 1700000000,
				"expiration_time": 1800000000,
				"deletion_time": 1900000000
			}
		})";
		addLot(lot_json.c_str());
	}

	// Verify all lots exist
	for (int i = 1; i <= 5; i++) {
		std::string lot_name = "multi_lot_" + std::to_string(i);
		char *raw_err = nullptr;
		int rv = lotman_lot_exists(lot_name.c_str(), &raw_err);
		UniqueCString err_msg(raw_err);
		ASSERT_EQ(rv, 1) << "Lot " << lot_name << " should exist";
	}
}

} // namespace
