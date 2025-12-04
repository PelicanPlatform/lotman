#include "../src/lotman.h"

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <nlohmann/json.hpp>
#include <typeinfo>

using json = nlohmann::json;

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

// Test class to handle setup and teardown for each individual test
class LotManTest : public ::testing::Test {
  protected:
	std::string tmp_dir;

	std::string create_temp_directory() {
		// Generate a unique name for the temporary directory
		std::string temp_dir_template = "/tmp/lotman_test_XXXXXX";
		char temp_dir_name[temp_dir_template.size() + 1]; // +1 for the null-terminator
		std::strcpy(temp_dir_name, temp_dir_template.c_str());

		// mkdtemp replaces 'X's with a unique directory name and creates the directory
		char *mkdtemp_result = mkdtemp(temp_dir_name);
		if (mkdtemp_result == nullptr) {
			std::cerr << "Failed to create temporary directory\n";
			exit(1);
		}

		return std::string(mkdtemp_result);
	}

	// Called before each test
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
		rv = lotman_set_context_str("caller", "owner1", &raw_err);
		UniqueCString err2(raw_err);
		if (rv != 0) {
			std::cerr << "Failed to set caller: " << err2.get() << std::endl;
			exit(1);
		}
	}

	// Called after each test
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

	// Helper to add default lot - MUST be created before any other lots
	void addDefaultLot() {
		const char *default_lot = R"({
			"lot_name": "default",
			"owner": "owner2",
			"parents": ["default"],
			"paths": [{"path": "/default/paths", "recursive": true}],
			"management_policy_attrs": {
				"dedicated_GB": 5,
				"opportunistic_GB": 2.5,
				"max_num_objects": 100,
				"creation_time": 123,
				"expiration_time": 234,
				"deletion_time": 345
			}
		})";
		addLot(default_lot);
	}

	// Helper to add lot1 (self-parent root lot)
	void addLot1() {
		const char *lot1 = R"({
			"lot_name": "lot1",
			"owner": "owner1",
			"parents": ["lot1"],
			"paths": [
				{"path": "/1/2/3", "recursive": false},
				{"path": "/foo/bar", "recursive": true}
			],
			"management_policy_attrs": {
				"dedicated_GB": 5,
				"opportunistic_GB": 2.5,
				"max_num_objects": 20,
				"creation_time": 123,
				"expiration_time": 234,
				"deletion_time": 345
			}
		})";
		addLot(lot1);
	}

	// Helper to add lot2 (child of lot1)
	void addLot2() {
		const char *lot2 = R"({
			"lot_name": "lot2",
			"owner": "owner1",
			"parents": ["lot1"],
			"paths": [
				{"path": "/1/2/4", "recursive": true},
				{"path": "/foo/baz", "recursive": true}
			],
			"management_policy_attrs": {
				"dedicated_GB": 6,
				"opportunistic_GB": 1.5,
				"max_num_objects": 100,
				"creation_time": 123,
				"expiration_time": 233,
				"deletion_time": 355
			}
		})";
		addLot(lot2);
	}

	// Helper to add lot3 (self-parent root lot)
	void addLot3() {
		const char *lot3 = R"({
			"lot_name": "lot3",
			"owner": "owner1",
			"parents": ["lot3"],
			"paths": [
				{"path": "/another/path", "recursive": false},
				{"path": "/123", "recursive": true}
			],
			"management_policy_attrs": {
				"dedicated_GB": 3,
				"opportunistic_GB": 2.0,
				"max_num_objects": 60,
				"creation_time": 123,
				"expiration_time": 232,
				"deletion_time": 325
			}
		})";
		addLot(lot3);
	}

	// Helper to add lot4 (child of lot2 and lot3)
	void addLot4() {
		const char *lot4 = R"({
			"lot_name": "lot4",
			"owner": "owner1",
			"parents": ["lot2", "lot3"],
			"paths": [
				{"path": "/1/2/3/4", "recursive": true},
				{"path": "/345", "recursive": true}
			],
			"management_policy_attrs": {
				"dedicated_GB": 3,
				"opportunistic_GB": 2.1,
				"max_num_objects": 40,
				"creation_time": 123,
				"expiration_time": 231,
				"deletion_time": 315
			}
		})";
		addLot(lot4);
	}

	// Helper to add lot5 (child of lot3, parent of lot4)
	void addLot5() {
		const char *lot5 = R"({
			"lot_name": "lot5",
			"owner": "owner1",
			"parents": ["lot3"],
			"children": ["lot4"],
			"paths": [
				{"path": "/456", "recursive": false},
				{"path": "/567", "recursive": true}
			],
			"management_policy_attrs": {
				"dedicated_GB": 10,
				"opportunistic_GB": 3.5,
				"max_num_objects": 20,
				"creation_time": 100,
				"expiration_time": 200,
				"deletion_time": 300
			}
		})";
		addLot(lot5);
	}

	// Helper to add sep_node (self-parent root lot with long expiration)
	void addSepNode() {
		const char *sep_node = R"({
			"lot_name": "sep_node",
			"owner": "owner1",
			"parents": ["sep_node"],
			"paths": [{"path": "/sep/node", "recursive": true}],
			"management_policy_attrs": {
				"dedicated_GB": 3,
				"opportunistic_GB": 2.1,
				"max_num_objects": 10,
				"creation_time": 123,
				"expiration_time": 99679525853643,
				"deletion_time": 9267952553643
			}
		})";
		addLot(sep_node);
	}

	// Helper to set up the standard lot hierarchy: default, lot1 -> lot2 -> lot4, lot3 -> lot4, sep_node
	void setupStandardHierarchy() {
		addDefaultLot(); // Must be created first
		addLot1();
		addLot2();
		addLot3();
		addLot4();
		addSepNode();
	}

	// Helper to set up the full hierarchy with lot5 inserted between lot3 and lot4
	void setupFullHierarchy() {
		addDefaultLot(); // Must be created first
		addLot1();
		addLot2();
		addLot3();
		addLot4();
		addSepNode();
		addLot5(); // lot5 becomes parent of lot4, child of lot3
	}
};

namespace {

TEST_F(LotManTest, DefaultLotTests) {
	// The default lot must be created first before any other lots
	addDefaultLot();

	// Test that we can add a self-parent lot after default exists
	char *raw_err = nullptr;
	const char *lot1 = R"({
		"lot_name": "lot1",
		"owner": "owner1",
		"parents": ["lot1"],
		"paths": [
			{"path": "/1/2/3", "recursive": false},
			{"path": "/foo/bar", "recursive": true}
		],
		"management_policy_attrs": {
			"dedicated_GB": 5,
			"opportunistic_GB": 2.5,
			"max_num_objects": 100,
			"creation_time": 123,
			"expiration_time": 234,
			"deletion_time": 345
		}
	})";

	// Add lot1 (should succeed as self-parent after default exists)
	auto rv = lotman_add_lot(lot1, &raw_err);
	UniqueCString err1(raw_err);
	ASSERT_EQ(rv, 0) << err1.get();

	// Try to remove the default lot - should fail
	raw_err = nullptr;
	rv = lotman_remove_lot("default", true, true, true, false, &raw_err);
	UniqueCString err2(raw_err);
	ASSERT_NE(rv, 0) << err2.get();
}

TEST_F(LotManTest, AddRemoveSublot) {
	// The default lot must be created first
	addDefaultLot();

	// Set up fresh database with lot1
	const char *lot1 = R"({
		"lot_name": "lot1",
		"owner": "owner1",
		"parents": ["lot1"],
		"paths": [
			{"path": "1/2/3", "recursive": false},
			{"path": "/foo/bar", "recursive": true}
		],
		"management_policy_attrs": {
			"dedicated_GB": 5,
			"opportunistic_GB": 2.5,
			"max_num_objects": 100,
			"creation_time": 123,
			"expiration_time": 234,
			"deletion_time": 345
		}
	})";

	char *raw_err = nullptr;
	auto rv = lotman_add_lot(lot1, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Remove the lot
	const char *deleted_lot = "lot1";
	raw_err = nullptr;
	rv = lotman_remove_lot(deleted_lot, false, false, false, false, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Verify it no longer exists
	raw_err = nullptr;
	rv = lotman_lot_exists(deleted_lot, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_FALSE(rv) << err_msg.get();

	// Try to remove a lot that doesn't exist
	const char *non_existent_lot = "non_existent_lot";
	raw_err = nullptr;
	rv = lotman_remove_lot(non_existent_lot, false, false, false, false, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 0) << err_msg.get();
}

TEST_F(LotManTest, AddInvalidLots) {
	// Set up fresh database with standard hierarchy
	setupStandardHierarchy();

	char *raw_err = nullptr;

	// Try to add a lot with cyclic dependency
	// Cycle created by trying to add lot5 with lot1 as a child
	const char *cyclic_lot = R"({
		"lot_name": "lot5",
		"owner": "owner1",
		"parents": ["lot4"],
		"children": ["lot1"],
		"paths": [
			{"path": "/456", "recursive": false},
			{"path": "/567", "recursive": true}
		],
		"management_policy_attrs": {
			"dedicated_GB": 5,
			"opportunistic_GB": 2.5,
			"max_num_objects": 100,
			"creation_time": 123,
			"expiration_time": 234,
			"deletion_time": 345
		}
	})";
	auto rv = lotman_add_lot(cyclic_lot, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_NE(rv, 0) << err_msg.get();

	// Try to add lot with no parent
	const char *no_parents_lot = R"({
		"lot_name": "lot5",
		"owner": "owner1",
		"parents": [],
		"children": ["lot1"],
		"paths": [
			{"path": "/456", "recursive": false},
			{"path": "/567", "recursive": true}
		],
		"management_policy_attrs": {
			"dedicated_GB": 111111,
			"opportunistic_GB": 2.5,
			"max_num_objects": 100,
			"creation_time": 123,
			"expiration_time": 234,
			"deletion_time": 345
		}
	})";
	raw_err = nullptr;
	rv = lotman_add_lot(no_parents_lot, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 0) << err_msg.get();
}

TEST_F(LotManTest, InsertionTest) {
	// Set up fresh database with standard hierarchy
	setupStandardHierarchy();

	char *raw_err = nullptr;
	// Insert lot5 between lot3 and lot4
	const char *lot5 = R"({
		"lot_name": "lot5",
		"owner": "owner1",
		"parents": ["lot3"],
		"children": ["lot4"],
		"paths": [
			{"path": "/456", "recursive": false},
			{"path": "/567", "recursive": true}
		],
		"management_policy_attrs": {
			"dedicated_GB": 10,
			"opportunistic_GB": 3.5,
			"max_num_objects": 20,
			"creation_time": 100,
			"expiration_time": 200,
			"deletion_time": 300
		}
	})";
	int rv = lotman_add_lot(lot5, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Check that lot5 is a parent to lot4 and that lot3 is a parent to lot5
	char **raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_parent_names("lot4", false, false, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueStringList output(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	bool check = false;
	for (int iter = 0; output.get()[iter]; iter++) {
		if (static_cast<std::string>(output.get()[iter]) == "lot5") {
			check = true;
		}
	}
	ASSERT_TRUE(check);

	check = false;
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_children_names("lot3", false, false, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueStringList output2(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	for (int iter = 0; output2.get()[iter]; iter++) {
		if (static_cast<std::string>(output2.get()[iter]) == "lot5") {
			check = true;
		}
	}
	ASSERT_TRUE(check);
}

TEST_F(LotManTest, ModifyLotTest) {
	// Set up fresh database with standard hierarchy (already includes default lot)
	setupStandardHierarchy();

	char *raw_err = nullptr;

	// Try to modify a non-existent lot
	const char *non_existent_lot = R"({
		"lot_name": "non_existent_lot",
		"owner": "owner1",
		"parents": ["lot1"],
		"paths": [
			{"path": "/1/2/3", "recursive": false},
			{"path": "/foo/bar", "recursive": true}
		],
		"management_policy_attrs": {
			"dedicated_GB": 5,
			"opportunistic_GB": 2.5,
			"max_num_objects": 100,
			"creation_time": 123,
			"expiration_time": 234,
			"deletion_time": 345
		}
	})";
	int rv = lotman_update_lot(non_existent_lot, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_NE(rv, 0);

	// Modify lot3: change owner, change parent from self to lot2, update paths
	const char *modified_lot = R"({
		"lot_name": "lot3",
		"owner": "not owner1",
		"parents": [{"current": "lot3", "new": "lot2"}],
		"paths": [
			{"current": "/another/path", "new": "/another/path", "recursive": true},
			{"current": "/123", "new": "/updated/path", "recursive": false}
		],
		"management_policy_attrs": {
			"dedicated_GB": 10.111,
			"opportunistic_GB": 6.6,
			"max_num_objects": 50,
			"expiration_time": 222,
			"deletion_time": 333
		}
	})";
	raw_err = nullptr;
	rv = lotman_update_lot(modified_lot, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Try to add a cycle --> this should fail
	const char *modified_lot2 = R"({
		"lot_name": "lot2",
		"parents": [{"current": "lot1", "new": "lot3"}]
	})";
	raw_err = nullptr;
	rv = lotman_update_lot(modified_lot2, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 0);

	char **raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_owners("lot3", false, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueStringList owner_out(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	bool check_old = false, check_new = false;
	for (int iter = 0; owner_out.get()[iter]; iter++) {
		if (strcmp(owner_out.get()[iter], "not owner1") == 0) {
			check_new = true;
		}
		if (strcmp(owner_out.get()[iter], "owner1") == 0) {
			check_old = true;
		}
	}
	ASSERT_TRUE(check_new & !check_old);

	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_parent_names("lot3", false, true, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueStringList parents_out(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	check_old = false, check_new = false;
	for (int iter = 0; parents_out.get()[iter]; iter++) {
		if (static_cast<std::string>(parents_out.get()[iter]) == "lot2") {
			check_new = true;
		}
		if (static_cast<std::string>(parents_out.get()[iter]) == "lot3") {
			check_old = true;
		}
	}
	ASSERT_TRUE(check_new & !check_old);

	const char *addition_JSON = R"({
		"lot_name": "lot3",
		"paths": [{"path": "/foo/barr", "recursive": true}],
		"parents": ["sep_node"]
	})";
	raw_err = nullptr;
	rv = lotman_add_to_lot(addition_JSON, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Try to add a cycle --> this should fail
	const char *addition_JSON2 = R"({
		"lot_name": "lot1",
		"parents": ["lot2"]
	})";
	raw_err = nullptr;
	rv = lotman_add_to_lot(addition_JSON2, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 0);

	// Test removing parents
	// Add default as parent to sep_node, then remove
	const char *addition_JSON3 = R"({
		"lot_name": "sep_node",
		"parents": ["default"]
	})";
	raw_err = nullptr;
	rv = lotman_add_to_lot(addition_JSON3, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Try (and hopefully fail) to remove all of the parents and orphan the lot
	const char *removal_JSON1 = R"({
		"lot_name": "sep_node",
		"parents": ["default", "sep_node", "non_existent_parent"]
	})";
	raw_err = nullptr;
	rv = lotman_rm_parents_from_lot(removal_JSON1, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 0);

	const char *removal_JSON2 = R"({
		"lot_name": "sep_node",
		"parents": ["default"]
	})";
	raw_err = nullptr;
	rv = lotman_rm_parents_from_lot(removal_JSON2, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Test adding paths to a few lots, then remove
	const char *addition_JSON4 = R"({
		"lot_name": "sep_node",
		"paths": [{"path": "/here/is/a/path", "recursive": true}]
	})";
	raw_err = nullptr;
	rv = lotman_add_to_lot(addition_JSON4, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	const char *addition_JSON5 = R"({
		"lot_name": "lot1",
		"paths": [{"path": "/here/is/another/path", "recursive": true}]
	})";
	raw_err = nullptr;
	rv = lotman_add_to_lot(addition_JSON5, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	const char *removal_JSON3 = R"({
		"paths": ["/here/is/a/path", "/path/does/not/exist", "/here/is/another/path"]
	})";
	raw_err = nullptr;
	rv = lotman_rm_paths_from_lots(removal_JSON3, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();
}

TEST_F(LotManTest, RemovePathsAdvancedTest) {
	setupStandardHierarchy();
	char *raw_err = nullptr;

	// Add multiple paths to lot1 (owned by owner1)
	const char *add_paths = R"({
		"lot_name": "lot1",
		"paths": [
			{"path": "/test/path1", "recursive": false},
			{"path": "/test/path2", "recursive": true},
			{"path": "/test/path3", "recursive": false}
		]
	})";
	int rv = lotman_add_to_lot(add_paths, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Verify paths were added (lot1 originally has 2 paths)
	char *raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lot_dirs("lot1", false, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueCString output(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	json paths_before = json::parse(output.get());
	ASSERT_EQ(paths_before.size(), 5); // 2 original + 3 new

	// Remove multiple paths from same lot
	const char *remove_multiple = R"({
		"paths": ["/test/path1", "/test/path3"]
	})";
	raw_err = nullptr;
	rv = lotman_rm_paths_from_lots(remove_multiple, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Verify paths were removed
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lot_dirs("lot1", false, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	output.reset(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	json paths_after = json::parse(output.get());
	ASSERT_EQ(paths_after.size(), 3); // 2 original + 1 remaining (/test/path2)

	// Verify the correct path remains
	bool found_path2 = false;
	bool found_path1 = false;
	bool found_path3 = false;
	for (const auto &path_obj : paths_after) {
		std::string path = path_obj["path"];
		if (path == "/test/path2/") {
			found_path2 = true;
		}
		if (path == "/test/path1/") {
			found_path1 = true;
		}
		if (path == "/test/path3/") {
			found_path3 = true;
		}
	}
	ASSERT_TRUE(found_path2);
	ASSERT_FALSE(found_path1);
	ASSERT_FALSE(found_path3);

	// Try to remove path from a lot we don't own (should fail)
	// First, change context to different owner
	raw_err = nullptr;
	rv = lotman_set_context_str("caller", "not_owner1", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	const char *unauthorized_remove = R"({
		"paths": ["/test/path2"]
	})";
	raw_err = nullptr;
	rv = lotman_rm_paths_from_lots(unauthorized_remove, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 0); // Should fail due to context check

	// Restore context
	raw_err = nullptr;
	rv = lotman_set_context_str("caller", "owner1", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Verify path was NOT removed (still 3 paths)
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lot_dirs("lot1", false, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	output.reset(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	json paths_final = json::parse(output.get());
	ASSERT_EQ(paths_final.size(), 3);

	// Test with duplicate paths in removal list (should succeed, removing path once)
	const char *duplicate_remove = R"({
		"paths": ["/test/path2", "/test/path2"]
	})";
	raw_err = nullptr;
	rv = lotman_rm_paths_from_lots(duplicate_remove, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Verify path2 is now gone
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lot_dirs("lot1", false, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	output.reset(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	json paths_dup_test = json::parse(output.get());
	ASSERT_EQ(paths_dup_test.size(), 2); // Back to original 2 paths
}

TEST_F(LotManTest, SetGetUsageTest) {
	// Set up fresh database with full hierarchy (already includes default lot and lot5)
	setupFullHierarchy();

	char *raw_err = nullptr;
	char *raw_output = nullptr;

	// Update/Get usage for a lot that doesn't exist
	bool deltaMode = false;
	const char *bad_usage_update_JSON = R"({
		"lot_name": "non_existent_lot",
		"self_GB": 10.5,
		"self_objects": 4,
		"self_GB_being_written": 2.2,
		"self_objects_being_written": 2
	})";
	int rv = lotman_update_lot_usage(bad_usage_update_JSON, deltaMode, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_NE(rv, 0);

	const char *bad_usage_query_JSON = R"({
		"lot_name": "non_existent_lot",
		"dedicated_GB": true,
		"opportunistic_GB": true,
		"total_GB": true
	})";
	raw_err = nullptr;
	rv = lotman_get_lot_usage(bad_usage_query_JSON, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 0);

	// Update by lot
	const char *usage1_update_JSON = R"({
		"lot_name": "lot4",
		"self_GB": 10.5,
		"self_objects": 4,
		"self_GB_being_written": 2.2,
		"self_objects_being_written": 2
	})";
	raw_err = nullptr;
	rv = lotman_update_lot_usage(usage1_update_JSON, deltaMode, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	const char *usage2_update_JSON = R"({
		"lot_name": "lot5",
		"self_GB": 3.5,
		"self_objects": 7,
		"self_GB_being_written": 1.2,
		"self_objects_being_written": 5
	})";
	raw_err = nullptr;
	rv = lotman_update_lot_usage(usage2_update_JSON, deltaMode, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	const char *usage_query_JSON = R"({
		"lot_name": "lot5",
		"dedicated_GB": true,
		"opportunistic_GB": true,
		"total_GB": true
	})";
	raw_err = nullptr;
	rv = lotman_get_lot_usage(usage_query_JSON, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueCString output(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	json json_out = json::parse(output.get());
	ASSERT_TRUE(json_out["dedicated_GB"]["children_contrib"] == 6.5 &&
				json_out["dedicated_GB"]["self_contrib"] == 3.5 && json_out["dedicated_GB"]["total"] == 10 &&
				json_out["opportunistic_GB"]["children_contrib"] == 3.5 &&
				json_out["opportunistic_GB"]["self_contrib"] == 0 && json_out["opportunistic_GB"]["total"] == 3.5 &&
				json_out["total_GB"]["children_contrib"] == 10.5 && json_out["total_GB"]["self_contrib"] == 3.5 &&
				json_out["total_GB"]["total"] == 14);

	// Update by dir -- This ends up updating default, lot1 and lot4
	const char *update_JSON_str = R"([
		{
			"includes_subdirs": true,
			"num_obj": 40,
			"path": "/1/2/3",
			"size_GB": 5.12,
			"subdirs": [
				{
					"includes_subdirs": true,
					"num_obj": 6,
					"path": "4",
					"size_GB": 3.14,
					"subdirs": [
						{"includes_subdirs": false, "num_obj": 0, "path": "5", "size_GB": 1.6, "subdirs": []}
					]
				},
				{"includes_subdirs": false, "num_obj": 0, "path": "5/6", "size_GB": 0.5, "subdirs": []},
				{"includes_subdirs": false, "num_obj": 0, "path": "6", "size_GB": 0.25, "subdirs": []}
			]
		},
		{
			"includes_subdirs": true,
			"num_obj": 6,
			"path": "foo/bar",
			"size_GB": 9.153,
			"subdirs": [
				{
					"includes_subdirs": true,
					"num_obj": 0,
					"path": "baz",
					"size_GB": 5.35,
					"subdirs": [
						{"includes_subdirs": false, "num_obj": 0, "path": "more_more_files", "size_GB": 2.2, "subdirs": []}
					]
				}
			]
		}
	])";
	raw_err = nullptr;
	rv = lotman_update_lot_usage_by_dir(update_JSON_str, deltaMode, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Check output for lot1
	const char *lot1_usage_query = R"({
		"lot_name": "lot1",
		"total_GB": false,
		"num_objects": false
	})";
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lot_usage(lot1_usage_query, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueCString lot1_output(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	json lot1_json_out = json::parse(lot1_output.get());
	ASSERT_TRUE(lot1_json_out["total_GB"]["self_contrib"] == 10.383 &&
				lot1_json_out["num_objects"]["self_contrib"] == 40);

	// Check output for lot4
	const char *lot4_usage_query = R"({
		"lot_name": "lot4",
		"total_GB": false,
		"num_objects": false
	})";
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lot_usage(lot4_usage_query, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueCString lot4_output(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	json lot4_json_out = json::parse(lot4_output.get());
	ASSERT_TRUE(lot4_json_out["total_GB"]["self_contrib"] == 3.14 && lot4_json_out["num_objects"]["self_contrib"] == 6);

	// Check output for default
	const char *default_usage_query = R"({
		"lot_name": "default",
		"total_GB": false,
		"num_objects": false
	})";
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lot_usage(default_usage_query, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueCString default_output(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	json default_json_out = json::parse(default_output.get());
	ASSERT_TRUE(default_json_out["total_GB"]["self_contrib"] == 0.75 &&
				default_json_out["num_objects"]["self_contrib"] == 0);

	// Update by dir in delta mode -- This ends up updating lot4
	deltaMode = true;
	const char *update2_JSON_str = R"([{
		"includes_subdirs": false,
		"num_obj": -3,
		"path": "/1/2/3/4",
		"size_GB": 2,
		"subdirs": []
	}])";
	raw_err = nullptr;
	rv = lotman_update_lot_usage_by_dir(update2_JSON_str, deltaMode, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Check output for lot4
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lot_usage(lot4_usage_query, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	lot4_output.reset(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	lot4_json_out = json::parse(lot4_output.get());
	ASSERT_TRUE(lot4_json_out["total_GB"]["self_contrib"] == 5.14 && lot4_json_out["num_objects"]["self_contrib"] == 3)
		<< lot4_json_out.dump();

	// Update by dir in delta mode, but attempt to make self_gb negative (should fail)
	const char *update3_JSON_str = R"([{
		"includes_subdirs": false,
		"num_obj": 0,
		"path": "/1/2/3/4",
		"size_GB": -10,
		"subdirs": []
	}])";
	raw_err = nullptr;
	rv = lotman_update_lot_usage_by_dir(update3_JSON_str, deltaMode, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 0) << err_msg.get();
}

TEST_F(LotManTest, GetOwnersTest) {
	// Set up fresh database with full hierarchy
	setupFullHierarchy();

	// Modify lot3 owner to "not owner1" for testing recursive owner lookup
	char *raw_err = nullptr;
	const char *modified_lot = R"({
		"lot_name": "lot3",
		"owner": "not owner1"
	})";
	int rv = lotman_update_lot(modified_lot, &raw_err);
	UniqueCString mod_err(raw_err);
	ASSERT_EQ(rv, 0) << mod_err.get();

	// Try with a lot that doesn't exist
	const char *non_existent_lot = "non_existent_lot";
	const bool recursive = true;
	char **raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_owners(non_existent_lot, recursive, &raw_output, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_NE(rv, 0);

	const char *lot_name = "lot4";
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_owners(lot_name, recursive, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueStringList output(raw_output);
	ASSERT_EQ(rv, 0);
	for (int iter = 0; output.get()[iter]; iter++) {
		ASSERT_TRUE(static_cast<std::string>(output.get()[iter]) == "owner1" ||
					static_cast<std::string>(output.get()[iter]) == "not owner1");
	}
}

TEST_F(LotManTest, GetParentsTest) {
	// Set up fresh database with full hierarchy
	setupFullHierarchy();

	// Modify lot3 to have lot2 as parent (instead of self) and add sep_node as parent
	char *raw_err = nullptr;
	const char *modified_lot = R"({
		"lot_name": "lot3",
		"parents": [{"current": "lot3", "new": "lot2"}]
	})";
	int rv = lotman_update_lot(modified_lot, &raw_err);
	UniqueCString mod_err(raw_err);
	ASSERT_EQ(rv, 0) << mod_err.get();

	raw_err = nullptr;
	const char *addition_JSON = R"({
		"lot_name": "lot3",
		"parents": ["sep_node"]
	})";
	rv = lotman_add_to_lot(addition_JSON, &raw_err);
	mod_err.reset(raw_err);
	ASSERT_EQ(rv, 0) << mod_err.get();

	char **raw_output = nullptr;
	raw_err = nullptr;
	const char *lot_name = "lot4";
	const bool recursive = true;
	const bool get_self = true;
	rv = lotman_get_parent_names(lot_name, recursive, get_self, &raw_output, &raw_err);
	UniqueCString err_msg(raw_err);
	UniqueStringList output(raw_output);
	ASSERT_EQ(rv, 0);
	for (int iter = 0; output.get()[iter]; iter++) {
		ASSERT_TRUE(static_cast<std::string>(output.get()[iter]) == "lot1" ||
					static_cast<std::string>(output.get()[iter]) == "lot2" ||
					static_cast<std::string>(output.get()[iter]) == "lot3" ||
					static_cast<std::string>(output.get()[iter]) == "lot5" ||
					static_cast<std::string>(output.get()[iter]) == "sep_node");
	}
}

TEST_F(LotManTest, GetChildrenTest) {
	// Set up fresh database with full hierarchy
	setupFullHierarchy();

	// Modify lot3 to have lot2 as parent (instead of self) so lot1 has lot3 as descendant
	char *raw_err = nullptr;
	const char *modified_lot = R"({
		"lot_name": "lot3",
		"parents": [{"current": "lot3", "new": "lot2"}]
	})";
	int rv = lotman_update_lot(modified_lot, &raw_err);
	UniqueCString mod_err(raw_err);
	ASSERT_EQ(rv, 0) << mod_err.get();

	// Try with a lot that doesn't exist
	const char *non_existent_lot = "non_existent_lot";
	const bool recursive = true;
	const bool get_self = false;
	char **raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_children_names(non_existent_lot, recursive, get_self, &raw_output, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_NE(rv, 0);

	// Now test that checks for good output
	const char *lot_name = "lot1";
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_children_names(lot_name, recursive, get_self, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueStringList output(raw_output);
	ASSERT_EQ(rv, 0);

	for (int iter = 0; output.get()[iter]; iter++) {
		ASSERT_TRUE(static_cast<std::string>(output.get()[iter]) == "lot2" ||
					static_cast<std::string>(output.get()[iter]) == "lot3" ||
					static_cast<std::string>(output.get()[iter]) == "lot4" ||
					static_cast<std::string>(output.get()[iter]) == "lot5");
	}
}

TEST_F(LotManTest, GetPolicyAttrs) {
	// Set up fresh database with full hierarchy
	setupFullHierarchy();

	// Modify lot3 to have lot2 as parent (instead of self) and add sep_node as parent
	// This matches the hierarchy state expected by this test
	char *raw_err = nullptr;
	const char *modified_lot = R"({
		"lot_name": "lot3",
		"parents": [{"current": "lot3", "new": "lot2"}]
	})";
	int rv = lotman_update_lot(modified_lot, &raw_err);
	UniqueCString mod_err(raw_err);
	ASSERT_EQ(rv, 0) << mod_err.get();

	raw_err = nullptr;
	const char *addition_JSON = R"({
		"lot_name": "lot3",
		"parents": ["sep_node"]
	})";
	rv = lotman_add_to_lot(addition_JSON, &raw_err);
	mod_err.reset(raw_err);
	ASSERT_EQ(rv, 0) << mod_err.get();

	// Try to get policy attributes for a lot that doesn't exist
	const char *bad_policy_attrs_JSON = R"({
		"lot_name": "non_existent_lot",
		"dedicated_GB": true,
		"opportunistic_GB": true,
		"max_num_objects": true,
		"creation_time": true,
		"expiration_time": true,
		"deletion_time": true
	})";
	char *raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_policy_attributes(bad_policy_attrs_JSON, &raw_output, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_NE(rv, 0);

	// Try with a key that doesn't exist
	const char *bad_policy_attrs_JSON2 = R"({
		"lot_name": "lot4",
		"bad_key": true,
		"opportunistic_GB": true,
		"max_num_objects": true,
		"creation_time": true,
		"expiration_time": true,
		"deletion_time": true,
		"non_existent_key": true
	})";
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_policy_attributes(bad_policy_attrs_JSON2, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 0);

	const char *policy_attrs_JSON_str = R"({
		"lot_name": "lot4",
		"dedicated_GB": true,
		"opportunistic_GB": true,
		"max_num_objects": true,
		"creation_time": true,
		"expiration_time": true,
		"deletion_time": true
	})";
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_policy_attributes(policy_attrs_JSON_str, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueCString output(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	json json_out = json::parse(output.get());
	ASSERT_TRUE(json_out["creation_time"]["lot_name"] == "lot5" && json_out["creation_time"]["value"] == 100 &&
				json_out["dedicated_GB"]["lot_name"] == "lot4" && json_out["dedicated_GB"]["value"] == 3.0 &&
				json_out["deletion_time"]["lot_name"] == "lot5" && json_out["deletion_time"]["value"] == 300 &&
				json_out["expiration_time"]["lot_name"] == "lot5" && json_out["expiration_time"]["value"] == 200 &&
				json_out["max_num_objects"]["lot_name"] == "sep_node" && json_out["max_num_objects"]["value"] == 10 &&
				json_out["opportunistic_GB"]["lot_name"] == "lot2" && json_out["opportunistic_GB"]["value"] == 1.5);
}

TEST_F(LotManTest, GetLotDirs) {
	// Set up fresh database with full hierarchy
	setupFullHierarchy();

	// Try to get a lot that doesn't exist
	const char *non_existent_lot = "non_existent_lot";
	const bool recursive = true;
	char *raw_output = nullptr;
	char *raw_err = nullptr;
	int rv = lotman_get_lot_dirs(non_existent_lot, recursive, &raw_output, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_NE(rv, 0);

	const char *lot_name = "lot5";
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lot_dirs(lot_name, recursive, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueCString output(raw_output);
	ASSERT_EQ(rv, 0);

	json json_out = json::parse(output.get());
	for (const auto &path_obj : json_out) {
		if (path_obj["path"] == "/1/2/3/4/" && path_obj["recursive"] == true && path_obj["lot_name"] == "lot4") {
			continue;
		} else if (path_obj["path"] == "/345/" && path_obj["recursive"] == true && path_obj["lot_name"] == "lot4") {
			continue;
		} else if (path_obj["path"] == "/456/" && path_obj["recursive"] == false && path_obj["lot_name"] == "lot5") {
			continue;
		} else if (path_obj["path"] == "/567/" && path_obj["recursive"] == true && path_obj["lot_name"] == "lot5") {
			continue;
		} else {
			ASSERT_TRUE(false) << "Unexpected path object: " << output.get();
		}
	}
}

TEST_F(LotManTest, ContextTest) {
	// Set up fresh database with full hierarchy
	setupFullHierarchy();

	// Any actions that modify the properties of a lot must have proper auth
	// These tests should all fail (context set to nonexistent owner)

	char *raw_err = nullptr;
	const char *lot6 = R"({
		"lot_name": "lot6",
		"owner": "owner1",
		"parents": ["lot5"],
		"paths": [],
		"management_policy_attrs": {
			"dedicated_GB": 3,
			"opportunistic_GB": 2.1,
			"max_num_objects": 40,
			"creation_time": 123,
			"expiration_time": 231,
			"deletion_time": 315
		}
	})";

	// Try to add a lot without correct context
	auto rv = lotman_set_context_str("caller", "notAnOwner", &raw_err);
	UniqueCString err_msg(raw_err);

	raw_err = nullptr;
	rv = lotman_add_lot(lot6, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 0) << err_msg.get();

	raw_err = nullptr;
	rv = lotman_lot_exists("lot6", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 1);

	// Try to remove a lot without correct context
	raw_err = nullptr;
	rv = lotman_remove_lots_recursive("lot1", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 0);

	raw_err = nullptr;
	rv = lotman_lot_exists("lot1", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 1);

	// Try to update a lot without correct context
	const char *modified_lot = R"({
		"lot_name": "lot3",
		"owner": "Bad Update"
	})";
	raw_err = nullptr;
	rv = lotman_update_lot(modified_lot, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 0);

	// Try to update lot usage without correct context
	const char *usage_update_JSON = R"({
		"lot_name": "lot5",
		"self_GB": 99
	})";
	raw_err = nullptr;
	rv = lotman_update_lot_usage(usage_update_JSON, false, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_NE(rv, 0);
}

TEST_F(LotManTest, LotsQueryTest) {
	// Set up fresh database with full hierarchy (already includes default lot)
	setupFullHierarchy();

	char *raw_err = nullptr;
	char **raw_output = nullptr;

	// Check for lots past expiration
	auto rv = lotman_get_lots_past_exp(true, &raw_output, &raw_err);
	UniqueCString err_msg(raw_err);
	UniqueStringList output(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	bool check = false;
	for (int iter = 0; output.get()[iter]; iter++) {
		if (strcmp(output.get()[iter], "sep_node") == 0) {
			check = true;
		}
	}
	ASSERT_FALSE(check);

	// Check for lots past deletion
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lots_past_del(true, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueStringList output2(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	check = false;
	for (int iter = 0; output2.get()[iter]; iter++) {
		if (strcmp(output2.get()[iter], "sep_node") == 0) {
			check = true;
		}
	}
	ASSERT_FALSE(check);

	// Check for lots past opportunistic storage limit
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lots_past_opp(true, true, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueStringList output3(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	check = false;
	for (int iter = 0; output3.get()[iter]; iter++) {
		if (strcmp(output3.get()[iter], "default") == 0) {
			check = true;
		}
	}
	ASSERT_FALSE(check);

	// Check for lots past dedicated storage limit
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lots_past_ded(true, true, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueStringList output4(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	check = false;
	for (int iter = 0; output4.get()[iter]; iter++) {
		if (strcmp(output4.get()[iter], "default") == 0) {
			check = true;
		}
	}
	ASSERT_FALSE(check);

	// Check for lots past object storage limit
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lots_past_obj(true, true, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueStringList output5(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	check = false;
	for (int iter = 0; output5.get()[iter]; iter++) {
		if (strcmp(output5.get()[iter], "default") == 0) {
			check = true;
		}
	}
	ASSERT_FALSE(check);
}

TEST_F(LotManTest, GetAllLotsTest) {
	// Set up fresh database with full hierarchy (7 lots: default, lot1-5, sep_node)
	setupFullHierarchy();

	char *raw_err = nullptr;
	char **raw_output = nullptr;
	auto rv = lotman_list_all_lots(&raw_output, &raw_err);
	UniqueCString err_msg(raw_err);
	UniqueStringList output(raw_output);
	int size = 0;
	for (size; output.get()[size]; ++size) {}
	ASSERT_EQ(size, 7);
}

TEST_F(LotManTest, GetLotJSONTest) {
	// Set up fresh database with full hierarchy
	setupFullHierarchy();

	// Set up the exact state expected by this test:
	// 1. Modify lot3: owner -> "not owner1", parent lot3 -> lot2, add sep_node as parent
	// 2. Update paths on lot3
	// 3. Add usage data to lot4 and lot5
	char *raw_err = nullptr;

	// Modify lot3
	const char *modified_lot = R"({
		"lot_name": "lot3",
		"owner": "not owner1",
		"parents": [{"current": "lot3", "new": "lot2"}],
		"paths": [
			{"current": "/another/path", "new": "/another/path", "recursive": true},
			{"current": "/123", "new": "/updated/path", "recursive": false}
		],
		"management_policy_attrs": {
			"dedicated_GB": 10.111,
			"opportunistic_GB": 6.6,
			"max_num_objects": 50,
			"expiration_time": 222,
			"deletion_time": 333
		}
	})";
	int rv = lotman_update_lot(modified_lot, &raw_err);
	UniqueCString setup_err(raw_err);
	ASSERT_EQ(rv, 0) << setup_err.get();

	// Add sep_node as parent and /foo/barr path to lot3
	const char *addition_JSON = R"({
		"lot_name": "lot3",
		"paths": [{"path": "/foo/barr", "recursive": true}],
		"parents": ["sep_node"]
	})";
	raw_err = nullptr;
	rv = lotman_add_to_lot(addition_JSON, &raw_err);
	setup_err.reset(raw_err);
	ASSERT_EQ(rv, 0) << setup_err.get();

	// Add usage data to lot4 and lot5
	const char *usage1_update_JSON = R"({
		"lot_name": "lot4",
		"self_GB": 10.5,
		"self_objects": 4,
		"self_GB_being_written": 2.2,
		"self_objects_being_written": 2
	})";
	raw_err = nullptr;
	rv = lotman_update_lot_usage(usage1_update_JSON, false, &raw_err);
	setup_err.reset(raw_err);
	ASSERT_EQ(rv, 0) << setup_err.get();

	const char *usage2_update_JSON = R"({
		"lot_name": "lot5",
		"self_GB": 3.5,
		"self_objects": 7,
		"self_GB_being_written": 1.2,
		"self_objects_being_written": 5
	})";
	raw_err = nullptr;
	rv = lotman_update_lot_usage(usage2_update_JSON, false, &raw_err);
	setup_err.reset(raw_err);
	ASSERT_EQ(rv, 0) << setup_err.get();

	// Update usage by dir to set lot4 usage to specific values
	const char *update_JSON_str = R"([{
		"includes_subdirs": true,
		"num_obj": 6,
		"path": "/1/2/3/4",
		"size_GB": 3.14,
		"subdirs": []
	}])";
	raw_err = nullptr;
	rv = lotman_update_lot_usage_by_dir(update_JSON_str, false, &raw_err);
	setup_err.reset(raw_err);
	ASSERT_EQ(rv, 0) << setup_err.get();

	// Delta update to get exact values expected
	const char *update2_JSON_str = R"([{
		"includes_subdirs": false,
		"num_obj": -3,
		"path": "/1/2/3/4",
		"size_GB": 2,
		"subdirs": []
	}])";
	raw_err = nullptr;
	rv = lotman_update_lot_usage_by_dir(update2_JSON_str, true, &raw_err);
	setup_err.reset(raw_err);
	ASSERT_EQ(rv, 0) << setup_err.get();

	// Try to get a lot that doesn't exist
	char *raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lot_as_json("non_existent_lot", true, &raw_output, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_NE(rv, 0);

	// Non-recursive test
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lot_as_json("lot3", false, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueCString output(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	json output_JSON = json::parse(output.get());
	json expected_output = R"({
		"children": ["lot5"],
		"lot_name": "lot3",
		"management_policy_attrs": {
			"creation_time": 123.0,
			"dedicated_GB": 10.111,
			"deletion_time": 333.0,
			"expiration_time": 222.0,
			"max_num_objects": 50.0,
			"opportunistic_GB": 6.6
		},
		"owner": "not owner1",
		"parents": [
			"lot2",
			"sep_node"
		],
		"paths": [
			{
				"lot_name": "lot3",
				"path": "/another/path/",
				"recursive": true
			},
			{
				"lot_name": "lot3",
				"path": "/updated/path/",
				"recursive": false
			},
			{
				"lot_name": "lot3",
				"path": "/foo/barr/",
				"recursive": true
			}
		],
		"usage": {
			"GB_being_written": { "self_contrib": 0.0 },
			"dedicated_GB": { "self_contrib": 0.0 },
			"num_objects": { "self_contrib": 0.0 },
			"objects_being_written": { "self_contrib": 0.0 },
			"opportunistic_GB": { "self_contrib": 0.0 },
			"total_GB": { "self_contrib": 0.0 }
		}
	})"_json;
	ASSERT_EQ(output_JSON, expected_output) << output_JSON;

	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_lot_as_json("lot3", true, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueCString output2(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	json output_JSON2 = json::parse(output2.get());
	json expected_output2 = R"({
		"children": [
			"lot4",
			"lot5"
		],
		"lot_name": "lot3",
		"management_policy_attrs": {
			"creation_time": 123.0,
			"dedicated_GB": 10.111,
			"deletion_time": 333.0,
			"expiration_time": 222.0,
			"max_num_objects": 50.0,
			"opportunistic_GB": 6.6
		},
		"owners": [
			"not owner1",
			"owner1"
		],
		"parents": [
			"lot1",
			"lot2",
			"sep_node"
		],
		"paths": [
			{
				"lot_name": "lot3",
				"path": "/another/path/",
				"recursive": true
			},
			{
				"lot_name": "lot3",
				"path": "/updated/path/",
				"recursive": false
			},
			{
				"lot_name": "lot3",
				"path": "/foo/barr/",
				"recursive": true
			},
			{
				"lot_name": "lot4",
				"path": "/1/2/3/4/",
				"recursive": true
			},
			{
				"lot_name": "lot4",
				"path": "/345/",
				"recursive": true
			},
			{
				"lot_name": "lot5",
				"path": "/456/",
				"recursive": false
			},
			{
				"lot_name": "lot5",
				"path": "/567/",
				"recursive": true
			}
		],
		"restrictive_management_policy_attrs": {
			"creation_time": {
				"lot_name": "lot3",
				"value": 123.0
			},
			"dedicated_GB": {
				"lot_name": "sep_node",
				"value": 3.0
			},
			"deletion_time": {
				"lot_name": "lot3",
				"value": 333.0
			},
			"expiration_time": {
				"lot_name": "lot3",
				"value": 222.0
			},
			"max_num_objects": {
				"lot_name": "sep_node",
				"value": 10.0
			},
			"opportunistic_GB": {
				"lot_name": "lot2",
				"value": 1.5
			}
		},
		"usage": {
			"GB_being_written": {
				"children_contrib": 3.4,
				"self_contrib": 0.0,
				"total": 3.4
			},
			"dedicated_GB": {
				"children_contrib": 8.64,
				"self_contrib": 0.0,
				"total": 8.64
			},
			"num_objects": {
				"children_contrib": 10.0,
				"self_contrib": 0.0,
				"total": 10.0
			},
			"objects_being_written": {
				"children_contrib": 7.0,
				"self_contrib": 0.0,
				"total": 7.0
			},
			"opportunistic_GB": {
				"children_contrib": 0.0,
				"self_contrib": 0.0,
				"total": 0.0
			},
			"total_GB": {
				"children_contrib": 8.64,
				"self_contrib": 0.0,
				"total": 8.64
			}
		}
	})"_json;
	ASSERT_EQ(output_JSON2, expected_output2) << output_JSON2;
}

TEST_F(LotManTest, LotsFromDirTest) {
	// Set up fresh database with full hierarchy
	setupFullHierarchy();

	// Modify lot3 to have lot2 as parent and add sep_node, plus add /foo/barr path
	char *raw_err = nullptr;
	const char *modified_lot = R"({
		"lot_name": "lot3",
		"parents": [{"current": "lot3", "new": "lot2"}]
	})";
	int rv = lotman_update_lot(modified_lot, &raw_err);
	UniqueCString setup_err(raw_err);
	ASSERT_EQ(rv, 0) << setup_err.get();

	raw_err = nullptr;
	const char *addition_JSON = R"({
		"lot_name": "lot3",
		"paths": [{"path": "/foo/barr", "recursive": true}],
		"parents": ["sep_node"]
	})";
	rv = lotman_add_to_lot(addition_JSON, &raw_err);
	setup_err.reset(raw_err);
	ASSERT_EQ(rv, 0) << setup_err.get();

	char **raw_output = nullptr;
	raw_err = nullptr;
	const char *dir = "/1/2/3/4"; // Get a path
	rv = lotman_get_lots_from_dir(dir, true, &raw_output, &raw_err);
	UniqueCString err_msg(raw_err);
	UniqueStringList output(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	for (int iter = 0; output.get()[iter]; iter++) {
		ASSERT_TRUE(static_cast<std::string>(output.get()[iter]) == "lot4" ||
					static_cast<std::string>(output.get()[iter]) == "lot1" ||
					static_cast<std::string>(output.get()[iter]) == "lot2" ||
					static_cast<std::string>(output.get()[iter]) == "lot3" ||
					static_cast<std::string>(output.get()[iter]) == "lot5" ||
					static_cast<std::string>(output.get()[iter]) == "sep_node");
	}

	raw_output = nullptr;
	raw_err = nullptr;
	const char *dir2 = "/foo/barr"; // Make sure parsing doesn't grab lot associated with /foo/bar
	rv = lotman_get_lots_from_dir(dir2, false, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueStringList output2(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	ASSERT_EQ(strcmp(output2.get()[0], "lot3"), 0);
}

TEST_F(LotManTest, GetVersionTest) {
	const char *version = lotman_version();
	std::string version_cpp(version);

	EXPECT_EQ(version_cpp, "v0.0.1");
}

TEST_F(LotManTest, IsRootTest) {
	// Set up a hierarchy: default (root), lot1 (root), lot2 (child of lot1), lot3 (root), lot4 (child)
	setupStandardHierarchy();

	char *raw_err = nullptr;

	// Test with non-existent lot
	auto rv = lotman_is_root("non_existent_lot", &raw_err);
	UniqueCString err_msg(raw_err);
	EXPECT_EQ(rv, -1) << "Expected error for non-existent lot";

	// "default" should be a root lot (self-parent)
	raw_err = nullptr;
	rv = lotman_is_root("default", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_GE(rv, 0) << err_msg.get();
	EXPECT_EQ(rv, 1) << "default should be a root lot";

	// "lot1" IS a root lot (self-parent)
	raw_err = nullptr;
	rv = lotman_is_root("lot1", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_GE(rv, 0) << err_msg.get();
	EXPECT_EQ(rv, 1) << "lot1 should be a root lot (self-parent)";

	// "lot2" should NOT be a root (has lot1 as parent)
	raw_err = nullptr;
	rv = lotman_is_root("lot2", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_GE(rv, 0) << err_msg.get();
	EXPECT_EQ(rv, 0) << "lot2 should not be a root lot";

	// "lot3" IS a root lot (self-parent)
	raw_err = nullptr;
	rv = lotman_is_root("lot3", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_GE(rv, 0) << err_msg.get();
	EXPECT_EQ(rv, 1) << "lot3 should be a root lot (self-parent)";

	// "lot4" should NOT be a root (has lot2 and lot3 as parents)
	raw_err = nullptr;
	rv = lotman_is_root("lot4", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_GE(rv, 0) << err_msg.get();
	EXPECT_EQ(rv, 0) << "lot4 should not be a root lot";
}

TEST_F(LotManTest, ContextStrTest) {
	char *raw_err = nullptr;
	char *raw_output = nullptr;

	// Test getting lot_home that was set in SetUp
	auto rv = lotman_get_context_str("lot_home", &raw_output, &raw_err);
	UniqueCString err_msg(raw_err);
	UniqueCString output(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	ASSERT_NE(output.get(), nullptr);
	// The lot_home should contain our temp directory path
	std::string lot_home(output.get());
	EXPECT_FALSE(lot_home.empty());

	// Test getting caller that was set in SetUp
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_context_str("caller", &raw_output, &raw_err);
	err_msg.reset(raw_err);
	output.reset(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	ASSERT_NE(output.get(), nullptr);
	EXPECT_STREQ(output.get(), "owner1");

	// Test setting and getting a new caller
	raw_err = nullptr;
	rv = lotman_set_context_str("caller", "new_owner", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_context_str("caller", &raw_output, &raw_err);
	err_msg.reset(raw_err);
	output.reset(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	ASSERT_NE(output.get(), nullptr);
	EXPECT_STREQ(output.get(), "new_owner");

	// Reset caller back to owner1 for any subsequent operations
	raw_err = nullptr;
	rv = lotman_set_context_str("caller", "owner1", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Test with invalid context key
	raw_output = nullptr;
	raw_err = nullptr;
	rv = lotman_get_context_str("invalid_key", &raw_output, &raw_err);
	err_msg.reset(raw_err);
	EXPECT_EQ(rv, -1) << "Expected error for invalid context key";
}

TEST_F(LotManTest, ContextIntTest) {
	char *raw_err = nullptr;
	int output = 0;

	// Test setting db_timeout
	auto rv = lotman_set_context_int("db_timeout", 5000, &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Test getting db_timeout
	raw_err = nullptr;
	rv = lotman_get_context_int("db_timeout", &output, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();
	EXPECT_EQ(output, 5000);

	// Test setting a different value
	raw_err = nullptr;
	rv = lotman_set_context_int("db_timeout", 10000, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	raw_err = nullptr;
	rv = lotman_get_context_int("db_timeout", &output, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();
	EXPECT_EQ(output, 10000);

	// Test with invalid context key
	raw_err = nullptr;
	rv = lotman_set_context_int("invalid_key", 100, &raw_err);
	err_msg.reset(raw_err);
	EXPECT_EQ(rv, -1) << "Expected error for invalid context key";

	raw_err = nullptr;
	rv = lotman_get_context_int("invalid_key", &output, &raw_err);
	err_msg.reset(raw_err);
	EXPECT_EQ(rv, -1) << "Expected error for invalid context key";
}

TEST_F(LotManTest, PathTrailingSlashNormalizationTest) {
	// This test verifies that paths input without trailing slashes are:
	// 1. Stored with trailing slashes in the database
	// 2. Can be looked up both with and without trailing slashes
	// 3. Are returned with trailing slashes in API output

	char *raw_err = nullptr;
	int rv = lotman_set_context_str("lot_home", tmp_dir.c_str(), &raw_err);
	UniqueCString err_msg(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	rv = lotman_set_context_str("caller", "owner1", &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Create default lot (required)
	const char *default_lot = R"({
		"lot_name": "default",
		"owner": "owner1",
		"parents": ["default"],
		"paths": [{"path": "/default", "recursive": true}],
		"management_policy_attrs": {
			"dedicated_GB": 100,
			"opportunistic_GB": 50,
			"max_num_objects": 1000,
			"creation_time": 1700000000,
			"expiration_time": 1800000000,
			"deletion_time": 1900000000
		}
	})";
	rv = lotman_add_lot(default_lot, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Create a lot with paths that do NOT have trailing slashes
	const char *test_lot = R"({
		"lot_name": "slash_test_lot",
		"owner": "owner1",
		"parents": ["slash_test_lot"],
		"paths": [
			{"path": "/no/slash/here", "recursive": true},
			{"path": "/another/path/no/slash", "recursive": false}
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
	rv = lotman_add_lot(test_lot, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Verify paths are returned WITH trailing slashes
	char *raw_output = nullptr;
	rv = lotman_get_lot_dirs("slash_test_lot", false, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	UniqueCString output(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();

	json paths_json = json::parse(output.get());
	ASSERT_EQ(paths_json.size(), 2);

	bool found_path1 = false;
	bool found_path2 = false;
	for (const auto &path_obj : paths_json) {
		std::string path = path_obj["path"];
		// All paths should end with /
		EXPECT_EQ(path.back(), '/') << "Path '" << path << "' should end with trailing slash";

		if (path == "/no/slash/here/") {
			found_path1 = true;
		}
		if (path == "/another/path/no/slash/") {
			found_path2 = true;
		}
	}
	EXPECT_TRUE(found_path1) << "Expected normalized path '/no/slash/here/'";
	EXPECT_TRUE(found_path2) << "Expected normalized path '/another/path/no/slash/'";

	// Verify lookup works WITHOUT trailing slash
	char **lots_output = nullptr;
	rv = lotman_get_lots_from_dir("/no/slash/here", false, &lots_output, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();
	ASSERT_NE(lots_output, nullptr);
	EXPECT_STREQ(lots_output[0], "slash_test_lot");
	lotman_free_string_list(lots_output);

	// Verify lookup works WITH trailing slash
	lots_output = nullptr;
	rv = lotman_get_lots_from_dir("/no/slash/here/", false, &lots_output, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();
	ASSERT_NE(lots_output, nullptr);
	EXPECT_STREQ(lots_output[0], "slash_test_lot");
	lotman_free_string_list(lots_output);

	// Verify subdirectory lookup works (for recursive path)
	lots_output = nullptr;
	rv = lotman_get_lots_from_dir("/no/slash/here/subdir/deeper", false, &lots_output, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();
	ASSERT_NE(lots_output, nullptr);
	EXPECT_STREQ(lots_output[0], "slash_test_lot");
	lotman_free_string_list(lots_output);

	// Verify path removal works without trailing slash
	const char *remove_path = R"({
		"paths": ["/another/path/no/slash"]
	})";
	rv = lotman_rm_paths_from_lots(remove_path, &raw_err);
	err_msg.reset(raw_err);
	ASSERT_EQ(rv, 0) << err_msg.get();

	// Verify only one path remains
	raw_output = nullptr;
	rv = lotman_get_lot_dirs("slash_test_lot", false, &raw_output, &raw_err);
	err_msg.reset(raw_err);
	output.reset(raw_output);
	ASSERT_EQ(rv, 0) << err_msg.get();
	paths_json = json::parse(output.get());
	ASSERT_EQ(paths_json.size(), 1);
	EXPECT_EQ(paths_json[0]["path"], "/no/slash/here/");
}
} // namespace

int main(int argc, char **argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
