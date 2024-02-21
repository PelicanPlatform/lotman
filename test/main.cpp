#include "../src/lotman.h"

#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>
#include <typeinfo>
#include <cstring>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// Test class to handle one-off setup and teardown for the entire test suite
class LotManTest : public ::testing::Test {
protected:
    static std::string tmp_dir;

    static std::string create_temp_directory() {
        // Generate a unique name for the temporary directory
        std::string temp_dir_template = "/tmp/lotman_test_XXXXXX";
        char temp_dir_name[temp_dir_template.size() + 1];  // +1 for the null-terminator
        std::strcpy(temp_dir_name, temp_dir_template.c_str());

        // mkdtemp replaces 'X's with a unique directory name and creates the directory
        char *mkdtemp_result = mkdtemp(temp_dir_name);
        if (mkdtemp_result == nullptr) {
            std::cerr << "Failed to create temporary directory\n";
            exit(1);
        }

        return std::string(mkdtemp_result);
    }

    static void SetUpTestSuite() {
        tmp_dir = create_temp_directory();
        std::cout << "Tests will use the following temp dir: " << tmp_dir << std::endl;
        char *err;
        auto rv = lotman_set_context_str("lot_home", tmp_dir.c_str(), &err);
    }

    static void TearDownTestSuite() {
        std::filesystem::remove_all(tmp_dir);
    }
};

// Don't forget to define the static member outside the class
std::string LotManTest::tmp_dir;

namespace {

    TEST_F(LotManTest, DefaultLotTests) {
        char *err1;
        const char *lot1 = "{\"lot_name\": \"lot1\", \"owner\": \"owner1\",  \"parents\": [\"lot1\"],\"paths\": [{\"path\":\"/1/2/3\", \"recursive\":false},{\"path\":\"/foo/bar\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        const char *default_lot = "{\"lot_name\": \"default\", \"owner\": \"owner2\",  \"parents\": [\"default\"],\"paths\": [{\"path\":\"/default/paths\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";

        auto rv = lotman_set_context_str("caller", "owner1", &err1);

        rv = lotman_add_lot(lot1, &err1);
        ASSERT_FALSE(!(rv != 0 && err1 != nullptr)) << err1;
        // There should be an err message, so we need to free
        free(err1);

        char *err2;
        rv = lotman_add_lot(default_lot, &err2);
        ASSERT_TRUE(rv == 0) << err2;

        rv = lotman_remove_lot("default", true, true, true, false, &err2);
        ASSERT_FALSE(rv == 0 && err2 == nullptr) << err2;
        // There should be an err message, so we need to free
        free(err2);
    }

    TEST_F(LotManTest, AddRemoveSublot) {
        char *err_msg;
        const char *lot1 = "{\"lot_name\": \"lot1\", \"owner\": \"owner1\",  \"parents\": [\"lot1\"],\"paths\": [{\"path\":\"1/2/3\", \"recursive\":false},{\"path\":\"/foo/bar\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";

        auto rv = lotman_add_lot(lot1, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;
    
        const char *deleted_lot = "lot1";
        rv = lotman_remove_lot(deleted_lot, false, false, false, false, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        rv = lotman_lot_exists(deleted_lot, &err_msg);
        ASSERT_FALSE(rv) << err_msg;

        // Try to remove a lot that doesn't exist
        const char *non_existent_lot = "non_existent_lot";
        rv = lotman_remove_lot(non_existent_lot, false, false, false, false, &err_msg);
        ASSERT_FALSE(rv == 0) << err_msg;
        free(err_msg);
    }

    TEST_F(LotManTest, AddInvalidLots) {
        char *err_msg;

        const char *lot1 = "{\"lot_name\": \"lot1\", \"owner\": \"owner1\",  \"parents\": [\"lot1\"],\"paths\": [{\"path\":\"/1/2/3\", \"recursive\":false},{\"path\":\"/foo/bar\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":20,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        const char *lot2 = "{\"lot_name\": \"lot2\",  \"owner\": \"owner1\",  \"parents\": [\"lot1\"],  \"paths\": [{\"path\":\"/1/2/4\", \"recursive\":true},{\"path\":\"/foo/baz\", \"recursive\":true}],  \"management_policy_attrs\": { \"dedicated_GB\":6,\"opportunistic_GB\":1.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":233,\"deletion_time\":355}}";
        const char *lot3 = "{ \"lot_name\": \"lot3\", \"owner\": \"owner1\",  \"parents\": [\"lot3\"],  \"paths\": [{\"path\":\"/another/path\", \"recursive\":false},{\"path\":\"/123\", \"recursive\":true}], \"management_policy_attrs\": { \"dedicated_GB\":3,\"opportunistic_GB\":2.0,\"max_num_objects\":60,\"creation_time\":123,\"expiration_time\":232,\"deletion_time\":325}}";
        const char *lot4 = "{ \"lot_name\": \"lot4\", \"owner\": \"owner1\", \"parents\": [\"lot2\",\"lot3\"], \"paths\": [{\"path\":\"/1/2/3/4\", \"recursive\":true},{\"path\":\"/345\", \"recursive\":true}], \"management_policy_attrs\": { \"dedicated_GB\":3,\"opportunistic_GB\":2.1,\"max_num_objects\":40,\"creation_time\":123,\"expiration_time\":231,\"deletion_time\":315}}";
        const char *sep_node = "{ \"lot_name\": \"sep_node\", \"owner\": \"owner1\", \"parents\": [\"sep_node\"], \"paths\": [{\"path\":\"/sep/node\", \"recursive\":true}], \"management_policy_attrs\": { \"dedicated_GB\":3,\"opportunistic_GB\":2.1,\"max_num_objects\":10,\"creation_time\":123,\"expiration_time\":99679525853643,\"deletion_time\":9267952553643}}";

        auto rv = lotman_add_lot(lot1, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        rv = lotman_add_lot(lot2, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;
  
        rv = lotman_add_lot(lot3, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        rv = lotman_add_lot(lot4, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        rv = lotman_add_lot(sep_node, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        // Try to add a lot with cyclic dependency
        // Cycle created by trying to add lot5 with lot1 as a child
        const char *cyclic_lot = "{\"lot_name\": \"lot5\",\"owner\": \"owner1\",\"parents\": [\"lot4\"],\"children\": [\"lot1\"],\"paths\": [{\"path\":\"/456\", \"recursive\":false},{\"path\":\"/567\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        rv = lotman_add_lot(cyclic_lot, &err_msg);
        ASSERT_FALSE(rv == 0) << err_msg;
        // There should be an err message, so we need to free
        free(err_msg);

        // Try to add lot with no parent
        char *err_msg2;
        const char *no_parents_lot = "{\"lot_name\": \"lot5\",\"owner\": \"owner1\",\"parents\": [],\"children\": [\"lot1\"],\"paths\": [{\"path\":\"/456\", \"recursive\":false},{\"path\":\"/567\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":111111,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        rv = lotman_add_lot(no_parents_lot, &err_msg2);
        ASSERT_FALSE(rv == 0) << err_msg;
        // There should be an err message, so we need to free
        free(err_msg2);
    }

    TEST_F(LotManTest, InsertionTest) {
        char *err_msg;
        const char *lot5 = "{\"lot_name\": \"lot5\",\"owner\":\"owner1\",\"parents\": [\"lot3\"],\"children\": [\"lot4\"],\"paths\": [{\"path\":\"/456\", \"recursive\":false},{\"path\":\"/567\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":10,\"opportunistic_GB\":3.5,\"max_num_objects\":20,\"creation_time\":100,\"expiration_time\":200,\"deletion_time\":300}}";
        int rv = lotman_add_lot(lot5, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        // Check that lot5 is a parent to lot4 and that lot3 is a parent to lot5
        char **output;
        rv = lotman_get_parent_names("lot4", false, false, &output, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        bool check = false;

        for (int iter = 0; output[iter]; iter++) {
            if (static_cast<std::string>(output[iter]) == "lot5") {
                check = true;
            }
        }

        ASSERT_TRUE(check);
        lotman_free_string_list(output);

        check = false;
        char **output2;
        rv = lotman_get_children_names("lot3", false, false, &output2, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        for (int iter = 0; output2[iter]; iter++) {
            if (static_cast<std::string>(output2[iter]) == "lot5") {
                check = true;
            }
        }
        ASSERT_TRUE(check);
        lotman_free_string_list(output2);
    }

    TEST_F(LotManTest, ModifyLotTest) {
        // Try to modify a non-existent log
        char *err_msg;
        const char *non_existent_lot = "{ \"lot_name\": \"non_existent_lot\", \"owner\": \"owner1\",  \"parents\": [\"lot1\"],  \"paths\": [{\"path\":\"/1/2/3\", \"recursive\":false},{\"path\":\"/foo/bar\", \"recursive\":true}], \"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        int rv = lotman_update_lot(non_existent_lot, &err_msg);
        ASSERT_FALSE(rv == 0);
        free(err_msg);

        
        char *err_msg2;
        const char *modified_lot = "{ \"lot_name\": \"lot3\", \"owner\": \"not owner1\",  \"parents\": [{\"current\":\"lot3\", \"new\":\"lot2\"}],  \"paths\": [{\"current\": \"/another/path\", \"new\": \"/another/path\", \"recursive\": true},{\"current\":\"/123\", \"new\" : \"/updated/path\", \"recursive\" : false}], \"management_policy_attrs\": { \"dedicated_GB\":10.111,\"opportunistic_GB\":6.6,\"max_num_objects\":50,\"expiration_time\":222,\"deletion_time\":333}}";
        rv = lotman_update_lot(modified_lot, &err_msg2);
        ASSERT_TRUE(rv == 0) << err_msg2;

        // Try to add a cycle --> this should fail
        const char *modified_lot2 = "{ \"lot_name\": \"lot2\", \"parents\": [{\"current\":\"lot1\", \"new\":\"lot3\"}]}";
        rv = lotman_update_lot(modified_lot2, &err_msg2);
        ASSERT_FALSE(rv == 0);
        free(err_msg2);

        char *err_msg3;
        char **owner_out;
        rv = lotman_get_owners("lot3", false, &owner_out, &err_msg3);
        ASSERT_TRUE(rv == 0) << err_msg3;
        
        bool check_old = false, check_new =  false;
        for (int iter = 0; owner_out[iter]; iter++) {
            if (strcmp(owner_out[iter], "not owner1") == 0) {
                check_new = true;
            }
            if (strcmp(owner_out[iter], "owner1") == 0) {
                check_old = true;
            }
        }
        ASSERT_TRUE(check_new & !check_old);
        lotman_free_string_list(owner_out);

        char **parents_out;
        rv = lotman_get_parent_names("lot3", false, true, &parents_out, &err_msg3);
        ASSERT_TRUE(rv == 0) << err_msg3;

        check_old = false, check_new = false;
        for (int iter = 0; parents_out[iter]; iter++) {
            if (static_cast<std::string>(parents_out[iter]) == "lot2") {
                check_new = true;
            }
            if (static_cast<std::string>(parents_out[iter]) == "lot3") {
                check_old = true;
            }
        }
        ASSERT_TRUE(check_new & !check_old);
        lotman_free_string_list(parents_out);

        const char *addition_JSON = "{\"lot_name\":\"lot3\", \"paths\": [{\"path\":\"/foo/barr\", \"recursive\": true}], \"parents\" : [\"sep_node\"]}";
        rv = lotman_add_to_lot(addition_JSON, &err_msg3);
        ASSERT_TRUE(rv == 0) << err_msg3;

        // Try to add a cycle --> this should fail
        const char *addition_JSON2 = "{ \"lot_name\": \"lot1\", \"parents\": [\"lot2\"]}";
        rv = lotman_add_to_lot(addition_JSON2, &err_msg3);
        ASSERT_FALSE(rv == 0);
        free(err_msg3);

        // Test removing parents
        // Add default as parent to sep_node, then remove
        char *err_msg4;
        const char *addition_JSON3 = "{\"lot_name\":\"sep_node\", \"parents\": [\"default\"]}";
        rv = lotman_add_to_lot(addition_JSON3, &err_msg4);
        ASSERT_TRUE(rv == 0) << err_msg4;

        // Try (and hopefully fail) to remove all of the parents and orphan the lot
        const char *removal_JSON1 = "{\"lot_name\":\"sep_node\", \"parents\":[\"default\", \"sep_node\", \"non_existent_parent\"]}";
        rv = lotman_rm_parents_from_lot(removal_JSON1, &err_msg4);
        ASSERT_FALSE(rv == 0);
        free(err_msg4);

        char *err_msg5;
        const char *removal_JSON2 = "{\"lot_name\":\"sep_node\", \"parents\":[\"default\"]}";
        rv = lotman_rm_parents_from_lot(removal_JSON2, &err_msg5);
        ASSERT_TRUE(rv == 0) << err_msg5;

        // Test adding paths to a few lots, then remove
        const char *addition_JSON4 = "{\"lot_name\":\"sep_node\", \"paths\":[{\"path\":\"/here/is/a/path\", \"recursive\": true}]}";
        rv = lotman_add_to_lot(addition_JSON4, &err_msg5);
        ASSERT_TRUE(rv == 0) << err_msg5;

        const char *addition_JSON5 = "{\"lot_name\":\"lot1\", \"paths\":[{\"path\":\"/here/is/another/path\", \"recursive\": true}]}";
        rv = lotman_add_to_lot(addition_JSON5, &err_msg5);
        ASSERT_TRUE(rv == 0) << err_msg5;

        const char *removal_JSON3 = "{\"paths\":[\"/here/is/a/path\", \"/path/does/not/exist\", \"/here/is/another/path\"]}";
        rv = lotman_rm_paths_from_lots(removal_JSON3, &err_msg5);
        ASSERT_TRUE(rv == 0) << err_msg5;
    }

    TEST_F(LotManTest, SetGetUsageTest) {
        // Update/Get usage for a lot that doesn't exist
        char *err_msg;
        const char *non_existent_lot = "non_existent_lot";
        bool deltaMode = false;
        const char *bad_usage_update_JSON = "{\"lot_name\":\"non_existent_lot\", \"self_GB\":10.5, \"self_objects\":4, \"self_GB_being_written\":2.2, \"self_objects_being_written\":2}";
        int rv = lotman_update_lot_usage(bad_usage_update_JSON, deltaMode, &err_msg);
        ASSERT_FALSE(rv == 0);
        free(err_msg);

        char *err_msg2;
        const char *bad_usage_query_JSON = "{\"lot_name\":\"non_existent_lot\", \"dedicated_GB\": true, \"opportunistic_GB\": true, \"total_GB\": true}";
        char *output;
        rv = lotman_get_lot_usage(bad_usage_query_JSON, &output, &err_msg2);
        ASSERT_FALSE(rv == 0);
        free(err_msg2);

        // Update by lot
        char *err_msg3;
        const char *usage1_update_JSON = "{\"lot_name\":\"lot4\", \"self_GB\":10.5, \"self_objects\":4, \"self_GB_being_written\":2.2, \"self_objects_being_written\":2}";
        rv = lotman_update_lot_usage(usage1_update_JSON, deltaMode, &err_msg3);
        ASSERT_TRUE(rv == 0) << err_msg3;

        const char *usage2_update_JSON = "{\"lot_name\":\"lot5\",\"self_GB\":3.5, \"self_objects\":7, \"self_GB_being_written\":1.2, \"self_objects_being_written\":5}";
        rv = lotman_update_lot_usage(usage2_update_JSON, deltaMode, &err_msg3);
        ASSERT_TRUE(rv == 0) << err_msg3;

        const char *usage_query_JSON = "{\"lot_name\":\"lot5\", \"dedicated_GB\": true, \"opportunistic_GB\": true, \"total_GB\": true}";
        rv = lotman_get_lot_usage(usage_query_JSON, &output, &err_msg3);
        ASSERT_TRUE(rv == 0) << err_msg3;

        json json_out = json::parse(output);
        ASSERT_TRUE(json_out["dedicated_GB"]["children_contrib"] == 6.5 && json_out["dedicated_GB"]["self_contrib"] == 3.5 && json_out["dedicated_GB"]["total"] == 10 &&
                    json_out["opportunistic_GB"]["children_contrib"] == 3.5 && json_out["opportunistic_GB"]["self_contrib"] == 0 && json_out["opportunistic_GB"]["total"] == 3.5 &&
                    json_out["total_GB"]["children_contrib"] == 10.5 && json_out["total_GB"]["self_contrib"] == 3.5);

        free(output);

        // Update by dir -- This ends up updating default, lot1 and lot4
        const char *update_JSON_str = "[{\"includes_subdirs\": true,\"num_obj\": 40,\"path\": \"/1/2/3\",\"size_GB\": 5.12,\"subdirs\": [{\"includes_subdirs\": true,\"num_obj\": 6,\"path\": \"4\",\"size_GB\": 3.14,\"subdirs\": [{\"includes_subdirs\": false,\"num_obj\": 0,\"path\": \"5\",\"size_GB\": 1.6,\"subdirs\": []}]},{\"includes_subdirs\": false,\"num_obj\": 0,\"path\": \"5/6\",\"size_GB\": 0.5,\"subdirs\": []},{\"includes_subdirs\": false,\"num_obj\": 0,\"path\": \"6\",\"size_GB\": 0.25,\"subdirs\": []}]},{\"includes_subdirs\": true,\"num_obj\": 6,\"path\": \"foo/bar\",\"size_GB\": 9.153,\"subdirs\": [{\"includes_subdirs\": true,\"num_obj\": 0,\"path\": \"baz\",\"size_GB\": 5.35,\"subdirs\": [{\"includes_subdirs\": false,\"num_obj\": 0,\"path\": \"more_more_files\",\"size_GB\": 2.2,\"subdirs\": []}]}]}]";
        rv = lotman_update_lot_usage_by_dir(update_JSON_str, deltaMode, &err_msg3);

        ASSERT_TRUE(rv == 0) << err_msg3;

        // Check output for lot1
        const char *lot1_usage_query = "{\"lot_name\": \"lot1\", \"total_GB\":false, \"num_objects\":false}";
        char *lot1_output;
        rv = lotman_get_lot_usage(lot1_usage_query, &lot1_output, &err_msg3);
        ASSERT_TRUE(rv == 0) << err_msg3;
        json lot1_json_out = json::parse(lot1_output);
        free(lot1_output);
        ASSERT_TRUE(lot1_json_out["total_GB"]["total"] == 10.383 && lot1_json_out["num_objects"]["total"] == 40);

        // Check output for lot4
        const char *lot4_usage_query = "{\"lot_name\": \"lot4\", \"total_GB\":false, \"num_objects\":false}";
        char *lot4_output;
        rv = lotman_get_lot_usage(lot4_usage_query, &lot4_output, &err_msg3);
        ASSERT_TRUE(rv == 0) << err_msg3;
        json lot4_json_out = json::parse(lot4_output);
        free(lot4_output);
        ASSERT_TRUE(lot4_json_out["total_GB"]["total"] == 3.14 && lot4_json_out["num_objects"]["total"] == 6);
        
        // Check output for default
        const char *default_usage_query = "{\"lot_name\": \"default\", \"total_GB\":false, \"num_objects\":false}";
        char *default_output;
        rv = lotman_get_lot_usage(default_usage_query, &default_output, &err_msg3);
        ASSERT_TRUE(rv == 0) << err_msg;
        json default_json_out = json::parse(default_output);
        free(default_output);
        ASSERT_TRUE(default_json_out["total_GB"]["total"] == 0.75 && default_json_out["num_objects"]["total"] == 0);

        // Update by dir in delta mode -- This ends up updating lot4
        deltaMode = true;
        const char *update2_JSON_str = "[{\"includes_subdirs\": false,\"num_obj\": -3,\"path\": \"/1/2/3/4\",\"size_GB\": 2,\"subdirs\": []}]";
        rv = lotman_update_lot_usage_by_dir(update2_JSON_str, deltaMode, &err_msg3);

        ASSERT_TRUE(rv == 0) << err_msg3;

        // Check output for lot4
        rv = lotman_get_lot_usage(lot4_usage_query, &lot4_output, &err_msg3);
        ASSERT_TRUE(rv == 0) << err_msg3;
        lot4_json_out = json::parse(lot4_output);
        free(lot4_output);
        ASSERT_TRUE(lot4_json_out["total_GB"]["total"] == 5.14 && lot4_json_out["num_objects"]["total"] == 3) << lot4_json_out.dump();

        // Update by dir in delta mode, but attempt to make self_gb negative (should fail)
        const char *update3_JSON_str = "[{\"includes_subdirs\": false,\"num_obj\": 0,\"path\": \"/1/2/3/4\",\"size_GB\": -10,\"subdirs\": []}]";
        rv = lotman_update_lot_usage_by_dir(update3_JSON_str, deltaMode, &err_msg3);
        ASSERT_FALSE(rv == 0) << err_msg3;
        free(err_msg3);
}

    TEST_F(LotManTest, GetOwnersTest) {
        // Try with a lot that doesn't exist
        char *err_msg;
        const char *non_existent_lot = "non_existent_lot";
        const bool recursive = true;
        char **output;
        int rv = lotman_get_owners(non_existent_lot, recursive, &output, &err_msg);
        ASSERT_FALSE(rv == 0);
        free(err_msg);

        char *err_msg2;
        const char *lot_name = "lot4";
        rv = lotman_get_owners(lot_name, recursive, &output, &err_msg2);
        ASSERT_TRUE(rv == 0);
        for (int iter = 0; output[iter]; iter++) {
            ASSERT_TRUE(static_cast<std::string>(output[iter]) == "owner1" || static_cast<std::string>(output[iter]) == "not owner1");
        }
        lotman_free_string_list(output);
    }

    TEST_F(LotManTest, GetParentsTest) {
        char *err_msg;
        const char *lot_name = "lot4";
        const bool recursive = true;
        const bool get_self = true;
        char **output;
        int rv = lotman_get_parent_names(lot_name, recursive, get_self, &output, &err_msg);
        ASSERT_TRUE(rv == 0);
        for (int iter = 0; output[iter]; iter++) {
            ASSERT_TRUE(static_cast<std::string>(output[iter]) == "lot1" || static_cast<std::string>(output[iter]) == "lot2" ||
                        static_cast<std::string>(output[iter]) == "lot3" || static_cast<std::string>(output[iter]) == "lot5" ||
                        static_cast<std::string>(output[iter]) == "sep_node");
        }
        lotman_free_string_list(output);
    }

    TEST_F(LotManTest, GetChildrenTest) {
        // Try with a lot that doesn't exist
        char *err_msg;
        const char *non_existent_lot = "non_existent_lot";
        const bool recursive = true;
        const bool get_self = false;
        char **output;
        int rv = lotman_get_children_names(non_existent_lot, recursive, get_self, &output, &err_msg);
        ASSERT_FALSE(rv == 0);
        free(err_msg);

        // Now test that checks for good output
        char *err_msg2;
        const char *lot_name = "lot1";
        rv = lotman_get_children_names(lot_name, recursive, get_self, &output, &err_msg2);
        ASSERT_TRUE(rv == 0);

        for (int iter = 0; output[iter]; iter++) {
            ASSERT_TRUE(static_cast<std::string>(output[iter]) == "lot2" || static_cast<std::string>(output[iter]) == "lot3" ||
                        static_cast<std::string>(output[iter]) == "lot4" || static_cast<std::string>(output[iter]) == "lot5");
        }
        lotman_free_string_list(output);
    }

    TEST_F(LotManTest, GetPolicyAttrs) {
        // Try to get policy attributes for a lot that doesn't exist
        char *err_msg;
        const char *bad_policy_attrs_JSON = "{\"lot_name\":\"non_existent_lot\", \"dedicated_GB\":true, \"opportunistic_GB\":true, \"max_num_objects\":true, \"creation_time\":true, \"expiration_time\":true, \"deletion_time\":true}";
        char *output;
        int rv = lotman_get_policy_attributes(bad_policy_attrs_JSON, &output, &err_msg);
        ASSERT_FALSE(rv == 0);
        free(err_msg);

        // Try with a key that doesn't exist
        char *err_msg2;
        const char *bad_policy_attrs_JSON2 = "{\"lot_name\":\"lot4\", \"bad_key\":true, \"opportunistic_GB\":true, \"max_num_objects\":true, \"creation_time\":true, \"expiration_time\":true, \"deletion_time\":true, \"non_existent_key\":true}";
        rv = lotman_get_policy_attributes(bad_policy_attrs_JSON2, &output, &err_msg2);
        ASSERT_FALSE(rv == 0);
        free(err_msg2);

        char *err_msg3;
        const char *policy_attrs_JSON_str = "{\"lot_name\":\"lot4\", \"dedicated_GB\":true, \"opportunistic_GB\":true, \"max_num_objects\":true, \"creation_time\":true, \"expiration_time\":true, \"deletion_time\":true}";
        rv = lotman_get_policy_attributes(policy_attrs_JSON_str, &output, &err_msg3);
        ASSERT_TRUE(rv == 0) << err_msg;

        json json_out = json::parse(output);
        ASSERT_TRUE(json_out["creation_time"]["lot_name"] == "lot5" && json_out["creation_time"]["value"] == 100 &&
                    json_out["dedicated_GB"]["lot_name"] == "lot4" && json_out["dedicated_GB"]["value"] == 3.0 &&
                    json_out["deletion_time"]["lot_name"] == "lot5" && json_out["deletion_time"]["value"] == 300 &&
                    json_out["expiration_time"]["lot_name"] == "lot5" && json_out["expiration_time"]["value"] == 200 &&
                    json_out["max_num_objects"]["lot_name"] == "sep_node" && json_out["max_num_objects"]["value"] == 10 &&
                    json_out["opportunistic_GB"]["lot_name"] == "lot2" && json_out["opportunistic_GB"]["value"] == 1.5);
        free(output);
    }

    TEST_F(LotManTest, GetLotDirs) {
        // Try to get a lot that doesn't exist
        char *err_msg;
        const char *non_existent_lot = "non_existent_lot";
        const bool recursive = true;
        char *output;
        int rv = lotman_get_lot_dirs(non_existent_lot, recursive, &output, &err_msg);
        ASSERT_FALSE(rv == 0);
        free(err_msg);

        char *err_msg2;
        const char *lot_name = "lot5";
        rv = lotman_get_lot_dirs(lot_name, recursive, &output, &err_msg2);
        ASSERT_TRUE(rv == 0);

        json json_out = json::parse(output);
        ASSERT_TRUE(json_out["/1/2/3/4"]["lot_name"] == "lot4" && json_out["/1/2/3/4"]["recursive"] == true &&
                    json_out["/345"]["lot_name"] == "lot4" && json_out["/345"]["recursive"] == true &&
                    json_out["/456"]["lot_name"] == "lot5" && json_out["/456"]["recursive"] == false &&
                    json_out["/567"]["lot_name"] == "lot5" && json_out["/567"]["recursive"] == true);
        free(output);
    }

    TEST_F(LotManTest, ContextTest) {
        // Any actions that modify the properties of a lot must have proper auth
        // These tests should all fail (context set to nonexistant owner)
        
        char *err_msg1;
        const char *lot6 = "{ \"lot_name\": \"lot6\", \"owner\": \"owner1\", \"parents\": [\"lot5\"], \"paths\": [], \"management_policy_attrs\": { \"dedicated_GB\":3,\"opportunistic_GB\":2.1,\"max_num_objects\":40,\"creation_time\":123,\"expiration_time\":231,\"deletion_time\":315}}";

        // Try to add a lot without correct context
        auto rv = lotman_set_context_str("caller", "notAnOwner", &err_msg1);
        rv = lotman_add_lot(lot6, &err_msg1);
        ASSERT_FALSE(rv == 0) << err_msg1;
        free(err_msg1);

        char *err_msg2;
        rv = lotman_lot_exists("lot6", &err_msg2);
        ASSERT_FALSE(rv == 1);

        // Try to remove a lot without correct context
        rv = lotman_remove_lots_recursive("lot1", &err_msg2);
        ASSERT_FALSE(rv == 0);
        free(err_msg2);

        char *err_msg3;
        rv = lotman_lot_exists("lot1", &err_msg3);
        ASSERT_TRUE(rv == 1);

        // Try to update a lot without correct context
        const char *modified_lot = "{ \"lot_name\": \"lot3\", \"owner\": \"Bad Update\"}";
        rv = lotman_update_lot(modified_lot, &err_msg3);
        ASSERT_FALSE(rv == 0);
        free(err_msg3);

        // Try to update lot usage without correct context
        char *err_msg4;
        const char *usage_update_JSON = "{\"lot_name\":\"lot5\",\"self_GB\":99}";
        rv = lotman_update_lot_usage(usage_update_JSON, false, &err_msg4);
        ASSERT_FALSE(rv == 0);
        free(err_msg4);
    }

    TEST_F(LotManTest, LotsQueryTest) {
        char *err_msg;
        char **output;
        // Check for lots past expiration
        auto rv = lotman_get_lots_past_exp(true, &output, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        bool check = false;
        for (int iter = 0; output[iter]; iter++) {
            if (strcmp(output[iter], "sep_node") == 0) {
                check = true;
            }
        }
        ASSERT_FALSE(check);
        lotman_free_string_list(output);

        // Check for lots past deletion
        char **output2;
        rv = lotman_get_lots_past_del(true, &output2, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;
        
        check = false;
        for (int iter = 0; output2[iter]; iter++) {
            if (strcmp(output2[iter], "sep_node") == 0) {
                check = true;
            }
        }
        ASSERT_FALSE(check);
        lotman_free_string_list(output2);

        // Check for lots past opportunistic storage limit
        char **output3;
        rv = lotman_get_lots_past_opp(true, true, &output3, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;
        
        check = false;
        for (int iter = 0; output3[iter]; iter++) {
            if (strcmp(output3[iter], "default") == 0) {
                check = true;
            }
        }
        ASSERT_FALSE(check);
        lotman_free_string_list(output3);

        // Check for lots past dedicated storage limit
        char **output4;
        rv = lotman_get_lots_past_ded(true, true, &output4, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;
        
        check = false;
        for (int iter = 0; output4[iter]; iter++) {
            if (strcmp(output4[iter], "default") == 0) {
                check = true;
            }
        }
        ASSERT_FALSE(check);
        lotman_free_string_list(output4);

        // Check for lots past object storage limit
        char **output5;
        rv = lotman_get_lots_past_obj(true, true, &output5, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;
        
        check = false;
        for (int iter = 0; output5[iter]; iter++) {
            if (strcmp(output5[iter], "default") == 0) {
                check = true;
            }
        }
        ASSERT_FALSE(check);
        lotman_free_string_list(output5);
    }

    TEST_F(LotManTest, GetAllLotsTest) {
        char *err_msg;
        char **output;
        auto rv = lotman_list_all_lots(&output, &err_msg);
        int size = 0;
        for (size; output[size]; ++size) {}
        ASSERT_TRUE(size == 7);
        lotman_free_string_list(output);
    }

    TEST_F(LotManTest, GetLotJSONTest) {
        // Try to get a lot that doesn't exist
        char *err_msg;
        char *output;
        auto rv = lotman_get_lot_as_json("non_existent_lot", true, &output, &err_msg);
        ASSERT_FALSE(rv == 0);
        free(err_msg);

        char *err_msg2;
        rv = lotman_get_lot_as_json("lot3", true, &output, &err_msg);
        ASSERT_TRUE(rv == 0);
        json output_JSON = json::parse(output);
        free(output);
        json expected_output = R"({"children":["lot4","lot5"],"lot_name":"lot3","management_policy_attrs":{"creation_time":{"lot_name":"lot3","value":123.0},"dedicated_GB":{"lot_name":"sep_node","value":3.0},"deletion_time":{"lot_name":"lot3","value":333.0},"expiration_time":{"lot_name":"lot3","value":222.0},"max_num_objects":{"lot_name":"sep_node","value":10.0},"opportunistic_GB":{"lot_name":"lot2","value":1.5}},"owners":["not owner1","owner1"],"parents":["lot1","lot2","sep_node"],"paths":{"/1/2/3/4":{"lot_name":"lot4","recursive":true},"/345":{"lot_name":"lot4","recursive":true},"/456":{"lot_name":"lot5","recursive":false},"/567":{"lot_name":"lot5","recursive":true},"/another/path":{"lot_name":"lot3","recursive":true},"/foo/barr":{"lot_name":"lot3","recursive":true},"/updated/path":{"lot_name":"lot3","recursive":false}},"usage":{"GB_being_written":{"children_contrib":3.4,"self_contrib":0.0},"dedicated_GB":{"children_contrib":8.64,"self_contrib":0.0,"total":8.64},"num_objects":{"children_contrib":10.0,"self_contrib":0.0},"objects_being_written":{"children_contrib":7.0,"self_contrib":0.0},"opportunistic_GB":{"children_contrib":0.0,"self_contrib":0.0,"total":0.0},"total_GB":{"children_contrib":8.64,"self_contrib":0.0}}})"_json;
        ASSERT_TRUE(output_JSON == expected_output);
    }

    TEST_F(LotManTest, LotsFromDirTest) {
        char *err_msg;
        char **output;
        const char *dir = "/1/2/3/4"; // Get a path
        auto rv = lotman_get_lots_from_dir(dir, true, &output, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        for (int iter = 0; output[iter]; iter++) {
            ASSERT_TRUE(static_cast<std::string>(output[iter]) == "lot4" || static_cast<std::string>(output[iter]) == "lot1" ||
                        static_cast<std::string>(output[iter]) == "lot2" || static_cast<std::string>(output[iter]) == "lot3" ||
                        static_cast<std::string>(output[iter]) == "lot5" || static_cast<std::string>(output[iter]) == "sep_node");
        }

        lotman_free_string_list(output);

        char **output2;
        const char *dir2 = "/foo/barr"; // Make sure parsing doesn't grab lot associated with /foo/bar
        rv = lotman_get_lots_from_dir(dir2, false, &output2, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        ASSERT_TRUE(strcmp(output2[0], "lot3") == 0);
        lotman_free_string_list(output2);
    }

    TEST_F(LotManTest, GetVersionTest) {
        const char *version = lotman_version();
        std::string version_cpp(version);

        EXPECT_EQ(version_cpp, "v0.0.1");
    }
} 

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
