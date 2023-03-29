#include "../src/lotman.h"

#include <gtest/gtest.h>
#include <iostream>
#include <typeinfo>
#include <cstring>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace {


    TEST(LotManTest, DefaultLotTests) {
        char *err1;
        const char *lot1 = "{\"lot_name\": \"lot1\", \"owner\": \"owner1\",  \"parents\": [\"lot1\"],\"paths\": [{\"path\":\"/1/2/3\", \"recursive\":false},{\"path\":\"/foo/bar\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        const char *default_lot = "{\"lot_name\": \"default\", \"owner\": \"owner2\",  \"parents\": [\"default\"],\"paths\": [{\"path\":\"/default/paths\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";

        auto rv = lotman_set_context("caller", "owner1", &err1);

        rv = lotman_add_lot(lot1, &err1);
        ASSERT_FALSE(!(rv != 0 && err1 != nullptr)) << err1;

        char *err2;
        rv = lotman_add_lot(default_lot, &err2);
        ASSERT_TRUE(rv == 0) << err2;

        rv = lotman_remove_lot("default", true, true, true, false, &err2);
        ASSERT_FALSE(rv == 0 && err2 == nullptr) << err2;
    }

    TEST(LotManTest, AddRemoveSublot) {
        char *err_msg;
        const char *context;

        const char *lot1 = "{\"lot_name\": \"lot1\", \"owner\": \"owner1\",  \"parents\": [\"lot1\"],\"paths\": [{\"path\":\"1/2/3\", \"recursive\":false},{\"path\":\"/foo/bar\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";

        auto rv = lotman_add_lot(lot1, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;
    
        const char *deleted_lot = "lot1";
        rv = lotman_remove_lot(deleted_lot, false, false, false, false, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        rv = lotman_lot_exists(deleted_lot, &err_msg);
        ASSERT_FALSE(rv) << err_msg;
    }

    TEST(LotManTest, AddInvalidLots) {
        char *err_msg;

        const char *lot1 = "{\"lot_name\": \"lot1\", \"owner\": \"owner1\",  \"parents\": [\"lot1\"],\"paths\": [{\"path\":\"/1/2/3\", \"recursive\":false},{\"path\":\"/foo/bar\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":20,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        const char *lot2 = "{\"lot_name\": \"lot2\",  \"owner\": \"owner1\",  \"parents\": [\"lot1\"],  \"paths\": [{\"path\":\"/1/2/4\", \"recursive\":true},{\"path\":\"/foo/baz\", \"recursive\":true}],  \"management_policy_attrs\": { \"dedicated_GB\":6,\"opportunistic_GB\":1.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":233,\"deletion_time\":355}}";
        const char *lot3 = "{ \"lot_name\": \"lot3\", \"owner\": \"owner1\",  \"parents\": [\"lot3\"],  \"paths\": [{\"path\":\"/another/path\", \"recursive\":false},{\"path\":\"/123\", \"recursive\":true}], \"management_policy_attrs\": { \"dedicated_GB\":3,\"opportunistic_GB\":2.0,\"max_num_objects\":60,\"creation_time\":123,\"expiration_time\":232,\"deletion_time\":325}}";
        const char *lot4 = "{ \"lot_name\": \"lot4\", \"owner\": \"owner1\", \"parents\": [\"lot2\",\"lot3\"], \"paths\": [{\"path\":\"/1/2/3/4\", \"recursive\":true},{\"path\":\"/345\", \"recursive\":true}], \"management_policy_attrs\": { \"dedicated_GB\":3,\"opportunistic_GB\":2.1,\"max_num_objects\":40,\"creation_time\":123,\"expiration_time\":231,\"deletion_time\":315}}";
        const char *sep_node = "{ \"lot_name\": \"sep_node\", \"owner\": \"owner1\", \"parents\": [\"sep_node\"], \"paths\": [{\"path\":\"/sep/node\", \"recursive\":true}], \"management_policy_attrs\": { \"dedicated_GB\":3,\"opportunistic_GB\":2.1,\"max_num_objects\":10,\"creation_time\":123,\"expiration_time\":91679525853643,\"deletion_time\":9267952553643}}";

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

        // Try to add lot with no parent
        const char *no_parents_lot = "{\"lot_name\": \"lot5\",\"owner\": \"owner1\",\"parents\": [],\"children\": [\"lot1\"],\"paths\": [{\"path\":\"/456\", \"recursive\":false},{\"path\":\"/567\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":111111,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        rv = lotman_add_lot(no_parents_lot, &err_msg);
        ASSERT_FALSE(rv == 0) << err_msg;
    }

    TEST(LotManTest, InsertionTest) {
        char *err_msg;
        const char *context;

        const char *lot5 = "{\"lot_name\": \"lot5\",\"owner\":\"owner1\",\"parents\": [\"lot3\"],\"children\": [\"lot4\"],\"paths\": [{\"path\":\"/456\", \"recursive\":false},{\"path\":\"/567\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":10,\"opportunistic_GB\":3.5,\"max_num_objects\":20,\"creation_time\":100,\"expiration_time\":200,\"deletion_time\":300}, \"test_bad_key\":10}";
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

    TEST(LotManTest, ModifyLotTest) {
        char *err_msg;
        const char *context;

        const char *modified_lot = "{ \"lot_name\": \"lot3\", \"owner\": \"not owner1\",  \"parents\": [{\"lot3\":\"lot2\"}],  \"paths\": [{\"/another/path\": {\"path\": \"/another/path\", \"recursive\": true}},{\"/123\": {\"path\" : \"/updated/path\", \"recursive\" : false}}], \"management_policy_attrs\": { \"dedicated_GB\":10.111,\"opportunistic_GB\":6.6,\"max_num_objects\":50,\"creation_time\":111,\"expiration_time\":222.111,\"deletion_time\":333}}";
        int rv = lotman_update_lot(modified_lot, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        char **owner_out;
        rv = lotman_get_owners("lot3", false, &owner_out, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;
        
        bool check_old, check_new = false;
        for (int iter = 0; owner_out[iter]; iter++) {
            if (static_cast<std::string>(owner_out[iter]) == "not owner1") {
                check_new = true;
            }
            if (static_cast<std::string>(owner_out[iter]) == "owner1") {
                check_old = true;
            }
        }
        ASSERT_TRUE(check_new & !check_old);
        lotman_free_string_list(owner_out);

        char **parents_out;
        rv = lotman_get_parent_names("lot3", false, true, &parents_out, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        check_old, check_new = false;
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
        rv = lotman_add_to_lot(addition_JSON, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;




    }

    TEST(LotManTest, SetGetUsageTest) {
        // Update by lot
        char *err_msg;
        const char *usage1_update_JSON = "{\"lot_name\":\"lot4\", \"self_GB\":10.5, \"self_objects\":4, \"self_GB_being_written\":2.2, \"self_objects_being_written\":2}";
        int rv = lotman_update_lot_usage(usage1_update_JSON, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        const char *usage2_update_JSON = "{\"lot_name\":\"lot5\",\"self_GB\":3.5, \"self_objects\":7, \"self_GB_being_written\":1.2, \"self_objects_being_written\":5}";
        rv = lotman_update_lot_usage(usage2_update_JSON, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        char *output;
        const char *usage_query_JSON = "{\"lot_name\":\"lot5\", \"dedicated_GB\": true, \"opportunistic_GB\": true, \"total_GB\": true}";
        rv = lotman_get_lot_usage(usage_query_JSON, &output, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        json json_out = json::parse(output);
        ASSERT_TRUE(json_out["dedicated_GB"]["children_contrib"] == 6.5 && json_out["dedicated_GB"]["self_contrib"] == 3.5 && json_out["dedicated_GB"]["total"] == 10 &&
                    json_out["opportunistic_GB"]["children_contrib"] == 3.5 && json_out["opportunistic_GB"]["self_contrib"] == 0 && json_out["opportunistic_GB"]["total"] == 3.5 &&
                    json_out["total_GB"]["children_contrib"] == 10.5 && json_out["total_GB"]["self_contrib"] == 3.5);

        free(output);


        // // Update by dir
        const char *update_JSON_str = "[{\"includes_subdirs\": true,\"num_obj\": 40,\"path\": \"/1/2/3\",\"size_GB\": 5.12,\"subdirs\": [{\"includes_subdirs\": true,\"num_obj\": 0,\"path\": \"4\",\"size_GB\": 3.14,\"subdirs\": [{\"includes_subdirs\": true,\"num_obj\": 0,\"path\": \"5\",\"size_GB\": 1.6,\"subdirs\": null}]},{\"includes_subdirs\": true,\"num_obj\": 0,\"path\": \"5/6\",\"size_GB\": 0.5,\"subdirs\": null},{\"includes_subdirs\": true,\"num_obj\": 0,\"path\": \"6\",\"size_GB\": 0.25,\"subdirs\": null}]},{\"includes_subdirs\": true,\"num_obj\": 0,\"path\": \"foo/bar\",\"size_GB\": 9.153,\"subdirs\": [{\"includes_subdirs\": true,\"num_obj\": 0,\"path\": \"baz\",\"size_GB\": 5.35,\"subdirs\": [{\"includes_subdirs\": false,\"num_obj\": 0,\"path\": \"more_more_files\",\"size_GB\": 2.2,\"subdirs\": null}]}]}]";
        rv = lotman_update_lot_usage_by_dir(update_JSON_str, &err_msg);

        


}

    TEST(LotManTest, GetOwnersTest) {
        char *err_msg;
        const char *lot_name = "lot4";
        const bool recursive = true;
        char **output;
        int rv = lotman_get_owners(lot_name, recursive, &output, &err_msg);
        ASSERT_TRUE(rv == 0);
        for (int iter = 0; output[iter]; iter++) {
            ASSERT_TRUE(static_cast<std::string>(output[iter]) == "owner1" || static_cast<std::string>(output[iter]) == "not owner1");
        }
        lotman_free_string_list(output);
    }

    TEST(LotManTest, GetParentsTest) {
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

    TEST(LotManTest, GetChildrenTest) {
        char *err_msg;
        const char *lot_name = "lot1";
        const bool recursive = true;
        const bool get_self = false;
        char **output;
        int rv = lotman_get_children_names(lot_name, recursive, get_self, &output, &err_msg);
        ASSERT_TRUE(rv == 0);

        for (int iter = 0; output[iter]; iter++) {
            ASSERT_TRUE(static_cast<std::string>(output[iter]) == "lot2" || static_cast<std::string>(output[iter]) == "lot3" ||
                        static_cast<std::string>(output[iter]) == "lot4" || static_cast<std::string>(output[iter]) == "lot5");
        }
        lotman_free_string_list(output);
    }

    TEST(LotManTest, GetPolicyAttrs) {
        char *err_msg;
        const char *policy_attrs_JSON_str = "{\"lot_name\":\"lot4\", \"dedicated_GB\":true, \"opportunistic_GB\":true, \"max_num_objects\":true, \"creation_time\":true, \"expiration_time\":true, \"deletion_time\":true}";
        char *output;
        int rv = lotman_get_policy_attributes(policy_attrs_JSON_str, &output, &err_msg);
        ASSERT_TRUE(rv == 0);

        json json_out = json::parse(output);
        ASSERT_TRUE(json_out["creation_time"]["lot_name"] == "lot5" && json_out["creation_time"]["value"] == 100 &&
                    json_out["dedicated_GB"]["lot_name"] == "lot4" && json_out["dedicated_GB"]["value"] == 3.0 &&
                    json_out["deletion_time"]["lot_name"] == "lot5" && json_out["deletion_time"]["value"] == 300 &&
                    json_out["expiration_time"]["lot_name"] == "lot5" && json_out["expiration_time"]["value"] == 200 &&
                    json_out["max_num_objects"]["lot_name"] == "sep_node" && json_out["max_num_objects"]["value"] == 10 &&
                    json_out["opportunistic_GB"]["lot_name"] == "lot2" && json_out["opportunistic_GB"]["value"] == 1.5);
        free(output);
    }

    TEST(LotManTest, GetLotDirs) {
        char *err_msg;
        const char *lot_name = "lot5";
        const bool recursive = true;
        char *output;
        int rv = lotman_get_lot_dirs(lot_name, recursive, &output, &err_msg);
        ASSERT_TRUE(rv == 0);

        json json_out = json::parse(output);
        ASSERT_TRUE(json_out["/1/2/3/4"]["lot_name"] == "lot4" && json_out["/1/2/3/4"]["recursive"] == true &&
                    json_out["/345"]["lot_name"] == "lot4" && json_out["/345"]["recursive"] == true &&
                    json_out["/456"]["lot_name"] == "lot5" && json_out["/456"]["recursive"] == false &&
                    json_out["/567"]["lot_name"] == "lot5" && json_out["/567"]["recursive"] == true);
        free(output);
    }

    TEST(LotManTest, ContextTest) {
        // Any actions that modify the properties of a lot must have proper auth
        // These tests should all fail (context set to nonexistant owner)
        
        char *err_msg;
        const char *lot6 = "{ \"lot_name\": \"lot6\", \"owner\": \"owner1\", \"parents\": [\"lot5\"], \"paths\": [], \"management_policy_attrs\": { \"dedicated_GB\":3,\"opportunistic_GB\":2.1,\"max_num_objects\":40,\"creation_time\":123,\"expiration_time\":231,\"deletion_time\":315}}";

        // Try to add a lot without correct context
        auto rv = lotman_set_context("caller", "notAnOwner", &err_msg);
        rv = lotman_add_lot(lot6, &err_msg);
        ASSERT_FALSE(rv == 0) << err_msg;
        rv = lotman_lot_exists("lot6", &err_msg);
        ASSERT_FALSE(rv == 1);

        // Try to remove a lot without correct context
        rv = lotman_remove_lots_recursive("lot1", &err_msg);
        ASSERT_FALSE(rv == 0);
        rv = lotman_lot_exists("lot1", &err_msg);
        ASSERT_TRUE(rv == 1);

        // Try to update a lot without correct context
        const char *modified_lot = "{ \"lot_name\": \"lot3\", \"owner\": \"Bad Update\"}";
        rv = lotman_update_lot(modified_lot, &err_msg);
        ASSERT_FALSE(rv == 0);

        // Try to update lot usage without correct context
        const char *usage_update_JSON = "{\"lot_name\":\"lot5\",\"self_GB\":99}";
        rv = lotman_update_lot_usage(usage_update_JSON, &err_msg);
        ASSERT_FALSE(rv == 0);

    }

    TEST(LotManTest, LotsQueryTest) {
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
            if (strcmp(output5[iter], "lot1") == 0 || strcmp(output5[iter], "lot2") == 0 || strcmp(output5[iter], "default") == 0) {
                check = true;
            }
        }
        ASSERT_FALSE(check);
        lotman_free_string_list(output5);
    }

    TEST(LotManTest, GetAllLotsTest) {
        char *err_msg;
        char **output;
        auto rv = lotman_list_all_lots(&output, &err_msg);
        int size = 0;
        for (size; output[size]; ++size) {}
        ASSERT_TRUE(size == 7);
        lotman_free_string_list(output);
    }

    TEST(LotManTest, GetLotJSONTest) {
        char *err_msg;
        char *output;
        auto rv = lotman_get_lot_as_json("lot3", true, &output, &err_msg);
        ASSERT_TRUE(rv == 0);
        json output_JSON = json::parse(output);
        free(output);


        // TODO: When done messing with tests, fix this:
        // json expected_output = json::parse("{\"children\":[\"lot4\",\"lot5\"],\"lot_name\":\"lot3\",\"management_policy_attrs\":{\"creation_time\":{\"lot_name\":\"lot3\",\"value\":111.0},\"dedicated_GB\":{\"lot_name\":\"sep_node\",\"value\":3.0},\"deletion_time\":{\"lot_name\":\"lot3\",\"value\":333.0},\"expiration_time\":{\"lot_name\":\"lot3\",\"value\":222.0},\"max_num_objects\":{\"lot_name\":\"sep_node\",\"value\":10.0},\"opportunistic_GB\":{\"lot_name\":\"lot2\",\"value\":1.5}},\"owners\":[\"not owner1\",\"owner1\"],\"parents\":[\"lot1\",\"lot2\",\"sep_node\"],\"paths\":{\"/1/2/3/4\":{\"lot_name\":\"lot4\",\"recursive\":true},\"/345\":{\"lot_name\":\"lot4\",\"recursive\":true},\"/456\":{\"lot_name\":\"lot5\",\"recursive\":false},\"/567\":{\"lot_name\":\"lot5\",\"recursive\":true},\"/another/path\":{\"lot_name\":\"lot3\",\"recursive\":true},\"/foo/barr\":{\"lot_name\":\"lot3\",\"recursive\":true},\"/updated/path\":{\"lot_name\":\"lot3\",\"recursive\":false}},\"usage\":{\"GB_being_written\":{\"children_contrib\":3.4,\"self_contrib\":0.0},\"dedicated_GB\":{\"children_contrib\":10.111,\"self_contrib\":0.0,\"total\":10.111},\"num_objects\":{\"children_contrib\":11.0,\"self_contrib\":0.0},\"objects_being_written\":{\"children_contrib\":7.0,\"self_contrib\":0.0},\"opportunistic_GB\":{\"children_contrib\":3.889,\"self_contrib\":0.0,\"total\":3.889},\"total_GB\":{\"children_contrib\":14.0,\"self_contrib\":0.0}}}");
        // ASSERT_TRUE(output_JSON == expected_output);
    }

    TEST(LotManTest, LotsFromDirTest) {
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

    TEST(LotManTest, GetVersionTest) {
        const char *version = lotman_version();
        std::string version_cpp(version);

        EXPECT_EQ(version_cpp, "0.1");
    }
} 

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
