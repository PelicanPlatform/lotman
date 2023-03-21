#include "../src/lotman.h"

#include <gtest/gtest.h>
#include <iostream>
#include <typeinfo>
#include <picojson.h>
#include <cstring>

namespace {


    TEST(LotManTest, DefaultLotTests) {
        char *err1;
        const char *context;
        const char *lot1 = "{\"lot_name\": \"lot1\", \"owner\": \"Justin\",  \"parents\": [\"lot1\"],\"paths\": [{\"path\":\"/a/path\", \"recursive\":false},{\"path\":\"/foo/bar\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        const char *default_lot = "{\"lot_name\": \"default\", \"owner\": \"Brian B\",  \"parents\": [\"default\"],\"paths\": [{\"path\":\"/default/paths\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";

        auto rv = lotman_add_lot(lot1, context, &err1);
        ASSERT_FALSE(!(rv != 0 && err1 != nullptr)) << err1;

        char *err2;
        rv = lotman_add_lot(default_lot, context, &err2);
        ASSERT_TRUE(rv == 0) << err2;

        rv = lotman_remove_lot("default", true, true, true, false, context, &err2);
        ASSERT_FALSE(rv == 0 && err2 == nullptr) << err2;
    }

    TEST(LotManTest, AddRemoveSublot) {
        char *err_msg;
        const char *context;

        const char *lot1 = "{\"lot_name\": \"lot1\", \"owner\": \"Justin\",  \"parents\": [\"lot1\"],\"paths\": [{\"path\":\"/a/path\", \"recursive\":false},{\"path\":\"/foo/bar\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";

        std::cout << "adding lot" << std::endl;
        auto rv = lotman_add_lot(lot1, context, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;
    
        std::cout << "deleting lot" << std::endl;
        const char *deleted_lot = "lot1";
        rv = lotman_remove_lot(deleted_lot, false, false, false, false, context, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        std::cout << "checking for lot" << std::endl;
        rv = lotman_lot_exists(deleted_lot, context, &err_msg);
        ASSERT_FALSE(rv) << err_msg;
    }

    TEST(LotManTest, AddInvalidLots) {

        // Setup
        char *err_msg;
        const char *context;

        const char *lot1 = "{\"lot_name\": \"lot1\", \"owner\": \"Justin\",  \"parents\": [\"lot1\"],\"paths\": [{\"path\":\"/a/path\", \"recursive\":false},{\"path\":\"/foo/bar\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":20,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        const char *lot2 = "{\"lot_name\": \"lot2\",  \"owner\": \"Justin\",  \"parents\": [\"lot1\"],  \"paths\": [{\"path\":\"/b/path\", \"recursive\":false},{\"path\":\"/foo/baz\", \"recursive\":true}],  \"management_policy_attrs\": { \"dedicated_GB\":6,\"opportunistic_GB\":1.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":233,\"deletion_time\":355}}";
        const char *lot3 = "{ \"lot_name\": \"lot3\", \"owner\": \"Justin\",  \"parents\": [\"lot3\"],  \"paths\": [{\"path\":\"/another/path\", \"recursive\":false},{\"path\":\"/123\", \"recursive\":true}], \"management_policy_attrs\": { \"dedicated_GB\":3,\"opportunistic_GB\":2.0,\"max_num_objects\":60,\"creation_time\":123,\"expiration_time\":232,\"deletion_time\":325}}";
        const char *lot4 = "{ \"lot_name\": \"lot4\", \"owner\": \"Justin\", \"parents\": [\"lot2\",\"lot3\"], \"paths\": [{\"path\":\"/234\", \"recursive\":false},{\"path\":\"/345\", \"recursive\":true}], \"management_policy_attrs\": { \"dedicated_GB\":3,\"opportunistic_GB\":2.1,\"max_num_objects\":40,\"creation_time\":123,\"expiration_time\":231,\"deletion_time\":315}}";

        std::cout << "adding 1" << std::endl;
        auto rv = lotman_add_lot(lot1, context, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        std::cout << "adding 2" << std::endl;
        rv = lotman_add_lot(lot2, context, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;
  
        std::cout << "adding 3" << std::endl;
        rv = lotman_add_lot(lot3, context, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        std::cout << "adding 4" << std::endl;
        rv = lotman_add_lot(lot4, context, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        // Try to add a lot with cyclic dependency
        // Cycle created by trying to add lot5 with lot1 as a child
        const char *cyclic_lot = "{\"lot_name\": \"lot5\",\"owner\": \"Brian\",\"parents\": [\"lot4\"],\"children\": [\"lot1\"],\"paths\": [{\"path\":\"/456\", \"recursive\":false},{\"path\":\"/567\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":5,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        std::cout << "adding cyclic" << std::endl;
        rv = lotman_add_lot(cyclic_lot, context, &err_msg);
        ASSERT_FALSE(rv == 0) << err_msg;

        // Try to add lot with no parent
        const char *no_parents_lot = "{\"lot_name\": \"lot5\",\"owner\": \"Justin\",\"parents\": [],\"children\": [\"lot1\"],\"paths\": [{\"path\":\"/456\", \"recursive\":false},{\"path\":\"/567\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":111111,\"opportunistic_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        std::cout << "adding no parent" << std::endl;
        rv = lotman_add_lot(no_parents_lot, context, &err_msg);
        ASSERT_FALSE(rv == 0) << err_msg;
    }

    TEST(LotManTest, InsertionTest) {
        char *err_msg;
        const char *context;

        const char *lot5 = "{\"lot_name\": \"lot5\",\"owner\":\"Justin\",\"parents\": [\"lot3\"],\"children\": [\"lot4\"],\"paths\": [{\"path\":\"/456\", \"recursive\":false},{\"path\":\"/567\", \"recursive\":true}],\"management_policy_attrs\": { \"dedicated_GB\":10,\"opportunistic_GB\":3.5,\"max_num_objects\":20,\"creation_time\":100,\"expiration_time\":200,\"deletion_time\":300}, \"test_bad_key\":10}";
        int rv = lotman_add_lot(lot5, context, &err_msg);
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

        std::cout << "updating lot" << std::endl;
        const char *modified_lot = "{ \"lot_name\": \"lot3\", \"owner\": \"Not Justin\",  \"parents\": [{\"lot3\":\"lot2\"}],  \"paths\": [{\"/another/path\": {\"path\": \"/another/path\", \"recursive\": true}},{\"/123\": {\"path\" : \"/updated/path\", \"recursive\" : false}}], \"management_policy_attrs\": { \"dedicated_GB\":10.111,\"opportunistic_GB\":6.6,\"max_num_objects\":50,\"creation_time\":111,\"expiration_time\":222.111,\"deletion_time\":333}}";
        int rv = lotman_update_lot(modified_lot, context, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;

        char **owner_out;
        
        std::cout << "getting owners" << std::endl;
        rv = lotman_get_owners("lot3", false, &owner_out, &err_msg);
        ASSERT_TRUE(rv == 0) << err_msg;
        
        std::cout << "onto checks" << std::endl;
        bool check_old, check_new = false;
        for (int iter = 0; owner_out[iter]; iter++) {


            if (static_cast<std::string>(owner_out[iter]) == "Not Justin") {
                check_new = true;
            }
            if (static_cast<std::string>(owner_out[iter]) == "Justin") {
                check_old = true;
            }
        }
        ASSERT_TRUE(check_new & !check_old);

        std::cout << "freeing owners" << std::endl;
        lotman_free_string_list(owner_out);

        char **parents_out;
        std::cout << "getting parents" << std::endl;
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
    }

    // TEST(LotManTest, SetGetUsageTest) {
    //     char *err_msg;
    //     const char *usage1_update_JSON = "{\"lot_name\":\"lot4\", \"self_GB\":10.5, \"self_objects\":4, \"self_GB_being_written\":2.2, \"self_objects_being_written\":2}";
    //     int rv = lotman_update_lot_usage(usage1_update_JSON, &err_msg);
    //     ASSERT_TRUE(rv == 0);

    //     const char *usage2_update_JSON = "{\"lot_name\":\"lot5\",\"self_GB\":3.5, \"self_objects\":7, \"self_GB_being_written\":1.2, \"self_objects_being_written\":5}";
    //     rv = lotman_update_lot_usage(usage2_update_JSON, &err_msg);
    //     ASSERT_TRUE(rv == 0);

    //     char *output;
    //     const char *usage_query_JSON = "{\"lot_name\":\"lot5\", \"dedicated_GB\": true, \"opportunistic_GB\": true, \"total_GB\": true}";
    //     rv = lotman_get_lot_usage(usage_query_JSON, &output, &err_msg);
    //     ASSERT_TRUE(rv == 0);
    //     std::cout << output << std::endl;
    //     free(output);
        
    //     // parse the output and make sure it's as expected
    // }

    // TEST(LotManTest, GetOwnersTest) {
    //     char *err_msg;
    //     const char *lot_name = "lot4";
    //     const bool recursive = true;
    //     char **output;
    //     int rv = lotman_get_owners(lot_name, recursive, &output, &err_msg);
    //     ASSERT_TRUE(rv == 0);
    //     for (int iter = 0; output[iter]; iter++) {
    //         std::cout << output[iter] << std::endl;
    //     }
    //     lotman_free_string_list(output);
    // }

    // TEST(LotManTest, GetParentsTest) {
    //     char *err_msg;
    //     const char *lot_name = "lot4";
    //     const bool recursive = true;
    //     const bool get_self = true;
    //     char **output;
    //     int rv = lotman_get_parent_names(lot_name, recursive, get_self, &output, &err_msg);
    //     ASSERT_TRUE(rv == 0);
    //     for (int iter = 0; output[iter]; iter++) {
    //         std::cout << output[iter] << std::endl;
    //     }
    //     lotman_free_string_list(output);
    // }

    // TEST(LotManTest, GetChildrenTest) {
    //     char *err_msg;
    //     const char *lot_name = "lot1";
    //     const bool recursive = true;
    //     const bool get_self = false;
    //     char **output;
    //     int rv = lotman_get_children_names(lot_name, recursive, get_self, &output, &err_msg);
    //     ASSERT_TRUE(rv == 0);
    //     for (int iter = 0; output[iter]; iter++) {
    //         std::cout << output[iter] << std::endl;
    //     }
    //     lotman_free_string_list(output);
    // }

    // TEST(LotManTest, GetPolicyAttrs) {
    //     char *err_msg;
    //     const char *policy_attrs_JSON_str = "{\"lot_name\":\"lot4\", \"dedicated_GB\":true, \"opportunistic_GB\":true, \"max_num_objects\":true, \"creation_time\":true, \"expiration_time\":true, \"deletion_time\":true}";
    //     char *output;
    //     int rv = lotman_get_policy_attributes(policy_attrs_JSON_str, &output, &err_msg);
    //     ASSERT_TRUE(rv == 0);
    //     std::cout << output << std::endl;
    //     free(output);
    // }

    // TEST(LotManTest, GetLotDirs) {
    //     char *err_msg;
    //     const char *lot_name = "lot5";
    //     const bool recursive = false;
    //     char *output;
    //     int rv = lotman_get_lot_dirs(lot_name, recursive, &output, &err_msg);
    //     ASSERT_TRUE(rv == 0);
    //     std::cout << output << std::endl;
    //     free(output);
    // }

    // TEST(LotManTest, GetVersionTest) {
    //     const char *version = lotman_version();
    //     std::string version_cpp(version);

    //     EXPECT_EQ(version_cpp, "0.1");
    // }
} 

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
