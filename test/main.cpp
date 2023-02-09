#include "../src/lotman.h"

#include <gtest/gtest.h>
#include <iostream>


namespace {

    TEST(LotManTest, AddRemoveSublot) {
        char *err_msg;
        const char *context;

        const char *lot1 = "{\"lot_name\": \"lot1\", \"owners\": [\"Justin\", \"Brian\", \"Cannon\"],  \"parents\": [\"lot1\"],\"paths\": [{\"/a/path\":0},{\"/foo/bar\":1}],\"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";

        auto rv = lotman_add_lot(lot1, context, &err_msg);
        ASSERT_TRUE(rv == 0);
    
        const char *deleted_lot = "lot1";
        rv = lotman_remove_lot(deleted_lot, true, true, true, true, true, context, &err_msg);
        ASSERT_TRUE(rv == 0);

        rv = lotman_lot_exists(deleted_lot, context, &err_msg);
        ASSERT_TRUE(rv == 0);
    }

    TEST(LotManTest, AddInvalidLots) {

        // Setup
        char *err_msg;
        const char *context;

        const char *lot1 = "{\"lot_name\": \"lot1\", \"owners\": [\"Justin\", \"Brian\", \"Cannon\"],  \"parents\": [\"lot1\"],\"paths\": [{\"/a/path\":0},{\"/foo/bar\":1}],\"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        const char *lot2 = "{\"lot_name\": \"lot2\",  \"owners\": [\"Justin\", \"Brian\"],  \"parents\": [\"lot1\"],  \"paths\": [{\"/b/path\":0},{\"/foo/baz\":1}],  \"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        const char *lot3 = "{ \"lot_name\": \"lot3\", \"owners\": [\"Justin\", \"Brian\"],  \"parents\": [\"lot3\"],  \"paths\": [{\"/another/path\":0},{\"/123\":1}], \"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        const char *lot4 = "{ \"lot_name\": \"lot4\", \"owners\": [\"Justin\", \"Brian\"], \"parents\": [\"lot2\",\"lot3\"], \"paths\": [{\"/234\":0},{\"/345\":1}], \"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";

        auto rv = lotman_add_lot(lot1, context, &err_msg);
        ASSERT_TRUE(rv == 0);

        rv = lotman_add_lot(lot2, context, &err_msg);
        ASSERT_TRUE(rv == 0);
  
        rv = lotman_add_lot(lot3, context, &err_msg);
        ASSERT_TRUE(rv == 0);

        rv = lotman_add_lot(lot4, context, &err_msg);
        ASSERT_TRUE(rv == 0);

        // Try to add a lot with cyclic dependency
        // Cycle created by trying to add lot5 with lot1 as a child
        const char *cyclic_lot = "{\"lot_name\": \"lot5\",\"owners\": [\"Justin\", \"Brian\"],\"parents\": [\"lot4\"],\"children\": [\"lot1\"],\"paths\": [{\"/456\":0},{\"/567\":1}],\"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        rv = lotman_add_lot(cyclic_lot, context, &err_msg);
        ASSERT_FALSE(rv == 0);

        // Try to add lot with no parent
        const char *no_parents_lot = "{\"lot_name\": \"lot5\",\"owners\": [\"Justin\", \"Brian\"],\"parents\": [],\"children\": [\"lot1\"],\"paths\": [{\"/456\":0},{\"/567\":1}],\"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        rv = lotman_add_lot(no_parents_lot, context, &err_msg);
        ASSERT_FALSE(rv == 0);
    }

    TEST(LotManTest, InsertionTest) {
        // TODO: Insertion test -- set this up
        // Insertion: 3->4 :-> 3->5->4
        char *err_msg;
        const char *context;

        const char *lot5 = "{\"lot_name\": \"lot5\",\"owners\": [\"Justin\", \"Brian\"],\"parents\": [\"lot3\"],\"children\": [\"lot4\"],\"paths\": [{\"/456\":0},{\"/567\":1}],\"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        int rv = lotman_add_lot(lot5, context, &err_msg);
        ASSERT_TRUE(rv == 0);
    }

    TEST(LotManTest, ModifyLotTest) {
        char *err_msg;
        const char *context;

        const char *modified_lot = "{ \"lot_name\": \"lot3\", \"owners\": [{\"Justin\":\"Not Justin\"}, {\"Brian\":\"Not Brian\"}],  \"parents\": [{\"lot3\":\"lot2\"}],  \"paths\": [{\"/another/path\":1},{\"/123\":0}], \"management_policy_attrs\": { \"dedicated_GB\":10.111,\"opportunistic_GB\":6.6,\"max_num_objects\":50,\"creation_time\":111,\"expiration_time\":222.111,\"deletion_time\":333}}";
        int rv = lotman_update_lot(modified_lot, context, &err_msg);
        ASSERT_TRUE(rv == 0);
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
