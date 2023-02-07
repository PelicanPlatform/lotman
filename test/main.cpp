#include "../src/lotman.h"

#include <gtest/gtest.h>
#include <iostream>


namespace {

// TODO: Set up tests to export LOT_HOME in test subdir of build
// TODO: Make tests delete /build/.lot at the end so the InitializeRootLot test passes each time it's run



    // TEST(LotManTest, InvalidInitializeRootLot) {
    //     char *err_msg;
    //     const char name[] = "Justin's Lot";
    //     const char initialized_path[] = "/workspaces/lotman";
    //     const char owner[] = "Justin";
    //     const char users[] = "NULL";
    //     const char reclamation_policy[] = "{\"reclamation_policy\":{\"creation_time\":\"NOW\",\"expiration_time\":\"TOMORROW\",\"deletion_time\":\"TWO DAYS\"}}";
    //     const char resource_limits[] = "{\"resource_limits\":{\"dedicated_storage\":\"10GB\",\"opportunistic_storage\":\"5GB\"}}";

    //     // Try to re-initialize the root lot
    //     auto rv = lotman_initialize_root(initialized_path, owner, users, resource_limits, reclamation_policy, &err_msg);
    //     ASSERT_FALSE(rv == 0);

    //     // Try to add root lot below initialized root lot
    //     const char downstream_path[] = "/workspaces/lotman/src";
    //     rv = lotman_initialize_root(downstream_path, owner, users, resource_limits, reclamation_policy, &err_msg);
    //     ASSERT_FALSE(rv == 0);
        
    //     // Try to add root lot above initialized root lot
    //     const char upstream_path[] = "/workspaces";
    //     rv = lotman_initialize_root(upstream_path, owner, users, resource_limits, reclamation_policy, &err_msg);
    //     ASSERT_FALSE(rv == 0);
    // }

    TEST(LotManTest, AddRemoveSublot) {
        char *err_msg;
        const char *context;

        const char *lot1 = "{\"lot_name\": \"lot1\", \"owners\": [\"Justin\", \"Brian\", \"Cannon\"],  \"parents\": [\"lot1\"],\"paths\": [{\"/a/path\":0},{\"/foo/bar\":1}],\"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        const char *lot2 = "{\"lot_name\": \"lot2\",  \"owners\": [\"Justin\", \"Brian\"],  \"parents\": [\"lot1\"],  \"paths\": [{\"/b/path\":0},{\"/foo/baz\":1}],  \"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        const char *lot3 = "{ \"lot_name\": \"lot3\", \"owners\": [\"Justin\", \"Brian\"],  \"parents\": [\"lot3\"],  \"paths\": [{\"/another/path\":0},{\"/123\":1}], \"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        const char *lot4 = "{ \"lot_name\": \"lot4\", \"owners\": [\"Justin\", \"Brian\"], \"parents\": [\"lot2\",\"lot3\"], \"paths\": [{\"/234\":0},{\"/345\":1}], \"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        // lot5 with cycle
        //const char *lot5 = "{\"lot_name\": \"lot5\",\"owners\": [\"Justin\", \"Brian\"],\"parents\": [\"lot4\"],\"children\": [\"lot1\"],\"paths\": [{\"/456\":0},{\"/567\":1}],\"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        // lot5 without children
        //const char *lot5 = "{\"lot_name\": \"lot5\",\"owners\": [\"Justin\", \"Brian\"],\"parents\": [\"lot4\"],\"paths\": [{\"/456\":0},{\"/567\":1}],\"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        // lot5 only self parent
        //const char *lot5 = "{\"lot_name\": \"lot5\",\"owners\": [\"Justin\", \"Brian\"],\"parents\": [\"lot5\"],\"children\": [\"lot1\"],\"paths\": [{\"/456\":0},{\"/567\":1}],\"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";
        // lot5 insertion
        const char *lot5 = "{\"lot_name\": \"lot5\",\"owners\": [\"Justin\", \"Brian\"],\"parents\": [\"lot3\"],\"children\": [\"lot4\"],\"paths\": [{\"/456\":0},{\"/567\":1}],\"management_policy_attrs\": { \"dedicated_storage_GB\":5,\"opportunistic_storage_GB\":2.5,\"max_num_objects\":100,\"creation_time\":123,\"expiration_time\":234,\"deletion_time\":345}}";


        auto rv = lotman_add_lot(lot1, context, &err_msg);
        ASSERT_TRUE(rv == 0);

        rv = lotman_add_lot(lot2, context, &err_msg);
        ASSERT_TRUE(rv == 0);
  
        rv = lotman_add_lot(lot3, context, &err_msg);
        ASSERT_TRUE(rv == 0);

        rv = lotman_add_lot(lot4, context, &err_msg);
        ASSERT_TRUE(rv == 0);

        rv = lotman_add_lot(lot5, context, &err_msg);
        ASSERT_TRUE(rv == 0);
    
        const char *lot_to_delete = "lot1";
        rv = lotman_remove_lot(lot_to_delete, context, &err_msg);
        ASSERT_TRUE(rv == 0);
    }




    TEST(LotManTest, GetVersionTest) {
        const char *version = lotman_version();
        std::string version_cpp(version);

        EXPECT_EQ(version_cpp, "0.1");
    }
    /*
    TEST(LotManTest, RemoveSublot) {
        char *err_msg;
        const char path[] = "/workspaces/lotman/build";

        auto rv = lotman_remove_sublot(path, &err_msg);
        ASSERT_TRUE(rv == 0);
    }
    */
} 

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
