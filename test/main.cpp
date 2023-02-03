#include "../src/lotman.h"

#include <gtest/gtest.h>
#include <iostream>


namespace {

// TODO: Set up tests to export LOT_HOME in test subdir of build
// TODO: Make tests delete /build/.lot at the end so the InitializeRootLot test passes each time it's run

    
    TEST(LotManTest, InitializeRootLot) {
        char *err_msg;
        const char name[] = "Justin's Lot";
        const char path[] = "/workspaces/lotman";
        const char owner[] = "Justin";
        const char reclamation_policy[] = "{\"reclamation_policy\":{\"creation_time\":\"NOW\",\"expiration_time\":\"TOMORROW\",\"deletion_time\":\"TWO DAYS\"}}";
        const char resource_limits[] = "{\"resource_limits\":{\"dedicated_storage\":\"10GB\",\"opportunistic_storage\":\"5GB\",\"max_num_objects\":100}}";

        auto rv = lotman_initialize_root(name, path, owner, resource_limits, reclamation_policy, &err_msg);
        ASSERT_TRUE(rv == 0);
    }

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
        const char name[] = "Not Justin's Lot";
        const char path[] = "/workspaces/lotman/build";
        const char parent[] = "Justin's Lot";
        const char owner[] = "Not_Justin";
        const char reclamation_policy[] = "{\"reclamation_policy\":{\"creation_time\":\"NOW\",\"expiration_time\":\"2day\",\"deletion_time\":\"2day\"}}";
        const char resource_limits[] = "{\"resource_limits\":{\"dedicated_storage\":\"5GB\",\"opportunistic_storage\":\"2.5GB\"}}";

        auto rv = lotman_add_sublot(name, path, parent, owner, reclamation_policy, resource_limits, &err_msg);
        ASSERT_TRUE(rv == 0);

        //rv = lotman_remove_sublot(name, &err_msg);
        //ASSERT_TRUE(rv == 0);
    }

    // TEST(LotManTest, AddInvalidSublot) {
    //     char *err_msg;
    //     const char name[] = "Justin's Lot";
    //     const char path[] = "/workspaces/lotman/build/test";
    //     const char owner[] = "Not_Justin";
    //     const char users[] = "Not_Justin_2";
    //     const char reclamation_policy[] = "{\"reclamation_policy\":{\"creation_time\":\"NOW\",\"expiration_time\":\"2day\",\"deletion_time\":\"2day\"}}";
    //     const char resource_limits[] = "{\"resource_limits\":{\"dedicated_storage\":\"5GB\",\"opportunistic_storage\":\"2.5GB\"}}";

    //     // Add a valid sublot
    //     auto rv = lotman_add_sublot(path, owner, users, reclamation_policy, resource_limits, &err_msg);
    //     ASSERT_TRUE(rv == 0);

    //     // Try to re-add the same sublot
    //     const char path2[] = "/workspaces/lotman/build/test";
    //     rv = lotman_add_sublot(path2, owner, users, reclamation_policy, resource_limits, &err_msg);
    //     ASSERT_FALSE(rv == 0);

    //     // Try to add a sublot between existing lots
    //     const char path3[] = "/workspaces/lotman/build";
    //     rv = lotman_add_sublot(path3, owner, users, reclamation_policy, resource_limits, &err_msg);
    //     ASSERT_FALSE(rv == 0);

    //     // Try to add a sublot that isn't contained by any root lot
    //     const char path4[] = "/workspaces";
    //     rv = lotman_add_sublot(path4, owner, users, reclamation_policy, resource_limits, &err_msg);
    //     ASSERT_FALSE(rv == 0);
    // }


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
