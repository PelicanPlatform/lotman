#include <stdio.h>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

// TODO: Go through and prefer overloading functions vs distinguishing between int and double inputs

class JsonException : public std::runtime_error {
public:
    JsonException(const std::string &msg)
        : std::runtime_error(msg)
    {}
};

namespace lotman {

class Validator;
class Context;
/**
 * The Lot class has member functions for handling top-level actions with lots
 * Eg, adding lots, removing lots, updating lots etc
*/



using json = nlohmann::json;
class Lot2 {
    public:
        // Non-object values used for lot initialization
        std::string lot_name;
        std::string owner;
        std::vector<std::string> parents;
        std::vector<std::string> children;
        std::vector<json> paths;

        // Things that belong to the lot when including parent/child relationships
        std::string self_owner;
        std::vector<Lot2> self_parents;
        std::vector<Lot2> self_children;


        // Things that belong to the lot when including parent/child relationships
        std::vector<std::string> recursive_owners;
        std::vector<Lot2> recursive_parents;
        std::vector<Lot2> recursive_children;


        // management policy attributes
        struct {
            double dedicated_GB;
            double opportunistic_GB;
            int64_t max_num_objects;
            int64_t creation_time;
            int64_t expiration_time;
            int64_t deletion_time;
        } man_policy_attr;

        // usage attributes
        struct {
            double self_GB;
            double children_GB;
            int64_t self_objects;
            int64_t children_objects;
            double self_GB_being_written;
            double children_GB_being_written;
            int64_t self_objects_being_written;
            int64_t children_objects_being_written;
        } usage;

        struct {
            bool assign_LTBR_parent_as_parent_to_orphans;
            bool assign_LTBR_parent_as_parent_to_non_orphans;
            bool assign_policy_to_children;
        } reassignment_policy;

        bool full_lot = false;
        bool has_name = false;
        bool has_reassignment_policy = false;
        bool is_root;
        
        std::pair<bool, std::string> init_full(json lot_JSON); // TODO: Turn this into a constructor to simplify code
        std::pair<bool, std::string> init_name(const std::string name); // TODO: Turn this into a constructor to simplify code
        std::pair<bool, std::string> init_reassignment_policy(const bool assign_LTBR_parent_as_parent_to_orphans, 
                                                              const bool assign_LTBR_parent_as_parent_to_non_orphans,
                                                              const bool assign_policy_to_children);
        std::pair<bool, std::string> init_self_usage(const json usage_JSON);

        static std::pair<bool, std::string> lot_exists(std::string lot_name);
        std::pair<bool, std::string> check_if_root();
        std::pair<bool, std::string> store_lot();
        std::pair<bool, std::string> destroy_lot();
        std::pair<bool, std::string> destroy_lot_recursive();

        std::pair<std::vector<Lot2>, std::string> get_children(const bool recursive = false,
                                                               const bool get_self = false);
                                    
        std::pair<std::vector<Lot2>, std::string> get_parents(const bool recursive = false,
                                                              const bool get_self = false);

        std::pair<std::vector<std::string>, std::string> get_owners(const bool recursive = false);

        std::pair<json, std::string> get_restricting_attribute(const std::string key,
                                                               const bool recursive);

        std::pair<json, std::string> get_lot_dirs(const bool recursive);

        std::pair<json, std::string> get_lot_usage(const std::string key,
                                                   const bool recursive);

        std::pair<bool, std::string> add_parents(std::vector<Lot2> parents);
        std::pair<bool, std::string> add_paths(std::vector<json> paths);

        std::pair<bool, std::string> update_owner(std::string update_val);
        std::pair<bool, std::string> update_parents(std::map<std::string, std::string> update_map);
        std::pair<bool, std::string> update_paths(std::map<std::string, json> update_map);
        std::pair<bool, std::string> update_man_policy_attrs(std::string update_key, double update_val);
        std::pair<bool, std::string> update_self_usage(const std::string key, const double value);
        std::pair<bool, std::string> update_parent_usage(Lot2 parent,
                                                         std::string update_stmt, 
                                                         std::map<std::string, std::vector<int>> update_str_map = std::map<std::string, std::vector<int>>(),
                                                         std::map<int64_t, std::vector<int>> update_int_map =std::map<int64_t, std::vector<int>>(),
                                                         std::map<double, std::vector<int>> update_dbl_map = std::map<double, std::vector<int>>());
        std::pair<bool, std::string> check_context_for_parents(std::vector<std::string> parents, bool include_self = false, bool new_lot = false);
        std::pair<bool, std::string> check_context_for_parents(std::vector<Lot2> parents, bool include_self = false, bool new_lot = false);

        std::pair<bool, std::string> check_context_for_children(std::vector<std::string> children, bool include_self = false);
        std::pair<bool, std::string> check_context_for_children(std::vector<Lot2> children, bool include_self = false);
        static std::pair<std::vector<std::string>, std::string> get_lots_past_exp(const bool recursive);
        static std::pair<std::vector<std::string>, std::string> get_lots_past_del(const bool recursive);
        static std::pair<std::vector<std::string>, std::string> get_lots_past_opp(const bool recursive_quota, const bool recursive_children);
        static std::pair<std::vector<std::string>, std::string> get_lots_past_ded(const bool recursive_quota, const bool recursive_children);
        static std::pair<std::vector<std::string>, std::string> get_lots_past_obj(const bool recursive_quota, const bool recursive_children);
        static std::pair<std::vector<std::string>, std::string> list_all_lots();
        static std::pair<std::vector<std::string>, std::string> get_lots_from_dir(std::string dir, const bool recursive);
        



    private:
        std::pair<bool, std::string> write_new();
        std::pair<bool, std::string> delete_lot_from_db();
        std::pair<bool, std::string> store_new_paths(std::vector<json> new_paths);
        std::pair<bool, std::string> store_new_parents(std::vector<Lot2> new_parents);
        std::pair<bool, std::string> store_updates(std::string update_query, 
                                                   std::map<std::string, std::vector<int>> update_str_map = std::map<std::string, std::vector<int>>(),
                                                   std::map<int64_t, std::vector<int>> update_int_map =std::map<int64_t, std::vector<int>>(),
                                                   std::map<double, std::vector<int>> update_dbl_map = std::map<double, std::vector<int>>());

};


class Context {
  public:
    Context() {}
    static void set_caller(std::string caller) {
        m_caller= caller;
    }
    static std::string get_caller() { return m_caller; }

  private:
    static std::string m_caller;
};


/**
 * The Validator class has member functions that check if functions in the Lot class should execute.
 * In this way, they check the validity of Lot functions executing with a specific set of input parameters
 * Eg, cycle_check() makes sure that a soon-to-be-added lot doesn't create any cyclic dependencies
*/

class Validator {
    //friend class lotman::Lot;
    friend class lotman::Lot2;
    public:
        Validator();
        static bool cycle_check(std::string start_node, std::vector<std::string> start_parents, std::vector<std::string> start_children); // Only checks for cycles that return to start_node. Returns true if cycle found, false otherwise
        static bool insertion_check(std::string LTBA, std::string parent, std::string child); // Check if lot-to-be-added (LTBA) is being inserted between a parent/child, which should update data for the child
        static bool will_be_orphaned(std::string LTBR, std::string child);

    private:
        //static std::vector<std::string> SQL_get_matches(std::string dynamic_query, std::map<std::string, std::vector<int>> str_map = std::map<std::string, std::vector<int>>(), std::map<int,std::vector<int>> int_map = std::map<int, std::vector<int>>(), std::map<double, std::vector<int>> double_map = std::map<double, std::vector<int>>()); // returns vector of matches to input query
        //static std::vector<std::vector<std::string>> SQL_get_matches_multi_col(std::string dynamic_query, int num_returns, std::map<std::string, std::vector<int>> str_map = std::map<std::string, std::vector<int>>(), std::map<int,std::vector<int>> int_map = std::map<int, std::vector<int>>(), std::map<double, std::vector<int>> double_map = std::map<double, std::vector<int>>()); // returns vector of matches to input query
        static std::pair<std::vector<std::string>, std::string> SQL_get_matches2(std::string dynamic_query, std::map<std::string, std::vector<int>> str_map = std::map<std::string, std::vector<int>>(), std::map<int64_t,std::vector<int>> int_map = std::map<int64_t, std::vector<int>>(), std::map<double, std::vector<int>> double_map = std::map<double, std::vector<int>>());
        static std::pair<std::vector<std::vector<std::string>>, std::string> SQL_get_matches_multi_col2(std::string dynamic_query, int num_returns, std::map<std::string, std::vector<int>> str_map = std::map<std::string, std::vector<int>>(), std::map<int64_t,std::vector<int>> int_map = std::map<int64_t, std::vector<int>>(), std::map<double, std::vector<int>> double_map = std::map<double, std::vector<int>>());
};

} // namespace lotman
