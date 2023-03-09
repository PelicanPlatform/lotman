#include <stdio.h>
#include <string>
#include <picojson.h>
#include <vector>
#include "json.hpp"

// TODO: Go through and prefer overloading functions vs distinguishing between int and double inputs

class JsonException : public std::runtime_error {
public:
    JsonException(const std::string &msg)
        : std::runtime_error(msg)
    {}
};

namespace lotman {

class Validator;

/**
 * The Lot class has static member functions for handling top-level actions with lots
 * Eg, adding lots, removing lots, updating lots etc
*/



using json = nlohmann::json;
class Lot2 {
    public:
        std::string lot_name;
        std::string owner;
        std::vector<std::string> parents;
        //std::string parents;
        std::vector<json> paths;
        
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

        bool full_lot = false;


        bool init_full(const json lot_JSON);
        static bool lot_exists(std::string lot_name);
        bool store_lot();

    private:
        bool write_new();

};






















class Lot {
    public:
        Lot() {}
        static bool lot_exists(std::string lot_name);
        static bool is_root(std::string lot_name);
        // TODO: change add_lot to take in a path_map and a management_policy_map. Front end all of the JSON parsing.
        static bool add_lot(std::string lot_name, 
                            std::string owner, 
                            std::vector<std::string> parents, 
                            std::vector<std::string> children, 
                            std::map<std::string, int> paths_map, 
                            std::map<std::string, int> management_policy_attrs_int_map,
                            std::map<std::string, unsigned long long> management_policy_attrs_tmstmp_map,
                            std::map<std::string, double> management_policy_attrs_dbl_map);

        static bool remove_lot(std::string lot_name, 
                               bool assign_LTBR_parent_as_parent_to_orphans, 
                               bool assign_LTBR_parent_as_parent_to_non_orphans, 
                               bool assign_policy_to_children);

        static bool update_lot_params(std::string lot_name, 
                               std::string owner = std::string(), 
                               std::map<std::string, std::string> parents_map = std::map<std::string, std::string>(), 
                               std::map<std::string, int> paths_map = std::map<std::string, int>(), 
                               std::map<std::string, int> management_policy_attrs_int_map = std::map<std::string, int>(), 
                               std::map<std::string, double> management_policy_attrs_double_map = std::map<std::string, double>());
        
        static bool add_to_lot(std::string lot_name,
                               std::vector<std::string> owners = std::vector<std::string>(),
                               std::vector<std::string> parents = std::vector<std::string>(),
                               std::map<std::string, int> paths_map = std::map<std::string, int>());

        static bool update_lot_usage(std::string lot_name,
                                     std::string key,
                                     double value);

        static picojson::object get_lot_usage(const std::string lot_name,
                                              const std::string key,
                                              const bool recursive);

        static std::vector<std::string> get_owners(std::string lot_name,
                                                   bool recursive=false);                               

        static std::vector<std::string> get_parent_names(std::string lot_name, 
                                                         bool recursive=false,
                                                         bool get_self=false);

        static std::vector<std::string> get_children_names(std::string lot_name,
                                                           const bool recursive=false,
                                                           const bool get_self=false);
                                                        
        static picojson::object get_restricting_attribute(const std::string lot_name,
                                                          const std::string key,
                                                          const bool recursive);
        
        static picojson::object get_lot_dirs(const std::string lot_name,
                                             const bool recursive);

        static std::vector<std::string> get_lots_from_owners(picojson::array owners_arr);

        static std::vector<std::string> get_lots_from_parents(picojson::array parents_arr);

        static std::vector<std::string> get_lots_from_children(picojson::array children_arr);

        static std::vector<std::string> get_lots_from_paths(picojson::array paths_arr);

        static std::vector<std::string> get_lots_from_int_policy_attr(std::string key, std::string comparator, int comp_value);

        static std::vector<std::string> get_lots_from_double_policy_attr(std::string key, std::string comparator, double comp_value);

        static std::vector<std::string> get_lots_from_usage(std::string key, std::string comparator, double comp_val, bool recursive);

    private:
        static bool store_lot(std::string lot_name, 
                              std::string owner, 
                              std::vector<std::string> parents,  
                              std::map<std::string, int> paths_map, 
                              std::map<std::string, int> management_policy_attrs_int_map,
                              std::map<std::string, unsigned long long> management_policy_attrs_tmstmp_map,
                              std::map<std::string, double> management_policy_attrs_double_map);

        static bool delete_lot(std::string lot_name);
        static bool store_modifications(std::string dynamic_query, 
                                        std::map<std::string, std::vector<int>> str_map = std::map<std::string, std::vector<int>>(), 
                                        std::map<int,std::vector<int>> int_map = std::map<int, std::vector<int>>(), 
                                        std::map<double, std::vector<int>> double_map = std::map<double, std::vector<int>>());
        static bool store_new_rows(std::string lot_name,
                                  std::vector<std::string> owners = std::vector<std::string>(),
                                  std::vector<std::string> parents = std::vector<std::string>(),
                                  std::map<std::string, int> paths_map = std::map<std::string, int>());
};

/**
 * The Validator class has member functions that check if functions in the Lot class should execute.
 * In this way, they check the validity of Lot functions executing with a specific set of input parameters
 * Eg, cycle_check() makes sure that a soon-to-be-added lot doesn't create any cyclic dependencies
*/

class Validator {
    friend class lotman::Lot;
    friend class lotman::Lot2;
    public:
        Validator();
        static bool cycle_check(std::string start_node, std::vector<std::string> start_parents, std::vector<std::string> start_children); // Only checks for cycles that return to start_node. Returns true if cycle found, false otherwise
        static bool insertion_check(std::string LTBA, std::string parent, std::string child); // Check if lot-to-be-added (LTBA) is being inserted between a parent/child, which should update data for the child
        static bool will_be_orphaned(std::string LTBR, std::string child);

    private:
        static std::vector<std::string> SQL_get_matches(std::string dynamic_query, std::map<std::string, std::vector<int>> str_map = std::map<std::string, std::vector<int>>(), std::map<int,std::vector<int>> int_map = std::map<int, std::vector<int>>(), std::map<double, std::vector<int>> double_map = std::map<double, std::vector<int>>()); // returns vector of matches to input query
        static std::vector<std::vector<std::string>> SQL_get_matches_multi_col(std::string dynamic_query, int num_returns, std::map<std::string, std::vector<int>> str_map = std::map<std::string, std::vector<int>>(), std::map<int,std::vector<int>> int_map = std::map<int, std::vector<int>>(), std::map<double, std::vector<int>> double_map = std::map<double, std::vector<int>>()); // returns vector of matches to input query

};


} // namespace lotman