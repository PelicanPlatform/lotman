#include <stdio.h>
#include <string>
#include <picojson.h>
#include <vector>



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
class Lot {
    public:
        Lot() {}
        static bool lot_exists(std::string lot_name);
        static bool is_root(std::string lot_name);
        // TODO: change add_lot to take in a path_map and a management_policy_map. Front end all of the JSON parsing.
        static bool add_lot(std::string lot_name, 
                            std::vector<std::string> owners, 
                            std::vector<std::string> parents, 
                            std::vector<std::string> children, 
                            picojson::array paths, 
                            picojson::value management_policy_attrs);

        static bool remove_lot(std::string lot_name, 
                               bool assign_default_as_parent_to_orphans, 
                               bool assign_default_as_parent_to_non_orphans, 
                               bool assign_LTBR_as_parent_to_orphans, 
                               bool assign_LTBR_as_parent_to_non_orphans, 
                               bool assign_policy_to_children);

        static bool update_lot(std::string lot_name, 
                               std::map<std::string, std::string> owners_map = std::map<std::string, std::string>(), 
                               std::map<std::string, std::string> parents_map = std::map<std::string, std::string>(), 
                               std::map<std::string, int> paths_map = std::map<std::string, int>(), 
                               std::map<std::string, int> management_policy_attrs_int_map = std::map<std::string, int>(), 
                               std::map<std::string, double> management_policy_attrs_double_map = std::map<std::string, double>());

        static std::vector<std::string> get_parent_names(std::string lot_name, bool get_root=false);
        //static std::vector<std::string> get_sublot_paths(std::string lot_path, bool recursive=false); // TODO: setting recursive to true gets ALL sublot paths of the supplied lot_path

    private:
        static bool store_lot(std::string lot_name, 
                              std::vector<std::string> owners, 
                              std::vector<std::string> parents, 
                              std::vector<std::string> children, 
                              picojson::array paths, 
                              picojson::value management_policy_attrs);

        static bool delete_lot(std::string lot_name);
        static bool store_modifications(std::string dynamic_query, 
                                        std::map<std::string, std::vector<int>> str_map = std::map<std::string, std::vector<int>>(), 
                                        std::map<int,std::vector<int>> int_map = std::map<int, std::vector<int>>(), 
                                        std::map<double, std::vector<int>> double_map = std::map<double, std::vector<int>>());
};

/**
 * The Validator class has member functions that check if functions in the Lot class should execute.
 * In this way, they check the validity of Lot functions executing with a specific set of input parameters
 * Eg, cycle_check() makes sure that a soon-to-be-added lot doesn't create any cyclic dependencies
*/

class Validator {
    friend class lotman::Lot;
    public:
        Validator();
        static bool cycle_check(std::string start_node, std::vector<std::string> start_parents, std::vector<std::string> start_children); // Only checks for cycles that return to start_node. Returns true if cycle found, false otherwise
        static bool insertion_check(std::string LTBA, std::string parent, std::string child); // Check if lot-to-be-added (LTBA) is being inserted between a parent/child, which should update data for the child
        //static bool check_for_parent_child_dirs(std::string lot_path);
        //static std::vector<std::string> get_parent_names(std::string lot_path, bool get_root=false);
        //static std::vector<std::string> get_child_dirs(std::string lot_path, bool recursive=false);

    private:
        static std::vector<std::string> SQL_get_matches(std::string dynamic_query, std::map<std::string, std::vector<int>> str_map = std::map<std::string, std::vector<int>>(), std::map<int,std::vector<int>> int_map = std::map<int, std::vector<int>>(), std::map<double, std::vector<int>> double_map = std::map<double, std::vector<int>>()); // returns vector of matches to input query
        static bool SQL_update_parent(std::string lot_name, std::string current_parent, std::string parent_set_val); // Returns true if successful, else false

};


} // namespace lotman