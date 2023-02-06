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
    friend class lotman::Validator;
    public:
        Lot() {}
        //static bool initialize_root(std::string lot_name, std::string lot_path, std::string owner, std::string resource_limits_str, std::string reclamation_policy_str); 
        static bool add_lot(std::string lot_name, std::vector<std::string> owners, std::vector<std::string> parents, std::vector<std::string> children, picojson::array paths, picojson::value management_policy_attrs);
        //static bool remove_sublot(std::string lot_path);
        //static std::string get_parent_lot_name(std::string lot_name, bool get_root=false);
        //static std::vector<std::string> get_sublot_paths(std::string lot_path, bool recursive=false); // TODO: setting recursive to true gets ALL sublot paths of the supplied lot_path
    
    private:
        static bool store_lot(std::string lot_name, std::vector<std::string> owners, std::vector<std::string> parents, std::vector<std::string> children, picojson::array paths, picojson::value management_policy_attrs);
        //static bool remove_lot(std::string lot_name);

};

/**
 * The Validator class has member functions that check if functions in the Lot class should execute.
 * In this way, they check the validity of Lot functions executing with a specific set of input parameters
 * Eg, Lot::initialize_root() should not initialize a root lot that conflicts with other root lots.
*/

class Validator {
    public:
        Validator();
        
        //static bool check_for_parent_child_dirs(std::string lot_path);
        //static std::vector<std::string> get_parent_names(std::string lot_path, bool get_root=false);
        //static std::vector<std::string> get_child_dirs(std::string lot_path, bool recursive=false);

    private:
        //static bool SQL_match_exists(std::string query);
        //static std::vector<std::string> SQL_get_matches(std::string query);

};


} // namespace lotman