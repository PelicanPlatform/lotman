// #include <algorithm>
// #include <stdio.h>
// #include <string>
#include <nlohmann/json.hpp>
#include <vector>

namespace lotman {

class Checks;
class Context;
/**
 * The Lot class has member functions for handling top-level actions with lots
 * Eg, adding lots, removing lots, updating lots etc
 */

using json = nlohmann::json;

// std::vector<std::string> get_lname_str_vec(std::vector<Lot> lot_vec) {
//     std::vector<std::string> str_vec(lot_vec.size());
//     std::transform(lot_vec.begin(), lot_vec.end(), str_vec.begin(),
//                     [](const Lot& lot) { return lot.lot_name; });
//     return str_vec;
// }

// std::vector<Lot> get_union_without_intersect(std::vector<std::vector<Lot>> input_vecs) {
//     std::vector<std::string> vecs_union;
//     std::vector<std::string> vecs_intersect;

//     // Convert to strings
//     std::vector<std::vector<std::string>> str_vecs;
//     for (const auto &vec : input_vecs) {
//         str_vecs.push_back(get_lname_str_vec(vec));
//     }

//     // Get the union of the vecs
//     for (const auto &vec : str_vecs) {
//         for (const auto &elem : vec) {
//             if (std::find(vecs_union.begin(), vecs_union.end(), elem) == vecs_union.end()) {
//                 vecs_union.push_back(elem);
//             }
//         }
//     }

//     // Get the intersection
//     vecs_intersect = str_vecs[0];
//     // Sort the first vector to prepare for binary search
//     std::sort(vecs_intersect.begin(), vecs_intersect.end());

//     // Iterate through the remaining vectors
//     for (size_t i = 1; i < str_vecs.size(); ++i) {
//         std::vector<std::string> currentVector = str_vecs[i];
//         std::sort(currentVector.begin(), currentVector.end());

//         std::vector<std::string> tempVector;

//         // Find the common elements using binary search
//         std::set_intersection(vecs_intersect.begin(), vecs_intersect.end(),
//                               currentVector.begin(), currentVector.end(),
//                               std::back_inserter(tempVector));

//         vecs_intersect = std::move(tempVector);
//     }

//     // Now go through union and only collect things not in intersection
//     std::vector<std::string> u_without_i;
//     for (const auto &elem : vecs_union) {
//         if (std::find(vecs_intersect.begin(), vecs_intersect.end(), elem) == vecs_intersect.end()) {
//             u_without_i.push_back(elem);
//         }
//     }

//     return u_without_i;
// }

class Lot {
  public:
	// Non-object values used for lot initialization
	std::string lot_name;
	std::string owner;
	std::vector<std::string> parents;
	std::vector<std::string> children;
	std::vector<json> paths;

	// Things that belong to the lot when including parent/child relationships
	std::string self_owner;
	std::vector<Lot> self_parents;
	bool self_parents_loaded = false;
	std::vector<Lot> self_children;
	bool self_children_loaded = false;

	// Things that belong to the lot when including parent/child relationships
	std::vector<std::string> recursive_owners;
	std::vector<Lot> recursive_parents;
	bool recursive_parents_loaded = false;
	std::vector<Lot> recursive_children;
	bool recursive_children_loaded = false;

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
		bool self_GB_update_staged;
		double children_GB;
		int64_t self_objects;
		bool self_objects_update_staged;
		int64_t children_objects;
		double self_GB_being_written;
		bool self_GB_being_written_update_staged;
		double children_GB_being_written;
		int64_t self_objects_being_written;
		bool self_objects_being_written_update_staged;
		int64_t children_objects_being_written;
	} usage;

	struct {
		bool assign_LTBR_parent_as_parent_to_orphans;
		bool assign_LTBR_parent_as_parent_to_non_orphans;
		bool assign_policy_to_children;
	} reassignment_policy;

	// Should re-evaluate whether all/any of these are needed...
	bool full_lot = false;
	bool has_name = false;
	bool has_reassignment_policy = false;
	bool is_root;

	// Constructors
	Lot() {};
	Lot(const char *lot_name) : lot_name{lot_name}, has_name{true} {};
	Lot(std::string lot_name) : lot_name{lot_name}, has_name{true} {};
	Lot(json lot_JSON) {
		init_full(lot_JSON);
	};

	std::pair<bool, std::string> init_full(json lot_JSON);
	std::pair<bool, std::string> init_reassignment_policy(const bool assign_LTBR_parent_as_parent_to_orphans,
														  const bool assign_LTBR_parent_as_parent_to_non_orphans,
														  const bool assign_policy_to_children);
	void init_self_usage();
	static std::pair<bool, std::string> lot_exists(const std::string &lot_name);
	std::pair<bool, std::string> check_if_root();
	std::pair<bool, std::string> store_lot();
	std::pair<bool, std::string> destroy_lot();
	std::pair<bool, std::string> destroy_lot_recursive();

	std::pair<std::vector<Lot>, std::string> get_children(const bool recursive = false, const bool get_self = false);

	std::pair<std::vector<Lot>, std::string> get_parents(const bool recursive = false, const bool get_self = false);

	std::pair<std::vector<std::string>, std::string> get_owners(const bool recursive = false);

	std::pair<json, std::string> get_restricting_attribute(const std::string &key, const bool recursive);

	std::pair<json, std::string> get_lot_dirs(const bool recursive);
	static std::pair<std::string, std::string> get_lot_from_dir(const std::string &dir_path);

	std::pair<json, std::string> get_lot_usage(const std::string &key, const bool recursive);

	std::pair<bool, std::string> add_parents(const std::vector<Lot> &parents);
	std::pair<bool, std::string> add_paths(const std::vector<json> &paths);

	std::pair<bool, std::string> remove_parents(const std::vector<std::string> &parents);
	std::pair<bool, std::string> remove_paths(const std::vector<std::string> &paths);

	std::pair<bool, std::string> update_owner(const std::string &update_val);
	std::pair<bool, std::string> update_parents(const json &update_arr);
	std::pair<bool, std::string> update_paths(const json &update_arr);
	std::pair<bool, std::string> update_man_policy_attrs(const std::string &update_key, double update_val);
	std::pair<bool, std::string> update_self_usage(const std::string &key, const double value, bool deltaMode);

	std::pair<bool, std::string> recalculate_children_usage();
	static std::pair<bool, std::string> update_db_children_usage();
	std::pair<bool, std::string> update_parent_usage(
		Lot parent, const std::string &update_stmt,
		const std::map<std::string, std::vector<int>> &update_str_map = std::map<std::string, std::vector<int>>(),
		const std::map<int64_t, std::vector<int>> &update_int_map = std::map<int64_t, std::vector<int>>(),
		const std::map<double, std::vector<int>> &update_dbl_map = std::map<double, std::vector<int>>());
	static std::pair<bool, std::string> update_usage_by_dirs(const json &update_JSON, bool deltaMode);
	std::pair<bool, std::string> check_context_for_parents(const std::vector<std::string> &parents,
														   bool include_self = false, bool new_lot = false);
	std::pair<bool, std::string> check_context_for_parents(const std::vector<Lot> &parents, bool include_self = false,
														   bool new_lot = false);

	std::pair<bool, std::string> check_context_for_children(const std::vector<std::string> &children,
															bool include_self = false);
	std::pair<bool, std::string> check_context_for_children(const std::vector<Lot> &children,
															bool include_self = false);
	static std::pair<std::vector<std::string>, std::string> get_lots_past_exp(const bool recursive);
	static std::pair<std::vector<std::string>, std::string> get_lots_past_del(const bool recursive);
	static std::pair<std::vector<std::string>, std::string> get_lots_past_opp(const bool recursive_quota,
																			  const bool recursive_children);
	static std::pair<std::vector<std::string>, std::string> get_lots_past_ded(const bool recursive_quota,
																			  const bool recursive_children);
	static std::pair<std::vector<std::string>, std::string> get_lots_past_obj(const bool recursive_quota,
																			  const bool recursive_children);
	static std::pair<std::vector<std::string>, std::string> list_all_lots();
	static std::pair<std::vector<std::string>, std::string> get_lots_from_dir(const std::string &dir,
																			  const bool recursive);

  private:
	std::pair<bool, std::string> write_new();
	std::pair<bool, std::string> delete_lot_from_db();
	std::pair<bool, std::string> store_new_paths(const std::vector<json> &new_paths);
	std::pair<bool, std::string> store_new_parents(const std::vector<Lot> &new_parents);
	std::pair<bool, std::string> store_updates(
		const std::string &update_query,
		const std::map<std::string, std::vector<int>> &update_str_map = std::map<std::string, std::vector<int>>(),
		const std::map<int64_t, std::vector<int>> &update_int_map = std::map<int64_t, std::vector<int>>(),
		const std::map<double, std::vector<int>> &update_dbl_map = std::map<double, std::vector<int>>());
	std::pair<bool, std::string> remove_parents_from_db(const std::vector<std::string> &parents);
	std::pair<bool, std::string> remove_paths_from_db(const std::vector<std::string> &paths);
};

class DirUsageUpdate : public Lot {
  public:
	int m_depth;
	std::string m_current_path;
	std::string m_parent_prefix;

	DirUsageUpdate() : m_depth{0}, m_current_path{}, m_parent_prefix{} {}
	DirUsageUpdate(int depth, std::string path) : m_depth{depth}, m_current_path{}, m_parent_prefix{path} {}

	std::pair<bool, std::string> JSON_math(json update_JSON, std::vector<Lot> *return_lots) {
		std::map<std::string, double> size_updates;
		std::map<std::string, int64_t> obj_updates;
		std::map<std::string, double> size_writing_updates;
		std::map<std::string, int64_t> obj_writing_updates;

		for (const auto &update : update_JSON) {

			double usage_GB;
			int64_t num_obj;
			double GB_being_written;
			int64_t objects_being_written;
			if (update.contains("size_GB")) {
				usage_GB = update["size_GB"].get<double>();
			}
			if (update.contains("num_obj")) {
				num_obj = update["num_obj"].get<int64_t>();
			}

			if (update.contains("GB_being_written")) {
				GB_being_written = update["GB_being_written"].get<double>();
			}
			if (update.contains("objects_being_written")) {
				objects_being_written = update["objects_being_written"].get<int64_t>();
			}

			std::string path = update["path"];

			if (path.substr(0, 1) != "/") { // get rid of preceding extra slash
				m_current_path = m_parent_prefix + "/" + path;
			} else {
				m_current_path = m_parent_prefix + path;
			}

			// Figure out which lot to associate with the dir
			auto rp_vec_str = get_lots_from_dir(m_current_path, false);
			if (!rp_vec_str.second.empty()) { // There was an error
				std::string int_err = rp_vec_str.second;
				std::string ext_err = "Failure on call to get_lots_from_dir: ";
				return std::make_pair(false, ext_err + int_err);
			}

			Lot lot(rp_vec_str.first[0]);
			lot.init_self_usage();

			// Need a way to check if the path is stored as recursive
			auto rp_json_str = lot.get_lot_dirs(
				false); // Probably don't need all the dirs unless the plan is to use this more than once...

			if (!rp_json_str.second.empty()) { // There was an error
				std::string int_err = rp_json_str.second;
				std::string ext_err = "Failure on call to get_lots_from_dir: ";
				return std::make_pair(false, ext_err + int_err);
			}

			json lot_dirs_json = rp_json_str.first;
			bool recursive;
			if (lot_dirs_json.contains(m_current_path)) {
				recursive = lot_dirs_json[m_current_path]["recursive"].get<bool>();
			} else {
				recursive = false;
			}

			// If the dir includes values from subdirs and it is not recursive, we need to subtract subdir usage per
			// attribute
			if (update["includes_subdirs"].get<bool>() && !recursive) {
				for (const auto &subdir : update["subdirs"]) {
					if (subdir.contains("size_GB")) {
						usage_GB -= subdir["size_GB"].get<double>();
					}
					if (subdir.contains("num_obj")) {
						num_obj -= subdir["num_obj"].get<int64_t>();
					}

					if (subdir.contains("GB_being_written")) {
						GB_being_written -= subdir["GB_being_written"].get<double>();
					}
					if (subdir.contains("objects_being_written")) {
						objects_being_written -= subdir["objects_being_written"].get<int64_t>();
					}
				}
				DirUsageUpdate dirUpdate(m_depth + 1, m_current_path);
				dirUpdate.JSON_math(update["subdirs"], return_lots);
			}

			std::vector<std::string> lot_update_keys;
			// For each of the updates contained in the update json, start staging usage updates on per-lot basis
			if (update.contains("size_GB")) {
				lot.usage.self_GB += usage_GB;
				lot.usage.self_GB_update_staged = true;
			}
			if (update.contains("num_obj")) {
				lot.usage.self_objects += num_obj;
				lot.usage.self_objects_update_staged = true;
			}
			if (update.contains("GB_being_written")) {
				lot.usage.self_GB_being_written += GB_being_written;
				lot.usage.self_GB_being_written_update_staged = true;
			}
			if (update.contains("objects_being_written")) {
				lot.usage.self_objects_being_written += objects_being_written;
				lot.usage.self_objects_being_written_update_staged = true;
			}

			// Get a list of the lot names currently in return_lots
			// the vector of names acts as a proxy for the lots in return_lots
			std::vector<std::string> return_lot_names;
			for (auto &return_lot : *return_lots) {
				return_lot_names.push_back(return_lot.lot_name);
			}

			// If the current lot is not in return_lots, add it
			if (std::find(return_lot_names.begin(), return_lot_names.end(), lot.lot_name) == return_lot_names.end()) {
				return_lots->push_back(lot);
			} else {
				for (auto &return_lot : *return_lots) {
					if (return_lot.lot_name ==
						lot.lot_name) { // They should be treated as same obj, so add their values
						return_lot.usage.self_GB += lot.usage.self_GB;
						return_lot.usage.self_objects += lot.usage.self_objects;
						return_lot.usage.self_GB_being_written += lot.usage.self_GB_being_written;
						return_lot.usage.self_objects_being_written += lot.usage.self_objects_being_written;
					}
				}
			}
		}

		return std::make_pair(true, "");
	}
};

class Context {
  public:
	Context() {}
	static void set_caller(const std::string caller);
	static std::pair<bool, std::string> set_lot_home(const std::string lot_home);

	static std::string get_caller() {
		return *m_caller;
	}
	static std::string get_lot_home() {
		return *m_home;
	}

  private:
	static std::shared_ptr<std::string> m_caller;
	static std::shared_ptr<std::string> m_home;

	static std::vector<std::string> path_split(const std::string dir_path);
	static std::pair<bool, std::string> mkdir_and_parents_if_needed(const std::string dir_path);
};

/**
 * The checks class has member functions that check if functions in the Lot class should execute.
 * In this way, they check the validity of Lot functions executing with a specific set of input parameters
 * Eg, cycle_check() makes sure that a soon-to-be-added lot doesn't create any cyclic dependencies
 */

class Checks {
	friend class lotman::Lot;

  public:
	static bool
	cycle_check(const std::string &start_node, const std::vector<std::string> &start_parents,
				const std::vector<std::string> &start_children); // Only checks for cycles that return to start_node.
																 // Returns true if cycle found, false otherwise
	static bool insertion_check(const std::string &LTBA, const std::string &parent,
								const std::string &child); // Check if lot-to-be-added (LTBA) is being inserted between
														   // a parent/child, which should update data for the child
	static bool will_be_orphaned(const std::string &LTBR, const std::string &child);
};
} // namespace lotman
