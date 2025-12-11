#include "lotman_internal.h"

#include "lotman_db.h"

#include <chrono>
#include <nlohmann/json.hpp>
#include <sys/stat.h>

using json = nlohmann::json;
using namespace lotman;

// TODO: Go through and make things const where they should be declared as such
// TODO: Optimize functions that instantiate a whole lot, when that isn't really needed

/**
 * Functions specific to Lot class
 */

std::pair<bool, std::string> lotman::Lot::init_full(json lot_JSON) {
	lot_name = lot_JSON["lot_name"];
	owner = lot_JSON["owner"];
	parents = lot_JSON["parents"];
	if (!lot_JSON["children"].is_null()) {
		children = lot_JSON["children"];
	}
	if (!lot_JSON["paths"].is_null()) {
		paths = lot_JSON["paths"];
	}

	man_policy_attr.dedicated_GB = lot_JSON["management_policy_attrs"]["dedicated_GB"];
	man_policy_attr.opportunistic_GB = lot_JSON["management_policy_attrs"]["opportunistic_GB"];
	man_policy_attr.max_num_objects = lot_JSON["management_policy_attrs"]["max_num_objects"];
	man_policy_attr.creation_time = lot_JSON["management_policy_attrs"]["creation_time"];
	man_policy_attr.expiration_time = lot_JSON["management_policy_attrs"]["expiration_time"];
	man_policy_attr.deletion_time = lot_JSON["management_policy_attrs"]["deletion_time"];

	usage.self_GB = 0;
	usage.children_GB = 0;
	usage.self_GB_being_written = 0;
	usage.children_GB_being_written = 0;
	usage.self_objects = 0;
	usage.children_objects = 0;
	usage.self_objects_being_written = 0;
	usage.children_objects_being_written = 0;

	full_lot = true;
	return std::make_pair(true, "");
}

std::pair<bool, std::string>
lotman::Lot::init_reassignment_policy(const bool assign_LTBR_parent_as_parent_to_orphans,
									  const bool assign_LTBR_parent_as_parent_to_non_orphans,
									  const bool assign_policy_to_children) {

	reassignment_policy.assign_LTBR_parent_as_parent_to_orphans = assign_LTBR_parent_as_parent_to_orphans;
	reassignment_policy.assign_LTBR_parent_as_parent_to_non_orphans = assign_LTBR_parent_as_parent_to_non_orphans;
	reassignment_policy.assign_policy_to_children = assign_policy_to_children;
	has_reassignment_policy = true;
	return std::make_pair(true, "");
}

void lotman::Lot::init_self_usage() {
	usage.self_GB = 0;
	usage.self_GB_update_staged = false;
	usage.children_GB = 0;
	usage.self_objects = 0;
	usage.self_objects_update_staged = false;
	usage.children_objects = 0;
	usage.self_GB_being_written = 0;
	usage.self_GB_being_written_update_staged = false;
	usage.children_GB_being_written = 0;
	usage.self_objects_being_written = 0;
	usage.self_objects_being_written_update_staged = false;
	usage.children_objects_being_written = 0;
}

std::pair<bool, std::string> lotman::Lot::store_lot() {
	if (!full_lot) {
		return std::make_pair(false, "Lot was not fully initialized");
	}

	// Check that any specified parents already exist, unless the lot has itself as parent
	for (auto &parent : parents) {
		if (parent != lot_name && !lot_exists(parent).first) {
			return std::make_pair(false, "A parent specified for the lot to be added does not exist in the database.");
		}
	}

	// Check that any specified children already exist
	if (children.size() > 0) {
		for (auto &child : children) {
			if (!lot_exists(child).first) {
				return std::make_pair(false,
									  "A child specified for the lot to be added does not exist in the database");
			}
		}
	}

	// Check that the added lot won't introduce any cycles
	bool self_parent; // When checking for cycles, we only care about lots who specify a parent other than themselves
	auto self_parent_iter = std::find(parents.begin(), parents.end(), lot_name);

	self_parent = (self_parent_iter != parents.end());
	if (!children.empty() && ((parents.size() == 1 && !self_parent) ||
							  (parents.size() > 1))) { // If there are children and a non-self parent
		bool cycle_exists = lotman::Checks::cycle_check(lot_name, parents, children);
		if (cycle_exists) {
			return std::make_pair(
				false, "The lot cannot be added because the combination of parents/children would introduce a "
					   "dependency cycle in the data structure."); // Return false, don't do anything with the lot
		}
	}

	// Store the lot, begin updating other lots after confirming the lot has been successfully stored
	auto rp = this->write_new();
	if (!rp.first) {
		std::string int_err = rp.second;
		std::string ext_err = "Failure to store new lot: ";
		return std::make_pair(false, ext_err + int_err);
	}

	bool parent_updated = true;
	for (auto &parents_iter : parents) {
		for (auto &children_iter : children) {
			if (lotman::Checks::insertion_check(lot_name, parents_iter, children_iter)) {
				// Update child to have lot_name as a parent instead of parents_iter. Later, save LTBA with all its
				// specified parents.
				Lot child(children_iter);

				json update_arr = json::array();
				update_arr.push_back({{"current", parents_iter}, {"new", lot_name}});

				rp = child.update_parents(update_arr);
				parent_updated = rp.first;
				if (!parent_updated) {
					std::string int_err = rp.second;
					std::string ext_err = "Failure on call to child.update_parents: ";
					return std::make_pair(false, ext_err + int_err);
				}
			}
		}
	}
	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::lot_exists(const std::string &lot_name) {
	try {
		auto &storage = db::StorageManager::get_storage();
		using namespace sqlite_orm;
		auto count = storage.count<db::ManagementPolicyAttributes>(
			where(c(&db::ManagementPolicyAttributes::lot_name) == lot_name));
		return std::make_pair(count > 0, "");
	} catch (const std::exception &e) {
		return std::make_pair(false, std::string("lot_exists failed: ") + e.what());
	}
}

std::pair<bool, std::string> lotman::Lot::check_if_root() {
	try {
		auto &storage = db::StorageManager::get_storage();
		using namespace sqlite_orm;
		auto parent_records = storage.select(&db::Parent::parent, where(c(&db::Parent::lot_name) == lot_name));

		if (parent_records.size() == 1 && parent_records[0] == lot_name) {
			// lot_name has only itself as a parent, indicating root
			is_root = true;
			return std::make_pair(true, "");
		} else {
			is_root = false;
			return std::make_pair(false, "");
		}
	} catch (const std::exception &e) {
		return std::make_pair(false, std::string("check_if_root failed: ") + e.what());
	}
}

std::pair<bool, std::string> lotman::Lot::destroy_lot() {

	/*
	FUNCTION FLOW
	Prechecks:
	* Check for reassignment policy. Lots MUST have a reassignment policy before deletion
	* Check if lot-to-be-removed (LTBR) is the default lot. The default lot MUST NOT be deleted while other lots exist.
	LotMan provides no method for deleting the default lot.
	* Check that LTBR actually exists in the database

	Meat:
	* Get the LTBR's immediate children, who need to be reassigned
	* Handle children according to policy
	* Delete LTBR
	*/

	if (!has_reassignment_policy) {
		return std::make_pair(false, "The lot has no defined reassignment policy.");
	}
	if (lot_name == "default") {
		return std::make_pair(false, "The default lot cannot be deleted.");
	}

	// Prechecks complete, get the children for LTBR
	auto rp_lotvec_str = this->get_children();
	if (!rp_lotvec_str.second.empty()) { // There is an error message
		std::string int_err = rp_lotvec_str.second;
		std::string ext_err = "Failed to get lot children: ";
		return std::make_pair(false, ext_err + int_err);
	}

	// std::vector<Lot> children = rp_lotvec_str.first;
	//  If there are no children, the lot can be deleted without issue
	if (self_children.empty()) {
		auto rp_bool_str = this->delete_lot_from_db();
		if (!rp_bool_str.first) {
			std::string int_err = rp_bool_str.second;
			std::string ext_err = "Failed to delete the lot from the database: ";
			return std::make_pair(false, ext_err + int_err);
		}
		return std::make_pair(true, "");
	}

	// Reaching this point means there are children --> Reassign them

	for (auto &child : self_children) {
		if (lotman::Checks::will_be_orphaned(lot_name,
											 child.lot_name)) { // Indicates whether LTBR is the only parent to child
			if (reassignment_policy.assign_LTBR_parent_as_parent_to_orphans) {
				auto rp_bool_str = this->check_if_root();
				if (!rp_bool_str.second.empty()) {
					std::string int_err = rp_bool_str.second;
					std::string ext_err = "Function call to lotman::Lot::check_if_root failed: ";
					return std::make_pair(false, ext_err + int_err);
				}
				if (is_root) {
					return std::make_pair(
						false, "The lot being removed is a root, and has no parents to assign to its children.");
				}

				// Since lots have only one explicit owner, we cannot reassign ownership.

				// Generate parents of current lot for assignment to children -- only need immediate parents
				this->get_parents();
				// Now assign parents of LTBR to orphan children of LTBR
				// No need to perform cycle check in this case
				rp_bool_str = child.add_parents(self_parents);
				if (!rp_bool_str.first) {
					std::string int_err = rp_bool_str.second;
					std::string ext_err = "Failure on call to lotman::Lot::add_parents for child lot: ";
					return std::make_pair(false, ext_err + int_err);
				}
			} else {
				return std::make_pair(false, "The operation cannot be completed as requested because deleting the lot "
											 "would create an orphan that requires explicit assignment to the default "
											 "lot. Set assign_LTBR_parent_as_parent_to_orphans=true.");
			}
		}
		if (reassignment_policy.assign_LTBR_parent_as_parent_to_non_orphans) {
			auto rp_bool_str = this->check_if_root();
			if (!rp_bool_str.second.empty()) {
				std::string int_err = rp_bool_str.second;
				std::string ext_err = "Function call to lotman::Lot::check_if_root failed: ";
				return std::make_pair(false, ext_err + int_err);
			}
			if (is_root) {
				return std::make_pair(false,
									  "The lot being removed is a root, and has no parents to assign to its children.");
			}
			this->get_parents();
			// Now assign parents of LTBR to non-orphan children of LTBR
			// No need to perform cycle check in this case
			rp_bool_str = child.add_parents(self_parents);
			if (!rp_bool_str.first) {
				std::string int_err = rp_bool_str.second;
				std::string ext_err = "Function call to lotman::Lot::add_parents for child lot failed: ";
				return std::make_pair(false, ext_err + int_err);
			}
		}
	}
	auto rp_bool_str = delete_lot_from_db();
	if (!rp_bool_str.first) {
		std::string int_err = rp_bool_str.second;
		std::string ext_err = "Function call to lotman::Lot::delete_lot_from_db failed: ";
		return std::make_pair(false, ext_err + int_err);
	}
	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::destroy_lot_recursive() {

	if (lot_name == "default") {
		return std::make_pair(false, "The default lot cannot be deleted.");
	}

	// Prechecks complete, get the children for LTBR
	auto rp_lotvec_str = this->get_children(true);
	if (!rp_lotvec_str.second.empty()) { // There is an error message
		std::string int_err = rp_lotvec_str.second;
		std::string ext_err = "Failed to get lot children: ";
		return std::make_pair(false, ext_err + int_err);
	}

	for (auto &child : recursive_children) {
		auto rp_bool_str = child.delete_lot_from_db();
		if (!rp_bool_str.first) {
			std::string int_err = rp_bool_str.second;
			std::string ext_err = "Failed to delete a lot from the database: ";
			return std::make_pair(false, ext_err + int_err);
		}
	}
	this->delete_lot_from_db();
	return std::make_pair(true, "");
}

std::pair<std::vector<Lot>, std::string> lotman::Lot::get_parents(const bool recursive, const bool get_self) {

	std::vector<Lot> parents;
	std::vector<std::string> parent_names_vec;

	try {
		auto &storage = db::StorageManager::get_storage();
		using namespace sqlite_orm;

		if (get_self) {
			parent_names_vec = storage.select(&db::Parent::parent, where(c(&db::Parent::lot_name) == lot_name));
		} else {
			parent_names_vec = storage.select(&db::Parent::parent, where(c(&db::Parent::lot_name) == lot_name and
																		 c(&db::Parent::parent) != lot_name));
		}

		if (recursive) {
			std::vector<std::string> current_parents{parent_names_vec};
			while (current_parents.size() > 0) {
				std::vector<std::string> grandparent_names;

				// Do not set const, because we may be updating these parents later
				for (auto &parent : current_parents) {
					auto gp = storage.select(&db::Parent::parent, where(c(&db::Parent::lot_name) == parent and
																		c(&db::Parent::parent) != parent));
					grandparent_names.insert(grandparent_names.end(), gp.begin(), gp.end());
				}

				// grandparent_names might have duplicates. Sort and make unique
				std::sort(grandparent_names.begin(), grandparent_names.end());
				auto last = std::unique(grandparent_names.begin(), grandparent_names.end());
				grandparent_names.erase(last, grandparent_names.end());

				current_parents = grandparent_names;
				parent_names_vec.insert(parent_names_vec.end(), grandparent_names.begin(), grandparent_names.end());
			}
		}

		// Final sort
		std::sort(parent_names_vec.begin(), parent_names_vec.end());
		auto last = std::unique(parent_names_vec.begin(), parent_names_vec.end());
		parent_names_vec.erase(last, parent_names_vec.end());

		// children_names now has names of all children according to get_self and recursion.
		// Create lot objects and return vector of lots
		for (const auto &parent_name : parent_names_vec) {
			Lot lot(parent_name);
			parents.push_back(lot);
		}

		// Assign to lot member vars.
		if (recursive) {
			recursive_parents = parents;
			recursive_parents_loaded = true;
		} else {
			self_parents = parents;
			self_parents_loaded = true;
		}
		return std::make_pair(parents, "");
	} catch (const std::exception &e) {
		return std::make_pair(std::vector<Lot>(), std::string("get_parents failed: ") + e.what());
	}
}

std::pair<std::vector<Lot>, std::string> lotman::Lot::get_children(const bool recursive, const bool get_self) {

	/*
	FUNCTION FLOW
	* Create queries and maps based on get_self
	* Get vector of immediate children
	* If recursive, get children of children
	*/

	std::vector<std::string> children_names;
	std::vector<Lot> children;
	try {
		auto &storage = db::StorageManager::get_storage();
		using namespace sqlite_orm;

		// Get first round of children
		if (get_self) {
			children_names = storage.select(&db::Parent::lot_name, where(c(&db::Parent::parent) == lot_name));
		} else {
			children_names = storage.select(&db::Parent::lot_name, where(c(&db::Parent::parent) == lot_name and
																		 c(&db::Parent::lot_name) != lot_name));
		}

		if (recursive) {
			std::vector<std::string> current_children_names{children_names};
			while (current_children_names.size() > 0) {
				std::vector<std::string> grandchildren_names;
				for (const auto &child_name : current_children_names) {
					auto gc = storage.select(&db::Parent::lot_name, where(c(&db::Parent::parent) == child_name and
																		  c(&db::Parent::lot_name) != child_name));
					grandchildren_names.insert(grandchildren_names.end(), gc.begin(), gc.end());
				}
				// grandchildren_names might have duplicates. Sort and make unique
				std::sort(grandchildren_names.begin(), grandchildren_names.end());
				auto last = std::unique(grandchildren_names.begin(), grandchildren_names.end());
				grandchildren_names.erase(last, grandchildren_names.end());

				current_children_names = grandchildren_names;
				children_names.insert(children_names.end(), grandchildren_names.begin(), grandchildren_names.end());
			}
		}

		// Final sort
		std::sort(children_names.begin(), children_names.end());
		auto last = std::unique(children_names.begin(), children_names.end());
		children_names.erase(last, children_names.end());

		// children_names now has names of all children according to get_self and recursion.
		// Create lot objects and return vector of lots
		for (const auto &child_name : children_names) {
			Lot lot(child_name);
			children.push_back(lot);
		}

		// Assign to lot member vars
		if (recursive) {
			recursive_children = children;
			recursive_children_loaded = true;
		} else {
			self_children = children;
			self_children_loaded = true;
		}
		return std::make_pair(children, "");
	} catch (const std::exception &e) {
		return std::make_pair(std::vector<Lot>(), std::string("get_children failed: ") + e.what());
	}
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_owners(const bool recursive) {
	std::vector<std::string> lot_owners_vec;

	try {
		auto &storage = db::StorageManager::get_storage();
		using namespace sqlite_orm;

		auto owners = storage.select(&db::Owner::owner, where(c(&db::Owner::lot_name) == lot_name));
		if (!owners.empty()) {
			lot_owners_vec.push_back(owners[0]);
		}

		if (recursive) {
			auto rp2 = this->get_parents(true, false);
			if (!rp2.second.empty()) { // There is an error message
				std::string int_err = rp2.second;
				std::string ext_err = "Failure to get parents: ";
				return std::make_pair(std::vector<std::string>(), ext_err + int_err);
			}
			std::vector<Lot> parents = rp2.first;

			for (const auto &parent : parents) {
				auto parent_owners =
					storage.select(&db::Owner::owner, where(c(&db::Owner::lot_name) == parent.lot_name));
				lot_owners_vec.insert(lot_owners_vec.end(), parent_owners.begin(), parent_owners.end());
			}
		}

		// Sort and remove any duplicates
		std::sort(lot_owners_vec.begin(), lot_owners_vec.end());
		auto last = std::unique(lot_owners_vec.begin(), lot_owners_vec.end());
		lot_owners_vec.erase(last, lot_owners_vec.end());

		// Assign to lot vars
		if (recursive) {
			recursive_owners = lot_owners_vec;
		} else {
			if (!lot_owners_vec.empty()) {
				self_owner = lot_owners_vec[0]; // Lots only have one explicit owner
			}
		}
		return std::make_pair(lot_owners_vec, "");
	} catch (const std::exception &e) {
		return std::make_pair(std::vector<std::string>(), std::string("get_owners failed: ") + e.what());
	}
}

std::pair<json, std::string> lotman::Lot::get_restricting_attribute(const std::string &key, const bool recursive) {
	json internal_obj;
	std::vector<std::string> value;
	std::array<std::string, 6> allowed_keys{
		{"dedicated_GB", "opportunistic_GB", "max_num_objects", "creation_time", "expiration_time", "deletion_time"}};
	if (std::find(allowed_keys.begin(), allowed_keys.end(), key) != allowed_keys.end()) {
		std::string policy_attr_query = "SELECT " + key + " FROM management_policy_attributes WHERE lot_name = ?;";
		std::map<std::string, std::vector<int>> policy_attr_query_str_map{{lot_name, {1}}};
		auto rp = lotman::db::SQL_get_matches(policy_attr_query, policy_attr_query_str_map);
		if (!rp.second.empty()) { // There was an error
			std::string int_err = rp.second;
			std::string ext_err = "Failure on call to SQL_get_matches: ";
			return std::make_pair(json(), ext_err + int_err);
		}
		value = rp.first;
		std::string restricting_parent_name = lot_name;

		if (recursive) {
			auto rp2 = this->get_parents(true);
			if (!rp2.second.empty()) { // There was an error
				std::string int_err = rp2.second;
				std::string ext_err = "Failure to get lot parents: ";
				return std::make_pair(json(), ext_err + int_err);
			}

			std::vector<lotman::Lot> parents = rp2.first;
			for (const auto &parent : parents) {
				std::map<std::string, std::vector<int>> policy_attr_query_parent_str_map{{parent.lot_name, {1}}};
				rp = lotman::db::SQL_get_matches(policy_attr_query, policy_attr_query_parent_str_map);
				if (!rp.second.empty()) { // There was an error
					std::string int_err = rp.second;
					std::string ext_err = "Failure on call to SQL_get_matches: ";
					return std::make_pair(json(), ext_err + int_err);
				}

				std::vector<std::string> compare_value = rp.first;
				if (std::stod(compare_value[0]) < std::stod(value[0])) {
					value[0] = compare_value[0];
					restricting_parent_name = parent.lot_name;
				}
			}
			internal_obj["lot_name"] = restricting_parent_name;
			internal_obj["value"] = std::stod(value[0]);
		} else {
			internal_obj["value"] = std::stod(value[0]);
		}
		return std::make_pair(internal_obj, "");
	} else {
		return std::make_pair(json(), " The key \"" + key + "\" is not recognized.");
	}
}

std::pair<json, std::string> lotman::Lot::get_lot_dirs(const bool recursive) {
	json path_arr = json::array();

	try {
		auto &storage = db::StorageManager::get_storage();
		using namespace sqlite_orm;

		auto path_records = storage.get_all<db::Path>(where(c(&db::Path::lot_name) == lot_name));

		for (const auto &path_rec : path_records) {
			json path_obj_internal;
			path_obj_internal["lot_name"] = this->lot_name;
			path_obj_internal["recursive"] = static_cast<bool>(path_rec.recursive);
			path_obj_internal["path"] = path_rec.path;
			path_arr.push_back(path_obj_internal);
		}

		if (recursive) { // Not recursion of path, but recursion of dirs associated to a lot
			auto rp_vec_str = this->get_children(true);
			if (!rp_vec_str.second.empty()) { // There was an error
				std::string int_err = rp_vec_str.second;
				std::string ext_err = "Failure to get children.";
				return std::make_pair(json::array(), ext_err + int_err);
			}
			for (const auto &child : recursive_children) {
				auto child_path_records = storage.get_all<db::Path>(where(c(&db::Path::lot_name) == child.lot_name));

				for (const auto &path_rec : child_path_records) {
					json path_obj_internal;
					path_obj_internal["lot_name"] = child.lot_name;
					path_obj_internal["recursive"] = static_cast<bool>(path_rec.recursive);
					path_obj_internal["path"] = path_rec.path;
					path_arr.push_back(path_obj_internal);
				}
			}
		}

		return std::make_pair(path_arr, "");
	} catch (const std::exception &e) {
		return std::make_pair(path_arr, std::string("get_lot_dirs failed: ") + e.what());
	}
}

std::pair<std::string, std::string> lotman::Lot::get_lot_from_dir(const std::string &dir_path) {
	try {
		auto &storage = db::StorageManager::get_storage();
		using namespace sqlite_orm;

		// Normalize path with trailing slash to match stored format
		std::string normalized_path = ensure_trailing_slash(dir_path);
		auto lot_names = storage.select(&db::Path::lot_name, where(c(&db::Path::path) == normalized_path));

		if (lot_names.empty()) {
			return std::make_pair("", ""); // Nothing existed, and no error. Return empty strings!
		}
		return std::make_pair(lot_names[0], "");
	} catch (const std::exception &e) {
		return std::make_pair("", std::string("get_lot_from_dir failed: ") + e.what());
	}
}

std::pair<json, std::string> lotman::Lot::get_lot_usage(const std::string &key, const bool recursive) {

	// TODO: Introduce some notion of verbocity to give options for output, like:
	// {"dedicated_GB" : 10} vs {"dedicated_GB" : {"personal": 5, "children" : 5}} vs {"dedicated_GB" : {"personal" : 5,
	// "child1" : 2.5, "child2" : 2.5}} Think a bit more about whether this makes sense.

	// TODO: Might be worthwhile to join some of these sections that share a common preamble

	json output_obj;
	std::array<std::string, 6> allowed_keys = {"dedicated_GB", "opportunistic_GB", "total_GB",
											   "num_objects",  "GB_being_written", "objects_being_written"};
	if (std::find(allowed_keys.begin(), allowed_keys.end(), key) == allowed_keys.end()) {
		return std::make_pair(json(), "The key \"" + key + "\" is not recognized.");
	}

	std::vector<std::string> query_output;
	std::vector<std::vector<std::string>> query_multi_out;

	if (key == "dedicated_GB") {
		if (recursive) {
			std::string rec_ded_usage_query =
				"SELECT "
				"CASE "
				"WHEN lot_usage.self_GB + lot_usage.children_GB <= management_policy_attributes.dedicated_GB THEN "
				"lot_usage.self_GB + lot_usage.children_GB "
				"ELSE management_policy_attributes.dedicated_GB "
				"END AS total, " // For readability, not actually referencing these column names
				"CASE "
				"WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB THEN "
				"management_policy_attributes.dedicated_GB "
				"ELSE lot_usage.self_GB "
				"END AS self_contrib, "
				"CASE "
				"WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB THEN '0' "
				"WHEN lot_usage.self_GB + lot_usage.children_GB >= management_policy_attributes.dedicated_GB THEN "
				"management_policy_attributes.dedicated_GB - lot_usage.self_GB "
				"ELSE lot_usage.children_GB "
				"END AS children_contrib "
				"FROM lot_usage "
				"INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
				"WHERE lot_usage.lot_name = ?;";
			std::map<std::string, std::vector<int>> ded_GB_query_str_map{{lot_name, {1}}};
			auto rp_multi = lotman::db::SQL_get_matches_multi_col(rec_ded_usage_query, 3, ded_GB_query_str_map);
			if (!rp_multi.second.empty()) { // There was an error
				std::string int_err = rp_multi.second;
				std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
				return std::make_pair(json(), ext_err + int_err);
			}
			query_multi_out = rp_multi.first;
			output_obj["total"] = std::stod(query_multi_out[0][0]);
			output_obj["self_contrib"] = std::stod(query_multi_out[0][1]);
			output_obj["children_contrib"] = std::stod(query_multi_out[0][2]);
		} else {
			std::string ded_GB_query =
				"SELECT "
				"CASE "
				"WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB THEN "
				"management_policy_attributes.dedicated_GB "
				"ELSE lot_usage.self_GB "
				"END AS total "
				"FROM "
				"lot_usage "
				"INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
				"WHERE lot_usage.lot_name = ?;";

			std::map<std::string, std::vector<int>> ded_GB_query_str_map{{lot_name, {1}}};
			auto rp_single = lotman::db::SQL_get_matches(ded_GB_query, ded_GB_query_str_map);
			if (!rp_single.second.empty()) { // There was an error
				std::string int_err = rp_single.second;
				std::string ext_err = "Failure on call to SQL_get_matches: ";
				return std::make_pair(json(), ext_err + int_err);
			}
			query_output = rp_single.first;
			output_obj["self_contrib"] = std::stod(query_output[0]);
		}
	}

	else if (key == "opportunistic_GB") {
		if (recursive) {
			std::string rec_opp_usage_query =
				"SELECT "
				"CASE "
				"WHEN lot_usage.self_GB + lot_usage.children_GB >= management_policy_attributes.opportunistic_GB "
				"+management_policy_attributes.dedicated_GB THEN management_policy_attributes.opportunistic_GB "
				"WHEN lot_usage.self_GB + lot_usage.children_GB >= management_policy_attributes.dedicated_GB THEN "
				"lot_usage.self_GB + lot_usage.children_GB - management_policy_attributes.dedicated_GB "
				"ELSE '0' "
				"END AS total, "
				"CASE "
				"WHEN lot_usage.self_GB >= management_policy_attributes.opportunistic_GB + "
				"management_policy_attributes.dedicated_GB THEN management_policy_attributes.opportunistic_GB "
				"WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB THEN  lot_usage.self_GB - "
				"management_policy_attributes.dedicated_GB "
				"ELSE '0' "
				"END AS self_contrib, "
				"CASE "
				"WHEN lot_usage.self_GB >= management_policy_attributes.opportunistic_GB + "
				"management_policy_attributes.dedicated_GB THEN '0' "
				"WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB AND lot_usage.self_GB + "
				"lot_usage.children_GB >= management_policy_attributes.opportunistic_GB + "
				"management_policy_attributes.dedicated_GB THEN management_policy_attributes.opportunistic_GB + "
				"management_policy_attributes.dedicated_GB - lot_usage.self_GB "
				"WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB AND lot_usage.self_GB + "
				"lot_usage.children_GB < management_policy_attributes.opportunistic_GB + "
				"management_policy_attributes.dedicated_GB THEN lot_usage.children_GB "
				"WHEN lot_usage.self_GB < management_policy_attributes.dedicated_GB AND lot_usage.self_GB + "
				"lot_usage.children_GB >= management_policy_attributes.opportunistic_GB + "
				"management_policy_attributes.dedicated_GB THEN management_policy_attributes.opportunistic_GB "
				"WHEN lot_usage.self_GB < management_policy_attributes.dedicated_GB AND lot_usage.self_GB + "
				"lot_usage.children_GB > management_policy_attributes.dedicated_GB THEN lot_usage.self_GB + "
				"lot_usage.children_GB - management_policy_attributes.dedicated_GB "
				"ELSE '0' "
				"END AS children_contrib "
				"FROM "
				"lot_usage "
				"INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
				"WHERE lot_usage.lot_name = ?;";
			std::map<std::string, std::vector<int>> opp_GB_query_str_map{{lot_name, {1}}};
			auto rp_multi = lotman::db::SQL_get_matches_multi_col(rec_opp_usage_query, 3, opp_GB_query_str_map);
			if (!rp_multi.second.empty()) { // There was an error
				std::string int_err = rp_multi.second;
				std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
				return std::make_pair(json(), ext_err + int_err);
			}
			query_multi_out = rp_multi.first;
			output_obj["total"] = std::stod(query_multi_out[0][0]);
			output_obj["self_contrib"] = std::stod(query_multi_out[0][1]);
			output_obj["children_contrib"] = std::stod(query_multi_out[0][2]);
		} else {
			std::string opp_GB_query =
				"SELECT "
				"CASE "
				"WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB + "
				"management_policy_attributes.opportunistic_GB THEN management_policy_attributes.opportunistic_GB "
				"WHEN lot_usage.self_GB >= management_policy_attributes.dedicated_GB THEN lot_usage.self_GB = "
				"management_policy_attributes.dedicated_GB "
				"ELSE '0' "
				"END AS total "
				"FROM "
				"lot_usage "
				"INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
				"WHERE lot_usage.lot_name = ?;";

			std::map<std::string, std::vector<int>> opp_GB_query_str_map{{lot_name, {1}}};
			auto rp_single = lotman::db::SQL_get_matches(opp_GB_query, opp_GB_query_str_map);
			if (!rp_single.second.empty()) { // There was an error
				std::string int_err = rp_single.second;
				std::string ext_err = "Failure on call to SQL_get_matches: ";
				return std::make_pair(json(), ext_err + int_err);
			}
			query_output = rp_single.first;
			output_obj["self_contrib"] = std::stod(query_output[0]);
		}
	}

	else if (key == "total_GB") {
		// Get the total usage
		if (recursive) {
			// Need to consider usage from children
			std::string child_usage_GB_query = "SELECT self_GB, children_GB FROM lot_usage WHERE lot_name = ?;";
			std::map<std::string, std::vector<int>> child_usage_GB_str_map{{lot_name, {1}}};
			auto rp_multi = lotman::db::SQL_get_matches_multi_col(child_usage_GB_query, 2, child_usage_GB_str_map);
			if (!rp_multi.second.empty()) { // There was an error
				std::string int_err = rp_multi.second;
				std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
				return std::make_pair(json(), ext_err + int_err);
			}
			query_multi_out = rp_multi.first;
			output_obj["self_contrib"] = std::stod(query_multi_out[0][0]);
			output_obj["children_contrib"] = std::stod(query_multi_out[0][1]);
			output_obj["total"] = std::stod(query_multi_out[0][0]) + std::stod(query_multi_out[0][1]);
		} else {
			std::string usage_GB_query = "SELECT self_GB FROM lot_usage WHERE lot_name = ?;";
			std::map<std::string, std::vector<int>> usage_GB_str_map{{lot_name, {1}}};
			auto rp_single = lotman::db::SQL_get_matches(usage_GB_query, usage_GB_str_map);
			if (!rp_single.second.empty()) { // There was an error
				std::string int_err = rp_single.second;
				std::string ext_err = "Failure on call to SQL_get_matches: ";
				return std::make_pair(json(), ext_err + int_err);
			}
			query_output = rp_single.first;
			output_obj["self_contrib"] = std::stod(query_output[0]);
		}
	}

	else if (key == "num_objects") {
		if (recursive) {
			std::string rec_num_obj_query = "SELECT self_objects, children_objects FROM lot_usage WHERE lot_name = ?;";
			std::map<std::string, std::vector<int>> rec_num_obj_str_map{{lot_name, {1}}};
			auto rp_multi = lotman::db::SQL_get_matches_multi_col(rec_num_obj_query, 2, rec_num_obj_str_map);
			if (!rp_multi.second.empty()) { // There was an error
				std::string int_err = rp_multi.second;
				std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
				return std::make_pair(json(), ext_err + int_err);
			}
			query_multi_out = rp_multi.first;
			output_obj["self_contrib"] = std::stod(query_multi_out[0][0]);
			output_obj["children_contrib"] = std::stod(query_multi_out[0][1]);
			output_obj["total"] = std::stod(query_multi_out[0][0]) + std::stod(query_multi_out[0][1]);
		} else {

			std::string num_obj_query = "SELECT self_objects FROM lot_usage WHERE lot_name = ?;";
			std::map<std::string, std::vector<int>> num_obj_str_map{{lot_name, {1}}};
			auto rp_single = lotman::db::SQL_get_matches(num_obj_query, num_obj_str_map);
			if (!rp_single.second.empty()) { // There was an error
				std::string int_err = rp_single.second;
				std::string ext_err = "Failure on call to SQL_get_matches: ";
				return std::make_pair(json(), ext_err + int_err);
			}
			query_output = rp_single.first;
			output_obj["self_contrib"] = std::stod(query_output[0]);
		}
	}

	else if (key == "GB_being_written") {
		if (recursive) {
			std::string rec_GB_being_written_query =
				"SELECT self_GB_being_written, children_GB_being_written FROM lot_usage WHERE lot_name = ?;";
			std::map<std::string, std::vector<int>> rec_GB_being_written_str_map{{lot_name, {1}}};
			auto rp_multi =
				lotman::db::SQL_get_matches_multi_col(rec_GB_being_written_query, 2, rec_GB_being_written_str_map);
			if (!rp_multi.second.empty()) { // There was an error
				std::string int_err = rp_multi.second;
				std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
				return std::make_pair(json(), ext_err + int_err);
			}
			query_multi_out = rp_multi.first;
			output_obj["self_contrib"] = std::stod(query_multi_out[0][0]);
			output_obj["children_contrib"] = std::stod(query_multi_out[0][1]);
			output_obj["total"] = std::stod(query_multi_out[0][0]) + std::stod(query_multi_out[0][1]);
		} else {

			std::string GB_being_written_query = "SELECT self_GB_being_written FROM lot_usage WHERE lot_name = ?;";
			std::map<std::string, std::vector<int>> GB_being_written_str_map{{lot_name, {1}}};
			auto rp_single = lotman::db::SQL_get_matches(GB_being_written_query, GB_being_written_str_map);
			if (!rp_single.second.empty()) { // There was an error
				std::string int_err = rp_single.second;
				std::string ext_err = "Failure on call to SQL_get_matches: ";
				return std::make_pair(json(), ext_err + int_err);
			}
			query_output = rp_single.first;
			output_obj["self_contrib"] = std::stod(query_output[0]);
		}
	}

	else if (key == "objects_being_written") {
		if (recursive) {
			std::string rec_objects_being_written_query =
				"SELECT self_objects_being_written, children_objects_being_written FROM lot_usage WHERE lot_name = ?;";
			std::map<std::string, std::vector<int>> rec_objects_being_written_str_map{{lot_name, {1}}};
			auto rp_multi = lotman::db::SQL_get_matches_multi_col(rec_objects_being_written_query, 2,
																  rec_objects_being_written_str_map);
			if (!rp_multi.second.empty()) { // There was an error
				std::string int_err = rp_multi.second;
				std::string ext_err = "Failure on call to SQL_get_matches_multi_col: ";
				return std::make_pair(json(), ext_err + int_err);
			}
			query_multi_out = rp_multi.first;
			output_obj["self_contrib"] = std::stod(query_multi_out[0][0]);
			output_obj["children_contrib"] = std::stod(query_multi_out[0][1]);
			output_obj["total"] = std::stod(query_multi_out[0][0]) + std::stod(query_multi_out[0][1]);
		} else {

			std::string objects_being_written_query =
				"SELECT self_objects_being_written FROM lot_usage WHERE lot_name = ?;";
			std::map<std::string, std::vector<int>> objects_being_written_str_map{{lot_name, {1}}};
			auto rp_single = lotman::db::SQL_get_matches(objects_being_written_query, objects_being_written_str_map);
			if (!rp_single.second.empty()) { // There was an error
				std::string int_err = rp_single.second;
				std::string ext_err = "Failure on call to SQL_get_matches: ";
				return std::make_pair(json(), ext_err + int_err);
			}
			query_output = rp_single.first;
			output_obj["self_contrib"] = std::stod(query_output[0]);
		}
	}

	return std::make_pair(output_obj, "");
}

std::pair<bool, std::string> lotman::Lot::add_parents(const std::vector<Lot> &parents) {
	// Perform a cycle check
	// Build the list of all proposed parents
	std::vector<std::string> parent_names;
	this->get_parents(true, true);
	for (const auto &parent : recursive_parents) {
		parent_names.push_back(parent.lot_name);
	}
	for (const auto &parent_lot : parents) {
		parent_names.push_back(parent_lot.lot_name);
	}

	// Build list of all children, minus self
	std::vector<std::string> children_names;
	this->get_children(true, false);
	for (const auto &child : recursive_children) {
		children_names.push_back(child.lot_name);
	}

	// Perform the cycle check
	if (Checks::cycle_check(lot_name, parent_names, children_names)) {
		std::string err = "The requested parent addition would introduce a dependency cycle.";
		return std::make_pair(false, err);
	}

	// We've passed the cycle check, store the parents
	auto rp = store_new_parents(parents);
	if (!rp.first) {
		std::string int_err = rp.second;
		std::string ext_err = "Call to lotman::Lot::store_new_parents failed: ";
		return std::make_pair(false, ext_err + int_err);
	}
	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::add_paths(const std::vector<json> &paths) {
	auto rp = store_new_paths(paths);
	if (!rp.first) {
		std::string int_err = rp.second;
		std::string ext_err = "Call to lotman::Lot::store_new_paths failed: ";
		return std::make_pair(false, ext_err + int_err);
	}
	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::remove_parents(const std::vector<std::string> &parents_to_remove) {
	/*
	First we need to check whether removing the specified parents would break the lot
	data structure, and if it would, the function should fail without deleting anything
	*/

	// Get the lot's actual set of non-recursive parents
	this->get_parents(false, true); // non recursive, but include self if self parent
	int remaining_parents = self_parents.size();

	// Sort and deduplicate, just in case...
	std::vector<std::string> parents_copy = parents_to_remove;
	std::sort(parents_copy.begin(), parents_copy.end());			   // sort vector to group duplicate elements
	auto last = std::unique(parents_copy.begin(), parents_copy.end()); // remove consecutive duplicates
	parents_copy.erase(last, parents_copy.end());					   // erase the duplicates

	for (const auto &parent : self_parents) {
		if (std::find(parents_copy.begin(), parents_copy.end(), parent.lot_name) != parents_copy.end()) {
			remaining_parents -= 1;
		}
	}

	// Make sure that there's at least one responsible parent left to be associated
	// with the lot if all the specified parents are removed.
	if (remaining_parents < 1) {
		return std::make_pair(false, "Could not remove parents because doing so would orphan the lot.");
	}

	auto rp = remove_parents_from_db(parents_copy);

	if (!rp.first) {
		std::string int_err = rp.second;
		std::string ext_err = "Call to lotman::Lot::remove_parents failed: ";
		return std::make_pair(false, ext_err + int_err);
	}
	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::remove_paths(const std::vector<std::string> &paths) {
	auto rp = remove_paths_from_db(paths);
	if (!rp.first) {
		std::string int_err = rp.second;
		std::string ext_err = "Call to lotman::Lot::remove_paths failed: ";
		return std::make_pair(false, ext_err + int_err);
	}
	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::update_owner(const std::string &update_val) {
	std::string owner_update_stmt = "UPDATE owners SET owner=? WHERE lot_name=?;";

	std::map<std::string, std::vector<int>> owner_update_str_map{{lot_name, {2}}, {update_val, {1}}};
	auto rp = store_updates(owner_update_stmt, owner_update_str_map);
	if (!rp.first) {
		std::string int_err = rp.second;
		std::string ext_err = "Failure on call to lotman::Lot::store_updates when storing owner update: ";
		return std::make_pair(false, ext_err + int_err);
	}
	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::update_parents(const json &update_arr) {
	// First, perform a cycle check on the whole update arr, and fail if any introduce a cycle
	// Cycle check takes in three arguments -- The start node (in this case, lot_name), and vectors of parents/children
	// of the start node as strings

	// Get all the existing parents
	std::vector<std::string> parents;
	this->get_parents(
		true, true); // get_self is true because either we need get_self to be true for get_parents or get_children.

	for (const auto &parent_lot : recursive_parents) {
		parents.push_back(parent_lot.lot_name);
	}
	// for each existing parent, if it's being updated, swap it out with the new parent.
	for (const auto &update : update_arr) {
		auto parent_iter = std::find(parents.begin(), parents.end(), update["current"]);
		if (parent_iter != parents.end()) {
			*parent_iter = update["new"];
		} else {
			return std::make_pair(false, "One of the current parents, " + update["current"].get<std::string>() +
											 ", to be updated is not actually a parent.");
		}
	}

	std::vector<std::string> children;
	this->get_children(true, false);
	for (const auto &child_lot : recursive_children) {
		children.push_back(child_lot.lot_name);
	}

	if (Checks::cycle_check(lot_name, parents, children)) {
		std::string err = "The requested parent update would introduce a dependency cycle.";
		return std::make_pair(false, err);
	}

	std::string parents_update_stmt = "UPDATE parents SET parent=? WHERE lot_name=? AND parent=?";
	// Need to store modifications per map entry
	for (const auto &update_obj : update_arr) {
		std::map<std::string, std::vector<int>> parents_update_str_map;
		if (update_obj["current"] == lot_name) {
			parents_update_str_map = {{update_obj["new"], {1}}, {update_obj["current"], {2, 3}}};
		} else {
			parents_update_str_map = {{update_obj["new"], {1}}, {lot_name, {2}}, {update_obj["current"], {3}}};
		}
		auto rp = store_updates(parents_update_stmt, parents_update_str_map);
		if (!rp.first) {
			std::string int_err = rp.second;
			std::string ext_err = "Failure on call to lotman::Lot::store_updates when storing parents update: ";
			return std::make_pair(false, ext_err + int_err);
		}
	}
	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::update_paths(const json &update_arr) {
	// incoming update map looks like {"path1" --> {"path" : "path2", "recursive" : false}}
	std::string paths_update_stmt = "UPDATE paths SET path=? WHERE lot_name=? and path=?;";
	std::string recursive_update_stmt = "UPDATE paths SET recursive=? WHERE lot_name=? and path=?;";

	// Iterate through updates, first perform recursive update THEN path
	for (const auto &update_obj : update_arr /*update_map*/) {
		// Normalize paths with trailing slash
		std::string current_path = ensure_trailing_slash(update_obj["current"].get<std::string>());
		std::string new_path = ensure_trailing_slash(update_obj["new"].get<std::string>());

		std::map<int64_t, std::vector<int>> recursive_update_int_map{
			{update_obj["recursive"].get<int>(),
			 {1}}}; // Unfortunately using int64_t for these bools to write less code
		std::map<std::string, std::vector<int>> recursive_update_str_map{{lot_name, {2}}, {current_path, {3}}};
		auto rp = store_updates(recursive_update_stmt, recursive_update_str_map, recursive_update_int_map);
		if (!rp.first) {
			std::string int_err = rp.second;
			std::string ext_err = "Failure on call to lotman::Lot::store_updates when storing paths recursive update: ";
			return std::make_pair(false, ext_err + int_err);
		}

		// Path update
		std::map<std::string, std::vector<int>> paths_update_str_map{
			{new_path, {1}}, {lot_name, {2}}, {current_path, {3}}};
		rp = store_updates(paths_update_stmt, paths_update_str_map);
		if (!rp.first) {
			std::string int_err = rp.second;
			std::string ext_err = "Failure on call to lotman::Lot::store_updates when storing paths path update: ";
			return std::make_pair(false, ext_err + int_err);
		}
	}
	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::update_man_policy_attrs(const std::string &update_key, double update_val) {
	std::string man_policy_attr_update_stmt_first_half = "UPDATE management_policy_attributes SET ";
	std::string man_policy_attr_update_stmt_second_half = "=? WHERE lot_name=?;";

	std::array<std::string, 2> dbl_keys = {"dedicated_GB", "opportunistic_GB"};
	std::array<std::string, 4> int_keys = {"max_num_objects", "creation_time", "expiration_time", "deletion_time"};

	if (std::find(dbl_keys.begin(), dbl_keys.end(), update_key) != dbl_keys.end()) {
		std::string man_policy_attr_update_stmt =
			man_policy_attr_update_stmt_first_half + update_key + man_policy_attr_update_stmt_second_half;
		std::map<std::string, std::vector<int>> man_policy_attr_update_str_map{{lot_name, {2}}};
		std::map<double, std::vector<int>> man_policy_attr_update_dbl_map{{update_val, {1}}};
		auto rp = store_updates(man_policy_attr_update_stmt, man_policy_attr_update_str_map,
								std::map<int64_t, std::vector<int>>(), man_policy_attr_update_dbl_map);
		if (!rp.first) {
			std::string int_err = rp.second;
			std::string ext_err =
				"Failure on call to lotman::Lot::store_updates when storing management policy attribute update: ";
			return std::make_pair(false, ext_err + int_err);
		}
	} else if (std::find(int_keys.begin(), int_keys.end(), update_key) != int_keys.end()) {
		std::string man_policy_attr_update_stmt =
			man_policy_attr_update_stmt_first_half + update_key + man_policy_attr_update_stmt_second_half;
		std::map<std::string, std::vector<int>> man_policy_attr_update_str_map{{lot_name, {2}}};
		std::map<int64_t, std::vector<int>> man_policy_attr_update_int_map{{update_val, {1}}};
		auto rp =
			store_updates(man_policy_attr_update_stmt, man_policy_attr_update_str_map, man_policy_attr_update_int_map);
		if (!rp.first) {
			std::string int_err = rp.second;
			std::string ext_err =
				"Failure on call to lotman::Lot::store_updates when storing management policy attribute update: ";
			return std::make_pair(false, ext_err + int_err);
		}
	} else {
		return std::make_pair(false, "Update key not found or not recognized.");
	}
	return std::make_pair(true, "");
}

std::pair<bool, std::string>
lotman::Lot::update_parent_usage(Lot parent, const std::string &update_stmt,
								 const std::map<std::string, std::vector<int>> &update_str_map,
								 const std::map<int64_t, std::vector<int>> &update_int_map,
								 const std::map<double, std::vector<int>> &update_dbl_map) {
	auto rp = parent.store_updates(update_stmt, update_str_map, update_int_map, update_dbl_map);
	if (!rp.first) { // There was an error
		std::string int_err = rp.second;
		std::string ext_err = "Failure on call to store_updates for parent: ";
		return std::make_pair(false, ext_err + int_err);
	}
	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::update_self_usage(const std::string &key, const double value,
															bool deltaMode) {

	/*
	Function Flow for update_lot_usage:
	* Sanitize inputs by making sure key is allowed/known
	* Get the current usage, used to calculate delta for updating parents' children_* columns
	* Calculate delta
	* Update lot proper
	* For each parent of lot proper, update children_key += delta
	*/

	std::array<std::string, 2> allowed_int_keys = {"self_objects", "self_objects_being_written"};
	std::array<std::string, 2> allowed_double_keys = {"self_GB", "self_GB_being_written"};

	std::string children_key =
		"children" + key.substr(4); // here, we strip out the "self" from the key to target the children col
	std::string update_parent_usage_stmt =
		"UPDATE lot_usage SET " + children_key + " = " + children_key + " + ? WHERE lot_name = ?;";

	// Get the current usage, which is needed in later sections
	std::string get_usage_query = "SELECT " + key + " FROM lot_usage WHERE lot_name = ?;";
	std::map<std::string, std::vector<int>> get_usage_query_str_map{{lot_name, {1}}};
	auto rp_vec_str = lotman::db::SQL_get_matches(get_usage_query, get_usage_query_str_map);

	if (!rp_vec_str.second.empty()) { // There was an error
		std::string int_err = rp_vec_str.second;
		std::string ext_err = "Failure on call to SQL_get_matches: ";
		return std::make_pair(false, ext_err + int_err);
	}

	if (deltaMode) {
		std::map<std::string, std::vector<int>> update_usage_str_map = {{lot_name, {2}}};
		std::string update_usage_delta_stmt = "UPDATE lot_usage SET " + key + " = " + key + " + ? WHERE lot_name = ?;";

		if (std::find(allowed_int_keys.begin(), allowed_int_keys.end(), key) != allowed_int_keys.end()) {
			int delta = value;
			int current_usage = std::stod(rp_vec_str.first[0]);

			if (current_usage + delta < 0) {
				return std::make_pair(
					false,
					"The attempted delta update would result in storing negative values for the key " + key + ".");
			}

			// Store updates for lot proper
			std::map<int64_t, std::vector<int>> update_usage_int_map = {{value, {1}}};
			auto rp_bool_str = this->store_updates(update_usage_delta_stmt, update_usage_str_map, update_usage_int_map);
			if (!rp_bool_str.first) {
				std::string int_err = rp_bool_str.second;
				std::string ext_err = "Failure on call to store_updates: ";
				return std::make_pair(false, ext_err + int_err);
			}

			// // Get the parents that need to be updated
			// auto rp_lotvec_str = this->get_parents(true);
			// if (!rp_lotvec_str.second.empty()) { // There was an error
			//     std::string int_err = rp_lotvec_str.second;
			//     std::string ext_err = "Failure on call to get_parents: ";
			//     return std::make_pair(false, ext_err + int_err);
			// }

			// for (const auto &parent : recursive_parents) {
			//     std::map<std::string, std::vector<int>> update_parent_str_map{{parent.lot_name, {2}}};
			//     std::map<int64_t, std::vector<int>> update_parent_int_map{{delta, {1}}}; // Update children_key to
			//     current_usage + delta rp_bool_str = this->update_parent_usage(parent, update_parent_usage_stmt,
			//     update_parent_str_map, update_parent_int_map); if (!rp_bool_str.first) { // There was an error
			//         std::string int_err = rp_bool_str.second;
			//         std::string ext_err = "Failure on call to store_updates when updating parent usage: ";
			//         return std::make_pair(false, ext_err + int_err);
			//     }
			// }
		} else if (std::find(allowed_double_keys.begin(), allowed_double_keys.end(), key) !=
				   allowed_double_keys.end()) {
			double delta = value;
			double current_usage = std::stod(rp_vec_str.first[0]);

			if (current_usage + delta < 0) {
				return std::make_pair(
					false,
					"The attempted delta update would result in storing negative values for the key " + key + ".");
			}

			std::map<double, std::vector<int>> update_usage_double_map = {{value, {1}}};
			std::map<int64_t, std::vector<int>> plc_hldr_int_map;
			auto rp_bool_str = this->store_updates(update_usage_delta_stmt, update_usage_str_map, plc_hldr_int_map,
												   update_usage_double_map);
			if (!rp_bool_str.first) { // There was an error
				std::string int_err = rp_bool_str.second;
				std::string ext_err = "Failure on call to store_updates for lot proper: ";
				return std::make_pair(false, ext_err + int_err);
			}

			// // Get parents to be updated
			// auto rp_lotvec_str = this->get_parents(true);
			// if (!rp_lotvec_str.second.empty()) { // There was an error
			//     std::string int_err = rp_lotvec_str.second;
			//     std::string ext_err = "Failure on call to get_parents: ";
			//     return std::make_pair(false, ext_err + int_err);
			// }

			// // Update the parents
			// for (const auto &parent : recursive_parents) {
			//     std::map<std::string, std::vector<int>> update_parent_str_map{{parent.lot_name, {2}}};
			//     std::map<double, std::vector<int>> update_parent_dbl_map{{delta, {1}}}; // Update children_key to
			//     current_usage + delta std::map<int64_t, std::vector<int>> plc_hldr_int_map; rp_bool_str =
			//     this->update_parent_usage(parent, update_parent_usage_stmt, update_parent_str_map, plc_hldr_int_map,
			//     update_parent_dbl_map); if (!rp_bool_str.first) { // There was an error
			//         std::string int_err = rp_bool_str.second;
			//         std::string ext_err = "Failure on call to store_updates";
			//         return std::make_pair(false, ext_err + int_err);
			//     }
			// }
		}
	} else {
		std::map<std::string, std::vector<int>> update_usage_str_map = {{lot_name, {2}}};
		std::string update_usage_stmt = "UPDATE lot_usage SET " + key + "=? WHERE lot_name=?;";
		std::string get_usage_query = "SELECT " + key + " FROM lot_usage WHERE lot_name = ?;";

		// std::string children_key = "children" + key.substr(4);
		std::string parent_usage_query = "SELECT " + children_key + " FROM lot_usage WHERE lot_name = ?;";
		// std::string update_parent_usage_stmt = "UPDATE lot_usage SET " + children_key + "=? WHERE lot_name=?;";

		if (std::find(allowed_int_keys.begin(), allowed_int_keys.end(), key) != allowed_int_keys.end()) {
			int current_usage = std::stoi(rp_vec_str.first[0]);
			int delta = value - current_usage;

			// Update lot proper
			std::map<int64_t, std::vector<int>> update_usage_int_map = {{value, {1}}};

			auto rp_bool_str = this->store_updates(update_usage_stmt, update_usage_str_map, update_usage_int_map);
			if (!rp_bool_str.first) {
				std::string int_err = rp_bool_str.second;
				std::string ext_err = "Failure on call to store_updates: ";
				return std::make_pair(false, ext_err + int_err);
			}

			/* TODO: need some kind of recovery file if we start updating parents and then fail --> for each parent,
			   store  in temp file current usage for key and delete temp file after done. Use a function "repair_db"
			   that checks for existence of temp file and restores things to those values, deleting the temp file upon
			   completion.
			*/

			// // Update parents
			// auto rp_lotvec_str = this->get_parents(true);
			// if (!rp_lotvec_str.second.empty()) { // There was an error
			//     std::string int_err = rp_lotvec_str.second;
			//     std::string ext_err = "Failure on call to get_parents: ";
			//     return std::make_pair(false, ext_err + int_err);
			// }

			// for (const auto &parent : recursive_parents) {
			//     std::map<std::string, std::vector<int>> parent_usage_query_str_map{{parent.lot_name, {1}}};
			//     auto rp_vec_str = lotman::db::SQL_get_matches(parent_usage_query, parent_usage_query_str_map);
			//     if (!rp_vec_str.second.empty()) { // There was an error
			//         std::string int_err = rp_vec_str.second;
			//         std::string ext_err = "Failure on call to get_parents: ";
			//         return std::make_pair(false, ext_err + int_err);
			//     }
			//     int current_usage = std::stoi(rp_vec_str.first[0]);
			//     std::map<std::string, std::vector<int>> update_parent_str_map{{parent.lot_name, {2}}};
			//     std::map<int64_t, std::vector<int>> update_parent_dbl_map{{delta, {1}}}; // Update children_key to
			//     current_usage + delta rp_bool_str = this->update_parent_usage(parent, update_parent_usage_stmt,
			//     update_parent_str_map, update_parent_dbl_map); if (!rp_bool_str.first) { // There was an error
			//         std::string int_err = rp_bool_str.second;
			//         std::string ext_err = "Failure on call to store_updates when updating parent usage: ";
			//         return std::make_pair(false, ext_err + int_err);
			//     }
			// }

		} else if (std::find(allowed_double_keys.begin(), allowed_double_keys.end(), key) !=
				   allowed_double_keys.end()) {
			double current_usage = std::stod(rp_vec_str.first[0]);
			double delta = value - current_usage;

			// Update lot proper
			std::map<std::string, std::vector<int>> update_usage_str_map = {{lot_name, {2}}};
			std::map<double, std::vector<int>> update_usage_dbl_map = {{value, {1}}};
			std::map<int64_t, std::vector<int>> plc_hldr_int_map;
			auto rp_bool_str =
				this->store_updates(update_usage_stmt, update_usage_str_map, plc_hldr_int_map, update_usage_dbl_map);
			if (!rp_bool_str.first) { // There was an error
				std::string int_err = rp_bool_str.second;
				std::string ext_err = "Failure on call to store_updates for lot proper: ";
				return std::make_pair(false, ext_err + int_err);
			}

			// // Update parents
			// auto rp_lotvec_str = this->get_parents(true);
			// if (!rp_lotvec_str.second.empty()) { // There was an error
			//     std::string int_err = rp_lotvec_str.second;
			//     std::string ext_err = "Failure on call to get_parents: ";
			//     return std::make_pair(false, ext_err + int_err);
			// }

			// std::string children_key = "children" + key.substr(4);
			// for (const auto &parent : recursive_parents) {
			//     std::map<std::string, std::vector<int>> parent_usage_query_str_map{{parent.lot_name, {1}}};
			//     rp_vec_str = lotman::db::SQL_get_matches(parent_usage_query, parent_usage_query_str_map);
			//     if (!rp_vec_str.second.empty()) { // There was an error
			//         std::string int_err = rp_vec_str.second;
			//         std::string ext_err = "Failure on call to SQL_get_matches: ";
			//         return std::make_pair(false, ext_err + int_err);
			//     }
			//     double current_usage = std::stod(rp_vec_str.first[0]);
			//     std::map<std::string, std::vector<int>> update_parent_str_map{{parent.lot_name, {2}}};
			//     std::map<double, std::vector<int>> update_parent_dbl_map{{delta, {1}}}; // Update children_key to
			//     current_usage + delta std::map<int64_t, std::vector<int>> plc_hldr_int_map; rp_bool_str =
			//     this->update_parent_usage(parent, update_parent_usage_stmt, update_parent_str_map, plc_hldr_int_map,
			//     update_parent_dbl_map); if (!rp_bool_str.first) { // There was an error
			//         std::string int_err = rp_bool_str.second;
			//         std::string ext_err = "Failure on call to store_updates for parents: ";
			//         return std::make_pair(false, ext_err + int_err);
			//     }
			// }
		}
	}
	return std::make_pair(true, "");
}

// SECTION UNDER MAINTENANCE

std::pair<bool, std::string> lotman::Lot::update_db_children_usage() {
	/*
	Function flow:
	- enumerate all the lots
	- For each lot, update it's children_usage.
	*/

	// Enumerate lots
	auto rp = list_all_lots();
	if (!rp.second.empty()) {
		std::string int_err = rp.second;
		std::string ext_err = "Failure on call to lotman::Lot::list_all_lots: ";
		return std::make_pair(false, ext_err + int_err);
	}

	// For each lot, update usage
	for (auto &lot_name : rp.first) {
		Lot lot(lot_name);
		auto rp_bool_str = lot.recalculate_children_usage();
		if (!rp_bool_str.first) {
			std::string int_err = rp_bool_str.second;
			std::string ext_err = "Failure on call to recalculate_children_usage for lot " + lot_name + ": ";
		}
	}

	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Lot::recalculate_children_usage() {
	/*
	Function flow for the lot this is being called on:
	- Get all the children
	- Sum the childrens' usage stats
	- Write to db
	*/

	if (!recursive_children_loaded) {
		this->get_children(true, false);
	}

	std::vector<std::vector<std::string>> updated_usages;
	if (recursive_children.size() > 0) {
		std::map<std::string, std::vector<int>> sum_str_map{};
		std::string sum_query =
			"SELECT SUM(self_GB), SUM(self_GB_being_written), SUM(self_objects), SUM(self_objects_being_written) "
			"FROM lot_usage WHERE lot_name IN (";

		// For each child we need to update both the query and the str map
		for (int i = 0; i < recursive_children.size(); i++) {
			// Update the query
			sum_query += "?";
			if (i != recursive_children.size() - 1) {
				sum_query += ", ";
			}

			// Update the str map
			sum_str_map[recursive_children[i].lot_name] = {i + 1};
		}
		sum_query += ");";

		// Get the sum
		auto rp_vec_vec_str = lotman::db::SQL_get_matches_multi_col(sum_query, 4, sum_str_map);
		if (!rp_vec_vec_str.second.empty()) {
			std::string int_err = rp_vec_vec_str.second;
			std::string ext_err = "Failure on call to SQL_get_matches_multi_col while summing child usage: ";
			return std::make_pair(false, ext_err + int_err);
		}
		if (rp_vec_vec_str.first.size() == 0) {
			return std::make_pair(false, "lotman::db::SQL_get_matches_multi_col returned an empty vector when "
										 "querying for child usage sums, but it shouldn't have");
		}
		updated_usages = rp_vec_vec_str.first;
	} else {
		updated_usages.push_back({"0", "0", "0", "0"}); // set the values to 0, there are no children!
	}

	// // Get current usages
	// std::string current_usage_query =   "SELECT children_GB, children_GB_being_written, children_objects,
	// children_objects_being_written "
	//                                     "FROM lot_usage WHERE lot_name = ?;";
	// std::map<std::string, std::vector<int>> current_usage_str_map{{this->lot_name, {1}}};
	// auto rp_vec_vec_str = lotman::db::SQL_get_matches_multi_col(current_usage_query, 4, current_usage_str_map);
	// if (!rp_vec_vec_str.second.empty()) {
	//     std::string int_err = rp_vec_vec_str.second;
	//     std::string ext_err = "Failure on call to SQL_get_matches_multi_col while getting current lot's child usage:
	//     "; return std::make_pair(false, ext_err + int_err);
	// }
	// if (rp_vec_vec_str.first.size() == 0) {
	//     return std::make_pair(false, "lotman::db::SQL_get_matches_multi_col returned an empty vector when
	//     querying for the lot's current child usage, but it shouldn't have");
	// }
	// std::vector<std::vector<std::string>> outdated_usages = rp_vec_vec_str.first;

	// // Calculate the deltas
	double children_GB = 0, children_GB_being_written = 0;
	int64_t children_objects = 0, children_objects_being_written = 0;
	children_GB = std::stod(updated_usages[0][0]);
	children_GB_being_written = std::stod(updated_usages[0][1]);
	// std::stoi will narrow the number, which should be int64_t, so we interpret string as double and cast to int64_t
	children_objects = (int64_t)std::stod(updated_usages[0][2]);
	children_objects_being_written = (int64_t)std::stod(updated_usages[0][3]);

	std::string update_stmt = "UPDATE lot_usage "
							  "SET "
							  "children_GB = ?, children_GB_being_written = ?, "
							  "children_objects = ?, children_objects_being_written = ? "
							  "WHERE lot_name = ?;";

	std::map<std::string, std::vector<int>> update_str_map{{this->lot_name, {5}}};
	std::map<double, std::vector<int>> update_dbl_map;
	std::map<int64_t, std::vector<int>> update_int_map;
	if (children_GB == children_GB_being_written) {
		update_dbl_map = {{children_GB, {1, 2}}};
	} else {
		update_dbl_map = {{children_GB, {1}}, {children_GB_being_written, {2}}};
	}
	if (children_objects == children_objects_being_written) {
		update_int_map = {{children_objects, {3, 4}}};
	} else {
		update_int_map = {{children_objects, {3}}, {children_objects_being_written, {4}}};
	}

	// Perform the updates
	auto rp_bool_str = lotman::Lot::store_updates(update_stmt, update_str_map, update_int_map, update_dbl_map);
	if (!rp_bool_str.first) {
		std::string int_err = rp_bool_str.second;
		std::string ext_err = "Failure while storing child usage delta updates: ";
		return std::make_pair(false, ext_err + int_err);
	}

	return std::make_pair(true, "");
}

// END SECTION

std::pair<bool, std::string> lotman::Lot::update_usage_by_dirs(const json &update_JSON, bool deltaMode) {
	// TODO: Should lots who don't show up when connecting lots to dirs be reset to have
	//       0 usage, or should the be kept the way they are?
	// --> kept the way they are, probably.

	DirUsageUpdate dirUpdate;
	std::vector<Lot> return_lots;
	auto rp = dirUpdate.JSON_math(update_JSON, &return_lots);
	if (!rp.first) {
		std::string int_err = rp.second;
		std::string ext_err = "Failure on call to JSON_math: ";
		return std::make_pair(false, ext_err + int_err);
	}

	for (auto &lot : return_lots) {
		// Since we don't know the lots beforehand, we have to check for their existence here.
		auto exists = lot_exists(lot.lot_name);
		if (!exists.second.empty()) {
			std::string int_err = exists.second;
			std::string ext_err = "Failed to check if lot exists: ";
			return std::make_pair(false, ext_err + int_err);
		}

		if (!exists.first) {
			std::string err = "The lot " + lot.lot_name + " does not exist in the db, so it cannot be updated...";
			return std::make_pair(false, err);
		}

		if (lot.usage.self_GB_update_staged) {
			auto rp_bool_str = lot.update_self_usage("self_GB", lot.usage.self_GB, deltaMode);
			if (!rp_bool_str.first) {
				std::string int_err = rp_bool_str.second;
				std::string ext_err = "Failure to update lot's self_GB: ";
				return std::make_pair(false, ext_err + int_err);
			}
		}

		if (lot.usage.self_objects_update_staged) {
			auto rp_bool_str = lot.update_self_usage("self_objects", lot.usage.self_objects, deltaMode);
			if (!rp_bool_str.first) {
				std::string int_err = rp_bool_str.second;
				std::string ext_err = "Failure to update lot's self_objects: ";
				return std::make_pair(false, ext_err + int_err);
			}
		}

		if (lot.usage.self_GB_being_written_update_staged) {
			auto rp_bool_str =
				lot.update_self_usage("self_GB_being_written", lot.usage.self_GB_being_written, deltaMode);
			if (!rp_bool_str.first) {
				std::string int_err = rp_bool_str.second;
				std::string ext_err = "Failure to update lot's self_GB_being_written: ";
				return std::make_pair(false, ext_err + int_err);
			}
		}

		if (lot.usage.self_objects_being_written_update_staged) {
			auto rp_bool_str =
				lot.update_self_usage("self_objects_being_written", lot.usage.self_objects_being_written, deltaMode);
			if (!rp_bool_str.first) {
				std::string int_err = rp_bool_str.second;
				std::string ext_err = "Failure to update lot's self_objects_being_written: ";
				return std::make_pair(false, ext_err + int_err);
			}
		}
	}

	return std::make_pair(true, "");
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_lots_past_exp(const bool recursive) {
	std::vector<std::string> expired_lots;
	auto now = std::chrono::system_clock::now();
	int64_t ms_since_epoch = std::chrono::time_point_cast<std::chrono::milliseconds>(now).time_since_epoch().count();

	std::string expired_query = "SELECT lot_name FROM management_policy_attributes WHERE expiration_time <= ?;";
	std::map<int64_t, std::vector<int>> expired_map{{ms_since_epoch, {1}}};
	auto rp = lotman::db::SQL_get_matches(expired_query, std::map<std::string, std::vector<int>>(), expired_map);
	if (!rp.second.empty()) { // There was an error
		std::string int_err = rp.second;
		std::string ext_err = "Failure on call to SQL_get_matches: ";
		return std::make_pair(std::vector<std::string>(), ext_err + int_err);
	}

	expired_lots = rp.first;
	if (recursive) { // Any child of an expired lot is also expired
		std::vector<std::string> tmp;
		for (auto &lot_name : expired_lots) {
			Lot _lot(lot_name);
			auto rp_lotvec_str = _lot.get_children(true);
			if (!rp_lotvec_str.second.empty()) { // There was an error
				std::string int_err = rp_lotvec_str.second;
				std::string ext_err = "Failure on call to get_children.";
				return std::make_pair(std::vector<std::string>(), ext_err + int_err);
			}

			std::vector<std::string> tmp;
			for (const auto &child : _lot.recursive_children) {
				tmp.push_back(child.lot_name);
			}
		}
		expired_lots.insert(expired_lots.end(), tmp.begin(), tmp.end());

		// Sort and remove any duplicates
		std::sort(expired_lots.begin(), expired_lots.end());
		auto last = std::unique(expired_lots.begin(), expired_lots.end());
		expired_lots.erase(last, expired_lots.end());
	}

	return std::make_pair(expired_lots, "");
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_lots_past_del(const bool recursive) {
	auto now = std::chrono::system_clock::now();
	int64_t ms_since_epoch = std::chrono::time_point_cast<std::chrono::milliseconds>(now).time_since_epoch().count();

	std::string deletion_query = "SELECT lot_name FROM management_policy_attributes WHERE deletion_time <= ?;";
	std::map<int64_t, std::vector<int>> deletion_map{{ms_since_epoch, {1}}};
	auto rp = lotman::db::SQL_get_matches(deletion_query, std::map<std::string, std::vector<int>>(), deletion_map);
	if (!rp.second.empty()) { // There was an error
		std::string int_err = rp.second;
		std::string ext_err = "Failure on call to SQL_get_matches: ";
		return std::make_pair(std::vector<std::string>(), ext_err + int_err);
	}

	std::vector<std::string> deletion_lots = rp.first;

	if (recursive) { // Any child of an expired lot is also expired
		std::vector<std::string> tmp;
		for (auto &lot_name : deletion_lots) {
			Lot _lot(lot_name);
			auto rp_lotvec_str = _lot.get_children(true);
			if (!rp_lotvec_str.second.empty()) { // There was an error
				std::string int_err = rp_lotvec_str.second;
				std::string ext_err = "Failure on call to get_children.";
				return std::make_pair(std::vector<std::string>(), ext_err + int_err);
			}

			for (const auto &child : _lot.recursive_children) {
				tmp.push_back(child.lot_name);
			}
		}
		deletion_lots.insert(deletion_lots.end(), tmp.begin(), tmp.end());

		// Sort and remove any duplicates
		std::sort(deletion_lots.begin(), deletion_lots.end());
		auto last = std::unique(deletion_lots.begin(), deletion_lots.end());
		deletion_lots.erase(last, deletion_lots.end());
	}

	return std::make_pair(deletion_lots, "");
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_lots_past_opp(const bool recursive_quota,
																				const bool recursive_children) {
	std::vector<std::string> lots_past_opp;
	if (recursive_quota) {
		std::string rec_opp_usage_query =
			"SELECT "
			"lot_usage.lot_name "
			"FROM lot_usage "
			"INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
			"WHERE lot_usage.self_GB + lot_usage.children_GB >= management_policy_attributes.dedicated_GB + "
			"management_policy_attributes.opportunistic_GB;";
		auto rp = lotman::db::SQL_get_matches(rec_opp_usage_query);
		if (!rp.second.empty()) { // There was an error
			std::string int_err = rp.second;
			std::string ext_err = "Failure on call to SQL_get_matches: ";
			return std::make_pair(std::vector<std::string>(), ext_err + int_err);
		}
		lots_past_opp = rp.first;
	} else {
		std::string opp_usage_query =
			"SELECT "
			"lot_usage.lot_name "
			"FROM lot_usage "
			"INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
			"WHERE lot_usage.self_GB >= management_policy_attributes.dedicated_GB + "
			"management_policy_attributes.opportunistic_GB;";

		auto rp = lotman::db::SQL_get_matches(opp_usage_query);
		if (!rp.second.empty()) { // There was an error
			std::string int_err = rp.second;
			std::string ext_err = "Failure on call to SQL_get_matches: ";
			return std::make_pair(std::vector<std::string>(), ext_err + int_err);
		}
		lots_past_opp = rp.first;
	}

	if (recursive_children) { // Get all children of the lots past opp
		std::vector<std::string> tmp;
		for (const auto lot_past_opp : lots_past_opp) {
			Lot _lot(lot_past_opp);
			auto rp_lotvec_str = _lot.get_children(true);
			if (!rp_lotvec_str.second.empty()) { // There was an error
				std::string int_err = rp_lotvec_str.second;
				std::string ext_err = "Failure on call to get_children.";
				return std::make_pair(std::vector<std::string>(), ext_err + int_err);
			}

			for (const auto &child : _lot.recursive_children) {
				tmp.push_back(child.lot_name);
			}
		}
		lots_past_opp.insert(lots_past_opp.end(), tmp.begin(), tmp.end());

		// Sort and remove any duplicates
		std::sort(lots_past_opp.begin(), lots_past_opp.end());
		auto last = std::unique(lots_past_opp.begin(), lots_past_opp.end());
		lots_past_opp.erase(last, lots_past_opp.end());
	}

	return std::make_pair(lots_past_opp, "");
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_lots_past_ded(const bool recursive_quota,
																				const bool recursive_children) {
	std::vector<std::string> lots_past_ded;
	if (recursive_quota) {
		std::string rec_ded_usage_query =
			"SELECT "
			"lot_usage.lot_name "
			"FROM lot_usage "
			"INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
			"WHERE lot_usage.self_GB + lot_usage.children_GB >= management_policy_attributes.dedicated_GB;";

		auto rp = lotman::db::SQL_get_matches(rec_ded_usage_query);
		if (!rp.second.empty()) { // There was an error
			std::string int_err = rp.second;
			std::string ext_err = "Failure on call to SQL_get_matches: ";
			return std::make_pair(std::vector<std::string>(), ext_err + int_err);
		}
		lots_past_ded = rp.first;
	} else {
		std::string ded_usage_query =
			"SELECT "
			"lot_usage.lot_name "
			"FROM lot_usage "
			"INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
			"WHERE lot_usage.self_GB >= management_policy_attributes.dedicated_GB;";

		auto rp = lotman::db::SQL_get_matches(ded_usage_query);
		if (!rp.second.empty()) { // There was an error
			std::string int_err = rp.second;
			std::string ext_err = "Failure on call to SQL_get_matches: ";
			return std::make_pair(std::vector<std::string>(), ext_err + int_err);
		}
		lots_past_ded = rp.first;
	}

	if (recursive_children) { // Get all children of the lots past opp
		std::vector<std::string> tmp;
		for (const auto lot_past_ded : lots_past_ded) {
			Lot _lot(lot_past_ded);
			auto rp_lotvec_str = _lot.get_children(true);
			if (!rp_lotvec_str.second.empty()) { // There was an error
				std::string int_err = rp_lotvec_str.second;
				std::string ext_err = "Failure on call to get_children.";
				return std::make_pair(std::vector<std::string>(), ext_err + int_err);
			}

			for (const auto &child : _lot.recursive_children) {
				tmp.push_back(child.lot_name);
			}
		}
		lots_past_ded.insert(lots_past_ded.end(), tmp.begin(), tmp.end());

		// Sort and remove any duplicates
		std::sort(lots_past_ded.begin(), lots_past_ded.end());
		auto last = std::unique(lots_past_ded.begin(), lots_past_ded.end());
		lots_past_ded.erase(last, lots_past_ded.end());
	}

	return std::make_pair(lots_past_ded, "");
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_lots_past_obj(const bool recursive_quota,
																				const bool recursive_children) {
	std::vector<std::string> lots_past_obj;
	if (recursive_quota) {
		std::string rec_obj_usage_query =
			"SELECT "
			"lot_usage.lot_name "
			"FROM lot_usage "
			"INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
			"WHERE lot_usage.self_objects + lot_usage.children_objects >= "
			"management_policy_attributes.max_num_objects;";

		auto rp = lotman::db::SQL_get_matches(rec_obj_usage_query);
		if (!rp.second.empty()) { // There was an error
			std::string int_err = rp.second;
			std::string ext_err = "Failure on call to SQL_get_matches: ";
			return std::make_pair(std::vector<std::string>(), ext_err + int_err);
		}
		lots_past_obj = rp.first;
	} else {
		std::string obj_usage_query =
			"SELECT "
			"lot_usage.lot_name "
			"FROM lot_usage "
			"INNER JOIN management_policy_attributes ON lot_usage.lot_name=management_policy_attributes.lot_name "
			"WHERE lot_usage.self_objects >= management_policy_attributes.max_num_objects;";

		auto rp = lotman::db::SQL_get_matches(obj_usage_query);
		if (!rp.second.empty()) { // There was an error
			std::string int_err = rp.second;
			std::string ext_err = "Failure on call to SQL_get_matches: ";
			return std::make_pair(std::vector<std::string>(), ext_err + int_err);
		}
		lots_past_obj = rp.first;
	}

	if (recursive_children) { // Get all children of the lots past opp
		std::vector<std::string> tmp;
		for (const auto lot_past_obj : lots_past_obj) {
			Lot _lot(lot_past_obj);
			auto rp_lotvec_str = _lot.get_children(true);
			if (!rp_lotvec_str.second.empty()) { // There was an error
				std::string int_err = rp_lotvec_str.second;
				std::string ext_err = "Failure on call to get_children.";
				return std::make_pair(std::vector<std::string>(), ext_err + int_err);
			}

			for (const auto &child : _lot.recursive_children) {
				tmp.push_back(child.lot_name);
			}
		}
		lots_past_obj.insert(lots_past_obj.end(), tmp.begin(), tmp.end());

		// Sort and remove any duplicates
		std::sort(lots_past_obj.begin(), lots_past_obj.end());
		auto last = std::unique(lots_past_obj.begin(), lots_past_obj.end());
		lots_past_obj.erase(last, lots_past_obj.end());
	}

	return std::make_pair(lots_past_obj, "");
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::list_all_lots() {
	try {
		auto &storage = db::StorageManager::get_storage();
		// Surjection between lots and owners means we'll get every lot without duplicates.
		auto lot_names = storage.select(&db::Owner::lot_name);
		return std::make_pair(lot_names, "");
	} catch (const std::exception &e) {
		return std::make_pair(std::vector<std::string>(), std::string("list_all_lots failed: ") + e.what());
	}
}

std::pair<std::vector<std::string>, std::string> lotman::Lot::get_lots_from_dir(const std::string &dir_input,
																				const bool recursive) {
	// Normalize: ensure input dir has trailing slash for consistent comparison
	// Database paths always have trailing slashes (e.g., "/foo/bar/")
	std::string dir = ensure_trailing_slash(dir_input);

	// For the LIKE comparison, we need the input path without trailing slash.
	// This is because: stored path "/foo/" with LIKE pattern means we check if
	// the input starts with "/foo/". Using "/foo/bar" LIKE "/foo/" || '%' works,
	// but "/foobar" LIKE "/foo/" || '%' correctly fails (no match).
	std::string dir_for_like = dir;
	if (dir_for_like.length() > 1 && dir_for_like.back() == '/') {
		dir_for_like.pop_back();
	}

	// Query logic:
	// - path = ?1 : exact match (normalized input matches stored path exactly)
	// - ?2 LIKE path || '%' : input is a subdirectory of a stored recursive path
	//   The trailing slash in stored paths prevents false prefix matches
	// - ?3 is used for non-recursive path exact matching
	std::string lots_from_dir_query =
		"SELECT lot_name FROM paths "
		"WHERE "
		"(path = ? OR ? LIKE path || '%') " // Exact match or subdirectory of stored path
		"AND "
		"(recursive OR path = ?) " // If the stored file path is not recursive, we only match the exact file path
		"ORDER BY LENGTH(path) DESC LIMIT 1;"; // We prefer longer matches over shorter ones
	std::map<std::string, std::vector<int>> dir_str_map{{dir, {1, 3}}, {dir_for_like, {2}}};
	auto rp = lotman::db::SQL_get_matches(lots_from_dir_query, dir_str_map);
	if (!rp.second.empty()) {
		std::string int_err = rp.second;
		std::string ext_err = "Failure on call to SQL_get_matches: ";
		return std::make_pair(std::vector<std::string>(), ext_err + int_err);
	}

	std::vector<std::string> matching_lots_vec;
	if (rp.first.empty()) { // No associated lots were found, indicating the directory should be associated with the
							// default lot
		matching_lots_vec = {"default"};
	} else {
		matching_lots_vec = rp.first;
	}

	if (recursive) { // Indicates we want all of the parent lots.
		Lot lot(matching_lots_vec[0]);
		lot.get_parents(true, false);
		for (auto &parent : lot.recursive_parents) {
			matching_lots_vec.push_back(parent.lot_name);
		}
	}

	return std::make_pair(matching_lots_vec, "");
}

std::pair<bool, std::string> lotman::Lot::check_context_for_parents(const std::vector<std::string> &parents,
																	bool include_self, bool new_lot) {
	if (new_lot && parents.size() == 1 &&
		parents[0] == lot_name) { // This is a self parent new lot with no other parents. No need to check context.
		return std::make_pair(true, "");
	}

	std::string caller = lotman::Context::get_caller();
	bool allowed = false;
	if (!include_self) {
		for (const auto &parent : parents) {

			if (parent != lot_name) {
				Lot parent_lot(parent);
				auto rp = parent_lot.get_owners(true);
				if (!rp.second.empty()) { // There was an error
					std::string int_err = rp.second;
					std::string ext_err = "Failed to get parent owners while checking validity of context: ";
					return std::make_pair(false, ext_err + int_err);
				}
				if (std::find(parent_lot.recursive_owners.begin(), parent_lot.recursive_owners.end(), caller) !=
					parent_lot.recursive_owners.end()) { // Caller is an owner
					allowed = true;
					break;
				}
			}
		}
	} else {
		for (const auto &parent : parents) {
			Lot parent_lot(parent);
			auto rp = parent_lot.get_owners(true);
			if (!rp.second.empty()) { // There was an error
				std::string int_err = rp.second;
				std::string ext_err = "Failed to get parent owners while checking validity of context: ";
				return std::make_pair(false, ext_err + int_err);
			}
			if (std::find(parent_lot.recursive_owners.begin(), parent_lot.recursive_owners.end(), caller) !=
				parent_lot.recursive_owners.end()) { // Caller is an owner
				allowed = true;
				break;
			}
		}
	}
	if (!allowed) {
		return std::make_pair(false, "Current context prohibits action on lot: Caller does not have proper ownership.");
	}

	return std::make_pair(true, "");
}
std::pair<bool, std::string> lotman::Lot::check_context_for_parents(const std::vector<Lot> &parents, bool include_self,
																	bool new_lot) {
	if (new_lot && parents.size() == 1 &&
		parents[0].lot_name ==
			lot_name) { // This is a self parent new lot with no other parents. No need to check context.
		return std::make_pair(true, "");
	}

	std::string caller = lotman::Context::get_caller();
	bool allowed = false;
	if (!include_self) {
		if (parents.size() == 1 && parents[0].lot_name == lot_name) {
			allowed = true;
		}
		for (const auto &parent : parents) {
			if (parent.lot_name != lot_name) {
				Lot temp_lot(parent.lot_name);
				auto rp = temp_lot.get_owners(true);
				if (!rp.second.empty()) {
					return std::make_pair(false, "Failed to get parent owners while checking validity of context: " +
													 rp.second);
				}
				const auto &owners = rp.first;
				if (std::find(owners.begin(), owners.end(), caller) != owners.end()) {
					allowed = true;
					break;
				}
			}
		}
	} else {
		for (const auto &parent : parents) {
			Lot temp_lot(parent.lot_name);
			auto rp = temp_lot.get_owners(true);
			if (!rp.second.empty()) {
				return std::make_pair(false,
									  "Failed to get parent owners while checking validity of context: " + rp.second);
			}
			const auto &owners = rp.first;
			if (std::find(owners.begin(), owners.end(), caller) != owners.end()) {
				allowed = true;
				break;
			}
		}
	}
	if (!allowed) {
		return std::make_pair(false, "Current context prohibits action on lot: Caller does not have proper ownership.");
	}
	return std::make_pair(true, "");
}
std::pair<bool, std::string> lotman::Lot::check_context_for_children(const std::vector<std::string> &children,
																	 bool include_self) {
	if (children.size() == 0) { // No children means no need to check for context.
		return std::make_pair(true, "");
	}

	std::string caller = lotman::Context::get_caller();
	bool allowed = false;
	if (!include_self) {
		for (const auto &child : children) {
			if (child != lot_name) {
				Lot child_lot(child);
				auto rp = child_lot.get_owners(true);
				if (!rp.second.empty()) { // There was an error
					std::string int_err = rp.second;
					std::string ext_err = "Failed to get child owners while checking validity of context: ";
					return std::make_pair(false, ext_err + int_err);
				}
				if (std::find(child_lot.recursive_owners.begin(), child_lot.recursive_owners.end(), caller) !=
					child_lot.recursive_owners.end()) { // Caller is an owner
					allowed = true;
					break;
				}
			}
		}
	} else {
		for (const auto &child : children) {
			Lot child_lot(child);
			auto rp = child_lot.get_owners(true);
			if (!rp.second.empty()) { // There was an error
				std::string int_err = rp.second;
				std::string ext_err = "Failed to get child owners while checking validity of context: ";
				return std::make_pair(false, ext_err + int_err);
			}
			if (std::find(child_lot.recursive_owners.begin(), child_lot.recursive_owners.end(), caller) !=
				child_lot.recursive_owners.end()) { // Caller is an owner
				allowed = true;
				break;
			}
		}
	}
	if (!allowed) {
		return std::make_pair(false, "Current context prohibits action on lot: Caller does not have proper ownership.");
	}
	return std::make_pair(true, "");
}
std::pair<bool, std::string> lotman::Lot::check_context_for_children(const std::vector<Lot> &children,
																	 bool include_self) {
	if (children.size() == 0) { // No children means no need to check for context.
		return std::make_pair(true, "");
	}

	std::string caller = lotman::Context::get_caller();
	bool allowed = false;
	if (!include_self) {
		for (const auto &child : children) {
			if (child.lot_name != lot_name) {
				Lot temp_lot(child.lot_name);
				auto rp = temp_lot.get_owners(true);
				if (!rp.second.empty()) {
					return std::make_pair(false, "Failed to get child owners while checking validity of context: " +
													 rp.second);
				}
				const auto &owners = rp.first;
				if (std::find(owners.begin(), owners.end(), caller) != owners.end()) {
					allowed = true;
					break;
				}
			}
		}
	} else {
		for (const auto &child : children) {
			Lot temp_lot(child.lot_name);
			auto rp = temp_lot.get_owners(true);
			if (!rp.second.empty()) {
				return std::make_pair(false,
									  "Failed to get child owners while checking validity of context: " + rp.second);
			}
			const auto &owners = rp.first;
			if (std::find(owners.begin(), owners.end(), caller) != owners.end()) {
				allowed = true;
				break;
			}
		}
	}
	if (!allowed) {
		return std::make_pair(false, "Current context prohibits action on lot: Caller does not have proper ownership.");
	}
	return std::make_pair(true, "");
}

/**
 * Functions specific to Checks class
 */

bool lotman::Checks::cycle_check(
	const std::string &start_node, const std::vector<std::string> &start_parents,
	const std::vector<std::string> &start_children) { // Returns true if invalid cycle is detected, else returns false
	// Basic DFS algorithm to check for cycle creation when adding a lot that has both parents and children.

	// Algorithm initialization
	std::vector<std::string> dfs_nodes_to_visit;
	dfs_nodes_to_visit.insert(dfs_nodes_to_visit.end(), start_parents.begin(), start_parents.end());
	for (const auto &children_iter : start_children) {
		auto check_iter = std::find(dfs_nodes_to_visit.begin(), dfs_nodes_to_visit.end(), children_iter);
		if (check_iter != dfs_nodes_to_visit.end()) {
			// Run, Luke! It's a trap! Erm... a cycle!
			return true;
		}
	}

	// Iterate
	while (dfs_nodes_to_visit.size() > 0) { // When dfs_nodes_to_visit has size 0, we're done checking
		lotman::Lot lot(dfs_nodes_to_visit[0]);
		auto rp = lot.get_parents();

		// TODO: expose errors
		if (!rp.second.empty()) { // There was an error
			std::string int_err = rp.second;
			std::string ext_err = "Failure on call to get_parents(): ";
			return false;
		}

		// convert to vec of strings instead of vec of lots
		std::vector<std::string> dfs_node_parents;
		for (const auto &parent : rp.first) {
			dfs_node_parents.push_back(parent.lot_name);
		}

		dfs_nodes_to_visit.insert(dfs_nodes_to_visit.end(), dfs_node_parents.begin(), dfs_node_parents.end());

		for (const auto &children_iter : start_children) { // For each specified child of start node ...
			auto check_iter = std::find(dfs_nodes_to_visit.begin(), dfs_nodes_to_visit.end(),
										children_iter); // ... check if that child is going to be visited ...
			if (check_iter != dfs_nodes_to_visit.end()) {
				// ... If it is going to be visited, cycle found!
				return true;
			}
		}
		dfs_nodes_to_visit.erase(
			dfs_nodes_to_visit.begin()); // Nothing consequential from this node, remove it from the nodes to visit
	}

	return false;
}

bool lotman::Checks::insertion_check(
	const std::string &LTBA, const std::string &parent,
	const std::string &child) { // Checks whether LTBA is being inserted between a parent and child.

	// TODO: expose errors
	lotman::Lot lot(child);
	auto rp = lot.get_parents();
	if (!rp.second.empty()) { // There was an error
		std::string int_err = rp.second;
		std::string ext_err = "Failure on call to get_parents(): ";
		return false;
	}

	std::vector<std::string> parents_vec;
	for (const auto &parent_lot : rp.first) {
		parents_vec.push_back(parent_lot.lot_name);
	}

	auto parent_iter = std::find(parents_vec.begin(), parents_vec.end(),
								 parent); // Check if the specified parent is listed as a parent to the child
	if (!(parent_iter == parents_vec.end())) {
		// Child has "parent" as a parent
		return true;
	}
	return false;
}

bool lotman::Checks::will_be_orphaned(const std::string &LTBR, const std::string &child) {

	// TODO: expose errors
	lotman::Lot lot(child);
	auto rp = lot.get_parents();
	if (!rp.second.empty()) { // There was an error
		std::string int_err = rp.second;
		std::string ext_err = "Failure on call to get_parents(): ";
		return false;
	}

	std::vector<std::string> parents_vec;
	for (const auto &parent : rp.first) {
		parents_vec.push_back(parent.lot_name);
	}

	if (parents_vec.size() == 1 && parents_vec[0] == LTBR) {
		return true;
	}
	return false;
}

/**
 * Implementation of sweep line algorithm for finding maximum MPAs during a time period.
 *
 * This implements the classic sweep line algorithm for interval scheduling problems.
 * See: https://www.geeksforgeeks.org/maximum-number-of-overlapping-intervals/
 *
 * The algorithm works by:
 * 1. Creating "events" for each lot's start (creation) and end (expiration/deletion)
 * 2. Sorting all events by time
 * 3. Sweeping through events chronologically, tracking current resource usage with deltas
 *    that correspond to each event's attributes
 * 4. Recording the maximum usage observed at any point
 *
 * Key semantic: Lot lifetimes are INCLUSIVE intervals [creation_time, end_time].
 * A lot is active at both its start and end timestamps. Therefore, we schedule
 * removal events at end_time + 1 (the first moment the lot is no longer active).
 */

std::pair<lotman::MaxMPAResult, std::string> lotman::get_max_mpas_for_period_internal(int64_t start_ms, int64_t end_ms,
																					  bool include_deletion) {
	// Validate input
	if (start_ms >= end_ms) {
		return {{0.0, 0.0, 0.0, 0}, "Error: start_ms must be less than end_ms"};
	}

	auto &storage = lotman::db::StorageManager::get_storage();

	// Determine which time field to use for lot end time
	using MPA = lotman::db::ManagementPolicyAttributes;
	using Parent = lotman::db::Parent;
	using namespace sqlite_orm;

	// Query lots that overlap with the period, filtering to only ROOT lots.
	//
	// IMPORTANT: We only count root lots (self-parent lots) to avoid double-counting in hierarchies.
	// A root lot is one where the lot has only itself as a parent in the parents table.
	// Child lots consume quota from their parents, so counting both would be incorrect.
	//
	// For example, if parent_lot has 5GB and child_lot (child of parent_lot) has 3GB,
	// the maximum capacity usage should be 5GB (from the parent), not 8GB (parent + child).
	//
	// Overlap condition for inclusive intervals: creation_time <= end_ms AND end_time >= start_ms
	// This correctly handles all overlap cases including point-in-time overlaps at boundaries.
	//
	// Root lot condition: EXISTS exactly one parent record WHERE parent = lot_name
	// We use a SQL subquery to identify root lots directly in the database for optimal performance.
	std::string time_field = include_deletion ? "deletion_time" : "expiration_time";
	std::string query = "SELECT mpa.lot_name, mpa.dedicated_GB, mpa.opportunistic_GB, mpa.max_num_objects, "
						"       mpa.creation_time, mpa." +
						time_field +
						" "
						"FROM management_policy_attributes mpa "
						"WHERE mpa.creation_time <= ? AND mpa." +
						time_field +
						" >= ? "
						"  AND mpa.lot_name IN ( "
						"    SELECT p.lot_name "
						"    FROM parents p "
						"    WHERE p.lot_name = p.parent "
						"    GROUP BY p.lot_name "
						"    HAVING COUNT(*) = 1 "
						"  )";

	std::map<int64_t, std::vector<int>> query_int_map{{end_ms, {1}}, {start_ms, {2}}};
	auto rp = lotman::db::SQL_get_matches_multi_col(query, 6, std::map<std::string, std::vector<int>>(), query_int_map);

	if (!rp.second.empty()) {
		return {{0.0, 0.0, 0.0, 0}, "Database query failed: " + rp.second};
	}

	auto &lots = rp.first;

	// If no root lots overlap, return zeros with no error
	if (lots.empty()) {
		return {{0.0, 0.0, 0.0, 0}, ""};
	}

	// Event structure for sweep line algorithm
	struct Event {
		int64_t time;
		double ded_delta;  // Change in dedicated storage
		double opp_delta;  // Change in opportunistic storage
		int64_t obj_delta; // Change in object count
		bool is_start;	   // true for creation event, false for expiration/deletion event
	};

	std::vector<Event> events;
	events.reserve(lots.size() * 2); // Each lot creates at most 2 events

	// Build event list from query results
	// Each row contains: [lot_name, dedicated_GB, opportunistic_GB, max_num_objects, creation_time, end_time]
	for (const auto &lot_row : lots) {
		// Parse query results from string vector (columns 0-5)
		// lot_row[0] = lot_name (string, not used in sweep line)
		double dedicated = std::stod(lot_row[1]);	  // dedicated_GB
		double opportunistic = std::stod(lot_row[2]); // opportunistic_GB
		int64_t objects = std::stoll(lot_row[3]);	  // max_num_objects
		int64_t creation = std::stoll(lot_row[4]);	  // creation_time
		int64_t end_time = std::stoll(lot_row[5]);	  // expiration_time or deletion_time

		// Clamp lot start to query range (if lot starts before start_ms, treat as starting at start_ms)
		int64_t effective_start = std::max(start_ms, creation);

		// Add creation/start event at the lot's effective start time
		events.push_back({effective_start, dedicated, opportunistic, objects, true});

		// Add expiration/deletion event AFTER the lot ends (since lot is active through end_time inclusive)
		// Only add if the lot ends before the query period ends
		if (end_time < end_ms) {
			// Schedule removal at end_time + 1 (first moment lot is no longer active)
			events.push_back({end_time + 1, -dedicated, -opportunistic, -objects, false});
		}
		// If end_time >= end_ms, the lot extends beyond our query range, so no removal event needed
	}

	// Sort events chronologically, with start events before end events at the same timestamp
	std::sort(events.begin(), events.end(), [](const Event &a, const Event &b) {
		if (a.time != b.time) {
			return a.time < b.time;
		}
		// At same time, process starts before ends (true sorts before false)
		// This ensures we correctly handle simultaneous creation/expiration events
		return a.is_start > b.is_start;
	});

	// Sweep through events chronologically, tracking current and maximum resource usage
	double current_ded = 0.0, current_opp = 0.0, current_combined = 0.0;
	double max_ded = 0.0, max_opp = 0.0, max_combined = 0.0;
	int64_t current_obj = 0, max_obj = 0;

	for (const auto &event : events) {
		// Update current resource usage based on event deltas
		current_ded += event.ded_delta;
		current_opp += event.opp_delta;
		current_obj += event.obj_delta;
		current_combined = current_ded + current_opp;

		// Track the maximum values observed at any point
		max_ded = std::max(max_ded, current_ded);
		max_opp = std::max(max_opp, current_opp);
		max_combined = std::max(max_combined, current_combined);
		max_obj = std::max(max_obj, current_obj);
	}

	return {{max_ded, max_opp, max_combined, max_obj}, ""};
}

void lotman::Context::set_caller(const std::string caller) {
	m_caller = std::make_shared<std::string>(caller);
}

std::pair<bool, std::string> lotman::Context::set_lot_home(const std::string dir_path) {
	// If setting to "", then we should treat as though it is unsetting the
	// config
	if (dir_path.length() == 0) { // User is configuring to empty string
		m_home = std::make_shared<std::string>(dir_path);
		return std::make_pair(true, "");
	}

	std::vector<std::string> path_components = path_split(dir_path); // cleans any extraneous /'s
	std::string cleaned_dir_path;
	for (const auto &component : path_components) { // add the / back to the path components
		cleaned_dir_path += "/" + component;
	}

	// Check that the cache_home exists, and if not try to create it
	auto rp = mkdir_and_parents_if_needed(cleaned_dir_path); // Structured bindings not introduced until cpp 17
	if (!rp.first) {										 //
		std::string err_prefix{"An issue was encountered with the provided cache home path: "};
		return std::make_pair(false, err_prefix + rp.second);
	}

	// Now it exists and we can write to it, set the value and let
	// scitokens_cache handle the rest
	m_home = std::make_shared<std::string>(cleaned_dir_path);

	// Reset the ORM storage manager so it re-initializes with the new path
	lotman::db::StorageManager::reset();

	return std::make_pair(true, "");
}

std::pair<bool, std::string> lotman::Context::mkdir_and_parents_if_needed(const std::string dir_path) {
	// SciTokens-cpp already makes assumptions about using Linux file paths,
	// so making that assumption here as well.

	// Using these perms because that's what the actual cache file uses in
	// scitokens_cache
	mode_t mode = 0700; // Maybe these permissions should be configurable?

	int result;
	std::string currentLevel;
	std::vector<std::string> path_components = path_split(dir_path);
	for (const auto &component : path_components) {
		currentLevel += "/" + component;
		result = mkdir(currentLevel.c_str(), mode);
		if ((result < 0) && errno != EEXIST) {
			std::string err_prefix{"There was an error while creating/checking "
								   "the directory: mkdir error: "};
			return std::make_pair(false, err_prefix + strerror(errno));
		}
	}

	return std::make_pair(true, "");
}

std::vector<std::string> lotman::Context::path_split(std::string path) {
	std::vector<std::string> path_components;
	std::stringstream ss(path);
	std::string component;

	while (std::getline(ss, component, '/')) {
		if (!component.empty()) {
			path_components.push_back(component);
		}
	}

	if (path_components[0] == "") {
		path_components.erase(path_components.begin());
	}
	return path_components;
}
