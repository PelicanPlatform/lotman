/**
 * Common test utilities for LotMan test suite
 *
 * This header provides shared RAII wrappers and helper functions
 * used across multiple test files.
 */

#ifndef LOTMAN_TEST_UTILS_H
#define LOTMAN_TEST_UTILS_H

#include "../src/lotman.h"

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

// RAII wrappers for C-style memory management
struct CStringDeleter {
	void operator()(char *ptr) const {
		if (ptr)
			free(ptr);
	}
};
using UniqueCString = std::unique_ptr<char, CStringDeleter>;

struct StringListDeleter {
	void operator()(char **ptr) const {
		if (ptr)
			lotman_free_string_list(ptr);
	}
};
using UniqueStringList = std::unique_ptr<char *, StringListDeleter>;

/**
 * Creates a unique temporary directory for test isolation.
 * @param prefix The prefix to use in the directory name template (e.g., "lotman_test")
 * @return The path to the created temporary directory
 */
inline std::string create_temp_directory(const std::string &prefix = "lotman_test") {
	std::string temp_dir_template = "/tmp/" + prefix + "_XXXXXX";
	char temp_dir_name[temp_dir_template.size() + 1];
	std::strcpy(temp_dir_name, temp_dir_template.c_str());

	char *mkdtemp_result = mkdtemp(temp_dir_name);
	if (mkdtemp_result == nullptr) {
		std::cerr << "Failed to create temporary directory\n";
		exit(1);
	}

	return std::string(mkdtemp_result);
}

#endif // LOTMAN_TEST_UTILS_H
