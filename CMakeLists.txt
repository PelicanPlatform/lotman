
cmake_minimum_required(VERSION 3.10)
project(
    Lotman
    DESCRIPTION "A C++ library to interface to LotMan"
    VERSION 0.0.1
    LANGUAGES CXX
    )

option(LOTMAN_BUILD_UNITTESTS "Build the lotman-cpp unit tests" OFF)
option(LOTMAN_EXTERNAL_GTEST "Use an external/pre-installed copy of GTest" OFF)
    
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")

set(CMAKE_BUILD_TYPE RelWithDebInfo) # -g -O2
# set(CMAKE_C_FLAGS_DEBUG "-g -DDEBUG") 
# set(CMAKE_CXX_GLAGS_DEBUG "-g -DDEBUG") 

include(GNUInstallDirs)

if(APPLE)
  find_package( Sqlite3 REQUIRED )
  find_package( nlohmann_json REQUIRED )
  find_package( nlohmann_json_schema_validator REQUIRED )
  set(CMAKE_MACOSX_RPATH ON)

elseif( UNIX )
  include (FindPkgConfig)
  
  pkg_check_modules(SQLITE REQUIRED sqlite3)
  find_package(nlohmann_json REQUIRED )
  find_package( nlohmann_json_schema_validator REQUIRED )
endif()

add_library(LotMan SHARED src/lotman.cpp src/lotman_db.cpp src/lotman_internal.cpp)
target_compile_features(LotMan PUBLIC cxx_std_17)

configure_file(lotman_version.h.in lotman_version.h)
include_directories("${PROJECT_BINARY_DIR}")

target_link_libraries(LotMan PUBLIC ${SQLITE_LIBRARIES} nlohmann_json nlohmann_json_schema_validator)

if (NOT APPLE AND UNIX)
  set_target_properties(LotMan PROPERTIES LINK_FLAGS "-Wl,--version-script=${PROJECT_SOURCE_DIR}/configs/export-symbols")
endif()

get_directory_property(TARGETS BUILDSYSTEM_TARGETS)
install(
  TARGETS ${TARGETS} 
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} 
  )

install(
  FILES src/lotman.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/lotman
  )


if(LOTMAN_BUILD_UNITTESTS)
	if(NOT LOTMAN_EXTERNAL_GTEST)
    include(ExternalProject)
    ExternalProject_Add(gtest
        PREFIX external/gtest
        URL ${CMAKE_CURRENT_SOURCE_DIR}/vendor/gtest
        INSTALL_COMMAND :
    )
  endif()
  enable_testing()
  add_subdirectory(test)
endif()
