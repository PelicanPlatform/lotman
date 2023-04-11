add_executable(lotman-gtest main.cpp)
if( NOT LOTMAN_EXTERNAL_GTEST )
    add_dependencies(lotman-gtest gtest)
    include_directories("${PROJECT_SOURCE_DIR}/vendor/gtest/googletest/include")
endif()

if(LOTMAN_EXTERNAL_GTEST)
    set(LIBGTEST "gtest")
else()
    set(LIBGTEST "${CMAKE_BINARY_DIR}/external/gtest/src/gtest-build/lib/libgtest.a")
endif()

target_link_libraries(lotman-gtest LotMan "${LIBGTEST}" pthread)

add_test(
  NAME
    unit
  COMMAND
    ${CMAKE_CURRENT_BINARY_DIR}/lotman-gtest
  )
