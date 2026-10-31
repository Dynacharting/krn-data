cmake_minimum_required(VERSION 3.20)
project(KRN_DATA LANGUAGES CXX)

# krn-data's own options are spelled KRN_DATA_BUILD_* and KRN_DATA_KRN_*.
# Never KRN_BUILD_*, which the engine reads, and never KRN_DATA_ARROW_*,
# KRN_DATA_DBN_* or KRN_DATA_TEST_*, which the engine's data/ already uses.
option(KRN_DATA_BUILD_TESTS "Build krn_data_tests and register it with ctest" ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---- The engine pin (purity SPEC D5) ----
# A full 40-character commit of Dynacharting/krn master. Bumping it is a
# one-line diff here; KRN_DATA_KRN_TARGETS is what the engine defines in
# consumed mode at that commit, and changes in the same diff when it changes.
set(KRN_DATA_KRN_REPOSITORY "https://github.com/Dynacharting/krn.git")
set(KRN_DATA_KRN_COMMIT     "ff6e1b490740acea4da729b72fc3130b64652667")
set(KRN_DATA_KRN_TARGETS    krn_engine krn_plugin_reference)

include("${PROJECT_SOURCE_DIR}/cmake/KrnEngine.cmake")

if (KRN_DATA_BUILD_TESTS)
  include(FetchContent)
  # googletest v1.14.0, the engine's own test framework version, by commit.
  FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        f8d7d77c06936315286eb55f8de22cd23c188571
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  enable_testing()
  add_executable(krn_data_tests tests/EngineLink.cpp)
  target_link_libraries(krn_data_tests PRIVATE krn::krn_engine GTest::gtest_main)
  add_test(NAME krn_data_tests COMMAND krn_data_tests)
else()
  message(STATUS "krn-data: KRN_DATA_BUILD_TESTS=OFF, no tests registered")
endif()
