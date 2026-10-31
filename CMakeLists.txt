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
set(KRN_DATA_KRN_COMMIT     "ca544ec5fff80e4a7cf0c4bf5c2feefacad596b3")
set(KRN_DATA_KRN_TARGETS    krn_engine krn_host krn_plugin_reference)

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

  # RecordIngressParityTest reads data/include/krn/data/RecordFieldSource.hpp
  # (here) and include/krn/engine/JsonFieldSource.hpp (the engine) under one
  # KRN_TEST_SOURCE_DIR, so that root is a build-tree overlay of both.
  set(_kd_test_source_root "${CMAKE_BINARY_DIR}/krn_test_source_dir")
  file(MAKE_DIRECTORY "${_kd_test_source_root}")
  file(CREATE_LINK "${PROJECT_SOURCE_DIR}/data" "${_kd_test_source_root}/data" SYMBOLIC)
  file(CREATE_LINK "${krn_SOURCE_DIR}/include" "${_kd_test_source_root}/include" SYMBOLIC)

  # The L1 schema gate and its self-test, as the engine registered them.
  find_package(Python3 COMPONENTS Interpreter)
  if (NOT Python3_Interpreter_FOUND)
    message(FATAL_ERROR
      "krn-data: no Python3 interpreter, so krn_schema_no_compute cannot run. "
      "Install one, or pass -DKRN_DATA_BUILD_TESTS=OFF.")
  endif()
  add_test(NAME krn_schema_no_compute
    COMMAND "${Python3_EXECUTABLE}"
            "${PROJECT_SOURCE_DIR}/data/schema/check_no_compute.py"
            lint "${PROJECT_SOURCE_DIR}/data/schema/clickhouse.sql")
  set_tests_properties(krn_schema_no_compute PROPERTIES TIMEOUT 120)
  add_test(NAME krn_schema_no_compute_selftest
    COMMAND "${Python3_EXECUTABLE}"
            "${PROJECT_SOURCE_DIR}/data/schema/check_no_compute.py"
            self-test)
  set_tests_properties(krn_schema_no_compute_selftest PROPERTIES TIMEOUT 120)
else()
  message(STATUS "krn-data: KRN_DATA_BUILD_TESTS=OFF, no tests registered")
endif()

# krn_data, krn_ingest and (with KRN_DATA_BUILD_TESTS) krn_data_tests.
add_subdirectory(data)
