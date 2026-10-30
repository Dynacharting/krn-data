# Fetch the engine at KRN_DATA_KRN_COMMIT and assert what was fetched
# (purity SPEC D5). Included from the root CMakeLists.txt, which sets
# KRN_DATA_KRN_REPOSITORY, KRN_DATA_KRN_COMMIT and KRN_DATA_KRN_TARGETS.
#
# Unmerged engine work: -DFETCHCONTENT_SOURCE_DIR_KRN=<krn checkout> builds
# against that tree instead of the pin. Nothing is written into it.

# CMake regexes have no {n} repetition, so the length is checked separately.
string(LENGTH "${KRN_DATA_KRN_COMMIT}" _kd_len)
if (NOT _kd_len EQUAL 40 OR NOT KRN_DATA_KRN_COMMIT MATCHES "^[0-9a-f]+$")
  message(FATAL_ERROR
    "krn-data: KRN_DATA_KRN_COMMIT must be a full 40-character commit, "
    "got '${KRN_DATA_KRN_COMMIT}'")
endif()

# D5: a FetchContent base dir outside this build tree is shared state between
# worktrees, and a green build against another ticket's engine.
if (DEFINED FETCHCONTENT_BASE_DIR)
  file(REAL_PATH "${FETCHCONTENT_BASE_DIR}" _kd_base)
  file(REAL_PATH "${CMAKE_BINARY_DIR}" _kd_bin)
  string(FIND "${_kd_base}/" "${_kd_bin}/" _kd_pos)
  if (NOT _kd_pos EQUAL 0)
    message(FATAL_ERROR
      "krn-data: FETCHCONTENT_BASE_DIR=${FETCHCONTENT_BASE_DIR} is outside "
      "this build tree. Never share it across worktrees (SPEC D5).")
  endif()
endif()

include(FetchContent)
FetchContent_Declare(krn
  GIT_REPOSITORY "${KRN_DATA_KRN_REPOSITORY}"
  GIT_TAG        "${KRN_DATA_KRN_COMMIT}"
)
FetchContent_MakeAvailable(krn)

file(REAL_PATH "${krn_SOURCE_DIR}" _kd_src)
execute_process(COMMAND git -C "${_kd_src}" rev-parse HEAD
  OUTPUT_VARIABLE _kd_head OUTPUT_STRIP_TRAILING_WHITESPACE
  RESULT_VARIABLE _kd_rc ERROR_QUIET)

if (FETCHCONTENT_SOURCE_DIR_KRN)
  # The pin is not in force; say so, and say what is.
  execute_process(COMMAND git -C "${_kd_src}" status --porcelain
    OUTPUT_VARIABLE _kd_dirty OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
  if (_kd_dirty)
    set(_kd_state "with uncommitted changes")
  else()
    set(_kd_state "clean")
  endif()
  message(WARNING
    "krn-data: engine OVERRIDDEN by FETCHCONTENT_SOURCE_DIR_KRN=${_kd_src} "
    "(HEAD '${_kd_head}', ${_kd_state}); the pin ${KRN_DATA_KRN_COMMIT} is "
    "NOT what this build compiles.")
else()
  if (NOT _kd_rc EQUAL 0 OR NOT _kd_head STREQUAL KRN_DATA_KRN_COMMIT)
    message(FATAL_ERROR
      "krn-data: fetched engine at ${_kd_src} is at '${_kd_head}', "
      "not the pin ${KRN_DATA_KRN_COMMIT}")
  endif()
  message(STATUS "krn-data: engine pinned at ${KRN_DATA_KRN_COMMIT} "
    "(fetched HEAD ${_kd_head})")
endif()

# ---- The engine defines exactly its library targets (krn's consumed-mode
# guard, ENC-1553), checked from here as the engine's own consumer smoke does.
# Only the engine's directories are walked, so this project's own
# krn_data_tests is not counted; an engine-side one would be.
set(_kd_dirs "${krn_SOURCE_DIR}")
set(_kd_all "")
while (_kd_dirs)
  list(POP_FRONT _kd_dirs _kd_d)
  get_directory_property(_kd_t DIRECTORY "${_kd_d}" BUILDSYSTEM_TARGETS)
  get_directory_property(_kd_sub DIRECTORY "${_kd_d}" SUBDIRECTORIES)
  list(APPEND _kd_all ${_kd_t})
  list(APPEND _kd_dirs ${_kd_sub})
endwhile()
list(SORT _kd_all)
set(_kd_expected ${KRN_DATA_KRN_TARGETS})
list(SORT _kd_expected)
if (NOT _kd_all STREQUAL _kd_expected)
  message(FATAL_ERROR
    "krn-data: the engine defines [${_kd_all}], expected exactly "
    "[${_kd_expected}] (KRN_DATA_KRN_TARGETS)")
endif()
foreach (_kd_x IN ITEMS krn_data krn_data_tests krn_ingest krn_tests
                        krn_server krn_connector_market)
  if (_kd_x IN_LIST _kd_all)
    message(FATAL_ERROR "krn-data: the engine defines ${_kd_x}")
  endif()
endforeach()
message(STATUS "krn-data: engine targets [${_kd_all}]")

# ---- Capture: every engine library SOURCE sits under the fetched tree. ----
# A non-relocatable engine binds its globs to this repo's src/ (SPEC D5).
foreach (_kd_lib IN LISTS _kd_expected)
  get_target_property(_kd_srcs ${_kd_lib} SOURCES)
  list(LENGTH _kd_srcs _kd_n)
  if (_kd_n EQUAL 0)
    message(FATAL_ERROR "krn-data: ${_kd_lib} has no SOURCES")
  endif()
  foreach (_kd_s IN LISTS _kd_srcs)
    file(REAL_PATH "${_kd_s}" _kd_rs BASE_DIRECTORY "${_kd_src}")
    string(FIND "${_kd_rs}" "${_kd_src}/" _kd_p)
    if (NOT _kd_p EQUAL 0)
      message(FATAL_ERROR
        "krn-data: ${_kd_lib} SOURCE ${_kd_rs} is outside the engine tree ${_kd_src}")
    endif()
  endforeach()
  message(STATUS "krn-data: ${_kd_lib}: ${_kd_n} SOURCES, all under ${_kd_src}")
endforeach()
