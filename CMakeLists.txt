cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME dcfinder)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/dcfinder_extension.cpp
    src/dcfinder/predicate.cpp
    src/dcfinder/pli.cpp
    src/dcfinder/evidence.cpp
    src/dcfinder/cover_search.cpp
    src/dcfinder/soundness.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
