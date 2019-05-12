cmake_minimum_required(VERSION 3.0)
project(BadgerDB)

set(CMAKE_CXX_STANDARD 14)

aux_source_directory(src CODESRC)
aux_source_directory(src/exceptions CODE_EXCEPTIONS)

add_executable(badgerdb_main ${CODESRC} ${CODE_EXCEPTIONS})
target_include_directories(badgerdb_main PUBLIC src src/exceptions)
