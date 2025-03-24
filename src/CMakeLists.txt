# minimum version of cmake
cmake_minimum_required (VERSION 3.1)

# project name
project (lmq_openec)

add_subdirectory(common)
add_subdirectory(ec)
add_subdirectory(fs)
add_subdirectory(protocol)
add_subdirectory(util)

aux_source_directory(. DIR_SRCS)
add_executable(ECCoordinator ECCoordinator.cc)
add_executable(ECAgent ECAgent.cc)
add_executable(ECClient ECClient.cc)
add_executable(HDFSDemo HDFSDemo.cc)


target_link_libraries(ECCoordinator common pthread fs)
target_link_libraries(ECAgent common pthread fs)
target_link_libraries(ECClient common pthread)
target_link_libraries(HDFSDemo fs)

