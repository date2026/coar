aux_source_directory(. DIR_LIB_SRCS)
add_library (common ${DIR_LIB_SRCS})
include_directories("../util")

# Add include directory for generated protobuf headers (parent directory's build directory)
get_filename_component(PARENT_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/.." ABSOLUTE)
target_include_directories(common PRIVATE "${PARENT_BINARY_DIR}")
target_link_libraries(common gRPC::grpc++ protobuf::libprotobuf)
target_link_libraries(common ec hiredis protocol util)
