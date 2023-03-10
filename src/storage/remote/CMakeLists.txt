set(
  sources
  FileStorage.cpp
  HttpStorage.cpp
  RemoteStorage.cpp
  MongoStorage.cpp
)
find_package(libmongocxx REQUIRED)
find_package(libbsoncxx REQUIRED)

if(REDIS_STORAGE_BACKEND)
  list(APPEND sources RedisStorage.cpp)
endif()

add_library(aws-cpp-sdk-secretsmanager SHARED IMPORTED) # or STATIC instead of SHARED
set_target_properties(aws-cpp-sdk-secretsmanager PROPERTIES
  IMPORTED_LOCATION "/home/ubuntu/sdk-build/install/lib/libaws-cpp-sdk-secretsmanager.so"
  INTERFACE_INCLUDE_DIRECTORIES "/home/ubuntu/sdk-build/install/include"
)

add_library(aws-cpp-sdk-s3 SHARED IMPORTED) # or STATIC instead of SHARED
set_target_properties(aws-cpp-sdk-s3 PROPERTIES
  IMPORTED_LOCATION "/home/ubuntu/sdk-build/install/lib/libaws-cpp-sdk-s3.so"
  INTERFACE_INCLUDE_DIRECTORIES "/home/ubuntu/sdk-build/install/include"
)


target_include_directories(ccache_framework PRIVATE ${LIBMONGOCXX_INCLUDE_DIRS} ${LIBMONGOCXX_INCLUDE_DIRS})
target_link_libraries(ccache_framework PRIVATE aws-cpp-sdk-s3 aws-cpp-sdk-secretsmanager ${AWSSDK_LINK_LIBRARIES} ${LIBMONGOCXX_LIBRARIES})
file(GLOB headers *.hpp)
list(APPEND sources ${headers})

target_sources(ccache_framework PRIVATE ${sources})
