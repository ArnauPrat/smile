add_definitions(${DEFAULT_DEFINES})

if (UNIX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++14 -fno-rtti -fno-exceptions")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -pg")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")
endif (UNIX)

add_subdirectory(base)
add_subdirectory(storage)
add_subdirectory(memory)
#add_subdirectory(data)

#add_library(smile STATIC)
#
#target_link_libraries(smile
#    base
#    storage
#    memory
#)

SET(SMILE_LIBRARIES base storage memory)
SET(SMILE_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/src)

add_subdirectory(tests)
