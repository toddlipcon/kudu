# Copyright (c) 2013, Cloudera, inc.

# where to put generated libraries
#set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/libkudu")

# where to put generated binaries
#set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/libkudu")

add_library(kudu_static STATIC
  libkudu.cc)
add_library(kudu SHARED
  libkudu.cc)

target_link_libraries(kudu client ${KUDU_LINK_LIBS})
target_link_libraries(kudu_static client ${KUDU_LINK_LIBS})