# rwlock-perf
ADD_EXECUTABLE(rwlock-perf rwlock-perf.cc)
TARGET_LINK_LIBRARIES(rwlock-perf ${KUDU_TEST_LINK_LIBS})

ADD_EXECUTABLE(merge-test merge-test.cc)
TARGET_LINK_LIBRARIES(merge-test ${KUDU_TEST_LINK_LIBS})


# index codegen test

ADD_EXECUTABLE(index-codegen-test index-codegen-test.cc)
TARGET_LINK_LIBRARIES(index-codegen-test ${LLVM_MODULE_LIBS} ${KUDU_TEST_LINK_LIBS})
