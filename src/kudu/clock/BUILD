package(default_visibility = ["//visibility:public"])

load("//:setup.bzl", "kudu_cc_library", "kudu_cc_test", "kudu_proto_library")

kudu_cc_library(
    name = "clock",
    srcs = glob(
        ["*.cc"],
        exclude = ["*-test.cc"],
    ),
    hdrs = glob(["*.h"]),
    deps = [
        "//kudu/common:common_cc_proto",
        "//kudu/common:timestamp",
        "//kudu/gutil:base",
        "//kudu/util:net",
    ],
)

# TODO(todd) tests
