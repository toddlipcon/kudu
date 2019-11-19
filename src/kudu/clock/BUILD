load("//:setup.bzl", "kudu_cc_library")

package(default_visibility = ["//visibility:public"])

kudu_cc_library(
    name = "clock",
    srcs = glob(
        ["*.cc"],
        exclude = ["*-test.cc"],
    ),
    hdrs = glob(["*.h"]),
    # TODO(todd) condition this on the system
    local_defines = ["KUDU_HAS_SYSTEM_TIME_SOURCE=1"],
    deps = [
        "//kudu/common:common_proto_cc",
        "//kudu/common:timestamp",
        "//kudu/gutil:base",
        "//kudu/util:net",
        "//kudu/util/cloud",
    ],
)

# TODO(todd) tests
