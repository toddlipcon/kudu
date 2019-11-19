load("//:setup.bzl", "kudu_cc_library")

package(default_visibility = ["//visibility:public"])

kudu_cc_library(
    name = "cloud",
    srcs = glob(
        ["*.cc"],
        exclude = ["*-test.cc"],
    ),
    hdrs = glob(["*.h"]),
    deps = [
        "//kudu/gutil:base",
        "//kudu/util",
        "//kudu/util:curl_util",
    ],
)

# TODO(todd) tests
