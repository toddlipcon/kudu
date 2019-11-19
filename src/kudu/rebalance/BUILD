load("//:setup.bzl", "kudu_cc_library")

package(default_visibility = ["//visibility:public"])

kudu_cc_library(
    name = "rebalance",
    srcs = [
        "cluster_status.cc",
        "placement_policy_util.cc",
        "rebalance_algo.cc",
        "rebalancer.cc",
    ],
    hdrs = glob(["*.h"]),
    deps = [
        "//kudu/consensus",
        "//kudu/gutil:base",
        "//kudu/util",
    ],
)

# TODO(todd) tests
