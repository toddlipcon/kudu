load("//:setup.bzl", "kudu_cc_library", "kudu_proto_library")

package(default_visibility = ["//visibility:public"])

kudu_proto_library(
    name = "transactions_proto",
    srcs = ["transactions.proto"],
)

kudu_cc_library(
    name = "transactions",
    srcs = [
        "txn_status_entry.cc",
        "txn_status_entry.h",
        "txn_status_manager.cc",
        "txn_status_tablet.cc",
    ],
    hdrs = [
        "txn_status_manager.h",
        "txn_status_tablet.h",
    ],
    deps = [
        ":transactions_proto_cc",
        "//kudu/gutil:base",
        "//kudu/tablet",
        "@boost//:optional",
    ],
)
