load("//:setup.bzl", "kudu_cc_library", "kudu_proto_library")

package(default_visibility = ["//visibility:public"])

kudu_proto_library(
    name = "ranger_proto",
    srcs = ["ranger.proto"],
    visibility = ["//visibility:private"],
)

kudu_cc_library(
    name = "client",
    srcs = ["ranger_client.cc"],
    hdrs = ["ranger_client.h"],
    deps = [
        "ranger_proto_cc",
        "//kudu/gutil:base",
        "//kudu/security",
        "//kudu/subprocess",
    ],
)
