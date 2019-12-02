package(default_visibility = ["//visibility:public"])

load("//:setup.bzl", "kudu_cc_library", "kudu_proto_library")

kudu_proto_library(
    name = "compression_proto",
    srcs = ["compression.proto"],
)

kudu_cc_library(
    name = "compression",
    srcs = glob(
        ["*.cc"],
        exclude = ["*-test.cc"],
    ),
    hdrs = glob(["*.h"]),
    deps = [
        "compression_cc_proto",
        "//bazel/external:lz4",
        "//bazel/external:snappy",
        "//kudu/gutil:strings",
        "//kudu/util",
    ],
)
