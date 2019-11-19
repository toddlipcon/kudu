load("//:setup.bzl", "kudu_cc_library", "kudu_cc_test", "kudu_proto_library")

package(default_visibility = ["//visibility:public"])

kudu_proto_library(
    name = "fs_proto",
    srcs = ["fs.proto"],
)

kudu_cc_library(
    name = "fs",
    srcs = [
        "block_id.cc",
        "block_manager.cc",
        "block_manager_metrics.cc",
        "data_dirs.cc",
        "dir_manager.cc",
        "dir_util.cc",
        "error_manager.cc",
        "file_block_manager.cc",
        "fs_manager.cc",
        "fs_report.cc",
        "log_block_manager.cc",
    ],
    hdrs = glob(["*.h"]),
    deps = [
        ":fs_proto_cc",
        "//bazel/external:sparsepp",
        "//kudu/gutil:base",
        "//kudu/util",
        "//kudu/util:file_cache",
        "//kudu/util:net",
        "//kudu/util:pb_util",
    ],
)

kudu_cc_library(
    name = "test_util",
    srcs = ["log_block_manager-test-util.cc"],
    hdrs = ["log_block_manager-test-util.h"],
    visibility = ["//visibility:private"],
    deps = [":fs"],
)

[
    kudu_cc_test(
        name = path.split(".")[0],
        srcs = [path],
        deps = [
            ":fs",
            ":test_util",
        ],
    )
    for path in glob(["*-test.cc"])
]
