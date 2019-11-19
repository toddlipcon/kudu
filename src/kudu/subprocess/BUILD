load("//:setup.bzl", "kudu_cc_library", "kudu_proto_library")

package(default_visibility = ["//visibility:public"])

kudu_proto_library(
    name = "subprocess_proto",
    srcs = ["subprocess.proto"],
    protodeps = ["//kudu/common:wire_protocol_proto"],
    visibility = ["//visibility:private"],
)

kudu_cc_library(
    name = "subprocess",
    srcs = [
        "server.cc",
        "subprocess_protocol.cc",
        "subprocess_protocol.h",
        "subprocess_proxy.cc",
    ],
    hdrs = [
        "server.h",
        "subprocess_proxy.h",
    ],
    deps = [
        "subprocess_proto_cc",
        "//bazel/external:glog",
        "//kudu/common:wire_protocol",
        "//kudu/gutil:base",
        "//kudu/tools:tool_proto_cc",
    ],
)
