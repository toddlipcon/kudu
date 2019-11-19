load("//:setup.bzl", "kudu_proto_library")

package(default_visibility = ["//visibility:public"])

kudu_proto_library(
    name = "tool_proto",
    srcs = ["tool.proto"],
    protodeps = [
        "//kudu/common:common_proto",
        "//kudu/common:wire_protocol_proto",
        "//kudu/tablet:metadata_proto",
        "//kudu/tablet:tablet_proto",
    ],
)
