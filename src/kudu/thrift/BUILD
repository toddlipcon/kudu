load("//:setup.bzl", "kudu_cc_library")

kudu_cc_library(
    name = "thrift",
    srcs = [
        "client.cc",
        "ha_client_metrics.h",
        "sasl_client_transport.cc",
    ],
    hdrs = [
        "client.h",
        "sasl_client_transport.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//bazel/external:glog",
        "//kudu/rpc",
        "@thrift",
    ],
)
