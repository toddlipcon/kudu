load("//:setup.bzl", "kudu_cc_library", "kudu_proto_library")

package(default_visibility = ["//visibility:public"])

kudu_proto_library(
    name = "server_base_proto",
    srcs = ["server_base.proto"],
    has_services = True,
    protodeps = [
        "//kudu/common:common_proto",
        "//kudu/rpc:rpc_header_proto",
        "//kudu/util:version_info_proto",
        "//kudu/util:mem_tracker_proto",
        "//kudu/common:wire_protocol_proto",
    ],
)

kudu_cc_library(
    name = "server",
    srcs = [
        "default_path_handlers.cc",
        "diagnostics_log.cc",
        "generic_service.cc",
        "glog_metrics.cc",
        "pprof_path_handlers.cc",
        "rpc_server.cc",
        "rpcz-path-handler.cc",
        "server_base.cc",
        "server_base_options.cc",
        "tcmalloc_metrics.cc",
        "tracing_path_handlers.cc",
        "webserver.cc",
        "webserver_options.cc",
        "webui_util.cc",
    ],
    hdrs = glob(["*.h"]),
    deps = [
        ":server_base_proto_cc",
        "//bazel/external:mustache",
        "//bazel/external:sparsehash",
        "//bazel/external:squeasel",
        "//kudu/clock",
        "//kudu/common",
        "//kudu/common:wire_protocol",
        "//kudu/fs",
        "//kudu/gutil:base",
        "//kudu/rpc",
        "//kudu/util",
        "//kudu/util:rolling_log",
    ],
)
