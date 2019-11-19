load("@rules_cc//cc:defs.bzl", "cc_binary")
load("//:setup.bzl", "kudu_cc_library", "kudu_copts", "kudu_proto_library")

package(default_visibility = ["//visibility:public"])

kudu_proto_library(
    name = "tserver_proto",
    srcs = ["tserver.proto"],
    protodeps = [
        "//kudu/common:common_proto",
        "//kudu/common:wire_protocol_proto",
        "//kudu/security:token_proto",
        "//kudu/tablet:tablet_proto",
        "//kudu/util:pb_util_proto",
    ],
)

kudu_proto_library(
    name = "tserver_service_proto",
    srcs = ["tserver_service.proto"],
    has_services = True,
    protodeps = [
        ":tserver_proto",
        ":tserver_admin_proto",
        "//kudu/rpc:rpc_header_proto",
    ],
)

kudu_proto_library(
    name = "tablet_copy_proto",
    srcs = ["tablet_copy.proto"],
    has_services = True,
    protodeps = [
        "//kudu/common:wire_protocol_proto",
        "//kudu/fs:fs_proto",
        "//kudu/consensus:metadata_proto",
        "//kudu/rpc:rpc_header_proto",
        "//kudu/util:pb_util_proto",
        "//kudu/tablet:metadata_proto",
    ],
)

kudu_proto_library(
    name = "tserver_admin_proto",
    srcs = ["tserver_admin.proto"],
    has_services = True,
    protodeps = [
        ":tserver_proto",
        "//kudu/common:common_proto",
        "//kudu/common:wire_protocol_proto",
        "//kudu/consensus:metadata_proto",
        "//kudu/rpc:rpc_header_proto",
        "//kudu/tablet:metadata_proto",
    ],
)

kudu_cc_library(
    name = "tserver",
    srcs = [
        "heartbeater.cc",
        "scanner_metrics.cc",
        "scanners.cc",
        "tablet_copy_client.cc",
        "tablet_copy_service.cc",
        "tablet_copy_source_session.cc",
        "tablet_server.cc",
        "tablet_server_options.cc",
        "tablet_service.cc",
        "ts_tablet_manager.cc",
        "tserver_path_handlers.cc",
    ],
    hdrs = [
        "heartbeater.h",
        "mini_tablet_server.h",
        "scanner_metrics.h",
        "scanners.h",
        "tablet_copy_client.h",
        "tablet_copy_service.h",
        "tablet_copy_source_session.h",
        "tablet_replica_lookup.h",
        "tablet_server.h",
        "tablet_server_options.h",
        "tablet_service.h",
        "ts_tablet_manager.h",
        "tserver_path_handlers.h",
    ],
    deps = [
        ":tablet_copy_proto_cc",
        ":tserver_service_proto_cc",
        "//kudu/common:iterator",
        "//kudu/consensus",
        "//kudu/kserver",
        "//kudu/master:master_proto_cc",
        "//kudu/rpc",
        "//kudu/security",
        "//kudu/tablet",
        "//kudu/transactions",
        "//kudu/util",
    ],
)

kudu_cc_library(
    name = "mini_tserver",
    srcs = [
        "mini_tablet_server.cc",
    ],
    hdrs = ["mini_tablet_server.h"],
    deps = [":tserver"],
)

cc_binary(
    name = "kudu-tserver",
    srcs = [
        "tablet_server_main.cc",
        "tablet_server_runner.cc",
        "tablet_server_runner.h",
    ],
    copts = kudu_copts(),
    deps = [":tserver"],
)

# TODO
#        "tablet_copy-test-base.h",
#        "tablet_server-test-base.h",
#        "tablet_server_test_util.h",
