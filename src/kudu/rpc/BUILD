load("@rules_cc//cc:defs.bzl", "cc_binary")
load("//:setup.bzl", "kudu_cc_library", "kudu_cc_test", "kudu_copts", "kudu_proto_library")

package(default_visibility = ["//visibility:public"])

kudu_proto_library(
    name = "rpc_header_proto",
    srcs = ["rpc_header.proto"],
    protodeps = [
        "//kudu/security:token_proto",
        "//kudu/util:pb_util_proto",
    ],
)

kudu_proto_library(
    name = "rpc_introspection_proto",
    srcs = ["rpc_introspection.proto"],
    protodeps = [":rpc_header_proto"],
)

cc_binary(
    name = "protoc_gen_krpc",
    srcs = ["protoc-gen-krpc.cc"],
    copts = kudu_copts(),
    deps = [
        ":rpc_header_proto_cc",
        "//bazel/external:glog",
        "//kudu/gutil:base",
        "//kudu/gutil:strings",
        "//kudu/util",
        "@com_google_protobuf//:protobuf",
        "@com_google_protobuf//:protoc_lib",
    ],
)

kudu_cc_library(
    name = "rpc",
    srcs = [
        "acceptor_pool.cc",
        "blocking_ops.cc",
        "client_negotiation.cc",
        "connection.cc",
        "connection_id.cc",
        "constants.cc",
        "inbound_call.cc",
        "messenger.cc",
        "negotiation.cc",
        "outbound_call.cc",
        "periodic.cc",
        "proxy.cc",
        "reactor.cc",
        "remote_method.cc",
        "remote_user.cc",
        "request_tracker.cc",
        "result_tracker.cc",
        "rpc.cc",
        "rpc_context.cc",
        "rpc_controller.cc",
        "rpc_sidecar.cc",
        "rpc_verification_util.cc",
        "rpcz_store.cc",
        "sasl_common.cc",
        "sasl_helper.cc",
        "serialization.cc",
        "server_negotiation.cc",
        "service_if.cc",
        "service_pool.cc",
        "service_queue.cc",
        "transfer.cc",
        "user_credentials.cc",
    ],
    hdrs = glob(
        ["*.h"],
        exclude = ["*-test-*"],
    ),
    linkopts = ["-lsasl2"],
    deps = [
        ":rpc_header_proto_cc",
        ":rpc_introspection_proto_cc",
        "//bazel/external:libev",
        "//kudu/gutil:base",
        "//kudu/security",
        "//kudu/util",
        "//kudu/util:pb_util",
    ],
)

# Test protos and libs

kudu_proto_library(
    name = "rtest_diff_package_proto",
    srcs = ["rtest_diff_package.proto"],
)

kudu_proto_library(
    name = "rtest_proto",
    srcs = ["rtest.proto"],
    has_services = True,
    protodeps = [
        ":rpc_header_proto",
        ":rtest_diff_package_proto",
    ],
)

[
    kudu_cc_test(
        name = t,
        srcs = [
            "{}.cc".format(t),
            "rpc-test-base.h",
        ],
        deps = [
            ":rpc",
            ":rtest_proto_cc",
            "//kudu/security:test_util",
        ],
    )
    for t in [
        "reactor-test",
        "rpc-test",
        "rpc-bench",
        "mt-rpc-test",
        "exactly_once_rpc-test",
        "periodic-test",
        "request_tracker-test",
    ]
]
