load("//:setup.bzl", "kudu_cc_library", "kudu_cc_test", "kudu_proto_library")
load(":krpc.bzl", "krpc_proto_library")

package(default_visibility = ["//visibility:public"])

kudu_proto_library(
    name = "rpc_header",
    proto_deps = [
        "//kudu/security:token_proto",
        "//kudu/util:pb_util_proto",
        "@com_google_protobuf//:descriptor_proto",
    ],
)

kudu_proto_library(
    name = "rpc_introspection",
    proto_deps = [":rpc_header_proto"],
)

cc_binary(
    name = "protoc_gen_krpc",
    srcs = ["protoc-gen-krpc.cc"],
    deps = [
        ":rpc_header_proto",
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
        ":rpc_header_proto",
        ":rpc_introspection",
        "//bazel/external:libev",
        "//kudu/gutil:base",
        "//kudu/security",
        "//kudu/util",
        "//kudu/util:pb_util",
    ],
)

# Test protos and libs

kudu_proto_library("rtest_diff_package")

krpc_proto_library(
    name = "rtest_proto",
    srcs = ["rtest.proto"],
    deps = [
        ":rpc_header_proto",
        ":rtest_diff_package_proto",
    ],
)

kudu_cc_test(
    name = "reactor-test",
    srcs = [
        "reactor-test.cc",
        "rpc-test-base.h",
    ],
    deps = [
        ":rpc",
        ":rtest",
        "//kudu/security:test_util",
    ],
)
