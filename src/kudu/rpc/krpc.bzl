load("@com_google_protobuf//:protobuf.bzl", "proto_gen")

def krpc_proto_library(
        name,
        srcs,
        hdrs = [],
        deps = [],
        protoc = "@com_google_protobuf//:protoc"):
    proto_gen(name = name, srcs = srcs, deps = deps, protoc = protoc)

PROTO_DEPS = [
    "@com_google_protobuf//:protoc_lib",
]

KRPC_DEPS = [
    "//kudu/gutil:base",
    "//kudu/rpc",
]
