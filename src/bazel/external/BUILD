package(default_visibility = ["//visibility:public"])

alias(
    name = "gtest",
    actual = "@com_google_googletest//:gtest",
)

alias(
    name = "gtest_prod",
    actual = "@com_google_googletest//:gtest_prod",
)

alias(
    name = "protobuf",
    actual = "@com_google_protobuf//:protobuf",
)

alias(
    name = "crcutil",
    actual = "@crcutil//:crcutil",
)

alias(
    name = "rapidjson",
    actual = "@rapidjson//:rapidjson",
)

alias(
    name = "yaml-cpp",
    actual = "@com_github_jbeder_yaml_cpp//:yaml-cxx",
)

alias(
    name = "gflags",
    actual = "@com_github_gflags_gflags//:gflags",
)

alias(
    name = "libev",
    actual = "@libev//:libev",
)

alias(
    name = "snappy",
    actual = "@com_github_google_snappy//:snappy",
)

alias(
    name = "glog",
    actual = "@com_google_glog//:glog",
)

alias(
    name = "sparsepp",
    actual = "@sparsepp//:sparsepp",
)

alias(
    name = "sparsehash",
    actual = "@sparsehash-c11//:sparsehash",
)

alias(
    name = "lz4",
    actual = "@lz4//:lz4",
)

alias(
    name = "bitshuffle",
    actual = "@bitshuffle//:bitshuffle",
)

load("@rules_foreign_cc//tools/build_defs:configure.bzl", "configure_make")

configure_make(
    name = "libunwind",
    configure_options = ["--disable-minidebuginfo"],
    lib_source = "@libunwind//:all",
    shared_libraries = glob(["libunwind*.so"]),
    static_libraries = glob(["libunwind*.a"]),
    visibility = ["//visibility:public"],
)
