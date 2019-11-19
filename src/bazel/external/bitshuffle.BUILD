load("@org_apache_kudu//bazel/external:bitshuffle.bzl", "bitshuffle_arch_library", "bitshuffle_library")

bitshuffle_library(name = "bitshuffle_default")

bitshuffle_arch_library(
    name = "bitshuffle_avx2",
    arch = "avx2",
)

cc_library(
    name = "bitshuffle",
    hdrs = glob(["src/*.h"]),
    strip_include_prefix = "src",
    visibility = ["//visibility:public"],
    deps = [
        ":bitshuffle_avx2",
        ":bitshuffle_default",
    ],
)
