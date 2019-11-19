# From kythe
package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # BSD 3-clause

filegroup(
    name = "license",
    srcs = ["COPYING"],
)

genrule(
    name = "snappy-stubs-public",
    srcs = ["@org_apache_kudu//bazel/external/snappy:snappy-stubs-public.h"],
    outs = ["snappy-stubs-public.h"],
    cmd = "cat $(location @org_apache_kudu//bazel/external/snappy:snappy-stubs-public.h) > $@",
)

cc_library(
    name = "snappy",
    srcs = [
        "snappy.cc",
        "snappy-c.cc",
        "snappy-sinksource.cc",
        "snappy-stubs-internal.cc",
    ],
    hdrs = [
        "snappy.h",
        "snappy-c.h",
        "snappy-internal.h",
        "snappy-sinksource.h",
        "snappy-stubs-internal.h",
        ":snappy-stubs-public",
    ],
    copts = [
        "-Wno-non-virtual-dtor",
        "-Wno-unused-variable",
        "-Wno-implicit-fallthrough",
        "-Wno-unused-function",
    ],
    includes = ["."],
)
