load("//:setup.bzl", "kudu_cc_library")

genrule(
    name = "copy_fb303",
    srcs = ["@thrift//:fb303_if"],
    outs = ["share/fb303/if/fb303.thrift"],
    cmd = "cp external/thrift/contrib/fb303/if/fb303.thrift $@",
)

genrule(
    name = "hms_thrift",
    srcs = [
        "hive_metastore.thrift",
        ":copy_fb303",
    ],
    outs = [
        "fb303_types.h",
        "fb303_constants.h",
        "fb303_types.cpp",
        "fb303_constants.cpp",
        "hive_metastore_types.h",
        "hive_metastore_constants.h",
        "hive_metastore_types.cpp",
        "hive_metastore_constants.cpp",
        "ThriftHiveMetastore.h",
        "ThriftHiveMetastore.cpp",
        "FacebookService.h",
        "FacebookService.cpp",
    ],
    cmd = "$(location @thrift//:compiler) -r  -I $(RULEDIR) -out $(RULEDIR) -gen cpp $(location hive_metastore.thrift)",
    tools = ["@thrift//:compiler"],
)

kudu_cc_library(
    name = "hms_thrift_cc",
    srcs = [":hms_thrift"],
    visibility = ["//visibility:public"],
    deps = ["@thrift"],
)

kudu_cc_library(
    name = "hms",
    srcs = [
        "hms_catalog.cc",
        "hms_client.cc",
    ],
    hdrs = [
        "hms_catalog.h",
        "hms_client.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":hms_thrift_cc",
        "//kudu/common",
        "//kudu/common:common_proto_cc",
        "//kudu/thrift",
        "//kudu/util",
    ],
)
