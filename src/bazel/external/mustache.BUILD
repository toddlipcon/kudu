cc_library(
    name = "mustache",
    srcs = ["mustache.cc"],
    hdrs = ["mustache.h"],
    copts = [
        "-Iexternal/mustache",
    ],
    includes = ["."],
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:algorithm",
        "@rapidjson",
    ],
)
