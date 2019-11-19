load("//:setup.bzl", "kudu_cc_library")

package(default_visibility = ["//visibility:public"])

kudu_cc_library(
    name = "kserver",
    srcs = ["kserver.cc"],
    hdrs = ["kserver.h"],
    deps = ["//kudu/server"],
)
