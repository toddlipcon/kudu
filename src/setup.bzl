load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_proto_grpc//cpp:defs.bzl", "cpp_proto_library")


def kudu_copts(is_external = False):
    return [
        "-DKUDU_HEADERS_USE_SHORT_STATUS_MACROS",
        "-DKUDU_HEADERS_USE_RICH_SLICE",
        "-DKUDU_HEADERS_NO_STUBS",
        "-Wno-sign-compare",
        "-msse4.2",
    ]

def kudu_cc_library(copts = None, is_external = False, **kwargs):
    """Generate a cc_library with platform copts
    Args:
    - copts: copts always passed to the cc_library.
    - kwargs: Any other argument to cc_library.
    """
    if not copts:
        copts = []
    native.cc_library(
        copts = copts + kudu_copts(is_external = is_external),
        **kwargs
    )

def kudu_cc_test(name, srcs = None, copts = None, deps = None, **kwargs):
    """Generate a cc_test with platform copts
    Args:
    - copts: copts always passed to the cc_test.
    - kwargs: Any other argument to cc_test.
    """
    if not copts:
        copts = []
    if not deps:
        deps = []
    if not srcs:
        srcs = [name + ".cc"]
    deps += [
        "//bazel/external:gtest",
        "//kudu/util:test_main",
        "//kudu/util:test_util",
    ]
    native.cc_test(
        name = name,
        srcs = srcs,
        copts = copts + kudu_copts(),
        deps = deps,
        **kwargs
    )

def x_kudu_proto_library(short_name, proto_deps = [], srcs = None):
    if not srcs:
        srcs = [short_name + ".proto"]
    proto_library(
        name = short_name + "_proto",
        srcs = srcs,
        deps = proto_deps,
    )
    cpp_proto_library(
        name = short_name + "_cc_proto",
        deps = [":" + short_name + "_proto"],
    )

def _kudu_proto_library_impl(ctx):
      return []

_kudu_proto_library_target = rule(
  implementation = _kudu_proto_library_impl,
  attrs = {
    "srcs": attr.label_list(),
    "deps": attr.label_list(),
    })

def kudu_proto_library(name, deps = [], srcs = None):
    if not srcs:
        srcs = [name + ".proto"]
    proto_library(
        name = name + "_pb_gen",
        srcs = srcs,
        deps = [d + "_pb_gen" for d in deps],
    )
    _kudu_proto_library_target(name=name, deps=deps)
