# load("@rules_proto//proto:defs.bzl", "proto_library")
load("@com_google_protobuf//:protobuf.bzl", "cc_proto_library")
#load("@rules_cc//cc:defs.bzl", pb_cc_proto_library="cc_proto_library")

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

def kudu_proto_library(name, cc_deps = [], proto_deps = [], srcs = None):
    if not srcs:
        srcs = [name + ".proto"]
    cc_proto_library(
        name = name,
        srcs = srcs,
        deps = proto_deps,
    )
