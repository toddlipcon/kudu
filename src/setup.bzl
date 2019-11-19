load("@com_google_protobuf//:protobuf.bzl", "proto_gen")

# TODO
def if_static(deps, otherwise = []):
    return deps

def kudu_copts(is_external = False):
    return [
        "-DKUDU_HEADERS_USE_SHORT_STATUS_MACROS",
        "-DKUDU_HEADERS_USE_RICH_SLICE",
        "-DKUDU_HEADERS_NO_STUBS",
        "-DDISABLE_CODEGEN",  # TODO(todd) codegen!
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

def _select_malloc():
    return select({
        "//bazel:disable_tcmalloc": "@bazel_tools//tools/cpp:malloc",
        "//conditions:default": "@com_google_tcmalloc//tcmalloc",
    })

def kudu_cc_binary(copts = None, is_external = False, **kwargs):
    """Generate a cc_binary with platform copts
    Args:
    - copts: copts always passed to the cc_binary.
    - kwargs: Any other argument to cc_binary.
    """
    if not copts:
        copts = []
    native.cc_binary(
        copts = copts + kudu_copts(is_external = is_external),
        malloc = _select_malloc(),
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
        malloc = _select_malloc(),
        deps = deps,
        **kwargs
    )

def well_known_proto_libs():
    """Set of standard protobuf protos, like Any and Timestamp.

    This list should be provided by protobuf.bzl, but it's not.
    """
    return [
        "@com_google_protobuf//:any_proto",
        "@com_google_protobuf//:api_proto",
        "@com_google_protobuf//:compiler_plugin_proto",
        "@com_google_protobuf//:descriptor_proto",
        "@com_google_protobuf//:duration_proto",
        "@com_google_protobuf//:empty_proto",
        "@com_google_protobuf//:field_mask_proto",
        "@com_google_protobuf//:source_context_proto",
        "@com_google_protobuf//:struct_proto",
        "@com_google_protobuf//:timestamp_proto",
        "@com_google_protobuf//:type_proto",
        "@com_google_protobuf//:wrappers_proto",
    ]

# Appends a suffix to a list of deps.
def tf_deps(deps, suffix):
    tf_deps = []

    # If the package name is in shorthand form (ie: does not contain a ':'),
    # expand it to the full name.
    for dep in deps:
        tf_dep = dep

        if not ":" in dep:
            dep_pieces = dep.split("/")
            tf_dep += ":" + dep_pieces[len(dep_pieces) - 1]

        tf_deps += [tf_dep + suffix]

    return tf_deps

def _proto_cc_hdrs(srcs):
    ret = [s[:-len(".proto")] + ".pb.h" for s in srcs]
    return ret

def _krpc_cc_hdrs(srcs):
    ret = []
    ret += [s[:-len(".proto")] + ".proxy.h" for s in srcs]
    ret += [s[:-len(".proto")] + ".service.h" for s in srcs]
    return ret

def _proto_cc_srcs(srcs, use_krpc_plugin = False):
    return [s[:-len(".proto")] + ".pb.cc" for s in srcs]

def _krpc_cc_srcs(srcs):
    ret = []
    ret += [s[:-len(".proto")] + ".proxy.cc" for s in srcs]
    ret += [s[:-len(".proto")] + ".service.cc" for s in srcs]
    return ret

def kudu_proto_library(
        name,
        srcs = [],
        has_services = None,
        protodeps = [],
        visibility = None,
        testonly = 0,
        cc_libs = [],
        cc_api_version = 2,
        make_default_target_header_only = False,
        exports = []):
    """Make a proto library, possibly depending on other proto libraries."""

    native.proto_library(
        name = name,
        srcs = srcs,
        deps = protodeps + well_known_proto_libs(),
        visibility = visibility,
        testonly = testonly,
    )

    kudu_proto_library_cc(
        name = name,
        testonly = testonly,
        srcs = srcs,
        cc_libs = cc_libs,
        use_krpc_plugin = has_services,
        make_default_target_header_only = make_default_target_header_only,
        protodeps = protodeps,
        visibility = visibility,
    )

# Re-defined protocol buffer rule to allow building "header only" protocol
# buffers, to avoid duplicate registrations. Also allows non-iterable cc_libs
# containing select() statements.
def cc_proto_library(
        name,
        srcs = [],
        deps = [],
        cc_libs = [],
        include = None,
        protoc = "@com_google_protobuf//:protoc",
        use_krpc_plugin = False,
        make_default_target_header_only = False,
        protolib_name = None,
        protolib_deps = [],
        **kargs):
    """Bazel rule to create a C++ protobuf library from proto source files.

    Args:
      name: the name of the cc_proto_library.
      srcs: the .proto files of the cc_proto_library.
      deps: a list of dependency labels; must be cc_proto_library.
      cc_libs: a list of other cc_library targets depended by the generated
          cc_library.
      include: a string indicating the include path of the .proto files.
      protoc: the label of the protocol compiler to generate the sources.
      use_krpc_plugin: a flag to indicate whether to call the krpc C++ plugin
          when processing the proto files.
      make_default_target_header_only: Controls the naming of generated
          rules. If True, the `name` rule will be header-only, and an _impl rule
          will contain the implementation. Otherwise the header-only rule (name
          + "_headers_only") must be referred to explicitly.
      protolib_name: the name for the proto library generated by this rule.
      protolib_deps: The dependencies to proto libraries.
      **kargs: other keyword arguments that are passed to cc_library.
    """

    wkt_deps = ["@com_google_protobuf//:cc_wkt_protos"]
    all_protolib_deps = protolib_deps + wkt_deps

    includes = []
    if include != None:
        includes = [include]
    if protolib_name == None:
        protolib_name = name

    krpc_cpp_plugin = None
    plugin_options = []
    plugin_suffixes = []
    gen_srcs = _proto_cc_srcs(srcs)
    gen_hdrs = _proto_cc_hdrs(srcs)

    if use_krpc_plugin:
        plugin_suffixes += [".service.cc", ".proxy.cc", ".service.h", ".proxy.h"]
        krpc_cpp_plugin = "//kudu/rpc:protoc_gen_krpc"
        gen_srcs += _krpc_cc_srcs(srcs)
        gen_hdrs += _krpc_cc_hdrs(srcs)

    outs = gen_srcs + gen_hdrs

    proto_gen(
        name = protolib_name + "_genproto",
        srcs = srcs,
        outs = outs,
        gen_cc = 1,
        includes = includes,
        plugin = krpc_cpp_plugin,
        plugin_language = "krpc",
        plugin_options = plugin_options,
        extra_cc_suffixes = plugin_suffixes,
        protoc = protoc,
        visibility = ["//visibility:public"],
        deps = [s + "_genproto" for s in all_protolib_deps],
    )
    if use_krpc_plugin:
        cc_libs += ["//kudu/gutil:base", "//kudu/rpc"]

    impl_name = name + "_impl"
    header_only_name = name + "_headers_only"
    header_only_deps = tf_deps(protolib_deps, "_cc_headers_only")

    if make_default_target_header_only:
        native.alias(
            name = name,
            actual = header_only_name,
            visibility = kargs["visibility"],
        )
    else:
        native.alias(
            name = name,
            actual = impl_name,
            visibility = kargs["visibility"],
        )

    native.cc_library(
        name = impl_name,
        srcs = gen_srcs,
        hdrs = gen_hdrs,
        deps = cc_libs + deps,
        includes = includes,
        alwayslink = 1,
        **kargs
    )
    native.cc_library(
        name = header_only_name,
        deps = [
            "@com_google_protobuf//:protobuf_headers",
        ] + header_only_deps + if_static([impl_name]),
        hdrs = gen_hdrs,
        **kargs
    )

def kudu_proto_library_cc(
        name,
        srcs = [],
        has_services = None,
        protodeps = [],
        visibility = None,
        testonly = 0,
        cc_libs = [],
        cc_stubby_versions = None,
        use_krpc_plugin = True,
        j2objc_api_version = 1,
        cc_api_version = 2,
        js_codegen = "jspb",
        make_default_target_header_only = False):
    js_codegen = js_codegen  # unused argument
    native.filegroup(
        name = name + "_proto_srcs",
        srcs = srcs + tf_deps(protodeps, "_proto_srcs"),
        testonly = testonly,
        visibility = visibility,
    )

    protolib_deps = tf_deps(protodeps, "")
    cc_deps = tf_deps(protodeps, "_cc")
    cc_name = name + "_cc"
    if not srcs:
        # This is a collection of sub-libraries. Build header-only and impl
        # libraries containing all the sources.
        proto_gen(
            name = name + "_genproto",
            protoc = "@com_google_protobuf//:protoc",
            visibility = ["//visibility:public"],
            deps = [s + "_genproto" for s in protolib_deps],
        )

        native.alias(
            name = cc_name + "_genproto",
            actual = name + "_genproto",
            testonly = testonly,
            visibility = visibility,
        )

        native.alias(
            name = cc_name + "_headers_only",
            actual = cc_name,
            testonly = testonly,
            visibility = visibility,
        )

        native.cc_library(
            name = cc_name,
            deps = cc_deps + ["@com_google_protobuf//:protobuf_headers"],  # + if_static([name + "_cc_impl"]),
            testonly = testonly,
            visibility = visibility,
        )
        native.cc_library(
            name = cc_name + "_impl",
            deps = [s + "_impl" for s in cc_deps],  # + ["@com_google_protobuf//:cc_wkt_protos"],
        )

        return

    cc_proto_library(
        name = cc_name,
        protolib_name = name,
        testonly = testonly,
        srcs = srcs,
        cc_libs = cc_libs + if_static(
            ["@com_google_protobuf//:protobuf"],
            ["@com_google_protobuf//:protobuf_headers"],
        ),
        copts = [
            "-Wno-unknown-warning-option",
            "-Wno-unused-but-set-variable",
            "-Wno-sign-compare",
        ] + kudu_copts(),
        make_default_target_header_only = make_default_target_header_only,
        protoc = "@com_google_protobuf//:protoc",
        use_krpc_plugin = use_krpc_plugin,
        visibility = visibility,
        deps = cc_deps,  #  + ["@com_google_protobuf//:cc_wkt_protos"],
        protolib_deps = protolib_deps,
    )
