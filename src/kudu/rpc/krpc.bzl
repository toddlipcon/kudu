load("@rules_proto_grpc//:plugin.bzl", "ProtoPluginInfo")
load(
    "@rules_proto_grpc//:aspect.bzl",
    "ProtoLibraryAspectNodeInfo",
    "proto_compile_aspect_attrs",
    "proto_compile_aspect_impl",
    "proto_compile_attrs",
    "proto_compile_impl",
)

# Create aspect for krpc_proto_compile
krpc_proto_compile_aspect = aspect(
    implementation = proto_compile_aspect_impl,
    provides = [ProtoLibraryAspectNodeInfo],
    attr_aspects = ["deps"],
    attrs = dict(
        proto_compile_aspect_attrs,
        _plugins = attr.label_list(
            doc = "List of protoc plugins to apply",
            providers = [ProtoPluginInfo],
            default = [
                Label("@rules_proto_grpc//cpp:cpp_plugin"),
                Label("//kudu/rpc:krpc_proto_plugin"),
            ],
        ),
        _prefix = attr.string(
            doc = "String used to disambiguate aspects when generating outputs",
            default = "krpc_proto_compile_aspect",
        )
    ),
    toolchains = [str(Label("@rules_proto_grpc//protobuf:toolchain_type"))],
)

# Create compile rule to apply aspect
_rule = rule(
    implementation = proto_compile_impl,
    attrs = dict(
        proto_compile_attrs,
        deps = attr.label_list(
            mandatory = True,
            providers = [ProtoInfo, ProtoLibraryAspectNodeInfo],
            aspects = [krpc_proto_compile_aspect],
        ),
    ),
)

# Create macro for converting attrs and passing to compile
def krpc_proto_compile(**kwargs):
    _rule(
        verbose_string = "{}".format(kwargs.get("verbose", 0)),
        merge_directories = True,
        **{k: v for k, v in kwargs.items() if k != "merge_directories"}
    )


def krpc_proto_library(**kwargs):
    # Compile protos
    name_pb = kwargs.get("name") + "_pb"
    krpc_proto_compile(
        name = name_pb,
        **{k: v for (k, v) in kwargs.items() if k in ("deps", "verbose")} # Forward args
    )

    # Create krpc library
    native.cc_library(
        name = kwargs.get("name"),
        srcs = [name_pb],
        deps = PROTO_DEPS + KRPC_DEPS,
        includes = [name_pb],
        visibility = kwargs.get("visibility"),
    )


PROTO_DEPS = [
    "@com_google_protobuf//:protoc_lib",
]

KRPC_DEPS = [
             "//kudu/gutil:base", "//kudu/rpc"
]
