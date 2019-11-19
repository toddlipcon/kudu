load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")

licenses(["notice"])

##############################
# Compiler
##############################

COMPILER_ROOT = "compiler/cpp/src/thrift/"

LANGS = [
    "cpp",
    "java",
]

genrule(
    name = "parse_flex",
    srcs = [COMPILER_ROOT + "thriftl.ll"],
    outs = ["thriftl.cc"],
    cmd = "M4=$(M4) $(FLEX) -o$@ $(SRCS)",
    toolchains = [
        "@rules_m4//m4:current_m4_toolchain",
        "@rules_flex//flex:current_flex_toolchain",
    ],
)

genrule(
    name = "parse_bison",
    srcs = [COMPILER_ROOT + "thrifty.yy"],
    outs = [
        "thrift/thrifty.cc",
        "thrift/thrifty.hh",
    ],
    cmd = "M4=$(M4) $(BISON) -d -o$(location thrift/thrifty.cc) $(SRCS)",
    toolchains = [
        "@rules_m4//m4:current_m4_toolchain",
        "@rules_bison//bison:current_bison_toolchain",
    ],
)

genrule(
    name = "gen_version",
    srcs = [COMPILER_ROOT + "version.h.in"],
    outs = [
        "thrift/version.h",
    ],
    cmd = "cat $(SRCS) > $@",
)

cc_library(
    name = "compiler_core",
    srcs = [
        ":parse_bison",
        ":parse_flex",
    ] + glob([
        COMPILER_ROOT + "parse/*.cc",
        COMPILER_ROOT + "common.cc",
        COMPILER_ROOT + "generate/t_generator.cc",
    ]),
    hdrs = [
        ":gen_version",
    ] + glob([
        COMPILER_ROOT + "generate/t_generator.h",
        COMPILER_ROOT + "generate/t_generator_registry.h",
        COMPILER_ROOT + "parse/*.h",
        COMPILER_ROOT + "*.h",
    ]),
    includes = ["compiler/cpp/src"],
)

[
    cc_library(
        name = "gen_{}".format(lang),
        srcs = [
            COMPILER_ROOT + "generate/t_{}_generator.cc".format(lang),
            COMPILER_ROOT + "generate/t_oop_generator.h",
        ],
        deps = [":compiler_core"],
        alwayslink = True,
    )
    for lang in LANGS
]

cc_binary(
    name = "compiler",
    srcs = [
        COMPILER_ROOT + "audit/t_audit.cpp",
        COMPILER_ROOT + "audit/t_audit.h",
        COMPILER_ROOT + "main.cc",
    ],
    includes = ["compiler/cpp/src/"],
    visibility = ["//visibility:public"],
    deps = [":compiler_core"] + [":gen_{}".format(lang) for lang in LANGS],
)

##############################
# C++ Library
##############################

LIB_ROOT = "lib/cpp/src/thrift/"

genrule(
    name = "gen_config_h",
    srcs = ["@org_apache_kudu//bazel/external/thrift:config.h"],
    outs = [LIB_ROOT+"config.h"],
    cmd = "cp $< $@",
)

# From lib/cpp/CMakeLists.txt
core_sources = [
    "TApplicationException.cpp",
    "TOutput.cpp",
    "async/TAsyncChannel.cpp",
    "async/TConcurrentClientSyncInfo.h",
    "async/TConcurrentClientSyncInfo.cpp",
    "concurrency/ThreadManager.cpp",
    "concurrency/TimerManager.cpp",
    "concurrency/Util.cpp",
    "processor/PeekProcessor.cpp",
    "protocol/TBase64Utils.cpp",
    "protocol/TDebugProtocol.cpp",
    "protocol/TJSONProtocol.cpp",
    "protocol/TMultiplexedProtocol.cpp",
    "protocol/TProtocol.cpp",
    "transport/TTransportException.cpp",
    "transport/TFDTransport.cpp",
    "transport/TSimpleFileTransport.cpp",
    "transport/THttpTransport.cpp",
    "transport/THttpClient.cpp",
    "transport/THttpServer.cpp",
    "transport/TSocket.cpp",
    "transport/TSocketPool.cpp",
    "transport/TServerSocket.cpp",
    "transport/TTransportUtils.cpp",
    "transport/TBufferTransports.cpp",
    "server/TConnectedClient.cpp",
    "server/TServerFramework.cpp",
    "server/TSimpleServer.cpp",
    "server/TThreadPoolServer.cpp",
    "server/TThreadedServer.cpp",
]

# Use std::thread based threads/mutexes
concurrency_sources = [
    "concurrency/StdThreadFactory.cpp",
    "concurrency/StdMutex.cpp",
    "concurrency/StdMonitor.cpp",
]

thriftcpp_sources = core_sources + concurrency_sources

cc_library(
    name = "thrift",
    srcs = [LIB_ROOT + p for p in thriftcpp_sources],
    hdrs = [":gen_config_h"] + glob(
        [
            LIB_ROOT + "**/*.h",
            LIB_ROOT + "**/*.tcc",
        ],
        exclude = ["windows/*.h"],
    ),
    strip_include_prefix = "lib/cpp/src",
    visibility = ["//visibility:public"],
    deps = [
        "@boost//:algorithm",
        "@boost//:atomic",
        "@boost//:format",
        "@boost//:locale",
        "@boost//:numeric_conversion",
        "@boost//:scoped_array",
        "@boost//:smart_ptr",
        "@boost//:thread",
    ],
)

##############################
# fb303
##############################
filegroup(
    name = "fb303_if",
    srcs = glob(["contrib/fb303/**/*.thrift"]),
    visibility = ["//visibility:public"],
)
