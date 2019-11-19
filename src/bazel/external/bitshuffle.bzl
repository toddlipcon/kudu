def bitshuffle_library(name, copts = [], extra_srcs = []):
    native.cc_library(
        name = name,
        srcs = [
            "src/bitshuffle_core.c",
            "src/bitshuffle.c",
            "src/iochain.c",
        ] + extra_srcs,
        copts = copts,
        hdrs = native.glob(["src/*.h"]),
        alwayslink = True,
        visibility = ["//visibility:public"],
        deps = ["@lz4//:lz4"],
    )

def bitshuffle_arch_library(name, arch):
    tmp_lib_target = name + "_tmp"
    mapping_target = name + "_suffix_map"

    copts = ["-std=c99"]
    if arch == "avx2":
        copts += ["-mavx2"]

    bitshuffle_library(tmp_lib_target, copts)

    # Generate a file which maps original symbol names to suffixed ones
    native.genrule(
        name = mapping_target,
        srcs = [tmp_lib_target],
        outs = [name + "_defs.h"],
        toolchains = ["@bazel_tools//tools/cpp:current_cc_toolchain"],
        cmd_bash = ("$(NM) --defined-only --extern-only $(SRCS) | " +
                    "grep ' T ' | " +
                    "while read addr type sym ; do " +
                    "echo \\#define $${sym} $${sym}_" + arch + "; " +
                    "done > $@"),
    )

    # Generate a suffixed library
    bitshuffle_library(name, copts + ["-include", name + "_defs.h"], extra_srcs = [mapping_target])
