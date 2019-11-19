load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "utf",
    srcs = glob(["utf/*.c"]),
    hdrs = glob(["utf/*.h"]),
    visibility = ["//visibility:public"],
)

cc_library(
    name = "hash",
    srcs = glob(
        ["hash/*.cc"],
        exclude = ["**/*-test.cc"],
    ),
    hdrs = glob(["hash/*.h"]),
    visibility = ["//visibility:public"],
    deps = [
        "base",
    ],
)

cc_library(
    name = "strings",
    srcs = glob(
        ["strings/*.cc"],
        exclude = ["**/*-test.cc"],
    ),
    hdrs = glob(["strings/*.h"]),
    copts = [
        "-funsigned-char",
        "-Wno-char-subscripts",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "base",
        "hash",
        "utf",
        "//bazel/external:glog",
    ],
)

cc_library(
    name = "base",
    srcs = glob(
        [
            "*.cc",
            "threading/*.cc",
            "*.c",
        ],
        exclude = ["*-test.*"],
    ) + [
        "spinlock_internal.h",
        "spinlock_linux-inl.h",
        "logging-inl.h",
    ],
    hdrs = [
        "arm_instruction_set_select.h",
        "atomic_refcount.h",
        "atomicops.h",
        "atomicops-internals-x86.h",
        "basictypes.h",
        "bits.h",
        "casts.h",
        "charmap.h",
        "cpu.h",
        "cycleclock-inl.h",
        "dynamic_annotations.h",
        "endian.h",
        "fixedarray.h",
        "int128.h",
        "integral_types.h",
        "linux_syscall_support.h",
        "macros.h",
        "manual_constructor.h",
        "map-util.h",
        "mathlimits.h",
        "move.h",
        "once.h",
        "port.h",
        "ref_counted.h",
        "ref_counted_memory.h",
        "singleton.h",
        "spinlock.h",
        "stl_util.h",
        "stringprintf.h",
        "strtoint.h",
        "synchronization_profiling.h",
        "sysinfo.h",
        "template_util.h",
        "thread_annotations.h",
        "threading/thread_collision_warner.h",
        "type_traits.h",
        "valgrind.h",
        "walltime.h",
    ],
    copts = ["-funsigned-char"],
    visibility = ["//visibility:public"],
    deps = ["//bazel/external:glog"],
)
