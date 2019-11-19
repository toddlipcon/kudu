cc_library(
    name = "crcutil",
    srcs = glob([
        "code/*.cc",
        "code/*.h",
        "examples/interface.*",
    ]) + [
        "tests/aligned_alloc.h",
    ],
    hdrs = ["examples/interface.h"],
    copts = [
        "-DCRCUTIL_USE_MM_CRC32=1",
        "-Iexternal/crcutil/code",
        "-Iexternal/crcutil/examples",
        "-Iexternal/crcutil/tests",
    ],
    include_prefix = "crcutil",
    strip_include_prefix = "examples",
    visibility = ["//visibility:public"],
)
