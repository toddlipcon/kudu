load("//:setup.bzl", "kudu_cc_library", "kudu_cc_test", "kudu_proto_library")

package(default_visibility = ["//visibility:public"])

kudu_proto_library(
    name = "cfile_proto",
    srcs = ["cfile.proto"],
    protodeps = [
        "//kudu/common:common_proto",
        "//kudu/util:pb_util_proto",
        "//kudu/util/compression:compression_proto",
    ],
)

kudu_cc_library(
    name = "block_cache",
    srcs = ["block_cache.cc"],
    hdrs = [
        "block_cache.h",
        "block_handle.h",
    ],
    deps = [
        "//kudu/fs",
        "//kudu/util:cache",
    ],
)

kudu_cc_test(
    name = "block_cache-test",
    deps = [":block_cache"],
)

kudu_cc_library(
    name = "prefix_key",
    srcs = ["prefix_key.cc"],
    hdrs = [
        "prefix_key.h",
    ],
    deps = [
        "//kudu/util",
    ],
)

kudu_cc_library(
    name = "options",
    srcs = ["cfile_options.cc"],
    hdrs = [
        "cfile_options.h",
    ],
    deps = [
        "//kudu/common",
        "//kudu/util",
    ],
)

kudu_cc_library(
    name = "encodings",
    srcs = [
        "binary_plain_block.cc",
        "binary_prefix_block.cc",
        "bitshuffle_arch_wrapper.cc",
        "bshuf_block.cc",
    ],
    hdrs = [
        "binary_plain_block.h",
        "binary_prefix_block.h",
        "bitshuffle_arch_wrapper.h",
        "block_encodings.h",
        "bshuf_block.h",
        "plain_bitmap_block.h",
        "plain_block.h",
        "rle_block.h",
    ],
    deps = [
        ":block_cache",
        ":cfile_proto_cc",
        ":options",
        ":prefix_key",
        "//bazel/external:bitshuffle",
        "//bazel/external:sparsehash",
        "//kudu/common:iterator",
        "//kudu/util:bits",
        "//kudu/util:memcmpable_varint",
        "//kudu/util:rle",
    ],
)

kudu_cc_test(
    name = "encoding-test",
    shard_count = 8,
    deps = [
        ":cfile",
        ":encodings",
        "//kudu/common:test_util",
    ],
)

kudu_cc_library(
    name = "cfile",
    srcs = [
        "block_compression.cc",
        "block_compression.h",
        "cfile_reader.cc",
        "cfile_util.cc",
        "cfile_writer.cc",
        "index_block.cc",
        "index_btree.cc",
        "type_encodings.cc",

        # The dict block is special -- goes here to avoid a circular reference since it
        # calls back into cfile reader/writer.
        "binary_dict_block.cc",
    ],
    hdrs = [
        "binary_dict_block.h",
        "block_pointer.h",
        "cfile_reader.h",
        "cfile_util.h",
        "cfile_writer.h",
        "index_block.h",
        "index_btree.h",
        "type_encodings.h",
    ],
    deps = [
        ":encodings",
        ":options",
        ":prefix_key",
        "//kudu/gutil:base",
        "//kudu/util:rle",
        "//kudu/util/compression",
    ],
)

kudu_cc_library(
    name = "bloomfile",
    srcs = ["bloomfile.cc"],
    hdrs = ["bloomfile.h"],
    deps = [
        ":cfile",
        "//kudu/util",
        "//kudu/util:bloom_filter",
    ],
)

kudu_cc_test(
    name = "bloomfile-test",
    srcs = [
        "bloomfile-test.cc",
        "bloomfile-test-base.cc",
        "bloomfile-test-base.h",
    ],
    deps = [":bloomfile"],
)

kudu_cc_test(
    name = "mt-bloomfile-test",
    srcs = [
        "bloomfile-test-base.cc",
        "bloomfile-test-base.h",
        "mt-bloomfile-test.cc",
    ],
    tags = ["exclusive"],
    deps = [":bloomfile"],
)

kudu_cc_test(
    name = "cfile-test",
    srcs = [
        "cfile-test.cc",
        "cfile-test-base.h",
    ],
    shard_count = 8,
    deps = [
        ":cfile",
        "//kudu/common:test_util",
    ],
)

kudu_cc_test(
    name = "index-test",
    deps = [":cfile"],
)
