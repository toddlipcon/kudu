package(default_visibility = ["//visibility:public"])

load("//:setup.bzl", "kudu_cc_library", "kudu_cc_test", "kudu_proto_library")

kudu_proto_library(
    "common",
    proto_deps = [
        "//kudu/util:hash_proto",
        "//kudu/util:pb_util_proto",
        "//kudu/util/compression:compression_proto",
    ],
)

kudu_proto_library(
    "wire_protocol",
    proto_deps = [
        ":common_proto",
        "//kudu/consensus:metadata_proto",
        "//kudu/util:pb_util_proto",
    ],
)

kudu_cc_library(
    name = "timestamp",
    srcs = ["timestamp.cc"],
    hdrs = ["timestamp.h"],
    deps = [
        "//kudu/gutil:base",
        "//kudu/util:memcmpable_varint",
    ],
)

kudu_cc_library(
    name = "id_mapping",
    srcs = ["id_mapping.cc"],
    hdrs = ["id_mapping.h"],
    deps = ["//kudu/util"],
)

kudu_cc_test(
    name = "id_mapping-test",
    srcs = ["id_mapping-test.cc"],
    deps = [
        ":id_mapping",
    ],
)

kudu_cc_library(
    name = "key_encoder",
    srcs = ["key_encoder.cc"],
    hdrs = ["key_encoder.h"],
    deps = [
        ":common_cc_proto",
        ":types",
        "//kudu/util",
    ],
)

kudu_cc_library(
    name = "encoded_key",
    srcs = ["encoded_key.cc"],
    hdrs = ["encoded_key.h"],
    deps = [
        ":common",
        ":key_encoder",
        ":key_util",
        ":types",
        "//kudu/util",
    ],
)

kudu_cc_test(
    name = "encoded_key-test",
    srcs = ["encoded_key-test.cc"],
    deps = [
        ":common",
        ":encoded_key",
        ":key_encoder",
    ],
)

kudu_cc_library(
    name = "key_util",
    srcs = ["key_util.cc"],
    hdrs = [
        "key_util.h",
        "key_util-inl.h",
    ],
    deps = [
        ":common",
        ":common_cc_proto",
        ":key_encoder",
        ":types",
        "//kudu/gutil:base",
    ],
)

kudu_cc_test(
    name = "key_util-test",
    srcs = ["key_util-test.cc"],
    deps = [
        ":key_util",
    ],
)

kudu_cc_library(
    name = "types",
    srcs = ["types.cc"],
    hdrs = ["types.h"],
    deps = [
        ":common_cc_proto",
        "//kudu/gutil:base",
        "//kudu/gutil:strings",
        "//kudu/util",
        "//kudu/util:bits",
    ],
)

kudu_cc_test(
    name = "types-test",
    srcs = ["types-test.cc"],
    deps = [
        ":types",
    ],
)

kudu_cc_library(
    name = "common",
    srcs = [
        "columnblock.cc",
        "partial_row.cc",
        "row_changelist.cc",
        "rowblock.cc",
        "schema.cc",
    ],
    hdrs = [
        "columnblock.h",
        "partial_row.h",
        "row.h",
        "row_changelist.h",
        "rowblock.h",
        "schema.h",
    ],
    deps = [
        ":common_cc_proto",
        ":id_mapping",
        ":key_encoder",
        ":types",
        "//bazel/external:glog",
    ],
)

[
    kudu_cc_test(
        name = test,
        srcs = [test + ".cc"],
        deps = [":common"],
    )
    for test in [
        "rowblock-test",
        "columnblock-test",
        "schema-test",
        "partial_row-test",
        "row_changelist-test",
    ]
]

kudu_cc_library(
    name = "column_predicate",
    srcs = [
        "column_predicate.cc",
        "key_predicates.cc",
    ],
    hdrs = [
        "column_predicate.h",
        "key_predicates.h",
    ],
    deps = [
        ":common",
        ":common_cc_proto",
        ":key_util",
        ":types",
        "//kudu/util",
        "//kudu/util:bloom_filter",
        "//kudu/util:hash_cc_proto",
    ],
)

kudu_cc_test(
    name = "column_predicate-test",
    srcs = ["column_predicate-test.cc"],
    deps = [
        ":column_predicate",
        ":common",
    ],
)

kudu_cc_library(
    name = "partition",
    srcs = ["partition.cc"],
    hdrs = ["partition.h"],
    deps = [
        ":common",
        "//kudu/util",
        "//kudu/util:bits",
        "//kudu/util:pb_util",
    ],
)

kudu_cc_test(
    name = "partition-test",
    srcs = ["partition-test.cc"],
    deps = [
        ":common",
        ":partition",
    ],
)

kudu_cc_library(
    name = "partition_pruner",
    srcs = ["partition_pruner.cc"],
    hdrs = ["partition_pruner.h"],
    deps = [
        ":column_predicate",
        ":common",
        ":encoded_key",
        ":key_encoder",
        ":key_util",
        ":partition",
        ":scan_spec",
        ":types",
        "//kudu/gutil:base",
        "//kudu/gutil:strings",
    ],
)

kudu_cc_test(
    name = "partition_pruner-test",
    srcs = ["partition_pruner-test.cc"],
    deps = [
        ":partition_pruner",
    ],
)

kudu_cc_library(
    name = "scan_spec",
    srcs = ["scan_spec.cc"],
    hdrs = ["scan_spec.h"],
    deps = [
        ":column_predicate",
        ":common",
        ":encoded_key",
    ],
)

kudu_cc_test(
    name = "scan_spec-test",
    srcs = ["scan_spec-test.cc"],
    deps = [
        ":common",
        ":scan_spec",
    ],
)

kudu_cc_library(
    name = "iterator",
    srcs = [
        "generic_iterators.cc",
        "iterator_stats.cc",
    ],
    hdrs = [
        "column_materialization_context.h",
        "generic_iterators.h",
        "iterator.h",
        "iterator_stats.h",
        "rowid.h",
    ],
    deps = [
        ":column_predicate",
        ":common",
        ":scan_spec",
    ],
)

kudu_cc_test(
    name = "generic_iterators-test",
    srcs = ["generic_iterators-test.cc"],
    deps = [
        ":iterator",
    ],
)

kudu_cc_library(
    name = "wire_protocol",
    srcs = [
        "row_operations.cc",
        "wire_protocol.cc",
    ],
    hdrs = [
        "row_operations.h",
        "wire_protocol.h",
    ],
    deps = [
        ":column_predicate",
        ":common",
        ":wire_protocol_cc_proto",
        "//kudu/gutil:base",
        "//kudu/util:net",
        "//kudu/util:pb_util",
    ],
)

[
    kudu_cc_test(
        name = test,
        srcs = [test + ".cc"],
        deps = [":wire_protocol"],
    )
    for test in [
        "wire_protocol-test",
        "row_operations-test",
    ]
]
