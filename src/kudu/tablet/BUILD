load("//:setup.bzl", "kudu_cc_library", "kudu_cc_test", "kudu_proto_library")

package(default_visibility = ["//visibility:public"])

kudu_proto_library(
    name = "metadata_proto",
    srcs = ["metadata.proto"],
    protodeps = [
        "//kudu/common:common_proto",
        "//kudu/consensus:opid_proto",
        "//kudu/fs:fs_proto",
    ],
)

kudu_proto_library(
    name = "tablet_proto",
    srcs = ["tablet.proto"],
    protodeps = [
        ":metadata_proto",
        "//kudu/common:common_proto",
        "//kudu/common:wire_protocol_proto",
    ],
)

kudu_cc_library(
    name = "tablet",
    srcs = [
        "cfile_set.cc",
        "compaction.cc",
        "compaction_policy.cc",
        "delta_applier.cc",
        "delta_compaction.cc",
        "delta_iterator_merger.cc",
        "delta_key.cc",
        "delta_stats.cc",
        "delta_store.cc",
        "delta_tracker.cc",
        "deltafile.cc",
        "deltamemstore.cc",
        "diskrowset.cc",
        "lock_manager.cc",
        "memrowset.cc",
        "multi_column_writer.cc",
        "mutation.cc",
        "mvcc.cc",
        "op_order_verifier.cc",
        "row_op.cc",
        "rowset.cc",
        "rowset_info.cc",
        "rowset_metadata.cc",
        "rowset_tree.cc",
        "svg_dump.cc",
        "tablet.cc",
        "tablet_bootstrap.cc",
        "tablet_metadata.cc",
        "tablet_metrics.cc",
        "tablet_mm_ops.cc",
        "tablet_replica.cc",
        "tablet_replica_mm_ops.cc",
        "txn_participant.cc",
    ] + glob(
        ["ops/*.cc"],
        exclude = ["**/*-test.cc"],
    ),
    hdrs = [
        "cfile_set.h",
        "compaction.h",
        "compaction_policy.h",
        "concurrent_btree.h",
        "delta_applier.h",
        "delta_compaction.h",
        "delta_iterator_merger.h",
        "delta_key.h",
        "delta_relevancy.h",
        "delta_stats.h",
        "delta_store.h",
        "delta_tracker.h",
        "deltafile.h",
        "deltamemstore.h",
        "diskrowset.h",
        "local_tablet_writer.h",
        "lock_manager.h",
        "memrowset.h",
        "mock-rowsets.h",
        "multi_column_writer.h",
        "mutation.h",
        "mvcc.h",
        "op_order_verifier.h",
        "row_op.h",
        "rowset.h",
        "rowset_info.h",
        "rowset_metadata.h",
        "rowset_tree.h",
        "svg_dump.h",
        "tablet.h",
        "tablet-harness.h",
        "tablet_bootstrap.h",
        "tablet_mem_trackers.h",
        "tablet_metadata.h",
        "tablet_metrics.h",
        "tablet_mm_ops.h",
        "tablet_replica.h",
        "tablet_replica_mm_ops.h",
        "txn_coordinator.h",
        "txn_participant.h",
    ] + glob(["ops/*.h"]),
    deps = [
        ":tablet_proto_cc",
        "//bazel/external:glog",
        "//bazel/external:gtest_prod",
        "//kudu/cfile",
        "//kudu/cfile:bloomfile",
        "//kudu/common:iterator",
        "//kudu/common:partition",
        "//kudu/common:timestamp",
        "//kudu/common:wire_protocol",
        "//kudu/consensus",
        "//kudu/fs",
        "//kudu/util:bloom_filter",
        "//kudu/util:maintenance_manager",
        "//kudu/util:memcmpable_varint",
        "//kudu/util:throttler",
    ],
)

kudu_cc_library(
    name = "test_util",
    srcs =
        [
            "tablet-test-util.cc",
            "tablet_replica-test-base.cc",
        ],
    hdrs = [
        "diskrowset-test-base.h",
        "key_value_test_schema.h",
        "tablet-test-base.h",
        "tablet-test-util.h",
        "tablet_replica-test-base.h",
        "txn_participant-test-util.h",
    ],
    deps = [
        ":tablet",
        "//bazel/external:gtest",
        "//kudu/common:test_util",
            "//kudu/util:test_util",
    ],
)

_shard_count = {
             "all_types-scan-correctness-test.cc": 8,
}

[
    kudu_cc_test(
        name = path.split(".")[0],
        srcs = [path],
        deps = [
            ":tablet",
            ":test_util",
            "//kudu/common:wire_protocol_test_util",
            "//kudu/consensus:test_util",
            "//kudu/util",
            "//kudu/util:test_graph",
        ],
        shard_count = _shard_count.get(path.split("/")[-1]),
    )
    for path in glob(["**/*-test.cc"])
]
