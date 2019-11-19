load("//:setup.bzl", "kudu_cc_library", "kudu_cc_test", "kudu_proto_library")

package(default_visibility = ["//visibility:public"])

kudu_proto_library(
    name = "metadata_proto",
    srcs = ["metadata.proto"],
    protodeps = ["//kudu/common:common_proto"],
)

kudu_proto_library(
    name = "opid_proto",
    srcs = ["opid.proto"],
)

kudu_proto_library(
    name = "replica_management_proto",
    srcs = ["replica_management.proto"],
)

kudu_proto_library(
    name = "log_proto",
    srcs = ["log.proto"],
    protodeps = [
        "//kudu/common:common_proto",
        "//kudu/consensus:consensus_proto",
        "//kudu/consensus:metadata_proto",
        "//kudu/util/compression:compression_proto",
    ],
)

kudu_proto_library(
    name = "consensus_proto",
    srcs = ["consensus.proto"],
    has_services = True,
    protodeps = [
        "//kudu/common:common_proto",
        ":replica_management_proto",
        ":metadata_proto",
        ":opid_proto",
        "//kudu/common:wire_protocol_proto",
        "//kudu/rpc:rpc_header_proto",
        "//kudu/tablet:tablet_proto",
        "//kudu/tablet:metadata_proto",
        "//kudu/tserver:tserver_proto",
        "//kudu/tserver:tserver_admin_proto",
    ],
)

kudu_cc_library(
    name = "consensus",
    srcs = [
        "consensus_meta.cc",
        "consensus_meta_manager.cc",
        "consensus_peers.cc",
        "consensus_queue.cc",
        "leader_election.cc",
        "log.cc",
        "log_anchor_registry.cc",
        "log_cache.cc",
        "log_index.cc",
        "log_metrics.cc",
        "log_reader.cc",
        "log_util.cc",
        "opid_util.cc",
        "peer_manager.cc",
        "pending_rounds.cc",
        "quorum_util.cc",
        "raft_consensus.cc",
        "time_manager.cc",
    ],
    hdrs = [
        "consensus_meta.h",
        "consensus_meta_manager.h",
        "consensus_peers.h",
        "consensus_queue.h",
        "leader_election.h",
        "log.h",
        "log_anchor_registry.h",
        "log_cache.h",
        "log_index.h",
        "log_metrics.h",
        "log_reader.h",
        "log_util.h",
        "opid_util.h",
        "peer_manager.h",
        "pending_rounds.h",
        "quorum_util.h",
        "raft_consensus.h",
        "ref_counted_replicate.h",
        "time_manager.h",
    ],
    deps = [
        "consensus_proto_cc",
        "log_proto_cc",
        "metadata_proto_cc",
        "//bazel/external:gtest_prod",
        "//kudu/clock",
        "//kudu/common",
        "//kudu/common:timestamp",
        "//kudu/common:wire_protocol",
        "//kudu/fs",
        "//kudu/gutil:base",
        "//kudu/util:metrics",
        "//kudu/util:pb_util",
        "//kudu/util/compression",
    ],
)

kudu_cc_library(
    name = "test_util",
    hdrs = [
        "consensus-test-util.h",
        "log-test-base.h",
    ],
    deps = [":consensus"],
)

kudu_cc_test(
    name = "consensus-tests",
    srcs = glob(["*-test.cc"]) + [
    ],
    deps = [
        ":consensus",
        ":test_util",
        "//kudu/common:wire_protocol_test_util",
        "//kudu/tablet",
    ],
)
