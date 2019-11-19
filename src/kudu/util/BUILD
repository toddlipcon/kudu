load("//:setup.bzl", "kudu_cc_library", "kudu_cc_test", "kudu_proto_library")

package(default_visibility = ["//visibility:public"])

#TODO: can we load from version.txt?
KUDU_VERSION = "1.12.0"

[
    kudu_proto_library(
        name = "{}_proto".format(n),
        srcs = ["{}.proto".format(n)],
    )
    for n in [
        "hash",
        "histogram",
        "maintenance_manager",
        "mem_tracker",
        "version_info",
    ]
]

kudu_proto_library(
    name = "block_bloom_filter_proto",
    srcs = ["block_bloom_filter.proto"],
    protodeps = [
        ":pb_util_proto",
        ":hash_proto",
    ],
)

# pb_util stuff

kudu_proto_library(
    name = "pb_util_proto",
    srcs = ["pb_util.proto"],
)

kudu_proto_library(
    name = "jsonwriter_test_proto",
    srcs = ["jsonwriter_test.proto"],
    protodeps = [":pb_util_proto"],
)

kudu_proto_library(
    name = "pb_util_test_proto",
    srcs = ["pb_util_test.proto"],
    protodeps = [":pb_util_proto"],
)

kudu_proto_library(
    name = "proto_container_test_proto",
    srcs = glob(["proto_container_test*.proto"]),
    protodeps = [":pb_util_proto"],
)

kudu_cc_library(
    name = "pb_util",
    srcs = [
        "pb_util.cc",
        "pb_util-internal.cc",
        "pb_util-internal.h",
    ],
    hdrs = ["pb_util.h"],
    deps = [
        ":crc",
        ":pb_util_proto_cc",
        ":util",
        "//bazel/external:gtest_prod",
        "//bazel/external:protobuf",
    ],
)

kudu_cc_test(
    name = "pb_util-test",
    srcs = ["pb_util-test.cc"],
    deps = [
        ":pb_util",
        ":pb_util_test_proto_cc",
    ],
)

# version_info

genrule(
    name = "gen_version_info_rule",
    outs = ["version_defines.h"],
    cmd = "$(location //bazel/build_support:gen_version_info) --version=" + KUDU_VERSION + " $(location version_defines.h)",
    tools = ["//bazel/build_support:gen_version_info"],
)

genrule(
    name = "gen_kudu_export",
    srcs = ["kudu_export.h.in"],
    outs = ["kudu_export.h"],
    cmd = "cp $< $@",
)

kudu_cc_library(
    name = "crc",
    srcs = ["crc.cc"],
    hdrs = ["crc.h"],
    deps = [
        ":util",
        "//bazel/external:crcutil",
        "//kudu/gutil:base",
    ],
)

kudu_cc_test(
    name = "crc-test",
    deps = [":crc"],
)

kudu_cc_library(
    name = "cache",
    srcs = [
        "block_cache_metrics.cc",
        "cache.cc",
        "nvm_cache.cc",
    ],
    hdrs = [
        "block_cache_metrics.h",
        "cache.h",
        "cache_metrics.h",
        "nvm_cache.h",
    ],
    deps = [
        "//kudu/gutil:base",
        "//kudu/gutil:strings",
        "//kudu/util",
    ],
)

kudu_cc_test(
    name = "cache-test",
    deps = [
        ":cache",
    ],
)

kudu_cc_library(
    name = "rolling_log",
    srcs = ["rolling_log.cc"],
    hdrs = ["rolling_log.h"],
    deps = [
        ":net",
        ":util",
    ],
)

kudu_cc_test(
    name = "rolling_log-test",
    deps = [":rolling_log"],
)

kudu_cc_library(
    name = "ttl_cache",
    srcs = [],
    hdrs = [
        "ttl_cache.h",
        "ttl_cache_metrics.h",
    ],
    deps = [
        ":cache",
        ":util",
    ],
)

kudu_cc_test(
    name = "ttl_cache-test",
    srcs = [
        "ttl_cache-test.cc",
    ],
    deps = [":ttl_cache"],
)

kudu_cc_library(
    name = "net",
    srcs = glob(
        ["net/*.cc"],
        exclude = ["**/*-test.cc"],
    ),
    hdrs = glob(["net/*.h"]),
    deps = [
        ":threadpool",
        ":ttl_cache",
        ":util",
    ],
)

kudu_cc_library(
    name = "bloom_filter",
    srcs = ["bloom_filter.cc"],
    hdrs = ["bloom_filter.h"],
    deps = [
        ":bits",
        ":util",
    ],
)

kudu_cc_library(
    name = "block_bloom_filter",
    srcs = ["block_bloom_filter.cc"],
    hdrs = ["block_bloom_filter.h"],
    deps = [
        ":bits",
        ":block_bloom_filter_proto_cc",
        ":util",
    ],
)

kudu_cc_test(
    name = "bloom_filter-test",
    deps = [":bloom_filter"],
)

kudu_cc_library(
    name = "bits",
    srcs = ["bitmap.cc"],
    hdrs = [
        "bit-stream-utils.h",
        "bit-stream-utils.inline.h",
        "bit-util.h",
        "bitmap.h",
        "bitset.h",
    ],
    deps = ["util"],
)

[
    kudu_cc_test(
        name = name,
        deps = [":bits"],
    )
    for name in [
        "bitmap-test",
        "bitset-test",
        "bit-util-test",
    ]
]

kudu_cc_library(
    name = "rle",
    hdrs = ["rle-encoding.h"],
    deps = [":bits"],
)

kudu_cc_test(
    name = "rle-test",
    deps = [":rle"],
)

kudu_cc_library(
    name = "curl_util",
    srcs = ["curl_util.cc"],
    hdrs = ["curl_util.h"],
    deps = [
        ":util",
        "//kudu/gutil:base",
        "//kudu/gutil:strings",
        "//kudu/security:openssl_util",
        "@curl",
    ],
)

kudu_cc_test(
    name = "curl_util-test",
    deps = [
        ":curl_util",
        ":threadpool",
    ],
)

kudu_cc_library(
    name = "file_cache",
    srcs = [
        "file_cache.cc",
        "file_cache_metrics.cc",
    ],
    hdrs = [
        "file_cache.h",
        "file_cache-test-util.h",
        "file_cache_metrics.h",
    ],
    deps = [
        ":cache",
        "//kudu/gutil:base",
    ],
)

[
    kudu_cc_test(
        name = name,
        deps = [":file_cache"],
    )
    for name in [
        "file_cache-test",
        "file_cache-stress-test",
    ]
]

kudu_cc_library(
    name = "throttler",
    srcs = ["throttler.cc"],
    hdrs = ["throttler.h"],
    deps = [":util"],
)

kudu_cc_test(
    name = "throttler-test",
    deps = [":throttler"],
)

kudu_cc_library(
    name = "memcmpable_varint",
    srcs = ["memcmpable_varint.cc"],
    hdrs = ["memcmpable_varint.h"],
    deps = [":util"],
)

kudu_cc_test(
    name = "memcmpable_varint-test",
    deps = [":memcmpable_varint"],
)

kudu_cc_library(
    name = "metrics",
    deps = [
        "//bazel/external:glog",
        "//bazel/external:gtest_prod",
        "//kudu/gutil:base",
    ],
)

kudu_cc_library(
    name = "threadpool",
    srcs = ["threadpool.cc"],
    hdrs = ["threadpool.h"],
    deps = [":util"],
)

kudu_cc_test(
    name = "threadpool-test",
    deps = [":threadpool"],
)

kudu_cc_library(
    name = "maintenance_manager",
    srcs = [
        "maintenance_manager.cc",
    ],
    hdrs = [
        "maintenance_manager.h",
    ],
    deps = [
        ":maintenance_manager_proto_cc",
        ":threadpool",
        ":util",
        "//bazel/external:gtest_prod",
        "//kudu/gutil:base",
    ],
)

kudu_cc_test(
    name = "maintenance_manager-test",
    deps = [":maintenance_manager"],
)

kudu_cc_library(
    name = "test_graph",
    srcs = ["test_graph.cc"],
    hdrs = ["test_graph.h"],
    deps = [
        ":util",
        "//kudu/gutil:base",
    ],
)

kudu_cc_library(
    name = "util",
    srcs = [
        "async_logger.cc",
        "atomic.cc",
        "char_util.cc",
        "coding.cc",
        "condition_variable.cc",
        "cow_object.cc",
        "debug/trace_event_impl.cc",
        "debug/trace_event_impl_constants.cc",
        "debug/trace_event_synthetic_delay.cc",
        "debug/trace_event_impl.h",
        "debug/unwind_safeness.cc",
        "debug-util.cc",
        "decimal_util.cc",
        "easy_json.cc",
        "env.cc",
        "env_posix.cc",
        "env_util.cc",
        "errno.cc",
        "faststring.cc",
        "fault_injection.cc",
        "flag_tags.cc",
        "flag_validators.cc",
        "flags.cc",
        "group_varint.cc",
        "hdr_histogram.cc",
        "hexdump.cc",
        "init.cc",
        "jsonreader.cc",
        "jsonwriter.cc",
        "kernel_stack_watchdog.cc",
        "locks.cc",
        "logging.cc",
        "malloc.cc",
        "mem_tracker.cc",
        "metrics.cc",
        "memory/arena.cc",
        "memory/memory.cc",
        "memory/overwrite.cc",
        "minidump.cc",
        "monotime.cc",
        "mutex.cc",
        "oid_generator.cc",
        "once.cc",
        "os-util.cc",
        "path_util.cc",
        "process_memory.cc",
        # "protoc-gen-insertions.cc", TODO
        "pstack_watcher.cc",
        "random_util.cc",
        "rwc_lock.cc",
        "rw_mutex.cc",
        "sanitizer_options.cc",
        "semaphore.cc",
        #"semaphore_macosx.cc", TODO
        "signal.cc",
        "slice.cc",
        "spinlock_profiling.cc",
        "status_callback.cc",
        "status.cc",
        "string_case.cc",
        "striped64.cc",
        "subprocess.cc",
        "test_util_prod.cc",
        "thread.cc",
        "threadlocal.cc",
        "thread_restrictions.cc",
        "trace.cc",
        "trace_metrics.cc",
        "url-coding.cc",
        "user.cc",
        "version_info.cc",
        "version_util.cc",
        "web_callback_registry.cc",
        "website_util.cc",
        "yamlreader.cc",
        "zlib.cc",
    ] + ["version_defines.h"],
    hdrs = [
        "alignment.h",
        "array_view.h",
        "async_logger.h",
        "async_util.h",
        "atomic.h",
        "auto_release_pool.h",
        "barrier.h",
        "blocking_queue.h",
        "boost_mutex_utils.h",
        "char_util.h",
        "coding.h",
        "coding-inl.h",
        "condition_variable.h",
        "countdown_latch.h",
        "cow_object.h",
        "debug-util.h",
        "debug/leak_annotations.h",
        "debug/leakcheck_disabler.h",
        "debug/sanitizer_scopes.h",
        "debug/trace_event.h",
        "debug/trace_event_memory.h",
        "debug/trace_event_synthetic_delay.h",
        "debug/trace_logging.h",
        "debug/unwind_safeness.h",
        "debug_ref_counted.h",
        "decimal_util.h",
        "easy_json.h",
        "env.h",
        "env_util.h",
        "errno.h",
        "faststring.h",
        "fault_injection.h",
        "flag_tags.h",
        "flag_validators.h",
        "flags.h",
        "group_varint-inl.h",
        "hash_util.h",
        "hdr_histogram.h",
        "hexdump.h",
        "high_water_mark.h",
        "init.h",
        "inline_slice.h",
        "int128.h",
        "int128_util.h",
        "interval_tree.h",
        "interval_tree-inl.h",
        "jsonreader.h",
        "jsonwriter.h",
        "kernel_stack_watchdog.h",
        "knapsack_solver.h",
        "kudu_export.h",
        "locks.h",
        "logging.h",
        "logging_callback.h",
        "make_shared.h",
        "malloc.h",
        "mem_tracker.h",
        "memory/arena.h",
        "memory/memory.h",
        "memory/overwrite.h",
        "metrics.h",
        "minidump.h",
        "monotime.h",
        "mutex.h",
        "notification.h",
        "object_pool.h",
        "oid_generator.h",
        "once.h",
        "os-util.h",
        "path_util.h",
        "process_memory.h",
        "promise.h",
        "protobuf-annotations.h",
        "protobuf_util.h",
        "pstack_watcher.h",
        "random.h",
        "random_util.h",
        "rw_mutex.h",
        "rw_semaphore.h",
        "rwc_lock.h",
        "safe_math.h",
        "scoped_cleanup.h",
        "semaphore.h",
        "signal.h",
        "slice.h",
        "sorted_disjoint_interval_list.h",
        "spinlock_profiling.h",
        "status.h",
        "status_callback.h",
        "stopwatch.h",
        "string_case.h",
        "striped64.h",
        "subprocess.h",
        "test_util_prod.h",
        "thread.h",
        "thread_restrictions.h",
        "threadlocal.h",
        "threadlocal_cache.h",
        "trace.h",
        "trace_metrics.h",
        "url-coding.h",
        "user.h",
        "version_info.h",
        "version_util.h",
        "web_callback_registry.h",
        "website_util.h",
        "yamlreader.h",
        "zlib.h",
    ],
    copts = ["-DDISABLE_BREAKPAD"],
    linkopts = [
        "-ldl",
    ],
    deps = [
        "//kudu/gutil:base",
        "//kudu/gutil:strings",
        "hash_proto_cc",
        "mem_tracker_proto_cc",
        "histogram_proto_cc",
        "version_info_proto_cc",
        "//bazel/external:libunwind",
        "//bazel/external:libev",
        "//bazel/external:gflags",
        "//bazel/external:gtest_prod",
        "//bazel/external:rapidjson",
        "//bazel/external:yaml-cpp",
        "//bazel/external:snappy",
        "@boost//:bind",
        "@boost//:function",
        "@boost//:algorithm",
        "@boost//:uuid",
        "@boost//:signals2",
        "@boost//:optional",
        "@boost//:utility",
        "@boost//:smart_ptr",

        # TODO(todd) move these to test deps
        "jsonwriter_test_proto_cc",
        "proto_container_test_proto_cc",
    ],
)

kudu_cc_library(
    name = "test_util",
    srcs = ["test_util.cc"],
    hdrs = [
        "test_macros.h",
        "test_util.h",
    ],
    deps = [
        ":util",
        "//bazel/external:gflags",
        "//bazel/external:gtest",
        "//kudu/gutil:base",
        "//kudu/gutil:strings",
    ],
)

kudu_cc_library(
    name = "test_main",
    srcs = ["test_main.cc"],
    hdrs = ["logging_test_util.h"],
    deps = [
        ":test_util",
        ":util",
        "//bazel/external:gtest",
        "//bazel/external:gtest_prod",
    ],
)

[
    kudu_cc_test(
        name = name,
        deps = [
            ":threadpool",
            ":util",
        ],
    )
    for name in [
        "async_util-test",
        "atomic-test",
        "blocking_queue-test",
        "countdown_latch-test",
        "debug-util-test",
        "decimal_util-test",
        "easy_json-test",
        "env-test",
        "env_util-test",
        "errno-test",
        "faststring-test",
        "flags-test",
        "flag_tags-test",
        "flag_validators-test",
        "group_varint-test",
        "hash_util-test",
        "hdr_histogram-test",
        "inline_slice-test",
        "int128-test",
        "interval_tree-test",
        "jsonreader-test",
        "jsonwriter-test",
        "knapsack_solver-test",
        "logging-test",
        "map-util-test",
        "mem_tracker-test",
        "metrics-test",
        #TODO(todd) enable minidump
        #"minidump-test",
        "monotime-test",
        "mt-hdr_histogram-test",
        "mt-metrics-test",
        "mt-threadlocal-test",
        "object_pool-test",
        "oid_generator-test",
        "once-test",
        "os-util-test",
        "path_util-test",
        "process_memory-test",
        "pstack_watcher-test",
        "random-test",
        "random_util-test",
        "rwc_lock-test",
        "rw_mutex-test",
        "rw_semaphore-test",
        "safe_math-test",
        "scoped_cleanup-test",
        "slice-test",
        "sorted_disjoint_interval_list-test",
        "spinlock_profiling-test",
        "stack_watchdog-test",
        "status-test",
        "string_case-test",
        "striped64-test",
        "subprocess-test",
        "thread-test",
        "trace-test",
        "url-coding-test",
        "user-test",
        "version_util-test",
        "yamlreader-test",
    ]
]
