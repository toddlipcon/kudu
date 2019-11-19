load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")

CLOUDFRONT_URL_PREFIX = "https://d3dr9sfxru4sde.cloudfront.net/"

def maybe(repo_rule, name, **kwargs):
    """Defines a repository if it does not already exist.
    """
    if name not in native.existing_rules():
        repo_rule(name = name, **kwargs)

def _kudu_thirdparty_dep(name, version, sha256, **kwargs):
    maybe(
        http_archive,
        name = name,
        strip_prefix = name + "-" + version,
        url = CLOUDFRONT_URL_PREFIX + name + "-" + version + ".tar.gz",
        **kwargs
    )

def _rules_cc():
    # rules_cc defines rules for generating C++ code from Protocol Buffers.
    http_archive(
        name = "rules_cc",
        sha256 = "35f2fb4ea0b3e61ad64a369de284e4fbbdcdba71836a5555abb5e194cf119509",
        strip_prefix = "rules_cc-624b5d59dfb45672d4239422fa1e3de1822ee110",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/rules_cc/archive/624b5d59dfb45672d4239422fa1e3de1822ee110.tar.gz",
            "https://github.com/bazelbuild/rules_cc/archive/624b5d59dfb45672d4239422fa1e3de1822ee110.tar.gz",
        ],
    )

def _rules_proto():
    http_archive(
        name = "rules_proto",
        strip_prefix = "rules_proto-45ad8c49fa41f53f2b6396a4208383c104e791d8",
        sha256 = "78fbfc814adaa0f17193454582a4867628a8471d58b80ecd0cfb7c84fe31581d",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/45ad8c49fa41f53f2b6396a4208383c104e791d8.tar.gz",
            "https://github.com/bazelbuild/rules_proto/archive/45ad8c49fa41f53f2b6396a4208383c104e791d8.tar.gz",
        ],
    )

def thirdparty_dependencies():
    _rules_cc()
    _rules_proto()

    http_archive(
        name = "rules_foreign_cc",
        strip_prefix = "rules_foreign_cc-14b79e5a04558cff964f6466a09d9d0cdee2c126",
        url = "https://github.com/bazelbuild/rules_foreign_cc/archive/14b79e5a04558cff964f6466a09d9d0cdee2c126.zip",
    )

    git_repository(
        name = "com_google_glog",
        commit = "96a2f23dca4cc7180821ca5f32e526314395d26a",
        remote = "https://github.com/google/glog.git",
    )

    new_git_repository(
        name = "crcutil",
        commit = "42148a6df6986a257ab21c80f8eca2e54544ac4d",
        remote = "https://github.com/adembo/crcutil.git",
        build_file = "//bazel/external:crcutil.BUILD",
    )

    maybe(
        http_archive,
        name = "com_google_protobuf",
        sha256 = "",
        strip_prefix = "protobuf-d09d649aea36f02c03f8396ba39a8d4db8a607e4",
        url = "https://github.com/google/protobuf/archive/d09d649aea36f02c03f8396ba39a8d4db8a607e4.tar.gz",
    )

    maybe(
        http_archive,
        name = "com_github_gflags_gflags",
        sha256 = "63ae70ea3e05780f7547d03503a53de3a7d2d83ad1caaa443a31cb20aea28654",
        strip_prefix = "gflags-28f50e0fed19872e0fd50dd23ce2ee8cd759338e",
        url = "https://github.com/gflags/gflags/archive/28f50e0fed19872e0fd50dd23ce2ee8cd759338e.tar.gz",
    )

    maybe(
        http_archive,
        name = "com_google_googletest",
        sha256 = "2f56064481649b68c98afb1b14d7b1c5e2a62ef0b48b6ba0a71f60ddd6628458",
        strip_prefix = "googletest-8756ef905878f727e8122ba25f483c887cbc3c17",
        url = "https://github.com/google/googletest/archive/8756ef905878f727e8122ba25f483c887cbc3c17.zip",
    )

    _kudu_thirdparty_dep(
        name = "libev",
        version = "4.20",
        sha256 = "f870334c7fa961e7f31087c7d76abf849f596e3048f8ed2a0aaa983cd73d449e",
        build_file = "//bazel/external:libev.BUILD",
    )

    _kudu_thirdparty_dep(
        name = "sparsepp",
        version = "1.22",
        sha256 = "5516c814fe56c692aaa36f49e696f4a6292f04b5ae79f4ab7bd121e2cc48b917",
        build_file = "//bazel/external:sparsepp.BUILD",
    )

    _kudu_thirdparty_dep(
        name = "sparsehash-c11",
        version = "cf0bffaa456f23bc4174462a789b90f8b6f5f42f",
        sha256 = "cd154b2e72af81ddce7963eb7eb2f695c60711f436c1278130f9afd8e3ea1f0e",
        build_file = "//bazel/external:sparsehash.BUILD",
    )

    maybe(
        http_archive,
        name = "rapidjson",
        sha256 = "bf7ced29704a1e696fbccf2a2b4ea068e7774fa37f6d7dd4039d0787f8bed98e",
        strip_prefix = "rapidjson-1.1.0",
        urls = ["https://github.com/Tencent/rapidjson/archive/v1.1.0.tar.gz"],
        build_file = "//bazel/external:rapidjson.BUILD",
    )
    http_archive(
        name = "com_github_jbeder_yaml_cpp",
        urls = ["https://github.com/jbeder/yaml-cpp/archive/yaml-cpp-0.6.2.tar.gz"],
        strip_prefix = "yaml-cpp-yaml-cpp-0.6.2",
        build_file = "//bazel/external:yaml.BUILD",
        sha256 = "e4d8560e163c3d875fd5d9e5542b5fd5bec810febdcba61481fe5fc4e6b1fd05",
    )
    maybe(
        http_archive,
        name = "com_github_google_snappy",
        build_file = "//bazel/external:snappy.BUILD",
        sha256 = "61e05a0295fd849072668b1f3494801237d809427cfe8fd014cda455036c3ef7",
        strip_prefix = "snappy-1.1.7",
        url = "https://github.com/google/snappy/archive/1.1.7.zip",
    )
    _kudu_thirdparty_dep(
        name = "lz4",
        version = "1.9.1",
        sha256 = "f8377c89dad5c9f266edc0be9b73595296ecafd5bfa1000de148096c50052dc4",
        build_file = "//bazel/external:lz4.BUILD",
    )
    _kudu_thirdparty_dep(
        name = "bitshuffle",
        version = "55f9b4c",
        sha256 = "a26d338c9024eeedb68e485db0b3a25a5fa00df2c23b9453f77e712c588d3079",
        build_file = "//bazel/external:bitshuffle.BUILD",
    )

    http_archive(
        name = "com_grail_bazel_toolchain",
        strip_prefix = "bazel-toolchain-8460bc20bcce06a0c4c992b1ba46e44bbe4588cc",
        sha256 = "d3fec94ffb549c4e0237cf7bebaf1cb6193a2dac4e61290941f1893b35934173",
        urls = ["https://github.com/grailbio/bazel-toolchain/archive/8460bc20bcce06a0c4c992b1ba46e44bbe4588cc.tar.gz"],
    )

    http_archive(
        name = "libunwind",
        strip_prefix = "libunwind-1.3.1",
        sha256 = "43997a3939b6ccdf2f669b50fdb8a4d3205374728c2923ddc2354c65260214f8",
        urls = ["https://github.com/libunwind/libunwind/releases/download/v1.3.1/libunwind-1.3.1.tar.gz"],
        build_file_content = """filegroup(name = "all", srcs = glob(["**"]), visibility = ["//visibility:public"])""",
    )
