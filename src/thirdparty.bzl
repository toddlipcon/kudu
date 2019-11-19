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
        sha256 = sha256,
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

def _rules_boost():
    git_repository(
        name = "com_github_nelhage_rules_boost",
        commit = "1e3a69bf2d5cd10c34b74f066054cd335d033d71",
        remote = "https://github.com/nelhage/rules_boost",
        shallow_since = "1591047380 -0700",
    )

def _thrift():
    # Flex
    http_archive(
        name = "rules_m4",
        sha256 = "c67fa9891bb19e9e6c1050003ba648d35383b8cb3c9572f397ad24040fb7f0eb",
        urls = ["https://github.com/jmillikin/rules_m4/releases/download/v0.2/rules_m4-v0.2.tar.xz"],
    )

    http_archive(
        name = "rules_flex",
        sha256 = "f1685512937c2e33a7ebc4d5c6cf38ed282c2ce3b7a9c7c0b542db7e5db59d52",
        urls = ["https://github.com/jmillikin/rules_flex/releases/download/v0.2/rules_flex-v0.2.tar.xz"],
    )

    # Bison
    http_archive(
        name = "rules_bison",
        sha256 = "6ee9b396f450ca9753c3283944f9a6015b61227f8386893fb59d593455141481",
        urls = ["https://github.com/jmillikin/rules_bison/releases/download/v0.2/rules_bison-v0.2.tar.xz"],
    )

    _kudu_thirdparty_dep(
        name = "thrift",
        version = "0.11.0",
        sha256 = "c4ad38b6cb4a3498310d405a91fef37b9a8e79a50cd0968148ee2524d2fa60c2",
        build_file = "//bazel/external:thrift.BUILD",
    )

def thirdparty_dependencies():
    # TODO(todd) should move all of these to use thirdparty URL with the upstream githubs
    # as backups.
    _rules_cc()
    _rules_proto()
    _rules_boost()

    http_archive(
        name = "rules_foreign_cc",
        strip_prefix = "rules_foreign_cc-14b79e5a04558cff964f6466a09d9d0cdee2c126",
        url = "https://github.com/bazelbuild/rules_foreign_cc/archive/14b79e5a04558cff964f6466a09d9d0cdee2c126.zip",
        sha256 = "16fa878b68fea8d8345bd8d17b36f36abb24a8459d343d3c2ac1d346ceb7ff90",
    )

    git_repository(
        name = "com_google_glog",
        commit = "96a2f23dca4cc7180821ca5f32e526314395d26a",
        remote = "https://github.com/google/glog.git",
        shallow_since = "1553223106 +0900",
    )

    new_git_repository(
        name = "crcutil",
        commit = "42148a6df6986a257ab21c80f8eca2e54544ac4d",
        remote = "https://github.com/cloudera/crcutil.git",
        build_file = "//bazel/external:crcutil.BUILD",
        shallow_since = "1508970556 -0700",
    )

    _kudu_thirdparty_dep(
        name = "squeasel",
        version = "030ccce87359d892e22fb368c5fc5b75d9a2a5f7",
        build_file = "//bazel/external:squeasel.BUILD",
        sha256 = "9677c2022997e4942cf8c9a3df0ea15905981ade0ea01fd5bbdd2f2ce0087e93",
    )

    _kudu_thirdparty_dep(
        name = "mustache",
        version = "b290952d8eb93d085214d8c8c9eab8559df9f606",
        sha256 = "6ef4698334c2c730e348d3ff04f999ef246550d5c325c9a9d75a285ffb2fa17f",
        build_file = "//bazel/external:mustache.BUILD",
    )

    # Local protobuf fork with some bazel changes.
    maybe(
        http_archive,
        name = "com_google_protobuf",
        sha256 = "b5d6dcd85a50f1685f0e70ea13aef65888feea8a129bc71f67ccad79b422e9ff",
        strip_prefix = "protobuf-0042c92f6e1232b615b76d56d2cb36ffbe0e77ae",
        url = "https://github.com/toddlipcon/protobuf/archive/0042c92f6e1232b615b76d56d2cb36ffbe0e77ae.tar.gz",
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
        sha256 = "9dc9157a9a1551ec7a7e43daea9a694a0bb5fb8bec81235d8a1e6ef64c716dcb",
        strip_prefix = "googletest-release-1.10.0",
        url = "https://github.com/google/googletest/archive/release-1.10.0.tar.gz",
    )

    maybe(
        http_archive,
        name = "libev",
        sha256 = "507eb7b8d1015fbec5b935f34ebed15bf346bed04a11ab82b8eee848c4205aea",
        url = "http://dist.schmorp.de/libev/libev-4.33.tar.gz",
        strip_prefix = "libev-4.33",
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
        version = "1.9.2",
        sha256 = "",
        build_file = "//bazel/external:lz4.BUILD",
    )
    _kudu_thirdparty_dep(
        name = "bitshuffle",
        version = "55f9b4c",
        sha256 = "a26d338c9024eeedb68e485db0b3a25a5fa00df2c23b9453f77e712c588d3079",
        build_file = "//bazel/external:bitshuffle.BUILD",
    )

    _kudu_thirdparty_dep(
        name = "curl",
        version = "7.68.0",
        sha256 = "1dd7604e418b0b9a9077f62f763f6684c1b092a7bc17e3f354b8ad5c964d7358",
        build_file = "//bazel/external:curl.BUILD",
    )

    http_archive(
        name = "com_grail_bazel_toolchain",
        strip_prefix = "bazel-toolchain-5d6406ee54b3aa04139f30769b4e94538a80bc52",
        sha256 = "",
        urls = ["https://github.com/grailbio/bazel-toolchain/archive/5d6406ee54b3aa04139f30769b4e94538a80bc52.tar.gz"],
    )

    http_archive(
        name = "libunwind",
        strip_prefix = "libunwind-1.3.1",
        sha256 = "43997a3939b6ccdf2f669b50fdb8a4d3205374728c2923ddc2354c65260214f8",
        urls = ["https://github.com/libunwind/libunwind/releases/download/v1.3.1/libunwind-1.3.1.tar.gz"],
        build_file_content = """filegroup(name = "all", srcs = glob(["**"]), visibility = ["//visibility:public"])""",
    )
    http_archive(
        name = "com_google_absl",
        urls = ["https://github.com/abseil/abseil-cpp/archive/ce4bc927755fdf0ed03d679d9c7fa041175bb3cb.tar.gz"],
        strip_prefix = "abseil-cpp-ce4bc927755fdf0ed03d679d9c7fa041175bb3cb",
        sha256 = "573baccd67aa591b8c7209bfb0c77e0d15633d77ced39d1ccbb1232828f7f7d9",
    )
    http_archive(
        name = "com_google_tcmalloc",
        urls = ["https://github.com/google/tcmalloc/archive/927ac6226d3e55aeb208b7c23425ffe5f4034f04.tar.gz"],
        strip_prefix = "tcmalloc-927ac6226d3e55aeb208b7c23425ffe5f4034f04",
        sha256 = "b80182d21048426c4fa069a8c23ff92e615c4b73feef5e66650f889cac498cbf",
    )

    _thrift()
