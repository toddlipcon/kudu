package(default_visibility = ["//visibility:public"])

load("//:setup.bzl", "kudu_cc_library", "kudu_cc_test", "kudu_proto_library")

kudu_proto_library(
    name="token_proto",
    deps = ["//kudu/util:pb_util_proto"],
)

kudu_cc_library(
    name = "openssl_util",
    srcs = ["openssl_util.cc"],
    hdrs = [
        "openssl_util.h",
        "openssl_util_bio.h",
    ],
    linkopts = [
        "-lssl",
        "-lcrypto",
    ],
    deps = [
        "//bazel/external:glog",
        "//kudu/gutil:base",
        "//kudu/gutil:strings",
        "//kudu/util",
    ],
)

kudu_cc_library(
    name = "security",
    srcs = [
        "ca/cert_management.cc",
        "cert.cc",
        "crypto.cc",
        "gssapi.cc",
        "init.cc",
        "kerberos_util.cc",
        "krb5_realm_override.cc",
        "security_flags.cc",
        "simple_acl.cc",
        "tls_context.cc",
        "tls_handshake.cc",
        "tls_socket.cc",
        "token_signer.cc",
        "token_signing_key.cc",
        "token_verifier.cc",
        "x509_check_host.cc",
    ],
    hdrs = [
        "ca/cert_management.h",
        "cert.h",
        "cert_store.h",
        "crypto.h",
        "gssapi.h",
        "init.h",
        "kerberos_util.h",
        "security_flags.h",
        "simple_acl.h",
        "tls_context.h",
        "tls_handshake.h",
        "tls_socket.h",
        "token_signer.h",
        "token_signing_key.h",
        "token_verifier.h",
        "x509_check_host.h",
    ],
    linkopts = [
        "-lkrb5",
        "-lgssapi_krb5",
    ],
    deps = [
        ":openssl_util",
        ":token_cc_proto",
        "//kudu/gutil:base",
        "//kudu/util",
        "//kudu/util:net",
    ],
)

kudu_cc_library(
    name = "test_util",
    srcs = glob(
        ["test/*.cc"],
        exclude = ["**/*-test.cc"],
    ) + [
        "security-test-util.cc",
    ],
    hdrs = glob(["test/*.h"]) + ["security-test-util.h"],
    deps = [
        ":security",
        "//kudu/util",
        "//kudu/util:test_util",
    ],
)

[
    kudu_cc_test(
        name = path.split(".")[0],
        srcs = [path],
        deps = [
            ":security",
            ":test_util",
            "//kudu/util:pb_util",
        ],
    )
    for path in glob(["**/*-test.cc"])
]
