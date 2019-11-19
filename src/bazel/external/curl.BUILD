config_setting(
    name = "windows",
    values = {"cpu": "x64_windows"},
    visibility = ["//visibility:private"],
)

config_setting(
    name = "osx",
    values = {"cpu": "darwin"},
    visibility = ["//visibility:private"],
)

genrule(
    name = "gen_config_h",
    srcs = ["@org_apache_kudu//bazel/external/curl:curl_config.h"],
    outs = ["lib/curl_config.h"],
    cmd = "cp $< $@",
)

cc_library(
    name = "curl",
    srcs = glob([
        "lib/**/*.c",
    ]) + ["lib/curl_config.h"],
    hdrs = glob([
        "include/curl/*.h",
        "lib/**/*.h",
    ]),
    includes = [
        "include/",
        "lib/",
    ],
    linkopts = [
        "-lgssapi_krb5",
        "-lkrb5",
    ],
    local_defines = [
        "HAVE_CONFIG_H",
        "BUILDING_LIBCURL",
        "CURL_HIDDEN_SYMBOLS",
        "PIC",
        "_REENTRANT",
    ],
    visibility = ["//visibility:public"],
)
