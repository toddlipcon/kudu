genrule(
    name = "gen_config_h",
    srcs = ["@org_apache_kudu//bazel/external/libev:config.h"],
    outs = ["config.h"],
    cmd = "cp $< $@",
)

cc_library(
    name = "libev",
    srcs = [
        "config.h",
        "ev.c",
        "ev_vars.h",
        "ev_wrap.h",
        "event.c",
    ],
    hdrs = [
        "ev.h",
        "ev++.h",
        "ev_epoll.c",
        "ev_poll.c",
        "ev_select.c",
        "event.h",
    ],
    copts = ["-Iexternal/libev"],
    defines = ["HAVE_CONFIG_H"],
    includes = ["."],
    visibility = ["//visibility:public"],
)
