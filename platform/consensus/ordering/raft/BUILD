package(default_visibility = ["//visibility:public"]) 

cc_library(
    name = "consensus_manager_raft",
    srcs = ["consensus_manager_raft.cpp"],
    hdrs = ["consensus_manager_raft.h"],
    deps = [
        "//platform/networkstrate:service_network",
        "//platform/config:resdb_config",
        "//executor/common:transaction_manager",
        "//executor/common:custom_query",
        "//platform/proto:resdb_proto",
        "//platform/proto:replica_info_proto",
        ":raft_proto_cc",
    ],
)

proto_library(
    name = "raft_proto",
    srcs = ["proto/raft.proto"],
    deps = ["//platform/proto:replica_info_proto"],
)

cc_proto_library(
    name = "raft_proto_cc",
    deps = [":raft_proto"],
)
