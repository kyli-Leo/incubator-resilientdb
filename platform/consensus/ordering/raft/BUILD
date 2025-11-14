package(default_visibility = ["//visibility:public"]) 

cc_library(
    name = "consensus_manager_raft",
    srcs = ["consensus_manager_raft.cpp"],
    hdrs = ["consensus_manager_raft.h"],
    deps = [
        "//common/crypto:signature_verifier",
        "//common/utils",
        "//executor/common:custom_query",
        "//executor/common:transaction_manager",
        "//platform/config:resdb_config",
        "//platform/consensus/ordering/pbft:query",
        "//platform/networkstrate:consensus_manager",
        "//platform/proto:replica_info_proto",
        "//platform/proto:resdb_proto",
        "//platform/statistic:stats",
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
