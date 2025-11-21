#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "executor/common/custom_query.h"
#include "executor/common/transaction_manager.h"
#include "platform/config/resdb_config.h"
#include "platform/consensus/ordering/pbft/query.h"
#include "platform/networkstrate/consensus_manager.h"
#include "platform/proto/replica_info.pb.h"
#include "platform/proto/resdb.pb.h"
#include "platform/statistic/stats.h"

namespace resdb {

class ConsensusManagerRaft : public ConsensusManager {
 public:
  ConsensusManagerRaft(const ResDBConfig& config,
                       std::unique_ptr<TransactionManager> executor,
                       std::unique_ptr<CustomQuery> query_executor = nullptr);
  ~ConsensusManagerRaft() override = default;

  // Entry point for processing incoming requests
  int ConsensusCommit(std::unique_ptr<Context> context,
                      std::unique_ptr<Request> request) override;

  std::vector<ReplicaInfo> GetReplicas() override;
  uint32_t GetPrimary() override;  // leader id
  uint32_t GetVersion() override;  // current term

  void SetPrimary(uint32_t primary, uint64_t version) override;
  void Start() override;

 private:
  // 类似 PBFT 的封装：对 Request 做分类分发
  int InternalConsensusCommit(std::unique_ptr<Context> context,
                              std::unique_ptr<Request> request);

  // Internal implementation
  // Handle TYPE_CLIENT_REQUEST: 
  // As we dont have batching at this moment, 
  // would just parse and process as a TYPE_NEW_TXNS with single request. 
  int HandleClientRequest(std::unique_ptr<Context> context,
                          std::unique_ptr<Request> request);

  // Handle transaction requests (TYPE_NEW_TXNS) - goes through Raft consensus
  // It would parse the reqeusts and then start new transactions.
  // Check if I'm the leader, if not, redirect to leader.
  
  // (currently no batching so only one request is processed)
  int HandleNewTransactions(std::unique_ptr<Context> context,
                            std::unique_ptr<Request> request);

  // Handle Raft-specific messages (TYPE_CUSTOM_CONSENSUS with user_type)
  // Routes to appropriate handler based on user_type
  int HandleRaftMessage(std::unique_ptr<Context> context,
                        std::unique_ptr<Request> request);

  // Handle AppendEntries requests
  // It would parse the requests and then append to the log.
  // (currently no batching so only one request is processed)
  int HandleAppendEntries(std::unique_ptr<Context> context,
                          std::unique_ptr<Request> request);

  int HandleAppendEntriesResponse(std::unique_ptr<Context> context,
                                  std::unique_ptr<Request> request);

  // Election RPC handlers
  int HandleRequestVote(std::unique_ptr<Context> context,
                        std::unique_ptr<Request> request);

  int HandleRequestVoteResponse(std::unique_ptr<Context> context,
                                std::unique_ptr<Request> request);

  // increment commited idx, then apply to log to state machine
  void ApplyCommittedEntries();
  
  // Advance commit_index_ if majority of followers have confirmed
  void AdvanceCommitIndex();
  
  // Send AppendEntries RPC to all followers
  void SendAppendEntriesToAll();
  
  // Helper: Send AppendEntriesResponse to leader
  void SendAppendEntriesResponse(int32_t leader_id, bool success, int32_t match_index);

 private:
  // raft role
  enum class Role { kFollower, kCandidate, kLeader };

  struct LogEntry {
    int64_t term = 0;
    int64_t index = 0;
    std::unique_ptr<Request> request;
  };

  ResDBConfig config_;
  std::unique_ptr<TransactionManager> executor_;
  std::unique_ptr<CustomQuery> custom_query_executor_;
  std::unique_ptr<Query> query_;
  Stats* global_stats_ = nullptr;

  std::mutex mutex_;
  Role role_ = Role::kFollower;
  int32_t self_id_ = 0;
  int32_t leader_id_ = -1;  // Track current leader (updated from AppendEntries)
  std::vector<ReplicaInfo> replicas_;

  // persistent state
  int64_t current_term_ = 0;
  int64_t voted_for_ = -1;
  std::vector<LogEntry> log_;  // log_[0] dummy

  // volatile state
  int64_t commit_index_ = 0;
  int64_t last_applied_ = 0;

  // leader state
  std::vector<int64_t> next_index_;
  std::vector<int64_t> match_index_;

  std::function<bool(const Request&)> pre_verify_func_;
  std::atomic<bool> stop_{false};

  // Further threads for election and heartbeat can be added here
  // std::thread election_thread_;
  // std::thread heartbeat_thread_;
};

}  // namespace resdb
