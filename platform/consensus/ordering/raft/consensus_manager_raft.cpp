/*
 * Raft version of consensus manager
 */

#include "platform/consensus/ordering/raft/consensus_manager_raft.h"

#include <glog/logging.h>
#include <unistd.h>

namespace resdb {

ConsensusManagerRaft::ConsensusManagerRaft(
    const ResDBConfig& config, std::unique_ptr<TransactionManager> executor,
    std::unique_ptr<CustomQuery> query_executor)
    : ConsensusManager(config),
      config_(config),
      executor_(std::move(executor)),
      custom_query_executor_(std::move(query_executor)) {
  global_stats_ = Stats::GetGlobalStats();

  // Raft replica meta info
  self_id_ = config_.GetSelfInfo().id();
  replicas_ = config_.GetReplicaInfos();

  // Raft log
  LogEntry dummy;
  dummy.term = 0;
  dummy.index = 0;
  log_.push_back(std::move(dummy));

  next_index_.assign(replicas_.size() + 1, 1);
  match_index_.assign(replicas_.size() + 1, 0);

  LOG(INFO) << "[Raft] ConsensusManagerRaft created, self_id=" << self_id_;
}

void ConsensusManagerRaft::Start() { ConsensusManager::Start(); }

std::vector<ReplicaInfo> ConsensusManagerRaft::GetReplicas() {
  return replicas_;
}

uint32_t ConsensusManagerRaft::GetPrimary() {
  // raft does not store the leader id persistently, so just return self id here
  return self_id_;
}

uint32_t ConsensusManagerRaft::GetVersion() {
  return static_cast<uint32_t>(current_term_);
}

void ConsensusManagerRaft::SetPrimary(uint32_t primary, uint64_t version) {
  // Raft does not use this method to set leader
}

int ConsensusManagerRaft::ConsensusCommit(std::unique_ptr<Context> context,
                                          std::unique_ptr<Request> request) {
  return InternalConsensusCommit(std::move(context), std::move(request));
}

// Internal implementation of ConsensusCommit
// switch by request type
int ConsensusManagerRaft::InternalConsensusCommit(
    std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  // LOG(INFO) << "[Raft] recv type:" << request->type()
  //           << " sender id:" << request->sender_id();

  switch (request->type()) {
    // create new user request currrently ignoerded in raft
    case Request::TYPE_CLIENT_REQUEST:
      return -1;
    // return HandleClientRequest(std::move(context), std::move(request));
    case Request::TYPE_RAFT_APPEND_ENTRIES:
      return HandleAppendEntries(std::move(context), std::move(request));
    case Request::TYPE_RAFT_APPEND_ENTRIES_RESPONSE:
      return HandleAppendEntriesResponse(std::move(context),
                                         std::move(request));
      // case Request::TYPE_RAFT_REQUEST_VOTE:
      //   return HandleRequestVote(std::move(context), std::move(request));
      // case Request::TYPE_RAFT_REQUEST_VOTE_RESPONSE:
      //   return HandleRequestVoteResponse(std::move(context),
      //   std::move(request));

      // // ======== 读操作：Query / ReplicaState / CustomQuery ========
      // case Request::TYPE_QUERY:
      //   if (query_) {
      //     return query_->ProcessQuery(std::move(context),
      //     std::move(request));
      //   }
      //   return 0;  // 暂时不支持 Query 的话可以这样

      // case Request::TYPE_REPLICA_STATE:
      //   if (query_) {
      //     return query_->ProcessGetReplicaState(std::move(context),
      //                                           std::move(request));
      //   }
      //   return 0;

      // case Request::TYPE_CUSTOM_QUERY:
      //   if (query_) {
      //     return query_->ProcessCustomQuery(std::move(context),
      //                                       std::move(request));
      //   }
      //   return 0;

      // // ======== 其他属于 PBFT 的类型可以忽略 / 返回错误 ========
      // case Request::TYPE_NEW_TXNS:
      // case Request::TYPE_PRE_PREPARE:
      // case Request::TYPE_PREPARE:
      // case Request::TYPE_COMMIT:
      // case Request::TYPE_CHECKPOINT:
      // case Request::TYPE_VIEWCHANGE:
      // case Request::TYPE_NEWVIEW:
      // LOG(WARNING) << "[Raft] Received PBFT-specific request type: "
      //              << request->type() << ", ignoring.";
      return 0;
  }
  return 0;
}

// ================= Raft 相关处理函数（先给出骨架） =================

int ConsensusManagerRaft::HandleClientRequest(
    std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  // TODO:
  // 1. 如果自己不是 leader，可以：
  //    - 返回错误码，或者
  //    - redirect 到 leader（类似 Query 里 redirect primary 的逻辑）
  // 2. 如果是 leader：
  //    - 把 request append 到 log_
  //    - 给其他 replica 发 AppendEntries
  //    - 等待多数派确认，再推进 commit_index_，然后 ApplyCommittedEntries()
  //
  // 现在先做一个“临时直接执行”的实现，保证系统能跑起来：
  if (pre_verify_func_) {
    if (!pre_verify_func_(*request)) {
      LOG(ERROR) << "[Raft] pre_verify failed.";
      return -1;
    }
  }

  if (executor_) {
    executor_->ExecuteData(
        std::move(request));  // 函数名按实际 TransactionManager 改
  }
  return 0;
}

int ConsensusManagerRaft::HandleAppendEntries(
    std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  // TODO: 解析 request->data() 为 AppendEntries RPC（按照 raft.proto）
  // 1. 检查 term/prevLogIndex/prevLogTerm
  // 2. 如果不匹配，返回 false，并把 conflict index 返回给 leader
  // 3. 如果匹配，追加新的 entries，更新本地 log_
  // 4. 如果 leader_commit > commit_index_，更新 commit_index_，调用
  // ApplyCommittedEntries()
  return 0;
}

int ConsensusManagerRaft::HandleAppendEntriesResponse(
    std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  // TODO: 解析响应，更新对应 follower 的 match_index_ / next_index_
  // 然后尝试推进 commit_index_，再调用 ApplyCommittedEntries()
  return 0;
}

int ConsensusManagerRaft::HandleRequestVote(std::unique_ptr<Context> context,
                                            std::unique_ptr<Request> request) {
  // TODO:
  // 1. 解析 RequestVote RPC
  // 2. 如果对方 term 比自己新，更新 current_term_ 并退为 follower
  // 3. 按 Raft 规则决定是否投票（看 term、日志新旧）
  // 4. 返回 RequestVoteResponse
  return 0;
}

int ConsensusManagerRaft::HandleRequestVoteResponse(
    std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  // TODO:
  // 1. 解析响应
  // 2. 统计选票
  // 3. 如果超过半数，成为 leader，并初始化 next_index_/match_index_
  return 0;
}

// ================= ApplyCommittedEntries =================

void ConsensusManagerRaft::ApplyCommittedEntries() {
  std::lock_guard<std::mutex> lk(mutex_);

  while (last_applied_ < commit_index_) {
    ++last_applied_;
    LogEntry& entry = log_[last_applied_];

    if (!entry.request) {
      continue;
    }
    LOG(INFO) << "[Raft] ApplyCommittedEntries index=" << entry.index
              << " term=" << entry.term;

    if (executor_) {
      executor_->ExecuteData(std::move(entry.request));
    }
  }
}

}  // namespace resdb
