/*
 * Raft version of consensus manager
 */

#include "platform/consensus/ordering/raft/consensus_manager_raft.h"

#include <glog/logging.h>
#include <unistd.h>

#include "common/utils/utils.h"
#include "common/crypto/signature_verifier.h"
#include "platform/proto/resdb.pb.h"
#include "platform/consensus/ordering/raft/proto/raft.pb.h"

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
  // Return current leader id
  // If I'm the leader, return self_id_
  // Otherwise return tracked leader_id_ (updated from AppendEntries)
  if (role_ == Role::kLeader) {
    return self_id_;
  }
  // If leader_id_ is not set, try to find from replicas (temporary fallback)
  if (leader_id_ == -1) {
    // Temporary: assume replica 0 is initial leader (until election is implemented)
    for (const auto& replica : replicas_) {
      if (replica.id() == 0) {
        return replica.id();
      }
    }
  }
  return leader_id_ != -1 ? leader_id_ : 0;
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
    // Handle TYPE_CLIENT_REQUEST: if leader, process directly; otherwise forward to leader
    case Request::TYPE_CLIENT_REQUEST:
      return HandleClientRequest(std::move(context), std::move(request));
    // Transaction requests - this is what goes through Raft consensus
    case Request::TYPE_NEW_TXNS:
      return HandleNewTransactions(std::move(context), std::move(request));
    // Raft-specific messages use TYPE_CUSTOM_CONSENSUS with user_type to distinguish
    case Request::TYPE_CUSTOM_CONSENSUS:
      return HandleRaftMessage(std::move(context), std::move(request));
    // Query functions - not needed for KV only (can be added later if needed)
    // case Request::TYPE_REPLICA_STATE:
    // case Request::TYPE_CUSTOM_QUERY:  // Only used by UTXO, not needed for KV
    // case Request::TYPE_QUERY:
    //   LOG(WARNING) << "[Raft] Query function not implemented (KV only mode)";
    //   return -1;  // Return error for now, can be implemented later if needed
    default:
      return 0;
  }
  return 0;
}

// HandleClientRequest: Handle TYPE_CLIENT_REQUEST from clients
// If not leader, forward to leader; if leader, convert to TYPE_NEW_TXNS and process
int ConsensusManagerRaft::HandleClientRequest(
    std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  // 1. Check if I'm the leader
  if (role_ != Role::kLeader) {
    // Not leader: forward to leader
    uint32_t leader_id = GetPrimary();
    if (leader_id > 0 && leader_id != self_id_) {
      LOG(INFO) << "[Raft] Not leader (self=" << self_id_ 
                << "), forwarding request to leader " << leader_id;
      GetBroadCastClient()->SendMessage(*request, leader_id);
      return 0;
    } else {
      LOG(WARNING) << "[Raft] Not leader and cannot find leader (leader_id=" 
                   << leader_id << "), dropping request";
      return -1;
    }
  }

  // 2. I'm the leader: convert TYPE_CLIENT_REQUEST to TYPE_NEW_TXNS and process
  // Create a BatchUserRequest (even for single request, to match expected format)
  BatchUserRequest batch_request;
  BatchUserRequest::UserRequest* user_req = batch_request.add_user_requests();
  *user_req->mutable_request() = *request;
  *user_req->mutable_signature() = context->signature;
  user_req->set_id(0);
  batch_request.set_createtime(GetCurrentTime());
  batch_request.set_local_id(0);  // Simple local_id
  
  // Create TYPE_NEW_TXNS request
  std::unique_ptr<Request> new_txns_request = std::make_unique<Request>();
  new_txns_request->set_type(Request::TYPE_NEW_TXNS);
  new_txns_request->set_sender_id(self_id_);
  new_txns_request->set_proxy_id(self_id_);
  new_txns_request->set_need_response(request->need_response());
  
  // Serialize BatchUserRequest
  std::string batch_data;
  batch_request.SerializeToString(&batch_data);
  new_txns_request->set_data(batch_data);
  new_txns_request->set_hash(SignatureVerifier::CalculateHash(batch_data));
  
  // Sign the request
  if (GetSignatureVerifier()) {
    auto signature_or = GetSignatureVerifier()->SignMessage(batch_data);
    if (signature_or.ok()) {
      *new_txns_request->mutable_data_signature() = *signature_or;
    }
  }
  
  // Save context for response (if needed)
  if (request->need_response()) {
    // TODO: Save context with seq/hash for later response
    // For now, pass context through
  }
  
  // Process as TYPE_NEW_TXNS
  return HandleNewTransactions(std::move(context), std::move(new_txns_request));
}

// HandleRaftMessage: Route Raft-specific messages based on user_type
int ConsensusManagerRaft::HandleRaftMessage(
    std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  // Parse user_type to determine which Raft message type this is
  // Values correspond to RaftMessageType enum in raft.proto:
  // RAFT_APPEND_ENTRIES = 0
  // RAFT_APPEND_ENTRIES_RESPONSE = 1
  // RAFT_REQUEST_VOTE = 2
  // RAFT_REQUEST_VOTE_RESPONSE = 3
  int user_type = request->user_type();
  
  switch (user_type) {
    case 0:  // RAFT_APPEND_ENTRIES
      return HandleAppendEntries(std::move(context), std::move(request));
    case 1:  // RAFT_APPEND_ENTRIES_RESPONSE
      return HandleAppendEntriesResponse(std::move(context), std::move(request));
    // TODO: Implement election messages
    // case 2:  // RAFT_REQUEST_VOTE
    //   return HandleRequestVote(std::move(context), std::move(request));
    // case 3:  // RAFT_REQUEST_VOTE_RESPONSE
    //   return HandleRequestVoteResponse(std::move(context), std::move(request));
    default:
      LOG(WARNING) << "[Raft] Unknown Raft message type: " << user_type;
      return -1;
  }
}

// HandleNewTransactions: Process TYPE_NEW_TXNS requests through Raft consensus
// Similar to Commitment::ProcessNewRequest in PBFT
int ConsensusManagerRaft::HandleNewTransactions(
    std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  // 1. Check if I'm the leader (if not, redirect to leader)
  if (role_ != Role::kLeader) {
    // TODO: Track leader_id_ and redirect
    // For now, return error if not leader
    LOG(WARNING) << "[Raft] Not leader, cannot process transaction";
    return -1;
  }

  // 2. Skip validation for now (assume requests are valid)
  // if (pre_verify_func_ && !pre_verify_func_(*request)) {
  //   LOG(ERROR) << "[Raft] pre_verify failed.";
  //   return -1;
  // }

  // 3. Append to log (like PBFT assigns seq)
  LogEntry entry;
  entry.term = current_term_;
  entry.index = log_.size();  // log_[0] is dummy, so next index is log_.size()
  entry.request = std::make_unique<Request>(*request);
  log_.push_back(std::move(entry));

  LOG(INFO) << "[Raft] Leader appended entry at index=" << entry.index
            << " term=" << current_term_;

  // 4. Send AppendEntries to all followers (like PBFT broadcasts Pre-prepare)
  SendAppendEntriesToAll();

  return 0;
}

int ConsensusManagerRaft::HandleAppendEntries(
    std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  std::unique_lock<std::mutex> lk(mutex_);
  
  // 1. Parse AppendEntries RPC
  raft::AppendEntries append_entries;
  if (!append_entries.ParseFromString(request->data())) {
    LOG(ERROR) << "[Raft] Failed to parse AppendEntries";
    return -1;
  }

  // 2. Check term - if leader's term is higher, update and become follower
  if (append_entries.term() > current_term_) {
    LOG(INFO) << "[Raft] Received higher term " << append_entries.term() 
              << " from leader " << append_entries.leader_id()
              << ", updating term and becoming follower";
    current_term_ = append_entries.term();
    role_ = Role::kFollower;
    leader_id_ = append_entries.leader_id();
    voted_for_ = -1;  // Reset voted_for when term changes
  }

  // 3. Reply false if term < current_term_ (should not happen after step 2)
  if (append_entries.term() < current_term_) {
    LOG(WARNING) << "[Raft] Received AppendEntries with stale term " 
                 << append_entries.term() << " (current=" << current_term_ << ")";
    // Send response with failure
    SendAppendEntriesResponse(request->sender_id(), false, 0);
    return 0;
  }

  // 4. Update leader_id_ and reset election timeout (if we had one)
  leader_id_ = append_entries.leader_id();

  // 5. Check prevLogIndex and prevLogTerm
  int32_t prev_log_index = append_entries.prev_log_index();
  int32_t prev_log_term = append_entries.prev_log_term();
  
  bool log_match = false;
  if (prev_log_index == 0) {
    // prev_log_index = 0 means log starts from beginning (after dummy entry)
    log_match = true;
  } else if (prev_log_index < static_cast<int32_t>(log_.size()) &&
             log_[prev_log_index].term == prev_log_term) {
    log_match = true;
  }

  // 6. If log doesn't match, return false
  if (!log_match) {
    LOG(INFO) << "[Raft] Log mismatch: prev_log_index=" << prev_log_index
              << " prev_log_term=" << prev_log_term
              << " local_log_size=" << log_.size();
    SendAppendEntriesResponse(request->sender_id(), false, 0);
    return 0;
  }

  // 7. Append new entries (if any)
  int32_t match_index = prev_log_index;
  if (append_entries.entries_size() > 0) {
    // Delete any conflicting entries and append new ones
    int64_t next_index = prev_log_index + 1;
    if (next_index < static_cast<int64_t>(log_.size())) {
      // Remove conflicting entries
      log_.erase(log_.begin() + next_index, log_.end());
    }
    
    // Append new entries
    for (int i = 0; i < append_entries.entries_size(); ++i) {
      Request entry_request;
      if (!entry_request.ParseFromString(append_entries.entries(i))) {
        LOG(ERROR) << "[Raft] Failed to parse entry " << i;
        SendAppendEntriesResponse(request->sender_id(), false, match_index);
        return -1;
      }
      
      LogEntry entry;
      entry.term = append_entries.term();
      entry.index = next_index + i;
      entry.request = std::make_unique<Request>(entry_request);
      log_.push_back(std::move(entry));
      match_index = entry.index;
    }
    
    LOG(INFO) << "[Raft] Follower appended " << append_entries.entries_size()
              << " entries, new log size=" << log_.size();
  } else {
    // Empty AppendEntries is a heartbeat
    match_index = prev_log_index;
  }

  // 8. Update commit_index_ if leader_commit > commit_index_
  bool need_apply = false;
  int64_t old_commit_index = commit_index_;
  
  if (append_entries.leader_commit() > commit_index_) {
    commit_index_ = std::min(static_cast<int64_t>(append_entries.leader_commit()),
                             static_cast<int64_t>(log_.size() - 1));
    LOG(INFO) << "[Raft] Updated commit_index_ to " << commit_index_;
    need_apply = (commit_index_ > old_commit_index);
  }

  // 9. Send success response (release lock before network I/O)
  lk.unlock();
  SendAppendEntriesResponse(request->sender_id(), true, match_index);
  
  // 10. Apply committed entries if commit_index_ was updated
  if (need_apply) {
    ApplyCommittedEntries();
  }
  
  return 0;
}

int ConsensusManagerRaft::HandleAppendEntriesResponse(
    std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  // Only leader processes AppendEntriesResponse
  if (role_ != Role::kLeader) {
    return 0;
  }

  // 1. Parse AppendEntriesResponse
  raft::AppendEntriesResp resp;
  if (!resp.ParseFromString(request->data())) {
    LOG(ERROR) << "[Raft] Failed to parse AppendEntriesResponse";
    return -1;
  }

  // 2. Check term - if response term is higher, step down to follower
  if (resp.term() > current_term_) {
    LOG(WARNING) << "[Raft] Received higher term " << resp.term() 
                 << " in AppendEntriesResponse, stepping down";
    current_term_ = resp.term();
    role_ = Role::kFollower;
    leader_id_ = -1;
    return 0;
  }

  // 3. Update follower's match_index_ and next_index_
  int follower_id = request->sender_id();
  
  // Ensure vectors are large enough
  if (match_index_.size() <= static_cast<size_t>(follower_id)) {
    match_index_.resize(follower_id + 1, 0);
  }
  if (next_index_.size() <= static_cast<size_t>(follower_id)) {
    next_index_.resize(follower_id + 1, 1);
  }

  if (resp.success()) {
    // Success: update match_index_ and next_index_
    match_index_[follower_id] = resp.match_index();
    next_index_[follower_id] = resp.match_index() + 1;
    LOG(INFO) << "[Raft] Follower " << follower_id 
              << " confirmed match_index=" << resp.match_index();
  } else {
    // Failure: decrement next_index_ and retry later
    next_index_[follower_id] = std::max(1LL, next_index_[follower_id] - 1);
    LOG(INFO) << "[Raft] Follower " << follower_id 
              << " rejected, decrementing next_index_ to " << next_index_[follower_id];
  }

  // 4. Try to advance commit_index_ if majority confirmed
  AdvanceCommitIndex();

  return 0;
}

// ================= Send AppendEntries To All =================

void ConsensusManagerRaft::SendAppendEntriesToAll() {
  if (role_ != Role::kLeader) {
    return;
  }

  std::lock_guard<std::mutex> lk(mutex_);

  // Send AppendEntries to each follower
  for (const auto& replica : replicas_) {
    int replica_id = replica.id();
    
    // Ensure vectors are large enough
    if (next_index_.size() <= static_cast<size_t>(replica_id)) {
      next_index_.resize(replica_id + 1, 1);
    }
    if (match_index_.size() <= static_cast<size_t>(replica_id)) {
      match_index_.resize(replica_id + 1, 0);
    }

    // Prepare AppendEntries RPC
    raft::AppendEntries append_entries;
    append_entries.set_term(current_term_);
    append_entries.set_leader_id(self_id_);
    
    int64_t next_idx = next_index_[replica_id];
    int64_t prev_log_index = next_idx - 1;
    int64_t prev_log_term = 0;
    
    if (prev_log_index > 0 && prev_log_index < static_cast<int64_t>(log_.size())) {
      prev_log_term = log_[prev_log_index].term;
    }
    
    append_entries.set_prev_log_index(prev_log_index);
    append_entries.set_prev_log_term(prev_log_term);
    append_entries.set_leader_commit(commit_index_);
    
    // Add entries from next_index_ to end of log
    for (int64_t i = next_idx; i < static_cast<int64_t>(log_.size()); ++i) {
      if (log_[i].request) {
        std::string entry_data;
        if (log_[i].request->SerializeToString(&entry_data)) {
          append_entries.add_entries(entry_data);
        } else {
          LOG(ERROR) << "[Raft] Failed to serialize entry at index=" << i;
        }
      }
    }
    
    // Serialize and send
    std::string append_data;
    if (!append_entries.SerializeToString(&append_data)) {
      LOG(ERROR) << "[Raft] Failed to serialize AppendEntries";
      continue;
    }
    
    std::unique_ptr<Request> raft_request = std::make_unique<Request>();
    raft_request->set_type(Request::TYPE_CUSTOM_CONSENSUS);
    raft_request->set_user_type(0);  // RAFT_APPEND_ENTRIES (from RaftMessageType enum)
    raft_request->set_sender_id(self_id_);
    raft_request->set_data(append_data);
    
    // Release lock before sending (network I/O)
    lk.unlock();
    GetBroadCastClient()->SendMessage(*raft_request, replica_id);
    lk.lock();
    
    LOG(INFO) << "[Raft] Sent AppendEntries to follower " << replica_id
              << " next_index=" << next_idx
              << " entries_count=" << append_entries.entries_size();
  }
}

// Helper function to send AppendEntriesResponse
void ConsensusManagerRaft::SendAppendEntriesResponse(int32_t leader_id, bool success, int32_t match_index) {
  raft::AppendEntriesResp resp;
  resp.set_term(current_term_);
  resp.set_success(success);
  resp.set_match_index(match_index);
  
  std::string resp_data;
  if (!resp.SerializeToString(&resp_data)) {
    LOG(ERROR) << "[Raft] Failed to serialize AppendEntriesResponse";
    return;
  }
  
  std::unique_ptr<Request> response_request = std::make_unique<Request>();
  response_request->set_type(Request::TYPE_CUSTOM_CONSENSUS);
  response_request->set_user_type(1);  // RAFT_APPEND_ENTRIES_RESPONSE (from RaftMessageType enum)
  response_request->set_sender_id(self_id_);
  response_request->set_data(resp_data);
  
  GetBroadCastClient()->SendMessage(*response_request, leader_id);
}

// int ConsensusManagerRaft::HandleRequestVote(std::unique_ptr<Context> context,
//                                             std::unique_ptr<Request> request) {
//   // TODO:
//   // 1. 解析 RequestVote RPC
//   // 2. 如果对方 term 比自己新，更新 current_term_ 并退为 follower
//   // 3. 按 Raft 规则决定是否投票（看 term、日志新旧）
//   // 4. 返回 RequestVoteResponse
//   return 0;
// }

// int ConsensusManagerRaft::HandleRequestVoteResponse(
//     std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
//   // TODO:
//   // 1. 解析响应
//   // 2. 统计选票
//   // 3. 如果超过半数，成为 leader，并初始化 next_index_/match_index_
//   return 0;
// }

// ================= Advance Commit Index =================

void ConsensusManagerRaft::AdvanceCommitIndex() {
  // Only leader can advance commit_index_
  if (role_ != Role::kLeader) {
    return;
  }

  std::lock_guard<std::mutex> lk(mutex_);

  // Find the highest N where majority of match_index_[i] >= N
  // Majority = (replicas.size() + 1) / 2 + 1 (including self)
  int total_nodes = replicas_.size() + 1;  // +1 for self
  int majority = total_nodes / 2 + 1;

  // Find the highest index that has majority confirmation
  // Check from highest to lowest to find the maximum commit_index_
  int64_t new_commit_index = commit_index_;
  
  // Check from last log entry down to commit_index_ + 1
  // Note: log_[0] is dummy, so valid indices are 1..(log_.size()-1)
  int64_t max_index = static_cast<int64_t>(log_.size()) - 1;
  if (max_index <= commit_index_) {
    return;  // No new entries to check
  }
  
  for (int64_t n = max_index; n > commit_index_; --n) {
    // Count how many nodes (including self) have match_index >= n
    int count = 1;  // Count self (leader always has all entries)
    
    for (const auto& replica : replicas_) {
      int replica_id = replica.id();
      if (replica_id < static_cast<int>(match_index_.size()) &&
          match_index_[replica_id] >= n) {
        count++;
      }
    }

    // If majority confirmed, this is the highest commit_index_ we can set
    // (Raft requires entries to be committed in order, so once we find
    //  the highest index with majority, all lower indices also have majority)
    if (count >= majority) {
      new_commit_index = n;
      LOG(INFO) << "[Raft] Found highest commit_index_=" << new_commit_index
                << " with majority=" << count << "/" << total_nodes;
      break;  // Found the highest, no need to check lower indices
    }
  }

  // Update commit_index_ if we found a higher value
  if (new_commit_index > commit_index_) {
    commit_index_ = new_commit_index;
    LOG(INFO) << "[Raft] Advanced commit_index_ to " << commit_index_;
  }

  // Apply committed entries
  ApplyCommittedEntries();
}

// ================= ApplyCommittedEntries =================

void ConsensusManagerRaft::ApplyCommittedEntries() {
  std::lock_guard<std::mutex> lk(mutex_);

  // log_[0] is dummy entry, so we start applying from index 1
  while (last_applied_ < commit_index_) {
    ++last_applied_;
    
    // Safety check: ensure we don't go out of bounds
    if (last_applied_ >= static_cast<int64_t>(log_.size())) {
      LOG(ERROR) << "[Raft] last_applied_=" << last_applied_ 
                 << " >= log_.size()=" << log_.size();
      --last_applied_;  // Roll back
      break;
    }
    
    LogEntry& entry = log_[last_applied_];

    if (!entry.request) {
      LOG(WARNING) << "[Raft] Entry at index=" << entry.index 
                   << " has no request, skipping";
      continue;
    }
    
    LOG(INFO) << "[Raft] ApplyCommittedEntries index=" << entry.index
              << " term=" << entry.term;

    // Release lock before executing (executor might take time)
    std::unique_ptr<Request> req = std::move(entry.request);
    lk.unlock();
    
    if (executor_) {
      executor_->ExecuteData(std::move(req));
    }
    
    lk.lock();
  }
}

}  // namespace resdb
