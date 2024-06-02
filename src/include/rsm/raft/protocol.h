#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs
{

    const std::string RAFT_RPC_START_NODE = "start node";
    const std::string RAFT_RPC_STOP_NODE = "stop node";
    const std::string RAFT_RPC_NEW_COMMEND = "new commend";
    const std::string RAFT_RPC_CHECK_LEADER = "check leader";
    const std::string RAFT_RPC_IS_STOPPED = "check stopped";
    const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
    const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

    const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
    const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
    const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

    template <typename Command>
    class log_entry
    {
    public:
        // Your code here
        Command command_;
        int term;
        int index;
    };

    struct RequestVoteArgs
    {
        /* Lab3: Your code here */
        int term;
        int candidate_id;
        int last_log_index;
        int last_log_term;
        MSGPACK_DEFINE(
            term,
            candidate_id,
            last_log_index,
            last_log_term)
    };

    struct RequestVoteReply
    {
        /* Lab3: Your code here */
        int current_term;
        bool vote_granted;
        MSGPACK_DEFINE(
            current_term,
            vote_granted)
    };

    template <typename Command>
    struct AppendEntriesArgs
    {
        /* Lab3: Your code here */
        int term;
        int leader_id;
        int prev_log_index;
        int prev_log_term;
        int leader_commit;
        std::vector<log_entry<Command>> entries;
        bool is_empty_entries;
    };

    struct RpcAppendEntriesArgs
    {
        /* Lab3: Your code here */
        // You may need to manually convert template types in custom data structure to basic types before you can use MSGPACK_DEFINE
        int term;
        int leader_id;
        int prev_log_index;
        int prev_log_term;
        int leader_commit;
        std::vector<int> entries_terms;
        std::vector<int> entries_indexs;
        std::vector<std::vector<u8>> entries_commands;
        int command_size;
        bool is_empty_entries;

        MSGPACK_DEFINE(
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            entries_terms,
            entries_indexs,
            entries_commands,
            command_size,
            is_empty_entries)
    };

    template <typename Command>
    RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
    {
        /* Lab3: Your code here */
        RpcAppendEntriesArgs rpcArgs;
        rpcArgs.term = arg.term;
        rpcArgs.leader_id = arg.leader_id;
        rpcArgs.prev_log_index = arg.prev_log_index;
        rpcArgs.prev_log_term = arg.prev_log_term;
        rpcArgs.leader_commit = arg.leader_commit;
        rpcArgs.is_empty_entries = arg.is_empty_entries;
        // You may need to manually convert template types in custom data structure to basic types before you can use MSGPACK_DEFINE
        if (arg.entries.size() == 0)
        {
            return rpcArgs;
        }
        rpcArgs.command_size = arg.entries[0].command_.size();
        rpcArgs.entries_terms.resize(arg.entries.size());
        rpcArgs.entries_indexs.resize(arg.entries.size());
        rpcArgs.entries_commands.resize(arg.entries.size());
        for (int i = 0; i < arg.entries.size(); i++)
        {
            rpcArgs.entries_terms[i] = arg.entries[i].term;
            rpcArgs.entries_indexs[i] = arg.entries[i].index;
            rpcArgs.entries_commands[i] = arg.entries[i].command_.serialize(rpcArgs.command_size);
        }

        return rpcArgs;
        // return RpcAppendEntriesArgs();
    }

    template <typename Command>
    AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
    {
        /* Lab3: Your code here */
        return AppendEntriesArgs<Command>();
    }

    struct AppendEntriesReply
    {
        /* Lab3: Your code here */
        int term;     // currentTerm for leader to update itself
        bool success; // true if follower contained entry matching prevLogIndex and prevLogTerm
        int conflictIndex;

        MSGPACK_DEFINE(
            term,
            success,
            conflictIndex)
    };

    struct InstallSnapshotArgs
    {
        /* Lab3: Your code here */
        int term;
        int lastIncludedIndex;
        int lastIncludedTerm;
        std::vector<u8> data;

        MSGPACK_DEFINE(
            term,
            lastIncludedIndex,
            lastIncludedTerm,
            data

        )
    };

    struct InstallSnapshotReply
    {
        /* Lab3: Your code here */
        int term;

        MSGPACK_DEFINE(
            term)
    };

} /* namespace chfs */