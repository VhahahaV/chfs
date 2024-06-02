#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>
#include <sys/time.h>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs
{

    enum class RaftRole
    {
        Follower,
        Candidate,
        Leader
    };

    struct RaftNodeConfig
    {
        int node_id;
        uint16_t port;
        std::string ip_address;
    };

    template <typename StateMachine, typename Command>
    class RaftNode
    {

#define RAFT_LOG(fmt, args...)                                                                                                       \
    do                                                                                                                               \
    {                                                                                                                                \
        auto now =                                                                                                                   \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                                                   \
                std::chrono::system_clock::now().time_since_epoch())                                                                 \
                .count();                                                                                                            \
        char buf[512];                                                                                                               \
        sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf; });                                                                           \
    } while (0);

    public:
        RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);
        ~RaftNode();

        RaftNode(const RaftNode &) = delete;
        RaftNode &operator=(const RaftNode &) = delete;

        /* interfaces for test */
        void set_network(std::map<int, bool> &network_availablility);
        void set_reliable(bool flag);
        int get_list_state_log_num();
        int rpc_count();
        std::vector<u8> get_snapshot_direct();

    private:
        /*
         * Start the raft node.
         * Please make sure all of the rpc request handlers have been registered before this method.
         */
        auto start() -> int;

        /*
         * Stop the raft node.
         */
        auto stop() -> int;

        /* Returns whether this node is the leader, you should also return the current term. */
        auto is_leader() -> std::tuple<bool, int>;

        /* Checks whether the node is stopped */
        auto is_stopped() -> bool;

        /*
         * Send a new command to the raft nodes.
         * The returned tuple of the method contains three values:
         * 1. bool:  True if this raft node is the leader that successfully appends the log,
         *      false If this node is not the leader.
         * 2. int: Current term.
         * 3. int: Log index.
         */
        auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

        /* Save a snapshot of the state machine and compact the log. */
        auto save_snapshot() -> bool;

        /* Get a snapshot of the state machine */
        auto get_snapshot() -> std::vector<u8>;

        /* Internal RPC handlers */
        auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
        auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
        auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

        /* RPC helpers */
        void send_request_vote(int target, RequestVoteArgs arg);
        void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

        void send_append_entries(int target, AppendEntriesArgs<Command> arg);
        void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

        void send_install_snapshot(int target, InstallSnapshotArgs arg);
        void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

        /* background workers */
        void run_background_ping();
        void run_background_election();
        void run_background_commit();
        void run_background_apply();

        /* Data structures */
        bool network_stat; /* for test */

        std::mutex mtx;         /* A big lock to protect the whole data structure. */
        std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
        std::unique_ptr<ThreadPool> thread_pool;
        std::unique_ptr<RaftLog<Command>> log_storage; /* To persist the raft log. */
        std::unique_ptr<StateMachine> state;           /*  The state machine that applies the raft log, e.g. a kv store. */

        std::unique_ptr<RpcServer> rpc_server;                     /* RPC server to recieve and handle the RPC requests. */
        std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map; /* RPC clients of all raft nodes including this node. */
        std::vector<RaftNodeConfig> node_configs;                  /* Configuration for all nodes */
        int my_id;                                                 /* The index of this node in rpc_clients, start from 0. */

        std::atomic_bool stopped;

        RaftRole role;
        int current_term;
        int leader_id;

        std::unique_ptr<std::thread> background_election;
        std::unique_ptr<std::thread> background_ping;
        std::unique_ptr<std::thread> background_commit;
        std::unique_ptr<std::thread> background_apply;

        // added by me
        int commitIndex;
        int lastApplied;
        int followCount;
        int my_timeout_follower;
        long long last_RPC_time;
        int voteFor;

        // some function
        void renewRPCtime();
        void sendHeartBeat();
        void readPersist();
        void persistCurrentTerm();

        /* Lab3: Your code here */
    };

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::persistCurrentTerm()
    {
        log_storage->saveCurrentTermToDir(current_term);
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::readPersist()
    {
        current_term = log_storage->readCurrentTerm();
        lastApplied = commitIndex = log_storage->readSnapShotIndexFromDir();
        RAFT_LOG("I read snapshot. My lastApplied is %d, my commitIndex is %d.", lastApplied, commitIndex);
        if (lastApplied)
        {
            state->apply_snapshot(log_storage->getSnapshotData());
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::renewRPCtime()
    {
        timeval current;
        gettimeofday(&current, NULL);
        last_RPC_time = current.tv_sec * 1000 + current.tv_usec / 1000;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::sendHeartBeat()
    {
        if (role != RaftRole::Leader)
        {
            return;
        }
        AppendEntriesArgs<Command> arg;
        arg.term = current_term;
        arg.is_empty_entries = true;
        arg.leader_id = my_id;
        arg.leader_commit = commitIndex;

        arg.prev_log_index = log_storage->back().index;
        arg.prev_log_term = log_storage->back().term;

        for (int i = 0; i < node_configs.size(); i++)
        {
            if (i == my_id)
                continue;
            send_append_entries(i, arg);
        }
    }

    template <typename StateMachine, typename Command>
    RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs) : network_stat(true),
                                                                                                  node_configs(configs),
                                                                                                  my_id(node_id),
                                                                                                  stopped(false),
                                                                                                  role(RaftRole::Follower),
                                                                                                  leader_id(-1)
    {
        auto my_config = node_configs[my_id];

        /* launch RPC server */
        rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

        /* Register the RPCs. */
        rpc_server->bind(RAFT_RPC_START_NODE, [this]()
                         { return this->start(); });
        rpc_server->bind(RAFT_RPC_STOP_NODE, [this]()
                         { return this->stop(); });
        rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]()
                         { return this->is_leader(); });
        rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]()
                         { return this->is_stopped(); });
        rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size)
                         { return this->new_command(data, cmd_size); });
        rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]()
                         { return this->save_snapshot(); });
        rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]()
                         { return this->get_snapshot(); });

        rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg)
                         { return this->request_vote(arg); });
        rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg)
                         { return this->append_entries(arg); });
        rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg)
                         { return this->install_snapshot(arg); });
        rpc_server->run(true, configs.size());

        /* Lab3: Your code here */

        last_RPC_time = 0;
        background_election = nullptr;
        background_ping = nullptr;
        background_commit = nullptr;
        background_apply = nullptr;
        std::string data_path = "/tmp/raft_log/" + std::to_string(my_id);
        bool is_initialed = is_file_exist(data_path);
        auto bm = std::make_shared<BlockManager>("/tmp/raft_log/" + std::to_string(my_id), 4096);
        log_storage = std::make_unique<RaftLog<Command>>(bm, is_initialed);

        thread_pool = std::make_unique<ThreadPool>(configs.size() + 1);
        state = std::make_unique<StateMachine>();
        readPersist();
    }

    template <typename StateMachine, typename Command>
    RaftNode<StateMachine, Command>::~RaftNode()
    {
        stop();

        thread_pool.reset();
        rpc_server.reset();
        state.reset();
        log_storage.reset();
    }

    /******************************************************************

                            RPC Interfaces

    *******************************************************************/

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::start() -> int
    {
        /* Lab3: Your code here */

        background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
        background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
        background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
        background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);
        std::map<int, bool> network_availability;
        for (auto config : this->node_configs)
        {
            network_availability[config.node_id] = true;
        }
        set_network(network_availability);
        return 0;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::stop() -> int
    {
        /* Lab3: Your code here */

        stopped.store(true);
        background_ping->join();
        background_election->join();
        background_commit->join();
        background_apply->join();
        return 0;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
    {
        /* Lab3: Your code here */
        return std::make_tuple(role == RaftRole::Leader, current_term);
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::is_stopped() -> bool
    {
        return stopped.load();
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
    {
        /* Lab3: Your code here */
        std::lock_guard<std::mutex> guard(mtx);
        if (role != RaftRole::Leader)
        {
            return std::make_tuple(false, -1, -1);
        }
        int index = log_storage->currentLatestIndex() + 1;
        int term = current_term;
        log_storage->setNextIndexs(my_id, index + 1);
        log_storage->setMatchedIndexs(my_id, index);
        log_entry<Command> newLog;
        newLog.index = index;
        newLog.term = term;
        Command command;
        command.deserialize(cmd_data, cmd_size);
        newLog.command_ = command;
        log_storage->pushbackEntry(newLog);

        RAFT_LOG("node %d receive new command , and my log_storage.size is %d", my_id, int(log_storage->size()));
        // RAFT_LOG("persistCurrentTerm")
        persistCurrentTerm();
        // persistmetadata
        return std::make_tuple(true, term, index);
        // return std::make_tuple(false, -1, -1);
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
    {
        /* Lab3: Your code here */
        // std::lock_guard<std::mutex> guard(mtx);
        log_storage->setSnapshotIndex(lastApplied);

        std::vector<u8> data = state->snapshot();
        log_storage->setSnapshotData(data);
        log_storage->eraseBeforeIndex(lastApplied);
        RAFT_LOG("I prepare to save snapshot. My lastApplied is %d, my commitIndex is %d.", lastApplied, commitIndex);
        RAFT_LOG("the snapshot 's datasize is %d", int(data.size()));

        return true;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
    {
        /* Lab3: Your code here */
        return get_snapshot_direct();
    }

    /******************************************************************

                             Internal RPC Related

    *******************************************************************/

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
    {
        /* Lab3: Your code here */
        // RAFT_LOG("node %d receive vote request from node %d", my_id, args.candidate_id);
        std::lock_guard<std::mutex> guard(mtx);
        RequestVoteReply reply;
        reply.vote_granted = false;
        reply.current_term = current_term;

        if (args.term <= current_term)
        {
            return reply;
        }
        else
        {
            role = RaftRole::Follower;
            current_term = args.term;
            // 还需要持久化
            // RAFT_LOG("persistCurrentTerm")
            persistCurrentTerm();
            RAFT_LOG("node %d become follower", my_id);
        }
        if (args.last_log_term < log_storage->back().term)
        {
            RAFT_LOG("I refuse %d. My lastLogTerm is %d, its is %d.", args.candidate_id, log_storage->back().term, args.last_log_term);
            return reply;
        }
        if (args.last_log_term == log_storage->back().term && args.last_log_index < log_storage->back().index)
        {
            RAFT_LOG("I refuse %d. My lastLogIndex is %d, its is %d.", args.candidate_id, log_storage->back().index, args.last_log_index);
            return reply;
        }

        RAFT_LOG("I accept %d.I am a follower now.", args.candidate_id);
        reply.vote_granted = true;
        role = RaftRole::Follower;
        renewRPCtime();

        return reply;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
    {
        /* Lab3: Your code here */

        if (arg.term > current_term)
        {
            mtx.lock();
            current_term = arg.term;
            role = RaftRole::Follower;
            mtx.unlock();
            RAFT_LOG("node %d become follower", my_id);
        }

        if (reply.vote_granted && role == RaftRole::Candidate)
        {
            followCount++;
            RAFT_LOG("I get vote from %d.Current vote I get is %d.", target, followCount);
            if (followCount >= (node_configs.size() + 1) / 2)
            {
                RAFT_LOG("node %d become leader", my_id);
                mtx.lock();
                role = RaftRole::Leader;
                // 并且还要初始化log_storage
                log_storage->initializeNextIndexs(node_configs.size(), log_storage->back().index + 1);
                log_storage->initializeMatchedIndexs(node_configs.size(), 0);
                mtx.unlock();
                sendHeartBeat();
            }
        }
        // RAFT_LOG("persistCurrentTerm")
        persistCurrentTerm();
        return;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
    {
        /* Lab3: Your code here */
        std::lock_guard<std::mutex> guard(mtx);

        renewRPCtime();

        if (rpc_arg.term >= current_term)
        {
            role = RaftRole::Follower;
            current_term = rpc_arg.term;
            RAFT_LOG("I receive %d's heartbeat. I am follower now!", rpc_arg.leader_id);
        }

        AppendEntriesReply reply;
        reply.term = current_term;

        if (rpc_arg.term < current_term)
        {
            reply.success = false;
            // RAFT_LOG("persistCurrentTerm")
            persistCurrentTerm();
            // persistence
            return reply;
        }
        reply.success = true;

        if (rpc_arg.prev_log_index < log_storage->getSnapshotIndex() || rpc_arg.prev_log_index > log_storage->currentLatestIndex() || log_storage->getEntry(rpc_arg.prev_log_index).term != rpc_arg.prev_log_term)
        {
            if (rpc_arg.prev_log_index > log_storage->currentLatestIndex() || rpc_arg.prev_log_index < log_storage->getSnapshotIndex())
            {
                reply.conflictIndex = log_storage->currentLatestIndex() + 1;
                RAFT_LOG("I refuse %d. My lastLogIndex is %d, its is %d.", rpc_arg.leader_id, log_storage->currentLatestIndex(), rpc_arg.prev_log_index);
            }
            else
            {
                int new_index = rpc_arg.prev_log_index;
                int tartget_term = log_storage->getEntry(new_index).term;
                while (new_index - 1 > log_storage->getSnapshotIndex() && log_storage->getEntry(new_index - 1).term == tartget_term)
                {
                    new_index--;
                }
                reply.conflictIndex = new_index;
                RAFT_LOG("rollback  %d", new_index);
            }
            reply.success = false;
            return reply;
        }

        log_entry<Command> prevLog = log_storage->back();
        while (prevLog.index > rpc_arg.prev_log_index)
        {
            log_storage->popbackEntry();
            prevLog = log_storage->back();
        }
        int entry_nums = rpc_arg.entries_terms.size();
        for (int i = 0; i < entry_nums; i++)
        {
            log_entry<Command> newLog;
            newLog.index = rpc_arg.entries_indexs[i];
            newLog.term = rpc_arg.entries_terms[i];
            Command command;
            // static_cast<ListCommand *>(command)->deserialize(rpc_arg.entries_commands[i], rpc_arg.command_size);
            command.deserialize(rpc_arg.entries_commands[i], rpc_arg.command_size);
            newLog.command_ = command;
            log_storage->pushbackEntry(newLog);
        }
        reply.success = true;
        RAFT_LOG("I accept %d's command. and it's leader_commit is %d,and my commitIndex is %d", rpc_arg.leader_id, rpc_arg.leader_commit, commitIndex);
        RAFT_LOG("it's rpc_arg.prev_log_index is %d, and log_storage->getSnapshotIndex() is %d, and log_storage->currentLatestIndex() is %d,and my last log index is %d ", int(rpc_arg.prev_log_index), log_storage->getSnapshotIndex(), log_storage->currentLatestIndex(), int(log_storage->back().index));

        RAFT_LOG("it's log_storage->getEntry(rpc_arg.prev_log_index).term  is %d, and rpc_arg.prev_log_term is %d", log_storage->getEntry(rpc_arg.prev_log_index).term, rpc_arg.prev_log_term);

        if (role == RaftRole::Follower && rpc_arg.leader_commit > commitIndex)
        {
            commitIndex = std::min(rpc_arg.leader_commit, log_storage->currentLatestIndex());
            RAFT_LOG("I receive %d's message and renew my commitIndex to %d,and my lastApplied is %d", rpc_arg.leader_id, commitIndex, lastApplied);
        }
        RAFT_LOG("persistCurrentTerm")
        persistCurrentTerm();

        return reply;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
    {
        /* Lab3: Your code here */
        if (reply.term > current_term && reply.success == false)
        {
            std::lock_guard<std::mutex> guard(mtx);
            current_term = reply.term;
            role = RaftRole::Follower;
            // RAFT_LOG("persistCurrentTerm")
            persistCurrentTerm();
            RAFT_LOG("node %d become follower and the new leader is %d , it's new term, is %d", my_id, arg.leader_id, reply.term);
            return;
        }
        if (arg.is_empty_entries)
        {
            return;
        }
        if (reply.success == false && reply.term == current_term)
        {
            std::lock_guard<std::mutex> guard(mtx);
            // set nextIndex
            log_storage->setNextIndexs(node_id, reply.conflictIndex);
            RAFT_LOG("I send append entries to %d. It refuse. So I decrease its nextIndex to %d.", node_id, reply.conflictIndex);
            return;
        }
        // if (reply.success == true && reply.term == current_term)
        else
        {
            std::lock_guard<std::mutex> guard(mtx);
            RAFT_LOG("I send append entries to %d. It accept. So I Renew its nextIndex to %d, its matchedIndex to %d.", node_id, (arg.entries).back().index + 1, (arg.entries).back().index);
            log_storage->setNextIndexs(node_id, (arg.entries).size() + log_storage->getNextIndexs(node_id));
            log_storage->setMatchedIndexs(node_id, log_storage->getNextIndexs(node_id) - 1);
            return;
        }

        return;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
    {
        /* Lab3: Your code here */
        std::lock_guard<std::mutex> guard(mtx);
        if (args.term > current_term)
        {
            current_term = args.term;
            role = RaftRole::Follower;
            // RAFT_LOG("persistCurrentTerm")
            persistCurrentTerm();
            RAFT_LOG("node %d become follower", my_id);
        }
        InstallSnapshotReply reply;
        reply.term = current_term;
        if (args.term < current_term)
        {
            RAFT_LOG("I refuse  snapshot. My term is %d, its is %d.", current_term, args.term);
            return reply;
        }

        renewRPCtime();
        if (log_storage->currentLatestIndex() >= args.lastIncludedIndex && log_storage->getSnapshotIndex() < args.lastIncludedIndex && log_storage->getEntry(args.lastIncludedIndex).term == args.lastIncludedTerm)
        {
            log_storage->eraseBeforeIndex(args.lastIncludedIndex);
        }
        else
        {
            log_storage->resetLog(args.lastIncludedIndex, args.lastIncludedTerm);
        }

        log_storage->setSnapshotIndex(args.lastIncludedIndex);
        log_storage->setSnapshotData(args.data);
        lastApplied = commitIndex = args.lastIncludedIndex;
        state->apply_snapshot(args.data);
        RAFT_LOG("I apply snapshot. My lastApplied is %d, my commitIndex is %d.", lastApplied, commitIndex);

        return reply;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
    {
        /* Lab3: Your code here */
        std::lock_guard<std::mutex> guard(mtx);
        if (arg.term > current_term)
        {
            current_term = arg.term;
            role = RaftRole::Follower;
            // RAFT_LOG("persistCurrentTerm")
            persistCurrentTerm();
        }

        log_storage->setNextIndexs(node_id, log_storage->getSnapshotIndex() + 1);
        log_storage->setMatchedIndexs(node_id, log_storage->getSnapshotIndex());

        return;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected)
        {
            RAFT_LOG("node %d is not connected in send_request_vote", target_id);
            return;
        }
        // RAFT_LOG("node %d is connecte to  node %d", my_id, target_id)
        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
        clients_lock.unlock();
        if (res.is_ok())
        {
            handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
        }
        else
        {
            // RPC fails
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected)
        {
            RAFT_LOG("node %d is not connected in send_append_entries", target_id);
            return;
        }

        RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
        clients_lock.unlock();
        if (res.is_ok())
        {
            handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
        }
        else
        {
            // RPC fails
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected)
        {
            return;
        }
        RAFT_LOG(" %d  send_install_snapshot to %d", my_id, target_id);
        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
        clients_lock.unlock();
        if (res.is_ok())
        {
            handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
        }
        else
        {
            // RPC fails
        }
    }

    /******************************************************************

                            Background Workers

    *******************************************************************/

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_election()
    {
        // Periodly check the liveness of the leader.

        // Work for followers and candidates.

        /* Uncomment following code when you finish */
        while (true)
        {

            if (is_stopped())
            {
                return;
            }
            // RAFT_LOG("node %d is running background election", my_id);
            /* Lab3: Your code here */
            timeval election;
            gettimeofday(&election, NULL);
            long long election_start = election.tv_sec * 1000 + election.tv_usec / 1000;
            long long wait_time = election_start - last_RPC_time;
            // 随机生成一个时间
            int randomSeed = rand() + rand() % 500;
            srand(randomSeed);
            long long timeout_ = 300 + (rand() % 400);
            int connectedCount = 1;
            for (int i = 0; i < node_configs.size(); i++)
            {
                if (i == my_id)
                {
                    continue;
                }
                if (rpc_clients_map[i] != nullptr && rpc_clients_map[i]->get_connection_state() == rpc::client::connection_state::connected)
                {
                    connectedCount++;
                }
            }
            RAFT_LOG("node %d is connected to %d nodes", my_id, connectedCount);
            if (role != RaftRole::Leader && wait_time > timeout_ && connectedCount >= (node_configs.size() + 1) / 2)
            {
                mtx.lock();
                last_RPC_time = election_start;
                role = RaftRole::Candidate;
                current_term++;
                followCount = 1;
                // 可能需要持久化
                RAFT_LOG("persistCurrentTerm")
                persistCurrentTerm();
                mtx.unlock();
                // 发送投票请求
                RequestVoteArgs arg;
                arg.term = current_term;
                arg.candidate_id = my_id;
                arg.last_log_index = log_storage->back().index;
                arg.last_log_term = log_storage->back().term;
                RAFT_LOG("node %d start election", my_id);
                for (int i = 0; i < node_configs.size(); i++)
                {
                    if (i == my_id)
                    {
                        continue;
                    }

                    send_request_vote(i, arg);
                    if (role == RaftRole::Follower)
                    {
                        break;
                    }
                }
            }

            // 睡一下
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_commit()
    {
        // Periodly send logs to the follower.

        // Only work for the leader.

        /* Uncomment following code when you finish */
        while (true)
        {
            if (is_stopped())
            {
                return;
            }
            if (role == RaftRole::Leader)
            {
                for (int target = 0; target < node_configs.size(); target++)
                {
                    if (role != RaftRole::Leader)
                    {
                        break;
                    }
                    if (target == my_id)
                    {
                        continue;
                    }
                    if (log_storage->getNextIndexs(target) > log_storage->getSnapshotIndex() && log_storage->getNextIndexs(target) <= log_storage->currentLatestIndex())
                    {
                        RAFT_LOG("prepare to send append entries and getNextIndexs is %d ,getSnapshotIndex is %d, currentLatestIndex is %d", log_storage->getNextIndexs(target), log_storage->getSnapshotIndex(), log_storage->currentLatestIndex());
                        AppendEntriesArgs<Command> arg;
                        arg.term = current_term;
                        arg.is_empty_entries = false;
                        arg.leader_id = my_id;

                        arg.prev_log_index = log_storage->getNextIndexs(target) - 1;

                        arg.prev_log_term = log_storage->getEntry(arg.prev_log_index).term;

                        arg.entries = log_storage->getEntriesAfter(log_storage->getNextIndexs(target));

                        // add arg.entries.size();
                        // arg.leader_commit = commitIndex + arg.entries.size();
                        arg.leader_commit = commitIndex;
                        RAFT_LOG("I send append entries to %d,   It's current next index is %d, my current log index is %d, %d logs sent.", target, log_storage->getNextIndexs(target), log_storage->currentLatestIndex(), int(arg.entries.size()));
                        if (role != RaftRole::Leader)
                            break;
                        send_append_entries(target, arg);
                    }
                    else
                    {
                        RAFT_LOG("I don't send append entries to %d,   It's current next index is %d, my current log index is %d", target, log_storage->getNextIndexs(target), log_storage->currentLatestIndex());
                    }
                    // snapshot
                    if (log_storage->getNextIndexs(target) <= log_storage->getSnapshotIndex())
                    {
                        InstallSnapshotArgs snaparg;
                        snaparg.term = current_term;
                        snaparg.lastIncludedIndex = log_storage->getSnapshotIndex();
                        snaparg.lastIncludedTerm = log_storage->getEntry(log_storage->getSnapshotIndex()).term;
                        RAFT_LOG("I send snapshot to %d", target);
                        snaparg.data = log_storage->getSnapshotData();
                        send_install_snapshot(target, snaparg);
                    }
                }
            }

            if (role == RaftRole::Leader)
            {
                int nextCommitIndex = commitIndex + 1;

                while (true)
                {
                    int count = 0;
                    for (int target = 0; target < node_configs.size(); target++)
                    {

                        if (log_storage->getMatchedIndexs(target) >= nextCommitIndex)
                        {
                            count++;
                        }
                    }
                    if (count < (int)(node_configs.size()) / 2 + 1)
                    {
                        break;
                    }
                    else
                    {
                        nextCommitIndex++;
                    }
                }
                if (nextCommitIndex - 1 != commitIndex)
                {
                    commitIndex = nextCommitIndex - 1;
                    RAFT_LOG("I renew my commitIndex to   %d", commitIndex);
                    sendHeartBeat();
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Change the timeout here!

            /* Lab3: Your code here */
        }

        return;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_apply()
    {
        // Periodly apply committed logs the state machine

        // Work for all the nodes.

        /* Uncomment following code when you finish */
        while (true)

        {
            if (is_stopped())
            {
                return;
            }
            // RAFT_LOG("node %d is running background apply", my_id);
            RAFT_LOG("commitIndex is %d, lastApplied is %d", commitIndex, lastApplied);
            while (commitIndex > lastApplied)
            {
                lastApplied++;
                Command command = log_storage->getEntry(lastApplied).command_;
                state->apply_log(command);
                RAFT_LOG("node %d apply log to state machine", my_id);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Change the timeout here!

            /* Lab3: Your code here */
        }

        return;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_ping()
    {
        // Periodly send empty append_entries RPC to the followers.

        // Only work for the leader.

        /* Uncomment following code when you finish */
        while (true)
        {
            {
                if (is_stopped())
                {
                    return;
                }
                if (role == RaftRole::Leader)
                    sendHeartBeat();

                std::this_thread::sleep_for(std::chrono::milliseconds(150)); // Change the timeout here!
                /* Lab3: Your code here */
            }
        }

        return;
    }

    /******************************************************************

                              Test Functions (must not edit)

    *******************************************************************/

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);

        /* turn off network */
        if (!network_availability[my_id])
        {
            for (auto &&client : rpc_clients_map)
            {
                if (client.second != nullptr)
                    client.second.reset();
            }

            return;
        }

        for (auto node_network : network_availability)
        {
            int node_id = node_network.first;
            bool node_status = node_network.second;

            if (node_status && rpc_clients_map[node_id] == nullptr)
            {
                RaftNodeConfig target_config;
                for (auto config : node_configs)
                {
                    if (config.node_id == node_id)
                        target_config = config;
                }

                rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
            }

            if (!node_status && rpc_clients_map[node_id] != nullptr)
            {
                rpc_clients_map[node_id].reset();
            }
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::set_reliable(bool flag)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        for (auto &&client : rpc_clients_map)
        {
            if (client.second)
            {
                client.second->set_reliable(flag);
            }
        }
    }

    template <typename StateMachine, typename Command>
    int RaftNode<StateMachine, Command>::get_list_state_log_num()
    {
        /* only applied to ListStateMachine*/
        std::unique_lock<std::mutex> lock(mtx);

        return state->num_append_logs;
    }

    template <typename StateMachine, typename Command>
    int RaftNode<StateMachine, Command>::rpc_count()
    {
        int sum = 0;
        std::unique_lock<std::mutex> clients_lock(clients_mtx);

        for (auto &&client : rpc_clients_map)
        {
            if (client.second)
            {
                sum += client.second->count();
            }
        }

        return sum;
    }

    template <typename StateMachine, typename Command>
    std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
    {
        if (is_stopped())
        {
            return std::vector<u8>();
        }

        std::unique_lock<std::mutex> lock(mtx);

        return state->snapshot();
    }

}