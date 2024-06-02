#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include <mutex>
#include <vector>
#include <cstring>
#include "protocol.h"
namespace chfs
{

    /**
     * RaftLog uses a BlockManager to manage the data..
     */
    template <typename Command>
    class RaftLog
    {
    public:
        RaftLog(std::shared_ptr<BlockManager> bm, bool is_init);
        ~RaftLog();
        int size() { return entryVector.size(); }
        void setNextIndexs(int index, int number);
        int getNextIndexs(int index);
        void decreaceNextIndexs(int index);
        void initializeNextIndexs(int size, int initial);
        void setMatchedIndexs(int index, int number);
        int getMatchedIndexs(int index);
        void initializeMatchedIndexs(int size, int initial);
        log_entry<Command> back();
        int currentLatestIndex() { return entryVector.size() - 1 + snapshotIndex; }           // 还需要加snapshot的index
        log_entry<Command> getEntry(int index) { return entryVector[index - snapshotIndex]; } // 还需要加snapshot的index
        std::vector<log_entry<Command>> getEntriesAfter(int index)
        {
            std::vector<log_entry<Command>> entries;
            for (auto entry : entryVector)
            {
                if (entry.index >= index)
                {
                    entries.push_back(entry);
                }
            }
            return entries;
        }
        void pushbackEntry(log_entry<Command> entry)
        {
            std::lock_guard<std::mutex> guard(mtx);
            entryVector.push_back(entry);
            saveLogToDir();
            // savelog
        }
        void popbackEntry()
        {
            std::lock_guard<std::mutex> guard(mtx);
            entryVector.pop_back();
            // savelog
            saveLogToDir();
        }
        int getSnapshotIndex() { return snapshotIndex; }
        void setSnapshotIndex(int index)
        {
            // std::lock_guard<std::mutex> guard(mtx);
            snapshotIndex = index;
        }
        void resetLog(int index, int term)
        {
            std::lock_guard<std::mutex> guard(mtx);
            log_entry<Command> placeholder;
            placeholder.index = index;
            placeholder.term = term;
            entryVector = std::vector<log_entry<Command>>(1, placeholder);
            saveLogToDir();
        }
        void eraseBeforeIndex(int index)
        {
            // entryVector.erase(entryVector.begin(), entryVector.begin() + index);
            std::cout << "before , eraseBeforeIndex, the entryVector size is " << int(entryVector.size()) << std::endl;
            while (entryVector.front().index < index)
                entryVector.erase(entryVector.begin());

            std::cout << "after , eraseBeforeIndex, the entryVector size is " << int(entryVector.size()) << std::endl;
            // log_entry<Command> placeholder;
            // placeholder.index = 0;
            // placeholder.term = 0;
            // std::vector<log_entry<Command>> newEntryVector(1, placeholder);
            // for (auto entry : entryVector)
            // {
            //     if (entry.index >= index)
            //     {
            //         newEntryVector.push_back(entry);
            //     }
            // }
            // entryVector = newEntryVector;
            saveLogToDir();
        }
        void saveLogToDir();
        void saveCurrentTermToDir(int term);
        int readCurrentTerm();
        void saveSnapShotToDir();
        void setSnapshotData(std::vector<u8> snapshotData);
        std::vector<u8> getSnapshotData();
        int readSnapShotIndexFromDir();
        /* Lab3: Your code here */

    private:
        std::shared_ptr<BlockManager> bm_;
        std::mutex mtx;
        /* Lab3: Your code here */
        std::vector<log_entry<Command>> entryVector;
        std::vector<int> nextIndexs;
        std::vector<int> matchedIndexs;
        int snapshotIndex;
        std::vector<u8> snapshotData;
        std::mutex snapshot_mtx;
    };

    template <typename Command>
    RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm, bool is_init)
    {
        /* Lab3: Your code here */
        std::lock_guard<std::mutex> guard(mtx);
        bm_ = bm;
        if (is_init == false)
        {
            std::cout << "start in scratch" << std::endl;
            log_entry<Command> placeholder;
            placeholder.index = 0;
            placeholder.term = 0;
            entryVector = std::vector<log_entry<Command>>(1, placeholder);

            std::vector<u8> buffer(bm_->block_size());
            int entryNum = entryVector.size();
            int nums[5];
            nums[0] = entryNum;
            nums[1] = 0;
            memcpy(buffer.data(), nums, sizeof(int) * 5);
            bm_->write_block(0, buffer.data());

            // 初始化snapshot相关参数
            snapshotIndex = 0;
            snapshotData = std::vector<u8>();
        }
        else
        {
            std::cout << "start in log" << std::endl;
            std::vector<u8> buffer(bm_->block_size());
            bm_->read_block(0, buffer.data());
            auto nums = reinterpret_cast<int *>(buffer.data());
            int entryNum = nums[0];
            std::cout << "when recover , the entryNum is  " << entryNum << std::endl;
            for (int i = 0; i < entryNum; i++)
            {
                log_entry<Command> entry;
                bm_->read_block(i + 1, buffer.data());
                auto entry_p = reinterpret_cast<log_entry<Command> *>(buffer.data());
                entry.index = entry_p->index;
                entry.term = entry_p->term;
                entry.command_ = entry_p->command_;
                std::cout << "when recover , the entry index is  and term is " << entry.index << " " << entry.term << std::endl;

                entryVector.push_back(entry);
            }

            // 初始化snapshot相关参数
            std::vector<u8> snapshot_buffer(bm_->block_size());
            bm_->read_block(1024, snapshot_buffer.data());
            auto snapshot_nums = reinterpret_cast<int *>(snapshot_buffer.data());
            snapshotIndex = snapshot_nums[0];
            std::cout << "when recover , the snapshotIndex is  " << snapshotIndex << std::endl;
            int snapshot_size = snapshot_nums[1];
            std::cout << "when recover , the snapshot_size is  " << snapshot_size << std::endl;
            snapshotData = std::vector<u8>(snapshot_size, 0);
            int block_offset = 1024;
            int block_num = snapshot_size / bm_->block_size() + 1;
            for (int i = 0; i < block_num; i++)
            {
                std::vector<u8> entry_buffer(bm_->block_size());
                bm_->read_block(block_offset + i + 1, entry_buffer.data());
                if (i == block_num - 1)
                {
                    memcpy(snapshotData.data() + i * bm_->block_size(), entry_buffer.data(), snapshot_size - i * bm_->block_size());
                }
                else
                {
                    memcpy(snapshotData.data() + i * bm_->block_size(), entry_buffer.data(), bm_->block_size());
                }
            }
        }
    }

    template <typename Command>
    RaftLog<Command>::~RaftLog()
    {
        /* Lab3: Your code here */
    }

    template <typename Command>
    log_entry<Command> RaftLog<Command>::back()
    {
        /* Lab3: Your code here */
        return entryVector.back();
    }

    template <typename Command>
    void RaftLog<Command>::setNextIndexs(int index, int number)
    {
        nextIndexs[index] = number;
        // saveNextIndexsToDir();
    }

    template <typename Command>
    int RaftLog<Command>::getNextIndexs(int index)
    {
        return nextIndexs[index];
    }

    template <typename Command>
    void RaftLog<Command>::decreaceNextIndexs(int index)
    {
        nextIndexs[index]--;
        // saveNextIndexsToDir();
    }

    template <typename Command>
    void RaftLog<Command>::initializeNextIndexs(int size, int initial)
    {
        nextIndexs = std::vector<int>(size, initial);
        // saveNextIndexsToDir();
    }

    template <typename Command>
    void RaftLog<Command>::setMatchedIndexs(int index, int number)
    {
        // std::lock_guard<std::mutex> guard(mtx);
        matchedIndexs[index] = number;
        // saveMatchedIndexsToDir();
    }

    template <typename Command>
    void RaftLog<Command>::initializeMatchedIndexs(int size, int initial)
    {
        matchedIndexs = std::vector<int>(size, initial);
        // saveMatchedIndexsToDir();
    }

    template <typename Command>
    int RaftLog<Command>::getMatchedIndexs(int index)
    {
        return matchedIndexs[index];
    }

    template <typename Command>
    void RaftLog<Command>::saveLogToDir()
    {
        std::vector<u8> buffer(bm_->block_size());
        int entryNum = entryVector.size();
        int nums[5];
        nums[0] = entryNum;
        memcpy(buffer.data(), nums, sizeof(int) * 5);
        bm_->write_block(0, buffer.data());
        for (int i = 0; i < entryNum; i++)
        {
            std::vector<u8> entry_buffer(bm_->block_size());
            auto entry_p = reinterpret_cast<log_entry<Command> *>(entry_buffer.data());
            entry_p->index = entryVector[i].index;
            entry_p->term = entryVector[i].term;
            entry_p->command_ = entryVector[i].command_;
            bm_->write_block(i + 1, entry_buffer.data());
        }
        bm_->flush();
    }

    template <typename Command>
    void RaftLog<Command>::saveCurrentTermToDir(int term)
    {
        std::vector<u8> buffer(bm_->block_size());
        bm_->read_block(0, buffer.data());
        auto nums = reinterpret_cast<int *>(buffer.data());
        nums[1] = term;
        // std::cout << "term: " << nums[1] << std::endl;
        bm_->write_block(0, buffer.data());
        bm_->flush();
    }

    template <typename Command>
    int RaftLog<Command>::readCurrentTerm()
    {
        std::vector<u8> buffer(bm_->block_size());
        bm_->read_block(0, buffer.data());
        auto nums = reinterpret_cast<int *>(buffer.data());

        return nums[1];
    }

    template <typename Command>
    void RaftLog<Command>::saveSnapShotToDir()
    {
        std::lock_guard<std::mutex> guard(snapshot_mtx);
        std::vector<u8> buffer(bm_->block_size());
        int block_offset = 1024;
        int block_num = snapshotData.size() / bm_->block_size() + 1;
        int nums[2];
        nums[0] = snapshotIndex;
        nums[1] = snapshotData.size();
        memcpy(buffer.data(), nums, sizeof(int) * 2);
        bm_->write_block(block_offset, buffer.data());
        for (int i = 0; i < block_num; i++)
        {
            std::vector<u8> entry_buffer(bm_->block_size());
            if (i == block_num - 1)
            {
                memcpy(entry_buffer.data(), snapshotData.data() + i * bm_->block_size(), snapshotData.size() - i * bm_->block_size());
            }
            else
            {
                memcpy(entry_buffer.data(), snapshotData.data() + i * bm_->block_size(), bm_->block_size());
            }
            bm_->write_block(block_offset + i + 1, entry_buffer.data());
        }
        bm_->flush();
    }

    template <typename Command>
    void RaftLog<Command>::setSnapshotData(std::vector<u8> snapshotData)
    {
        this->snapshotData = std::vector<u8>(snapshotData.begin(), snapshotData.end());
        saveSnapShotToDir();
    }

    template <typename Command>
    std::vector<u8> RaftLog<Command>::getSnapshotData()
    {
        std::lock_guard<std::mutex> guard(snapshot_mtx);
        return this->snapshotData;
    }

    template <typename Command>
    int RaftLog<Command>::readSnapShotIndexFromDir()
    {
        std::vector<u8> buffer(bm_->block_size());
        bm_->read_block(1024, buffer.data());
        auto nums = reinterpret_cast<int *>(buffer.data());

        return nums[0];
    }
    /* Lab3: Your code here */

} /* namespace chfs */
