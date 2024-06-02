#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs
{
  /**
   * `CommitLog` part
   */
  // {Your code here}
  std::vector<block_id_t> log_block_ids;
  std::vector<std::vector<u8> > log_block_states;
  std::vector<usize> log_block_sizes;
  bool is_log_block = false;
  usize global_block_nums = 0;
  txn_id_t txn_id = 0;

  CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                       bool is_checkpoint_enabled)
      : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm)
  {
  }

  CommitLog::~CommitLog() {}

  // {Your code here}
  auto CommitLog::get_log_entry_num() -> usize
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();

    return 0;
  }

  // {Your code here}
  auto CommitLog::append_log(txn_id_t txn_id,
                             std::vector<std::shared_ptr<BlockOperation> > ops)
      -> void
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    // Write the block value changes into the disk for persistence.
    // The block value changes are stored in `ops`.
    // UNIMPLEMENTED();

    usize logblock_start = this->bm_->total_blocks() + global_block_nums;
    usize block_nums = ops.size() + 1;
    global_block_nums += block_nums;
    std::cout << "in append_log ,global_block_nums: " << global_block_nums << std::endl;
    std::vector<u8> buffer(this->bm_->block_size());
    struct head_block
    {
      usize block_nums;
      txn_id_t txn_id;
    };
    auto head_block_tmp = reinterpret_cast<head_block *>(buffer.data());
    head_block_tmp->block_nums = block_nums;
    head_block_tmp->txn_id = txn_id;
    block_id_t *block_ids = reinterpret_cast<block_id_t *>(buffer.data() + sizeof(usize) + sizeof(txn_id_t));
    usize *block_sizes = reinterpret_cast<usize *>(buffer.data() + sizeof(usize) + sizeof(txn_id_t) + sizeof(block_id_t) * block_nums);

    for (size_t i = 0; i < block_nums - 1; i++)
    {
      std::cout << ops[i]->block_id_ << " " << ops[i]->size;
      block_ids[i] = ops[i]->block_id_;
      block_sizes[i] = ops[i]->size;
    }

    bool flag = is_log_block;
    is_log_block = false;
    // write the head block
    // this->bm_->set_may_fail(false);
    this->bm_->write_block(logblock_start, buffer.data());
    // this->bm_->set_may_fail(true);
    this->bm_->sync(logblock_start);
    // write the data block
    for (usize i = 0; i < ops.size(); i++)
    {
      // this->bm_->set_may_fail(false);
      this->bm_->write_partial_block(logblock_start + i + 1, ops[i]->new_block_state_.data(), 0, ops[i]->size);
      // this->bm_->set_may_fail(true);
      this->bm_->sync(logblock_start + i + 1);
    }
    is_log_block = flag;
  }

  // {Your code here}
  auto CommitLog::commit_log(txn_id_t txn_id) -> void
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    recover();
    // this->bm_->flush();
    // checkpoint();
    // global_block_nums = 0;
    // txn_id = 0;
  }

  // {Your code here}
  auto CommitLog::checkpoint() -> void
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    usize total_blocks = this->bm_->total_blocks();
    // clear all the log blocks
    for (usize i = total_blocks - 1024; i < total_blocks; i++)
    {
      this->bm_->zero_block(i);
    }

    this->bm_->flush();
  }

  // {Your code here}
  auto CommitLog::recover() -> void
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    usize total_blocks = this->bm_->total_blocks();
    usize logblock_start = total_blocks;
    std::vector<u8> buffer(this->bm_->block_size());
    bool flag = is_log_block;
    is_log_block = false;
    struct head_block
    {
      usize block_nums;
      txn_id_t txn_id;
    };
    while (true)
    {
      this->bm_->read_block(logblock_start, buffer.data());
      auto head_block_tmp = reinterpret_cast<head_block *>(buffer.data());

      if (head_block_tmp->block_nums == 0)
      {
        break;
      }
      usize block_nums = head_block_tmp->block_nums;
      block_id_t *block_ids = reinterpret_cast<block_id_t *>(buffer.data() + sizeof(usize) + sizeof(txn_id_t));
      usize *block_sizes = reinterpret_cast<usize *>(buffer.data() + sizeof(usize) + sizeof(txn_id_t) + sizeof(block_id_t) * block_nums);

      std::cout << "recovery the log and the block_nums is: " << block_nums << std::endl;
      std::vector<u8> log_buffer(this->bm_->block_size());
      for (size_t i = 1; i < block_nums; i++)
      {
        std::cout << "recover the block : " << block_ids[i - 1] << std::endl;
        this->bm_->read_block(logblock_start + i, log_buffer.data());
        auto block_id = block_ids[i - 1];
        // this->bm_->set_may_fail(false);
        this->bm_->write_partial_block(block_id, log_buffer.data(), 0, block_sizes[i - 1]);
        // this->bm_->set_may_fail(true);
      }
      logblock_start += block_nums;
    }
    // clear all the log blocks
    for (usize i = total_blocks - 1024; i < total_blocks; i++)
    {
      this->bm_->zero_block(i);
    }

    this->bm_->flush();
    is_log_block = flag;
  }
}; // namespace chfs