#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs
{

  auto DataServer::initialize(std::string const &data_path)
  {
    /**
     * At first check whether the file exists or not.
     * If so, which means the distributed chfs has
     * already been initialized and can be rebuilt from
     * existing data.
     */
    bool is_initialized = is_file_exist(data_path);

    auto bm = std::shared_ptr<BlockManager>(
        new BlockManager(data_path, KDefaultBlockCnt));
    if (is_initialized)
    {
      usize reverse_block = KDefaultBlockCnt / (bm->block_size() / sizeof(version_t));
      block_allocator_ =
          std::make_shared<BlockAllocator>(bm, reverse_block, false);
    }
    else
    {
      // We need to reserve some blocks for storing the version of each block

      usize reverse_block = KDefaultBlockCnt / (bm->block_size() / sizeof(version_t));
      block_allocator_ = std::shared_ptr<BlockAllocator>(
          new BlockAllocator(bm, reverse_block, true));
    }

    // Initialize the RPC server and bind all handlers
    server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                      usize len, version_t version)
                  { return this->read_data(block_id, offset, len, version); });
    server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                       std::vector<u8> &buffer)
                  { return this->write_data(block_id, offset, buffer); });
    server_->bind("alloc_block", [this]()
                  { return this->alloc_block(); });
    server_->bind("free_block", [this](block_id_t block_id)
                  { return this->free_block(block_id); });

    // Launch the rpc server to listen for requests
    server_->run(true, num_worker_threads);
  }

  DataServer::DataServer(u16 port, const std::string &data_path)
      : server_(std::make_unique<RpcServer>(port))
  {
    initialize(data_path);
  }

  DataServer::DataServer(std::string const &address, u16 port,
                         const std::string &data_path)
      : server_(std::make_unique<RpcServer>(address, port))
  {
    initialize(data_path);
  }

  DataServer::~DataServer() { server_.reset(); }

  // {Your code here}
  auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                             version_t version) -> std::vector<u8>
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    std::cout << "DataServer : read_data: " << block_id << " " << offset << " " << len << " " << version << std::endl;
    block_id_t version_block_id = block_id / (this->block_allocator_->bm->block_size() / sizeof(version_t));
    usize version_block_offset = block_id % (this->block_allocator_->bm->block_size() / sizeof(version_t));
    std::vector<u8> version_buffer(this->block_allocator_->bm->block_size());
    this->block_allocator_->bm->read_block(version_block_id, version_buffer.data());
    auto versions = reinterpret_cast<version_t *>(version_buffer.data());
    if (versions[version_block_offset] != version)
    {
      std::cout << "version not match" << std::endl;
      std::cout << "versions[version_block_offset]: " << versions[version_block_offset] << std::endl;
      std::cout << "version: " << version << std::endl;
      return {};
    }
    std::vector<u8> buffer(this->block_allocator_->bm->block_size());
    this->block_allocator_->bm->read_block(block_id, buffer.data());
    std::vector<u8> data(len);
    for (int i = 0; i < len; i++)
    {
      data[i] = buffer[offset + i];
    }
    return data;
  }

  // {Your code here}
  auto DataServer::write_data(block_id_t block_id, usize offset,
                              std::vector<u8> &buffer) -> bool
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    std::cout << "DataServer : write_data: " << block_id << " " << offset << " " << buffer.size() << std::endl;
    auto res = this->block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());
    if (res.is_err())
    {
      return false;
    }

    return true;
  }

  // {Your code here}
  auto DataServer::alloc_block() -> std::pair<block_id_t, version_t>
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto block_res = this->block_allocator_->allocate();
    if (block_res.is_err())
    {
      return std::pair<block_id_t, version_t>();
    }
    auto block_id = block_res.unwrap();
    std::shared_ptr<BlockManager> bm = this->block_allocator_->bm;
    std::vector<u8> buffer(bm->block_size());
    block_id_t version_block_id = block_id / (bm->block_size() / sizeof(version_t));
    bm->read_block(version_block_id, buffer.data());
    block_id_t version_block_offset = block_id % (bm->block_size() / sizeof(version_t));
    auto version = reinterpret_cast<version_t *>(buffer.data());
    version[version_block_offset] += 1;
    bm->write_block(version_block_id, buffer.data());
    return std::pair<block_id_t, version_t>(block_id, version[version_block_offset]);
  }

  // {Your code here}
  auto DataServer::free_block(block_id_t block_id) -> bool
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res = this->block_allocator_->deallocate(block_id);
    if (res.is_err())
    {
      return false;
    }

    std::shared_ptr<BlockManager> bm = this->block_allocator_->bm;
    std::vector<u8> buffer(bm->block_size());
    block_id_t version_block_id = block_id / (bm->block_size() / sizeof(version_t));
    bm->read_block(version_block_id, buffer.data());
    block_id_t version_block_offset = block_id % (bm->block_size() / sizeof(version_t));
    auto version = reinterpret_cast<version_t *>(buffer.data());
    version[version_block_offset] += 1;
    bm->write_block(version_block_id, buffer.data());

    return true;
  }
} // namespace chfs