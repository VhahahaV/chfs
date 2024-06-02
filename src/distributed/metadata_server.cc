#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs
{
  std::mutex mtx;
  extern std::vector<block_id_t> log_block_ids;
  extern std::vector<std::vector<u8> > log_block_states;
  extern std::vector<usize> log_block_sizes;
  extern bool is_log_block;
  extern txn_id_t txn_id;
  inline auto MetadataServer::bind_handlers()
  {
    server_->bind("mknode",
                  [this](u8 type, inode_id_t parent, std::string const &name)
                  {
                    return this->mknode(type, parent, name);
                  });
    server_->bind("unlink", [this](inode_id_t parent, std::string const &name)
                  { return this->unlink(parent, name); });
    server_->bind("lookup", [this](inode_id_t parent, std::string const &name)
                  { return this->lookup(parent, name); });
    server_->bind("get_block_map",
                  [this](inode_id_t id)
                  { return this->get_block_map(id); });
    server_->bind("alloc_block",
                  [this](inode_id_t id)
                  { return this->allocate_block(id); });
    server_->bind("free_block",
                  [this](inode_id_t id, block_id_t block, mac_id_t machine_id)
                  {
                    return this->free_block(id, block, machine_id);
                  });
    server_->bind("readdir", [this](inode_id_t id)
                  { return this->readdir(id); });
    server_->bind("get_type_attr",
                  [this](inode_id_t id)
                  { return this->get_type_attr(id); });
  }

  inline auto MetadataServer::init_fs(const std::string &data_path)
  {
    /**
     * Check whether the metadata exists or not.
     * If exists, we wouldn't create one from scratch.
     */
    bool is_initialed = is_file_exist(data_path);

    auto block_manager = std::shared_ptr<BlockManager>(nullptr);
    if (is_log_enabled_)
    {
      block_manager =
          std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
    }
    else
    {
      block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
    }

    CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

    if (is_initialed)
    {
      auto origin_res = FileOperation::create_from_raw(block_manager);
      std::cout << "Restarting..." << std::endl;
      if (origin_res.is_err())
      {
        std::cerr << "Original FS is bad, please remove files manually."
                  << std::endl;
        exit(1);
      }

      operation_ = origin_res.unwrap();
    }
    else
    {
      operation_ = std::make_shared<FileOperation>(block_manager,
                                                   DistributedMaxInodeSupported);
      std::cout << "We should init one new FS..." << std::endl;
      /**
       * If the filesystem on metadata server is not initialized, create
       * a root directory.
       */
      auto init_res = operation_->alloc_inode(InodeType::Directory);
      if (init_res.is_err())
      {
        std::cerr << "Cannot allocate inode for root directory." << std::endl;
        exit(1);
      }

      CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
    }

    running = false;
    num_data_servers =
        0; // Default no data server. Need to call `reg_server` to add.

    if (is_log_enabled_)
    {
      if (may_failed_)
        operation_->block_manager_->set_may_fail(true);
      commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                               is_checkpoint_enabled_);
    }

    bind_handlers();

    /**
     * The metadata server wouldn't start immediately after construction.
     * It should be launched after all the data servers are registered.
     */
  }

  MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                                 bool is_log_enabled, bool is_checkpoint_enabled,
                                 bool may_failed)
      : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
        is_checkpoint_enabled_(is_checkpoint_enabled)
  {
    server_ = std::make_unique<RpcServer>(port);

    // initialize islogblock
    is_log_block = is_log_enabled;

    init_fs(data_path);
    if (is_log_enabled_)
    {
      commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                               is_checkpoint_enabled);
    }
  }

  MetadataServer::MetadataServer(std::string const &address, u16 port,
                                 const std::string &data_path,
                                 bool is_log_enabled, bool is_checkpoint_enabled,
                                 bool may_failed)
      : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
        is_checkpoint_enabled_(is_checkpoint_enabled)
  {
    server_ = std::make_unique<RpcServer>(address, port);

    // initialize islogblock
    is_log_block = is_log_enabled;

    init_fs(data_path);
    if (is_log_enabled_)
    {
      commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                               is_checkpoint_enabled);
    }
  }

  // {Your code here}
  auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
      -> inode_id_t
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    // std::cout << "inode type: " << type << std::endl;
    std::unique_lock<std::mutex> lck(mtx);
    std::cout << "MetadataServer::mknode: " << type << " " << parent << " " << name << std::endl;
    auto res = operation_->mk_helper(parent, name.c_str(), static_cast<InodeType>(type));
    if (res.is_err())
    {
      std::cerr << "Cannot create inode." << std::endl;
      return 0;
    }

    if (is_log_block)
    {
      usize total_nums = log_block_ids.size();
      if (total_nums == 0)
      {
        std::cout << "log block is empty" << std::endl;
      }
      else
      {
        std::vector<std::shared_ptr<BlockOperation> > ops;
        for (size_t i = 0; i < total_nums; i++)
        {
          ops.push_back(std::make_shared<BlockOperation>(log_block_ids[i], log_block_states[i], log_block_sizes[i]));
        }
        this->commit_log->append_log(txn_id, ops);
        txn_id += 1;
        log_block_ids.clear();
        log_block_states.clear();
        log_block_sizes.clear();
      }
    }
    if (this->may_failed_)
      return 0;

    // std::cout << "inode id: " << res.unwrap() << std::endl;

    return res.unwrap();
  }

  // {Your code here}
  auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
      -> bool
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    std::unique_lock<std::mutex> lck(mtx);
    std::cout << "MetadataServer::unlink: " << parent << " " << name << std::endl;
    inode_id_t child_inode_id = lookup(parent, name);
    if (child_inode_id == 0)
    {
      return false;
    }
    std::vector<u8> buffer(operation_->block_manager_->block_size());
    auto res = operation_->inode_manager_->read_inode(child_inode_id, buffer);
    if (res.is_err())
    {
      return false;
    }
    Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
    if (inode_p->get_type() == InodeType::Directory)
    {
      auto res1 = operation_->remove_file(child_inode_id);
      if (res1.is_err())
      {
        return false;
      }
    }
    else if (inode_p->get_type() == InodeType::FILE)
    {
      // std::vector<BlockInfo> block_map = get_block_map(child_inode_id);
      // for (auto it = block_map.begin(); it != block_map.end(); ++it)
      // {
      //   std::cout << "free block: " << std::get<0>(*it) << std::endl;
      //   mac_id_t mac_id = std::get<1>(*it);
      //   block_id_t block_id = std::get<0>(*it);
      //   auto res = this->free_block(child_inode_id, block_id, mac_id);
      //   if (!res)
      //   {
      //     return false;
      //   }
      // }

      for (int flag = 0; flag < 1; flag++)
      {
        uint64_t *block_num = reinterpret_cast<uint64_t *>(inode_p->blocks);
        regular_file_info *file_info = reinterpret_cast<regular_file_info *>(inode_p->blocks + 8);
        uint64_t nums = *block_num;
        if (nums == 0)
        {
          std::cout << "no block to free" << std::endl;
          break;
        }
        for (uint64_t i = 0; i < nums; i++)
        {
          file_info[i] = regular_file_info(0, 0, 0);
          auto res3 = clients_[file_info[i].mac_id]->call("free_block", file_info[i].block_id);
          if (res3.is_err())
          {
            return false;
          }
        }
        *block_num = 0;
        auto inode_block_id_res = operation_->inode_manager_->get(child_inode_id);
        if (inode_block_id_res.is_err())
        {
          std::cerr << "Cannot get inode block id." << std::endl;
          return false;
        }
        block_id_t inode_block_id = inode_block_id_res.unwrap();
        auto res2 = operation_->block_manager_->write_block(inode_block_id, buffer.data());
        if (res2.is_err())
        {
          return false;
        }
      }
      // free the inode
      auto free_inode_res = operation_->inode_manager_->free_inode(child_inode_id);
      if (free_inode_res.is_err())
      {
        return false;
      }
      // free the block
      auto free_block_res = operation_->block_allocator_->deallocate(res.unwrap());
      if (free_block_res.is_err())
      {
        return false;
      }
    }

    // 2. Remove the entry from the directory.
    auto read_res = this->operation_->read_file(parent);
    if (read_res.is_err())
    {
      return false;
    }
    std::vector<u8> src0 = read_res.unwrap();
    auto src = std::string(src0.begin(), src0.end());
    std::list<DirectoryEntry> list;
    parse_directory(src, list);
    // std::cout << "before rm_from_directory list size: " << list.size() << std::endl;
    list.clear();
    src = rm_from_directory(src, name);
    parse_directory(src, list);
    // std::cout << "after rm_from_directory list size: " << list.size() << std::endl;
    std::vector<u8> src_vec(src.begin(), src.end());
    auto write_res = this->operation_->write_file(parent, src_vec);
    if (write_res.is_err())
    {
      return false;
    }

    if (is_log_block)
    {
      usize total_nums = log_block_ids.size();
      if (total_nums == 0)
      {
        std::cout << "log block is empty" << std::endl;
      }
      else
      {
        std::vector<std::shared_ptr<BlockOperation> > ops;
        for (size_t i = 0; i < total_nums; i++)
        {
          ops.push_back(std::make_shared<BlockOperation>(log_block_ids[i], log_block_states[i], log_block_sizes[i]));
        }
        this->commit_log->append_log(txn_id, ops);
        txn_id += 1;
        log_block_ids.clear();
        log_block_states.clear();
        log_block_sizes.clear();
      }
    }

    return true;
  }

  // {Your code here}
  auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
      -> inode_id_t
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    std::cout << "MetadataServer::lookup: " << parent << " " << name << std::endl;
    auto res = operation_->lookup(parent, name.c_str());
    if (res.is_err())
    {
      std::cerr << "Cannot find inode." << std::endl;
      return 0;
    }
    // std::cout << "inode id: " << res.unwrap() << std::endl;

    return res.unwrap();
  }

  // {Your code here}
  auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo>
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    std::cout << "MetadataServer::get_block_map: " << id << std::endl;
    std::vector<BlockInfo> res_block_map;
    std::vector<u8> buffer(operation_->block_manager_->block_size());
    auto res = operation_->inode_manager_->read_inode(id, buffer);
    if (res.is_err())
    {
      return res_block_map;
    }
    Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
    if (inode_p->get_type() == InodeType::Directory)
    {
      return res_block_map;
    }
    else
    {
      uint64_t *block_num = reinterpret_cast<uint64_t *>(inode_p->blocks);
      regular_file_info *file_info = reinterpret_cast<regular_file_info *>(inode_p->blocks + 8);
      // std::cout << "block num: " << *block_num << std::endl;

      for (uint64_t i = 0; i < *block_num; i++)
      {
        res_block_map.push_back(std::make_tuple(file_info[i].block_id, file_info[i].mac_id, file_info[i].version_id));
      }
    }

    return res_block_map;
  }

  // {Your code here}
  auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    std::unique_lock<std::mutex> lck(mtx);
    std::cout << "MetadataServer::allocate_block: " << id << std::endl;
    std::vector<u8> buffer(operation_->block_manager_->block_size());
    auto res = operation_->inode_manager_->read_inode(id, buffer);
    if (res.is_err())
    {
      std::cerr << "Cannot read inode." << std::endl;
      return BlockInfo{0, 0, 0};
    }
    auto inode_p = reinterpret_cast<Inode *>(buffer.data());
    mac_id_t mac_id = generator.rand(1, num_data_servers);
    auto alloc_res = clients_[mac_id]->call("alloc_block");
    if (alloc_res.is_err())
    {
      std::cerr << "Cannot allocate block." << std::endl;
      return BlockInfo{0, 0, 0};
    }
    auto [new_block_id, new_version] =
        alloc_res.unwrap()->as<std::pair<block_id_t, version_t> >();

    uint64_t *block_num = reinterpret_cast<uint64_t *>(inode_p->blocks);
    regular_file_info *file_info = reinterpret_cast<regular_file_info *>(inode_p->blocks + 8);
    file_info[*block_num] = regular_file_info(new_block_id, mac_id, new_version);
    *block_num += 1;
    // std::cout << "*block_num: " << *block_num << std::endl;

    // std::cout << "new version: : " << new_version << std::endl;
    auto inode_block_id_res = operation_->inode_manager_->get(id);
    if (inode_block_id_res.is_err())
    {
      std::cerr << "Cannot get inode block id." << std::endl;
      return BlockInfo{0, 0, 0};
    }
    block_id_t inode_block_id = inode_block_id_res.unwrap();
    auto res3 = operation_->block_manager_->write_block(inode_block_id, buffer.data());
    if (res3.is_err())
    {
      std::cerr << "Cannot write inode." << std::endl;
      return BlockInfo{0, 0, 0};
    }

    BlockInfo block_info = std::make_tuple(new_block_id, mac_id, new_version);
    return block_info;
  }

  // {Your code here}
  auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                  mac_id_t machine_id) -> bool
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    std::unique_lock<std::mutex> lck(mtx);
    std::cout << "MetadataServer::free_block: " << id << " " << block_id << " " << machine_id << std::endl;
    std::vector<u8> buffer(operation_->block_manager_->block_size());
    auto res = operation_->inode_manager_->read_inode(id, buffer);
    if (res.is_err())
    {
      return false;
    }
    Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
    if (inode_p->get_type() == InodeType::Directory)
    {
      return false;
    }
    else
    {
      uint64_t *block_num = reinterpret_cast<uint64_t *>(inode_p->blocks);
      regular_file_info *file_info = reinterpret_cast<regular_file_info *>(inode_p->blocks + 8);
      uint64_t nums = *block_num;
      for (uint64_t i = 0; i < nums; i++)
      {
        if (file_info[i].block_id == block_id && file_info[i].mac_id == machine_id)
        {
          for (uint64_t j = i; j < nums - 1; j++)
          {
            file_info[j] = file_info[j + 1];
          }
          *block_num = nums - 1;
          // std::cout << "free block and block_num is " << *block_num << std::endl;
          auto inode_block_id_res = operation_->inode_manager_->get(id);
          if (inode_block_id_res.is_err())
          {
            std::cerr << "Cannot get inode block id." << std::endl;
            return false;
          }
          block_id_t inode_block_id = inode_block_id_res.unwrap();
          auto res2 = operation_->block_manager_->write_block(inode_block_id, buffer.data());
          if (res2.is_err())
          {
            return false;
          }
          auto res3 = clients_[machine_id]->call("free_block", block_id);
          if (res3.is_err())
          {
            return false;
          }
          return true;
        }
      }
    }

    return true;
  }

  // {Your code here}
  auto MetadataServer::readdir(inode_id_t node)
      -> std::vector<std::pair<std::string, inode_id_t> >
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    std::cout << "MetadataServer::readdir: " << node << std::endl;
    std::vector<std::pair<std::string, inode_id_t> > res_vec;
    std::list<DirectoryEntry> list;
    auto res = this->operation_->read_file(node);

    if (res.is_err())
    {
      return {};
    }
    std::vector<u8> src = res.unwrap();
    std::string src_str(src.begin(), src.end());
    chfs::parse_directory(src_str, list);
    for (auto it = list.begin(); it != list.end(); ++it)
    {
      res_vec.push_back(std::make_pair(it->name, it->id));
    }

    return res_vec;
  }

  // {Your code here}
  auto MetadataServer::get_type_attr(inode_id_t id)
      -> std::tuple<u64, u64, u64, u64, u8>
  {
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    //  @param id: The inode id of the file
    //  @return: a tuple of <size, atime, mtime, ctime, type>
    std::cout << "MetadataServer::get_type_attr: " << id << std::endl;
    std::vector<u8> buffer(operation_->block_manager_->block_size());
    auto res = operation_->inode_manager_->read_inode(id, buffer);
    if (res.is_err())
    {
      return std::make_tuple(0, 0, 0, 0, 0);
    }
    Inode *inode_p = reinterpret_cast<Inode *>(buffer.data());
    FileAttr attr = inode_p->get_attr();
    u64 size = attr.size;
    u64 atime = attr.atime;
    u64 mtime = attr.mtime;
    u64 ctime = attr.ctime;
    u8 type = 0;
    switch (inode_p->get_type())
    {
    case InodeType::FILE:
      type = 1;
      break;
    case InodeType::Directory:
      type = 2;
      break;
    case InodeType::Unknown:
      type = 0;
      break;
    }
    return std::make_tuple(size, atime, mtime, ctime, type);
  }

  auto MetadataServer::reg_server(const std::string &address, u16 port,
                                  bool reliable)
      -> bool
  {
    std::cout << "MetadataServer::reg_server: " << address << " " << port << std::endl;
    num_data_servers += 1;
    auto cli = std::make_shared<RpcClient>(address, port, reliable);
    clients_.insert(std::make_pair(num_data_servers, cli));

    return true;
  }

  auto MetadataServer::run() -> bool
  {
    if (running)
      return false;

    // Currently we only support async start
    server_->run(true, num_worker_threads);
    running = true;
    return true;
  }

} // namespace chfs