#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs
{
  struct regular_file_size
  {
    u64 size;
    inode_id_t inode_id;
    regular_file_size(u64 size, inode_id_t inode_id) : size(size), inode_id(inode_id) {}
  };

  std::vector<regular_file_size> regular_file_sizes;

  ChfsClient::ChfsClient() : num_data_servers(0) {}

  auto ChfsClient::reg_server(ServerType type, const std::string &address,
                              u16 port, bool reliable) -> ChfsNullResult
  {
    switch (type)
    {
    case ServerType::DATA_SERVER:
      num_data_servers += 1;
      data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                  address, port, reliable)});
      break;
    case ServerType::METADATA_SERVER:
      metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
      break;
    default:
      std::cerr << "Unknown Type" << std::endl;
      exit(1);
    }

    return KNullOk;
  }

  // {Your code here}
  auto ChfsClient::mknode(FileType type, inode_id_t parent,
                          const std::string &name) -> ChfsResult<inode_id_t>
  {
    std::cout << "client mknode: " << int(type) << " " << parent << " " << name << std::endl;
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    u8 type_u8 = static_cast<u8>(type);
    auto res = this->metadata_server_->call("mknode", type_u8, parent, name);
    if (res.is_err() || res.unwrap()->as<inode_id_t>() == 0)
    {
      return ChfsResult<inode_id_t>(ErrorType::INVALID);
    }
    else
    {
      inode_id_t inode_id = res.unwrap()->as<inode_id_t>();
      return ChfsResult<inode_id_t>(inode_id);
    }
    // return ChfsResult<inode_id_t>(0);
  }

  // {Your code here}
  auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
      -> ChfsNullResult
  {
    std::cout << "client unlink: " << parent << " " << name << std::endl;
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res = this->metadata_server_->call("unlink", parent, name);
    if (res.is_err() || res.unwrap()->as<bool>() == false)
    {
      return ChfsNullResult(ErrorType::INVALID);
    }
    else
    {
      return KNullOk;
    }
    // return KNullOk;
  }

  // {Your code here}
  auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
      -> ChfsResult<inode_id_t>
  {
    std::cout << "client lookup: " << parent << " " << name << std::endl;
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res = this->metadata_server_->call("lookup", parent, name);
    if (res.is_err() || res.unwrap()->as<inode_id_t>() == 0)
    {
      return ChfsResult<inode_id_t>(ErrorType::NotExist);
    }
    else
    {
      inode_id_t inode_id = res.unwrap()->as<inode_id_t>();
      return ChfsResult<inode_id_t>(inode_id);
    }
  }

  // {Your code here}
  auto ChfsClient::readdir(inode_id_t id)
      -> ChfsResult<std::vector<std::pair<std::string, inode_id_t> > >
  {
    std::cout << "client readdir: " << id << std::endl;
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res = this->metadata_server_->call("readdir", id);
    if (res.is_err() || res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t> > >().size() == 0)
    {
      return ChfsResult<std::vector<std::pair<std::string, inode_id_t> > >(ErrorType::INVALID);
    }
    else
    {
      std::vector<std::pair<std::string, inode_id_t> > result = res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t> > >();
      return ChfsResult<std::vector<std::pair<std::string, inode_id_t> > >(result);
    }
  }

  // {Your code here}
  auto ChfsClient::get_type_attr(inode_id_t id)
      -> ChfsResult<std::pair<InodeType, FileAttr> >
  {
    std::cout << "client get_type_attr: " << id << std::endl;
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res = this->metadata_server_->call("get_type_attr", id);
    if (res.is_err())
    {
      return ChfsResult<std::pair<InodeType, FileAttr> >(ErrorType::INVALID);
    }
    else
    {
      std::tuple<u64, u64, u64, u64, u8> result = res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8> >();
      InodeType type = static_cast<InodeType>(std::get<4>(result));
      FileAttr attr;
      bool is_exist = false;
      for (auto it = regular_file_sizes.begin(); it != regular_file_sizes.end(); it++)
      {
        if (it->inode_id == id)
        {
          attr.size = it->size;
          is_exist = true;
          break;
        }
      }
      if (!is_exist)
      {
        attr.size = std::get<0>(result);
        regular_file_sizes.push_back(regular_file_size(attr.size, id));
      }
      // attr.size = file_size;
      attr.atime = std::get<1>(result);
      attr.mtime = std::get<2>(result);
      attr.ctime = std::get<3>(result);
      std::cout << "client get_type_attr: "
                << "type: " << int(type) << " size: " << attr.size << " atime: " << attr.atime << " mtime: " << attr.mtime << " ctime: " << attr.ctime << std::endl;
      return ChfsResult<std::pair<InodeType, FileAttr> >(std::pair<InodeType, FileAttr>(type, attr));
    }
    // return ChfsResult<std::pair<InodeType, FileAttr>>({});
  }

  /**
   * Read and Write operations are more complicated.
   */
  // {Your code here}
  auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
      -> ChfsResult<std::vector<u8> >
  {
    std::cout << "client read_file: " << id << " " << offset << " " << size << std::endl;
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    // first get the block mapping
    auto res_block_map = this->metadata_server_->call("get_block_map", id);
    if (res_block_map.is_err())
    {
      return ChfsResult<std::vector<u8> >(ErrorType::INVALID);
    }
    auto block_map = res_block_map.unwrap()->as<std::vector<BlockInfo> >();
    std::vector<u8> data(size);
    usize block_id = offset / DiskBlockSize;
    usize block_offset = offset % DiskBlockSize;
    usize read_len = size + block_offset > DiskBlockSize ? DiskBlockSize - block_offset : size;
    usize data_offset = 0;
    while (data_offset < size)
    {
      std::cout << "read n's block_id: " << block_id << std::endl;
      if (block_id >= block_map.size())
      {
        break;
      }
      auto block_info = block_map[block_id];
      auto res_data = this->data_servers_[std::get<1>(block_info)]->call("read_data", std::get<0>(block_info), block_offset, read_len, std::get<2>(block_info));
      if (res_data.is_err() || res_data.unwrap()->as<std::vector<u8> >().size() == 0)
      {
        return ChfsResult<std::vector<u8> >(ErrorType::INVALID);
      }
      auto data_block = res_data.unwrap()->as<std::vector<u8> >();
      for (int i = 0; i < read_len; i++)
      {
        std::cout << "read data: " << data_block[i] << std::endl;
        data[data_offset + i] = data_block[i];
      }
      data_offset += read_len;
      block_offset = 0;
      read_len = size - data_offset > DiskBlockSize ? DiskBlockSize : size - data_offset;
      block_id += 1;
    }

    // return ChfsResult<std::vector<u8> >({});
    return ChfsResult<std::vector<u8> >(data);
  }

  // {Your code here}
  auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
      -> ChfsNullResult
  {
    std::cout << "client write_file: " << id << " " << offset << " " << data.size() << std::endl;
    bool is_exist = false;
    for (auto it = regular_file_sizes.begin(); it != regular_file_sizes.end(); it++)
    {
      if (it->inode_id == id)
      {
        it->size = offset + data.size();
        is_exist = true;
        break;
      }
    }
    if (!is_exist)
    {
      regular_file_sizes.push_back(regular_file_size(offset + data.size(), id));
    }
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    // first get the block mapping
    auto res_block_map = this->metadata_server_->call("get_block_map", id);
    if (res_block_map.is_err())
    {
      return ChfsNullResult(ErrorType::INVALID);
    }
    auto block_map = res_block_map.unwrap()->as<std::vector<BlockInfo> >();
    //  If the client wants to write an empty file, it should first call the metadata server to allocate a block on a data server.
    usize block_num = (offset + data.size()) / DiskBlockSize + ((offset + data.size()) % DiskBlockSize ? 1 : 0);
    if (block_num > block_map.size())
    {
      while (block_num > block_map.size())
      {
        auto res_alloc = this->metadata_server_->call("alloc_block", id);
        if (res_alloc.is_err())
        {
          return ChfsNullResult(ErrorType::INVALID);
        }
        auto alloc_block = res_alloc.unwrap()->as<BlockInfo>();
        block_map.push_back(alloc_block);
      }
    }

    // 查看是否分配成功
    assert(this->metadata_server_->call("get_block_map", id).unwrap()->as<std::vector<BlockInfo> >().size() == block_map.size());

    //  Then, the client should write the data to the data server.
    usize block_id = offset / DiskBlockSize;
    usize block_offset = offset % DiskBlockSize;
    usize write_len = data.size() + block_offset > DiskBlockSize ? DiskBlockSize - block_offset : data.size();
    usize data_offset = 0;
    while (data_offset < data.size())
    {
      auto block_info = block_map[block_id];
      auto res_write = this->data_servers_[std::get<1>(block_info)]->call("write_data", std::get<0>(block_info), block_offset, std::vector<u8>(data.begin() + data_offset, data.begin() + data_offset + write_len));
      if (res_write.is_err() || res_write.unwrap()->as<bool>() == false)
      {
        return ChfsNullResult(ErrorType::INVALID);
      }
      data_offset += write_len;
      block_offset = 0;
      write_len = data.size() - data_offset > DiskBlockSize ? DiskBlockSize : data.size() - data_offset;
      block_id += 1;
    }

    return KNullOk;
  }

  // {Your code here}
  auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                   mac_id_t mac_id) -> ChfsNullResult
  {
    std::cout << "client free_file_block: " << id << " " << block_id << " " << mac_id << std::endl;
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res = this->metadata_server_->call("free_block", id, block_id, mac_id);
    if (res.is_err() || res.unwrap()->as<bool>() == false)
    {
      return ChfsNullResult(ErrorType::INVALID);
    }
    else
    {
      return KNullOk;
    }
    // return KNullOk;
  }

} // namespace chfs