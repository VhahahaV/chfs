#include <algorithm>
#include <sstream>
#include <mutex>
#include "filesystem/directory_op.h"

namespace chfs
{
  std::mutex mtx_dir;

  /**
   * Some helper functions
   */
  auto string_to_inode_id(std::string &data) -> inode_id_t
  {
    std::stringstream ss(data);
    inode_id_t inode;
    ss >> inode;
    return inode;
  }

  auto inode_id_to_string(inode_id_t id) -> std::string
  {
    std::stringstream ss;
    ss << id;
    return ss.str();
  }

  // {Your code here}
  auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
      -> std::string
  {
    std::ostringstream oss;
    usize cnt = 0;
    for (const auto &entry : entries)
    {
      oss << entry.name << ':' << entry.id;
      if (cnt < entries.size() - 1)
      {
        oss << '/';
      }
      cnt += 1;
    }
    return oss.str();
  }

  // {Your code here}
  auto append_to_directory(std::string src, std::string filename, inode_id_t id)
      -> std::string
  {

    // TODO: Implement this function.
    //       Append the new directory entry to `src`.
    // UNIMPLEMENTED();
    //       You can use `inode_id_to_string` to convert the inode_id_t to string
    src += '/' + filename + ':' + inode_id_to_string(id);

    return src;
  }

  // {Your code here}
  void parse_directory(std::string &src, std::list<DirectoryEntry> &list)
  {

    // TODO: Implement this function.
    // UNIMPLEMENTED();
    //       Parse the directory entry from `src`.
    //       You can use `string_to_inode_id` to convert the string to inode_id_t
    if (src.length() == 0)
    {
      std::cout << "src is empty" << std::endl;
      return;
    }

    src += '/';
    // std::cout << "src:  " << src << std::endl;
    auto last_it = 0;
    for (int i = 0; i < src.length(); ++i)
    {
      if (src[i] == '/' && i != 0)
      {
        // std::cout << "i:  " << i << std::endl;
        int i2 = i;
        while (src[i2] != ':')
        {
          i2--;
          // std::cout << "i2:  " << i2 << std::endl;
        }
        auto name = src.substr(last_it, i2 - last_it);
        if (name[0] == '/')
        {
          name = name.substr(1, name.length() - 1);
        }

        // std::cout << "name: " << name << std::endl;
        std::string data = src.substr(i2 + 1, i - i2 - 1);
        // std::cout << "data: " << data << std::endl;
        inode_id_t id = string_to_inode_id(data);
        DirectoryEntry entry;
        entry.name = name;
        entry.id = id;
        list.push_back(entry);
        if (i + 1 < src.length())
          last_it = i + 1;
      }
    }
    src = src.substr(0, src.length() - 1);
  }

  // {Your code here}
  auto rm_from_directory(std::string src, std::string filename) -> std::string
  {

    auto res = std::string("");

    // TODO: Implement this function.
    //       Remove the directory entry from `src`.
    // UNIMPLEMENTED();
    //       You can use `string_to_inode_id` to convert the string to inode_id_t
    std::list<DirectoryEntry> list;
    parse_directory(src, list);
    // std::cout << "remove scr: " << src << std::endl
    //           << "  filename: " << filename << std::endl;

    for (auto it = list.begin(); it != list.end();)
    {
      // std::cout << "it->name: " << it->name << std::endl
      //           << "  filename: " << filename << std::endl;
      if (it->name == filename)
      {
        // std::cout << "find filename " << std::endl;
        it = list.erase(it);
        break;
      }
      else
      {
        ++it;
      }
    }
    res = dir_list_to_string(list);

    return res;
  }

  /**
   * { Your implementation here }
   */
  auto read_directory(FileOperation *fs, inode_id_t id,
                      std::list<DirectoryEntry> &list) -> ChfsNullResult
  {

    // TODO: Implement this function.
    // UNIMPLEMENTED();
    //       Read the directory entry from the inode `id`.
    //       You can use `string_to_inode_id` to convert the string to inode_id_t
    auto res = fs->read_file(id);

    if (res.is_err())
    {
      return ChfsNullResult(res.unwrap_error());
    }
    std::vector<u8> src = res.unwrap();
    std::string src_str(src.begin(), src.end());

    parse_directory(src_str, list);
    return KNullOk;
  }

  // {Your code here}
  auto FileOperation::lookup(inode_id_t id, const char *name)
      -> ChfsResult<inode_id_t>
  {
    std::list<DirectoryEntry> list;

    // TODO: Implement this function.
    // UNIMPLEMENTED();
    // Given the filename and the inode id of its parent directory, return its inode id.
    // If the file doesn't exist, return ErrorType::NotExist.
    auto res = read_directory(this, id, list);
    if (res.is_err())
    {
      return ChfsResult<inode_id_t>(res.unwrap_error());
    }
    for (auto it = list.begin(); it != list.end(); ++it)
    {
      if (it->name == name)
      {
        return ChfsResult<inode_id_t>(it->id);
      }
    }

    return ChfsResult<inode_id_t>(ErrorType::NotExist);
  }

  // {Your code here}
  auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
      -> ChfsResult<inode_id_t>
  {

    // TODO:
    // 1. Check if `name` already exists in the parent.
    //    If already exist, return ErrorType::AlreadyExist.

    // add lock
    std::lock_guard<std::mutex> lock(mtx_dir);

    auto res = lookup(id, name);
    if (res.is_ok())
    {
      std::cout << name << "  AlreadyExist!" << std::endl;
      return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
    }

    // 2. Create the new inode.
    //    If the inode number is not enough, return ErrorType::NoInode.
    std::cout << "auto res1 = alloc_inode(type);";
    auto res1 = alloc_inode(type);
    if (res1.is_err())
    {
      return ChfsResult<inode_id_t>(res1.unwrap_error());
    }
    auto inode_id = res1.unwrap();

    // 3. Append the new entry to the parent directory.
    //    If the parent directory is full, return ErrorType::NoSpace.
    auto res2 = read_file(id);
    if (res2.is_err())
    {
      return ChfsResult<inode_id_t>(res2.unwrap_error());
    }
    std::vector<u8> src0 = res2.unwrap();
    auto src = std::string(src0.begin(), src0.end());
    // std::cout << "src before: " << src << std::endl;
    src = append_to_directory(src, name, inode_id);
    // std::cout << "src after: " << src << std::endl;
    std::vector<u8> src_vec(src.begin(), src.end());
    // std::cout << "src_vec: " << src_vec.size() << std::endl;
    auto res3 = write_file(id, src_vec);
    if (res3.is_err())
    {
      return ChfsResult<inode_id_t>(res3.unwrap_error());
    }

    // UNIMPLEMENTED();

    return ChfsResult<inode_id_t>(inode_id);
  }

  // {Your code here}
  auto FileOperation::unlink(inode_id_t parent, const char *name)
      -> ChfsNullResult
  {
    std::lock_guard<std::mutex> lock(mtx_dir);
    // TODO:
    // 1. Remove the file, you can use the function `remove_file`
    auto res = lookup(parent, name);
    if (res.is_err())
    {
      return ChfsNullResult(res.unwrap_error());
    }
    auto inode_id = res.unwrap();
    auto res1 = remove_file(inode_id);
    if (res1.is_err())
    {
      std::cout << "remove_file error" << std::endl;
      return ChfsNullResult(res1.unwrap_error());
    }

    // 2. Remove the entry from the directory.
    //    If the file doesn't exist, return ErrorType::NotExist.
    auto res2 = read_file(parent);
    if (res2.is_err())
    {
      std::cout << "read_file error" << std::endl;
      return ChfsNullResult(res2.unwrap_error());
    }
    std::vector<u8> src0 = res2.unwrap();
    auto src = std::string(src0.begin(), src0.end());
    src = rm_from_directory(src, name);
    std::vector<u8> src_vec(src.begin(), src.end());
    auto res3 = write_file(parent, src_vec);
    if (res3.is_err())
    {
      return ChfsNullResult(res3.unwrap_error());
    }

    // UNIMPLEMENTED();

    return KNullOk;
  }

} // namespace chfs
