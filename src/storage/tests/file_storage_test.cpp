

#include <gtest/gtest.h>
#include "storage/file_storage.h"
#include <algorithm>

SMILE_NS_BEGIN




/** Tests the opening and closing of the file storage and checks
 * that the config information has been persisted properly
 */
TEST(FileStorageTest, FileStorageOpen) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", ISequentialStorage::SequentialStorageConfig{4}, true) == storageError_t::E_NO_ERROR);
  ASSERT_TRUE(fileStorage.close() == storageError_t::E_NO_ERROR);
  ASSERT_TRUE(fileStorage.open("./test.db") == storageError_t::E_NO_ERROR);
  ASSERT_TRUE(fileStorage.config().m_extentSizeKB == 4);
  ASSERT_TRUE(fileStorage.close() == storageError_t::E_NO_ERROR);
}

/**
 * Tests that extents are properly reserved and the returned extentIds 
 * are consistent with the amount of extents reserved
 */
TEST(FileStorageTest, FileStorageReserve) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", ISequentialStorage::SequentialStorageConfig{64}, true) == storageError_t::E_NO_ERROR);
  ASSERT_TRUE(fileStorage.close() == storageError_t::E_NO_ERROR);

  ASSERT_TRUE(fileStorage.open("./test.db") == storageError_t::E_NO_ERROR);
  extentId_t eId;
  ASSERT_TRUE(fileStorage.reserve(1,eId) == storageError_t::E_NO_ERROR);
  ASSERT_TRUE(eId == 1);
  ASSERT_TRUE(fileStorage.reserve(1,eId) == storageError_t::E_NO_ERROR);
  ASSERT_TRUE(eId == 2);
  ASSERT_TRUE(fileStorage.reserve(4,eId) == storageError_t::E_NO_ERROR);
  ASSERT_TRUE(eId == 3);
  ASSERT_TRUE(fileStorage.reserve(1,eId) == storageError_t::E_NO_ERROR);
  ASSERT_TRUE(eId == 7);
  ASSERT_TRUE(fileStorage.size() == 8);
  ASSERT_TRUE(fileStorage.close() == storageError_t::E_NO_ERROR);
}

/**
 * Tests Read and Write operations. We first write 63 pages entirely filled with
 * one character from the contents vector (selected in a round robin fashion). 
 * Then the file storage is closed an opened again, and the contents of the
 * pages are asserted
 **/
TEST(FileStorageTest, FileStorageReadWrite) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", ISequentialStorage::SequentialStorageConfig{64}, true) == storageError_t::E_NO_ERROR);

  auto storageConfig = fileStorage.config();
  std::vector<char> data(storageConfig.m_extentSizeKB*1024);
  extentId_t eid;
  ASSERT_TRUE(fileStorage.reserve(63,eid) == storageError_t::E_NO_ERROR);
  std::vector<char> contents{'0','1','2','3','4','5','6','7','8','9'};
  for( auto i = eid; i < (eid+63); ++i ) {
    std::fill(data.begin(), data.end(), contents[i%contents.size()]);
    ASSERT_TRUE(fileStorage.write(data.data(),i) == storageError_t::E_NO_ERROR);
  }
  ASSERT_TRUE(fileStorage.close() == storageError_t::E_NO_ERROR);

  ASSERT_TRUE(fileStorage.open("./test.db") == storageError_t::E_NO_ERROR);
  for( auto i = eid; i < (eid+63); ++i ) {
    ASSERT_TRUE(fileStorage.read(data.data(),i) == storageError_t::E_NO_ERROR);
    for( auto it = data.begin(); it != data.end(); ++it ) {
      ASSERT_TRUE(*it == contents[i%contents.size()]);
    }
  }
  ASSERT_TRUE(fileStorage.close() == storageError_t::E_NO_ERROR);
}

/**
 * Tests that the file storage is properly reporting errors, specially
 * for out of bounds accesses and database overwrites.
 **/
TEST(FileStorageTest, FileStorageErrors) {
  FileStorage fileStorage;
  ASSERT_TRUE(fileStorage.create("./test.db", ISequentialStorage::SequentialStorageConfig{64}, true) == storageError_t::E_NO_ERROR);
  auto storageConfig = fileStorage.config();
  std::vector<char> data(storageConfig.m_extentSizeKB*1024);
  ASSERT_TRUE(fileStorage.write(data.data(),63) == storageError_t::E_OUT_OF_BOUNDS_EXTENT);
  ASSERT_TRUE(fileStorage.read(data.data(),32) == storageError_t::E_OUT_OF_BOUNDS_EXTENT);
  ASSERT_TRUE(fileStorage.close() == storageError_t::E_NO_ERROR);
  ASSERT_TRUE(fileStorage.create("./test.db", ISequentialStorage::SequentialStorageConfig{64}) == storageError_t::E_PATH_ALREADY_EXISTS);
}


SMILE_NS_END

int main(int argc, char* argv[]){
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}

