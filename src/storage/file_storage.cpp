

#include "file_storage.h"

SMILE_NS_BEGIN


FileStorage::FileStorage() noexcept : 
  m_flags( std::ios_base::in | std::ios_base::out | std::ios_base::binary  )
{
}

FileStorage::~FileStorage() noexcept {
  if(m_file) m_file.close();
}

ErrorCode FileStorage::open( const std::string& path ) noexcept {
  m_file.open( path, m_flags );
  if(!m_file){
    return ErrorCode::E_STORAGE_INVALID_PATH;
  }
  m_file.seekg(0,std::ios_base::beg);
  m_file.read(reinterpret_cast<char*>(&m_config), sizeof(m_config));
  m_extentFiller.resize(getExtentSize(),'\0');
  m_file.seekg(0,std::ios_base::end);
  m_size = bytesToExtent(m_file.tellg());
  return ErrorCode::E_NO_ERROR;
}

ErrorCode FileStorage::create( const std::string& path, const FileStorageConfig& config, const bool overwrite ) noexcept {
  if(!overwrite && std::ifstream(path)) {
    return ErrorCode::E_STORAGE_PATH_ALREADY_EXISTS;
  }

  m_file.open( path, m_flags | std::ios_base::trunc );
  if(!m_file) {
    return ErrorCode::E_STORAGE_INVALID_PATH;
  }
  m_config = config;
  m_extentFiller.resize(getExtentSize(),'\0');
  extentId_t eid;
  reserve(1,eid);
  m_file.seekp(0,std::ios_base::beg);
  m_file.write(reinterpret_cast<char*>(&m_config), sizeof(m_config));
  m_file.flush();
  if(!m_file) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_WRITE;
  }
  return ErrorCode::E_NO_ERROR;
}

ErrorCode FileStorage::close() noexcept {
  if(m_file) {
    m_file.close();
    return ErrorCode::E_NO_ERROR;
  }
  return ErrorCode::E_STORAGE_NOT_OPEN;
}

ErrorCode FileStorage::reserve( const uint32_t numExtents, extentId_t& extent ) noexcept {
  m_file.seekp(0,std::ios_base::end);
  if(!m_file) {
    return ErrorCode::E_STORAGE_CRITICAL_ERROR;
  }
  extent = bytesToExtent(m_file.tellp());
  m_file.seekp(extentToBytes((numExtents-1)),std::ios_base::end);
  m_file.write(m_extentFiller.data(), m_extentFiller.size());
  if(!m_file) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_WRITE;
  }
  m_size = bytesToExtent(m_file.tellp());
  return ErrorCode::E_NO_ERROR;
}

ErrorCode FileStorage::read( char* data, const extentId_t extent ) noexcept {
  if(extent == 0 || extent >= m_size) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_EXTENT;
  }
  m_file.seekg(extentToBytes(extent), std::ios_base::beg);
  if(!m_file) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_EXTENT;
  }
  m_file.read(data,getExtentSize());
  if(!m_file) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_READ;
  }
  return ErrorCode::E_NO_ERROR;
}

ErrorCode FileStorage::write( const char* data, const extentId_t extent ) noexcept {
  if(extent == 0 || extent >= m_size) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_EXTENT;
  }
  m_file.seekp(extentToBytes(extent), std::ios_base::beg);
  if(!m_file) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_EXTENT;
  }
  m_file.write(data,getExtentSize());
  if(!m_file) {
    return ErrorCode::E_STORAGE_OUT_OF_BOUNDS_WRITE;
  }
  return ErrorCode::E_NO_ERROR;
}

uint64_t FileStorage::size() const noexcept {
  return m_size;
}

const FileStorageConfig& FileStorage::config() const noexcept {
  return m_config;
}


uint32_t FileStorage::getExtentSize() const noexcept {
  return m_config.m_extentSizeKB*1024;
}

extentId_t FileStorage::bytesToExtent( const uint64_t bytes ) const noexcept {
  return bytes / getExtentSize();
}

uint64_t FileStorage::extentToBytes( const extentId_t extent ) const noexcept {
  return extent * getExtentSize();
}

SMILE_NS_END
