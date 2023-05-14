#pragma once
#ifndef SRC_DOCDB_LEVELDB_ADAPTERS_H_
#define SRC_DOCDB_LEVELDB_ADAPTERS_H_

#include <leveldb/slice.h>


namespace docdb {


inline leveldb::Slice to_slice(const std::string_view &v) {
    return {v.data(), v.size()};
}

inline std::string_view to_string(const leveldb::Slice &slice) {
    return {slice.data(), slice.size()};
}


}




#endif /* SRC_DOCDB_LEVELDB_ADAPTERS_H_ */
