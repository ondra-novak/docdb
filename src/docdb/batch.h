#pragma once
#ifndef SRC_DOCDB_BATCH_H_
#define SRC_DOCDB_BATCH_H_

#include <leveldb/write_batch.h>
#include "buffer.h"

namespace docdb {

class Batch: public leveldb::WriteBatch {
public:
    using BufferType = Buffer<char, 32>;
    using leveldb::WriteBatch::WriteBatch;

    BufferType &get_buffer() {
        _buffer.clear();
        return _buffer;
    }
protected:
    BufferType _buffer;
};


}




#endif /* SRC_DOCDB_BATCH_H_ */
