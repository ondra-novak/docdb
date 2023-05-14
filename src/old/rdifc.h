/*
 * rdifc.h
 *
 *  Created on: 8. 11. 2022
 *      Author: ondra
 */

#ifndef SRC_DOCDB_RDIFC_H_
#define SRC_DOCDB_RDIFC_H_
#include "key.h"

#include <leveldb/iterator.h>
#include <memory>



namespace docdb {

using PIterator =  std::unique_ptr<leveldb::Iterator>;

///Interface to access database database for reading
/**
 * Interface enables to access snapshots etc
 */
class IReadAccess {
public:
    
    virtual ~IReadAccess() = default;
    
    ///Find key
    virtual bool find(const KeyView &key, std::string &value) const = 0;
    ///Create iterator
    virtual PIterator iterate() const = 0;
    
    virtual void getApproximateSizes(const leveldb::Range* range, int n, uint64_t* sizes) const = 0;
};


}



#endif /* SRC_DOCDB_RDIFC_H_ */
