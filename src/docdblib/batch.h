/*
 * batch.h
 *
 *  Created on: 8. 5. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDB_SRC_DOCDBLIB_BATCH_H_
#define SRC_DOCDB_SRC_DOCDBLIB_BATCH_H_
#include "keyspace.h"

namespace docdb {

class Batch : public ::leveldb::WriteBatch {
public:
	using ::leveldb::WriteBatch::WriteBatch;

	virtual ~Batch() {}
	virtual void Clear() {::leveldb::WriteBatch::Clear();}
};


}



#endif /* SRC_DOCDB_SRC_DOCDBLIB_BATCH_H_ */
