/*
 * class table.h
 *
 *  Created on: 8. 11. 2022
 *      Author: ondra
 */

#ifndef SRC_DOCDB_TABLE_H_
#define SRC_DOCDB_TABLE_H_

#include "table_handle.h"
#include "rdifc.h"
#include <leveldb/write_batch.h>
#include <mutex>

namespace docdb {

//Base class for all tables

class Table {
public:
    Table (PTableHandle th):_th(th), _ks(th->keyspace()) {}
    
    
    ///Opens write transaction
    class WriteTx: public Batch {
    public:
        WriteTx(Table &tb):Batch(tb._batch), _tb(tb) {
            _tb._mx.lock();
        }
        ~WriteTx() {
            _tb._th->commit(_tb._batch);
            _tb._batch.Clear();
            _tb._mx.unlock();
        }
        WriteTx (const WriteTx &) = delete;
        WriteTx &operator=(const WriteTx &) = delete;
                        
    protected:
        Table &_tb;

    };
    

    Table(Table &&other)
        :_th(other._th)
        ,_ks(other._ks) {}
    Table(const Table &other) = delete;
    Table &operator=(const Table &other) = delete;
    
  
protected:
    
    leveldb::WriteBatch _batch;
    PTableHandle _th;
    KeyspaceID _ks;
    std::recursive_mutex _mx;
    
    
};

}



#endif /* SRC_DOCDB_TABLE_H_ */
