/*
 * table_handle.h
 *
 *  Created on: 6. 11. 2022
 *      Author: ondra
 */

#ifndef SRC_DOCDB_TABLE_HANDLE_H_
#define SRC_DOCDB_TABLE_HANDLE_H_
#include "key.h"

#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <memory>

namespace docdb {

class Batch {
public:
    Batch (leveldb::WriteBatch &ref):_ref(ref) {};
    Batch (const Batch &) = default;
    Batch &operator=(const Batch &) = delete;
    
    void put(const KeyView &k, const std::string_view &v) {
        _ref.Put(k, leveldb::Slice(v.data(), v.size()));        
    }
    void erase(const KeyView &k) {
        _ref.Delete(k);        
    }
    
    
    
protected:
    leveldb::WriteBatch &_ref;
};

///Function which receives update when record is updated
/**
 * @param b batch. Any subsequent writes triggered with this update should be placed into this batch. Note
 * that caller can optionally discard the batch, so no writes are happen. All observes must expect this can happen.
 * Source of all data are data written to the database (including caching)
 * 
 * @param k modified key
 * @param v pointer to written data (content). If this pointer is nullptr, it is interpreted as erasing the key
 * @param s source of change. This is pointer to table-depend source, which must be understandable by all
 * sides. Pointer can be nullptr. If the observer doesn't understand to the source, it can ignore the parameter    
 * 
 * @retval true continue observing
 * @retval false stop observing 
 *   
 */
using UpdateObserver = std::function<bool(Batch b, const KeyView &k, const std::string_view *v, const void *s)>;

class IReadAccess;



///Object which is connected with associated source in main database
class TableHandle{
public:
    
    
    
    virtual ~TableHandle() = default;
    
    
    ///Retrieve this table's keyspace id
    virtual KeyspaceID keyspace() const = 0;
    ///Retrieve this table's name
    virtual std::string_view name() const = 0;
    ///Retrieve stored metadate
    virtual std::string metadata() const = 0;
    ///Commits batch to the database
    /**
     * Writes the metadata.
     * 
     * @param batch batch
     * @param metadata metadata to write
     * 
     * @note if the batch is discarded, metadata are not written. 
     */
    virtual void metadata(Batch batch, const std::string_view &metadata) = 0;
    ///Commits batch to the database,
    /**
     * @param batch batch to commit
     */
    virtual void commit(leveldb::WriteBatch &batch) = 0;
    
    ///table can call this function to write data into batch and notify about change of key/value
    /**
     * @param batch write batch, any associated updates should be put into same batch
     * @param key key which has been changed
     * @param data new data. It is pointer, where setting this pointer to nullptr means, that key has been deleted
     * @param source_data pointer to source of update, can contains source data to avoid unnecessery parsing. This
     * needs to notified object understand this data, otherwise the pointer cannot be used. For example if
     * the source table is JSON storage, then pointer can point to the JSON document, to subscribers be
     * able to process original document, without parsing the data back. 
     */
    virtual void update(Batch batch, const KeyView &key, const std::string_view *data, const void *source_data) = 0;
    
    ///Retrieve opened database object
    virtual leveldb::DB *database() = 0;

    ///Retrieve read access reference
    /**
     * @return reference to interface to uniform access to database or snapshot
     */
    virtual IReadAccess &read_access() = 0;

    ///Erase list of keys
    /**
     * Erase large list of keys. This operation can be optimized 
     * 
     * @param from from key
     * @param to to key
     */
    virtual void erase(const KeyView &from, const KeyView &to) = 0;
    
    ///Compact key range
    /**
     * Optimizes space. Useful, when content of table is settled and no changes are expected. Note that
     * operation can take long time
     * 
     * @param from from key
     * @param to to key
     */
    virtual void compact(const KeyView &from, const KeyView &to) = 0;
    
    ///Install observer
    virtual void observe(UpdateObserver obs)  = 0;
    
    ///Enable destroy on close
    /**
     * @param x set true to destroy table on close. This stays in effect even if
     * the program crashes. Then database is deleted on startup.
     */
    virtual void destoy_on_close(bool x) = 0;
    
    ///Create new table handle as snapshot of current database state
    /**
     * @return handle to read-only copy of this table / snapshot
     * 
     * @note if this function is used on handle which is already snapshot, it just returns self
     */
    virtual std::shared_ptr<TableHandle> snapshot() = 0;
    
    
    Key key() const {return Key(keyspace());}
};


using PTableHandle = std::shared_ptr<TableHandle>;


}



#endif /* SRC_DOCDB_TABLE_HANDLE_H_ */
