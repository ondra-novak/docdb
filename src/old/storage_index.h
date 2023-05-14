/*
 * index.h
 *
 *  Created on: 15. 11. 2022
 *      Author: ondra
 */

#ifndef SRC_DOCDB_STORAGE_INDEX_H_
#define SRC_DOCDB_STORAGE_INDEX_H_
#include "table_handle.h"
#include "key.h"
#include "value.h"

#include "storage.h"
#include <functional>
#include <memory>

namespace docdb {


using EmitFn = std::function<void(Key &&key, const ValueView &value)>;
using IndexFn = std::function<void(EmitFn &&emit, const Value &document)>;


class StorageIndex {
public:    
    
    
    StorageIndex(PTableHandle th);

    template<typename IDType>
    void bind(const Storage<IDType> &storage, IndexFn &&indexFn);
    
    struct Indexer {
        IndexFn fn;
        PTableHandle tbl;
    };
    
    Key key() const {
        return Key(_ks);
    }
    
    void init_key(Key &k) {
        k.keyspace(_ks);
    }
    
    void observe(UpdateObserver obs) {
        _th->observe(std::move(obs));
    }

    class Iterator: public IteratorBase<IteratorRaw> {
    public:
        Iterator(PTableHandle th, std::unique_ptr<leveldb::Iterator>  &&iter, const KeyView &begin, const KeyView &end, bool include_begin, bool include_end);
        ~Iterator();
        
        ValueView doc() const {
            
            _th->read_access().find(key, value)
        }
        
    protected:
        Batch _deleted;
        mutable std::string _tmp;
        PTableHandle _th;
        bool filter() {
            
        }
    };
    
    
protected:
    PTableHandle _th;
    KeyspaceID _ks;
    std::size_t _fkeysz;
    std::shared_ptr<Indexer> _indexer;
    
};

inline StorageIndex::StorageIndex(PTableHandle th) 
    :_th(th),_ks(_th->keyspace()) {
    
}

template<typename IDType>
inline void StorageIndex::bind(const Storage<IDType> &storage, IndexFn &&indexFn) {
    std::shared_ptr<Indexer> idx_obj = std::make_shared<Indexer>(Indexer{std::move(indexFn), _th});
    storage.observe([wkobj = std::weak_ptr<Indexer>(idx_obj), ks = _ks]
      (Batch b, const KeyView &k, const std::string_view *v, const void *s) {
           auto idxobj = wkobj.lock();
           if (!idxobj) return false;
           idxobj->fn([&](Key &&key, const ValueView &value){
               key.keyspace(ks);
               key.append_untagged(k.get_raw());
               b.put(key, value);
               key.clear();
           });
           return true;
        });
}


} /* namespace docdb */

#endif /* SRC_DOCDB_STORAGE_INDEX_H_ */
