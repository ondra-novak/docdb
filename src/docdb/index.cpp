#include "index.h"

#include "storage.h"

#include "database.h"
namespace docdb {

class Index::IndexBuilder: public Indexer {
public:
    IndexBuilder(KeyspaceID kid, Batch &b, PDatabase &db)
        :Indexer(kid),_b(b),_db(db) {}

    virtual void put(const Key &key, std::string_view value) {
        if (key.get_kid(key) == _kid) {
            _b.Put(key, to_slice(value));
        }
    }
    virtual void erase(const Key &key) {
        if (key.get_kid(key) == _kid) {
            _b.Delete(key);
        }
    }
    virtual bool get(const Key &key, std::string &value) {
        return _db->get(key, value);
    }


protected:
    Batch &_b;
    PDatabase &_db;


};

Storage::DocID Index::update_index(const Storage &storage, std::uint64_t rev, IndexFn indexFn) {
    Storage::DocID startId = 0;
    Value v;
    if (lookup(key(), v))  {
        std::string_view ss(v);
        std::uint64_t cur_rev;
        KeyspaceID srcid;
        int c = Value::deserialize(ss,srcid,startId,cur_rev);
        if (c < 3 || srcid != storage.get_kid() || rev != cur_rev) {
            startId = 0;
        }
    }
    if (startId == 0) {
        _db->clear_table(_kid);
    }

    Batch b;
    IndexBuilder bld(_kid, b, _db);


    auto iter = storage.scan(storage.key(startId));
    while (iter.next()) {
        startId = Storage::key2docid(iter.key());
        indexFn(startId, iter.value(), bld);
        _db->commit_batch(b);
    }

    startId += 1;
    v.clear();
    v.add(storage.get_kid(), startId, rev);
    b.Put(key(), v);
    _db->commit_batch(b);

    return startId;
}


}
