#include "keyvalue.h"
#include "database.h"
#include "view.h"

#include "leveldb_adapters.h"

#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <mutex>
namespace docdb {

Database::Database(leveldb::DB *dbinst)
        :_dbinst(dbinst) {
    scan_tables();
}

std::optional<KeyspaceID> Database::find_table(std::string_view v) const {
    std::shared_lock lk(_mx);
    auto iter = _table_map.find(v);
    if (iter == _table_map.end()) return {};
    else return iter->second;
}

KeyspaceID Database::open_table(std::string_view v)  {
    std::unique_lock lk(_mx);
    auto iter = _table_map.find(v);
    if (iter == _table_map.end()) {
        KeyspaceID id;
        if (_free_ids.empty()) {
           if (_min_free_id == system_table) throw std::runtime_error("No free keyspace available");
           id = _min_free_id++;
        } else {
            id = _free_ids.top();;
            _free_ids.pop();
        }
        Key k(system_table, id);
        leveldb::WriteOptions wr;
        wr.sync = true;
        if (!_dbinst->Put(wr, k, to_slice(v)).ok()) throw std::runtime_error("Failed to write table record");
        _table_map.emplace(std::string(v), id);
        return id;
    } else {
        return iter->second;
    }
}

void Database::clear_table(KeyspaceID id) {
    std::unique_ptr<leveldb::Iterator> iter2(_dbinst->NewIterator({}));
    iter2->Seek(Key(id));
    Key endKey(id+1);
    while (iter2->Valid()) {
        auto k = iter2->key();
        if (to_string(k) < endKey) {
            _dbinst->Delete({}, k);
        }
        iter2->Next();
    }

}

void Database::delete_table(std::string_view v) {
    std::unique_lock lk(_mx);
    auto iter = _table_map.find(v);
    if (iter == _table_map.end()) return ;
    KeyspaceID id = iter->second;
    _free_ids.push(id);
    Key k(system_table, id);
    leveldb::WriteOptions wr;
    wr.sync = true;
    _dbinst->Delete(wr, k);
    _table_map.erase(iter);
    lk.unlock();
    clear_table(id);

}

std::optional<std::string> Database::name_from_id(KeyspaceID id) const {
    Key k(system_table, id);
    std::string out;
    if (_dbinst->Get({}, k, &out).ok()) {
        return out;
    } else {
        return {};
    }
}

std::map<std::string, KeyspaceID, std::less<> > Database::list() const {
    std::shared_lock lk(_mx);
    return _table_map;
}

void Database::scan_tables() {

    std::unique_lock lk(_mx);
    _table_map.clear();
    std::string name;
    std::unique_ptr<leveldb::Iterator> iter(_dbinst->NewIterator({}));
    iter->Seek(Key(system_table));
    _min_free_id = 0;
    _free_ids = {};

    while (iter->Valid()) {
        auto k = iter->key();
        if (k.size() >= sizeof(KeyspaceID)*2) {
            KeyspaceID id = 0;
            KeyspaceID tmp = 0;
            Value::parse(to_string(k), tmp, id);
            auto vv = iter->value();
            name.clear();
            name.append(vv.data(), vv.size());

            while (_min_free_id < id) {
                _free_ids.push(_min_free_id);
                ++_min_free_id;
            }
            _table_map.emplace(std::move(name), std::move(id));
            _min_free_id = id+1;
        }
        iter->Next();
    }
}

PSnapshot Database::make_snapshot() {
    return PSnapshot(_dbinst->GetSnapshot(), [me = shared_from_this()](const leveldb::Snapshot *snap){
        me->_dbinst->ReleaseSnapshot(snap);
    });
}

std::unique_ptr<leveldb::Iterator> Database::make_iterator(bool cache, const PSnapshot &snap) {
    leveldb::ReadOptions opts;
    opts.snapshot = snap.get();
    opts.fill_cache = cache;
    return std::unique_ptr<leveldb::Iterator>(_dbinst->NewIterator(opts));
}

std::unique_ptr<leveldb::Iterator> Database::make_iterator(bool cache) {
    leveldb::ReadOptions opts;
    opts.fill_cache = cache;
    return std::unique_ptr<leveldb::Iterator>(_dbinst->NewIterator(opts));
}

PDatabase Database::create(leveldb::DB *db) {
    return std::make_shared<Database>(db);
}

bool Database::get(std::string_view key, std::string &result) {
    leveldb::Status st = _dbinst->Get({}, to_slice(key), &result);
    if (st.ok()) return true;
    if (st.IsNotFound()) return false;
    throw DatabaseError(std::move(st));
}

bool Database::get(std::string_view key, std::string &result,
        const PSnapshot &snap) {
    leveldb::ReadOptions opts;
    opts.snapshot = snap.get();
    leveldb::Status st = _dbinst->Get({}, to_slice(key), &result);
    if (st.ok()) return true;
    if (st.IsNotFound()) return false;
    throw DatabaseError(std::move(st));

}



DatabaseError::DatabaseError(leveldb::Status st)
:_st(std::move(st)),_msg(_st.ToString()) {}

std::uint64_t Database::get_index_size(std::string_view key1,
        std::string_view key2) {
    leveldb::Range ranges[1];
    ranges[0] = leveldb::Range(to_slice(key1),to_slice(key2));
    std::uint64_t r[1];
    _dbinst->GetApproximateSizes(ranges, 1, r);
    return r[0];
}

void Database::commit_batch(Batch &batch) {
    auto st =_dbinst->Write(_write_opts, &batch);
    if (!st.ok()) {
        throw DatabaseError(st);
    }
    batch.Clear();
}

}
