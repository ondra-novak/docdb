#pragma once
#ifndef SRC_DOCDB_DATABASE_H_
#define SRC_DOCDB_DATABASE_H_

#include "recordset.h"
#include "batch.h"
#include "key.h"
#include "purpose.h"
#include "task_thread.h"
#include "exceptions.h"

#include <leveldb/db.h>
#include <string>
#include <stack>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <variant>



namespace docdb {

class Database;

using PDatabase = std::shared_ptr<Database>;
using PSnapshot = std::shared_ptr<const leveldb::Snapshot>;





///Database base
class Database: public std::enable_shared_from_this<Database> {
public:
    ///Initialize database
    /**
     * @param dbinst leveldb instance
     */
    Database(leveldb::DB *dbinst):_dbinst(dbinst) {
        scan_tables();
    }


    ///Find table by name
    /**
     * @param v name of table
     * @return keyspace id for given table, if exist, otherwise returns no value
     */
    std::optional<KeyspaceID> find_table(std::string_view v) const {
        std::shared_lock lk(_mx);
        auto iter = _table_map.find(v);
        if (iter == _table_map.end()) return {};
        else return iter->second.first;
    }

    auto get_table_info(std::string_view v) const {
        std::shared_lock lk(_mx);
        auto iter = _table_map.find(v);
        using Ret = std::optional<decltype(iter->second)>;
        if (iter == _table_map.end()) return Ret{};
        else return Ret(iter->second);
    }

    ///Retrieves name of the table from the id
    /**
     * @param id keyspace id
     * @return name of table if exists
     */
    std::optional<std::string> name_from_id(KeyspaceID id) const {
        RawKey k(system_table,system_table, id);
        std::string out;
        if (_dbinst->Get({}, k, &out).ok()) {
            return out.substr(1);
        } else {
            return {};
        }
    }

    ///Retrieves snapshot of list of tables
    /**
     * @return a map object which contains a name as key and keyspace id as value
     */
    std::map<std::string, std::pair<KeyspaceID, Purpose>, std::less<> > list() const {
        std::shared_lock lk(_mx);
        return _table_map;
    }

    ///Create or open the table
    /**
     * @param name name of table
     * @param purpose purpose of keyspace
     * @return keyspace id for given table. The table is automatically create if not exists
     */
    KeyspaceID open_table(std::string_view name, Purpose purpose) {
        std::unique_lock lk(_mx);
        auto iter = _table_map.find(name);
        if (iter == _table_map.end()) {
            KeyspaceID id;
            if (_free_ids.empty()) {
               if (_min_free_id == system_table) throw std::runtime_error("No free keyspace available");
               id = _min_free_id++;
            } else {
                id = _free_ids.top();;
                _free_ids.pop();
            }
            RawKey k(system_table, system_table, id);
            leveldb::WriteOptions wr;
            Row row(purpose, Blob(name));
            wr.sync = true;
            if (!_dbinst->Put(wr, k, row).ok()) throw std::runtime_error("Failed to write table record");
            _table_map.emplace(std::string(name), std::pair(id, purpose));
            return id;
        } else {
            if (iter->second.second != purpose && purpose != Purpose::undefined && iter->second.second != Purpose::undefined) {
                throw std::runtime_error("Keyspace has different purpose");
            }
            return iter->second.first;
        }
    }
    ///Delete table
    /**
     * @param name of table to delete
     */
    void delete_table(std::string_view vname) {
        std::unique_lock lk(_mx);
        auto iter = _table_map.find(vname);
        if (iter == _table_map.end()) return ;
        KeyspaceID id = iter->second.first;
        _free_ids.push(id);
        RawKey k(system_table,system_table, id);
        leveldb::WriteOptions wr;
        wr.sync = true;
        _dbinst->Delete(wr, k);
        _table_map.erase(iter);
        lk.unlock();
        clear_table(id, true);
        clear_table(id, false);

    }

    ///Deletes all rows in given table
    /**
     * @param id keyspace id of table
     * @param private_area set true to clear private area, otherwise clear public area
     */
    void clear_table(KeyspaceID id, bool private_area) {
        std::unique_ptr<leveldb::Iterator> iter2(_dbinst->NewIterator({}));
        RawKey endKey(private_area?system_table:id+1);
        if (private_area) {
            iter2->Seek(RawKey(system_table, id));
            endKey.append(id+1);
        } else {
            iter2->Seek(RawKey(id));
        }
        while (iter2->Valid()) {
            auto k = iter2->key();
            if (to_string(k) >= endKey) break;
            _dbinst->Delete({}, k);
            iter2->Next();
        }

    }


    ///rescan for all tables in database and create local index
    /**
     * This is done automatically when database is constructed, however if the
     * local index is not in sync, this will recreate the index (by default,
     * index should be update in normal condition, so you don't need to call this
     * function manually)
     *
     */
    void scan_tables(){

        std::unique_lock lk(_mx);
        _table_map.clear();
        std::unique_ptr<leveldb::Iterator> iter(_dbinst->NewIterator({}));
        iter->Seek(RawKey(system_table, system_table));
        _min_free_id = 0;
        _free_ids = {};

        while (iter->Valid()) {
            auto k = iter->key();
            if (k.size() >= sizeof(KeyspaceID)*2) {
                auto [tmp, tmp2, id] = Row::extract<KeyspaceID,KeyspaceID,KeyspaceID>(to_string(k));
                auto [purpose, name] = Row::extract<Purpose, Blob>(to_string(iter->value()));

                while (_min_free_id < id) {
                    _free_ids.push(_min_free_id);
                    ++_min_free_id;
                }
                _table_map.emplace(name, std::pair(id, purpose));
                _min_free_id = id+1;
            }
            iter->Next();
        }
    }


    ///Snapshot current database state
    PSnapshot make_snapshot() {
        return PSnapshot(_dbinst->GetSnapshot(), [me = shared_from_this()](const leveldb::Snapshot *snap){
            me->_dbinst->ReleaseSnapshot(snap);
        });
    }

    ///Create iterator instance
    /**
     * @param cache set true to cache results for late access. In most cases this is
     * not needed
     * @param snap use snapshot
     * @return iterator
     */
    std::unique_ptr<leveldb::Iterator>  make_iterator(const PSnapshot &snap = {}, bool no_cache = false) {
        leveldb::ReadOptions opts;
        opts.snapshot = snap.get();
        opts.fill_cache = !no_cache;
        return std::unique_ptr<leveldb::Iterator>(_dbinst->NewIterator(opts));
    }

    static PDatabase create(leveldb::DB *db) {
        return std::make_shared<Database>(db);
    }

    bool get(std::string_view key, std::string &result, const PSnapshot &snap = {}) {
        leveldb::ReadOptions opts;
        opts.snapshot = snap.get();
        leveldb::Status st = _dbinst->Get({}, to_slice(key), &result);
        if (st.ok()) return true;
        if (st.IsNotFound()) return false;
        throw DatabaseError(std::move(st));

    }


    template<DocumentWrapper Document>
    Document get_as_document(std::string_view key, const PSnapshot &snap = {}) {
        return [&](std::string &buff) {
           return get(key, buff, snap);
        };
    }

    std::uint64_t get_index_size(std::string_view key1, std::string_view key2) {
        if (key1 > key2) return get_index_size(key2, key1);
        if (key1 == key2) return 0;

        leveldb::Range ranges[1];
        ranges[0] = leveldb::Range(to_slice(key1),to_slice(key2));
        std::uint64_t r[1];
        _dbinst->GetApproximateSizes(ranges, 1, r);
        return r[0];
    }

    ///Commits the batch and clear the batch
    void commit_batch(Batch &batch) {
        batch.before_commit();
        auto st =_dbinst->Write(_write_opts, &batch);
        if (!st.ok()) {
            throw DatabaseError(st);
        }
        batch.after_commit();
    }


    ///Generate key to private area.
    template<typename ... Args>
    static RawKey get_private_area_key(KeyspaceID id, const Args & ... args) {
        return RawKey(system_table, id, args...);
    }


    void compact() {
        _dbinst->CompactRange(nullptr,nullptr);
    }

    void compact_range(std::string_view from, std::string_view to) {
        auto f = to_slice(from);
        auto t = to_slice(to);
        _dbinst->CompactRange(&f,&t);
    }

    ///Start asynchronous task
    /**
     * Creates thread and starts task. If other task is already running, this task is queued
     * and it is executed once previous task is done. So there is always maximum 1 thread which
     * executing one task at background. Use this function to start short tasks at background,
     * do not block the execution for long tasks
     *
     * @note This feature is used by aggregators
     *
     * @param fn function to executed
     */
    template<std::invocable<> Fn>
    void run_async(Fn &&fn) {
        _async.run(std::forward<Fn>(fn));
    }

    leveldb::DB& get_level_db() const {return *_dbinst;}

    static constexpr KeyspaceID system_table = std::numeric_limits<KeyspaceID>::max();

protected:

    std::unique_ptr<leveldb::DB> _dbinst;
    leveldb::WriteOptions _write_opts;

    std::map<std::string, std::pair<KeyspaceID, Purpose>, std::less<> > _table_map;
    std::stack<KeyspaceID> _free_ids;
    KeyspaceID _min_free_id = 0;
    mutable std::shared_mutex _mx;
    TaskThread _async;

};

}


#endif /* SRC_DOCDB_DATABASE_H_ */

