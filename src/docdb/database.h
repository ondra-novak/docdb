#pragma once
#ifndef SRC_DOCDB_DATABASE_H_
#define SRC_DOCDB_DATABASE_H_

#include "recordset.h"
#include "batch.h"
#include "key.h"
#include "purpose.h"
#include "exceptions.h"

#include <leveldb/db.h>
#include <string>
#include <stack>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <variant>
#include <optional>



namespace docdb {

class Database;

using PDatabase = std::shared_ptr<Database>;
using PSnapshot = std::shared_ptr<const leveldb::Snapshot>;





///Database base
class Database: public std::enable_shared_from_this<Database> {
public:

    ///Create database object
    static PDatabase create(leveldb::DB *db) {
        return std::make_shared<Database>(db);
    }

    ///Create database object
    /**
     * @param path path to database (or path to be created)
     * @param options Options (leveldb options)
     * @return PDatabase object
     */
    static PDatabase create(const std::string &path, const leveldb::Options &options) {
        leveldb::DB *db;
        leveldb::Status st = leveldb::DB::Open(options, path, &db);
        if (!st.ok()) throw DatabaseError(st);
        return create(db);
    }


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
                if (id != system_table)  { //skip config section
                    auto [purpose, name] = Row::extract<Purpose, Blob>(to_string(iter->value()));

                    while (_min_free_id < id) {
                        _free_ids.push(_min_free_id);
                        ++_min_free_id;
                    }
                    _table_map.emplace(name, std::pair(id, purpose));
                    _min_free_id = id+1;
                }
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

    static thread_local std::string global_buffer_for_get;

    ///Retrieve single item from the database
    /**
     * @param key binary key to retrieve
     * @return return binary value. If the key not found, returned variable doesn't contain a value
     * @note Returned data are temporalily allocated in context of current thread. They are
     * discarded by calling this function again on the same thread.
     */
    std::optional<std::string_view> get(const std::string_view &key) {
        leveldb::ReadOptions opts;
        leveldb::Status st = _dbinst->Get(opts, to_slice(key), &global_buffer_for_get);
        if (st.ok()) return global_buffer_for_get;
        if (st.IsNotFound()) return {};
        throw DatabaseError(std::move(st));
    }

    ///Retrieve single item from the snapshot
    /**
     * @param key binary key to retrieve
     * @return return binary value. If the key not found, returned variable doesn't contain a value
     * @note Returned data are temporalily allocated in context of current thread. They are
     * discarded by calling this function again on the same thread.
     */
    std::optional<std::string_view> get(const std::string_view &key, const PSnapshot &snap) {
        leveldb::ReadOptions opts;
        opts.snapshot = snap.get();
        leveldb::Status st = _dbinst->Get(opts, to_slice(key), &global_buffer_for_get);
        if (st.ok()) return global_buffer_for_get;
        if (st.IsNotFound()) return {};
        throw DatabaseError(std::move(st));
    }

    ///Retrieve single item from the database and parse it as a document
    /**
     * @tparam _DocDef document definition
     * @param key binary key to retrieve
     * @return returns std::optional<Document>, if the key doesn't exists, it returns
     *    object without value
     */
    template<typename _DocDef>
    std::optional<typename _DocDef::Type> get_document(const std::string_view &key) {
        auto v = get(key);
        if (v.has_value()) {
            return std::optional<typename _DocDef::Type>(EmplaceByReturn(
                    [&]{return _DocDef::from_binary(unmove(v->begin()), v->end());}
            ));
        } else {
            return {};
        }
    }

    ///Retrieve single item from the snapshot and parse it as a document
    /**
     * @tparam _DocDef document definition
     * @param key binary key to retrieve
     * @return returns std::optional<Document>, if the key doesn't exists, it returns
     *    object without value
     */
    template<typename _DocDef>
    std::optional<typename _DocDef::Type> get_document(const std::string_view &key, const PSnapshot &snap) {
        auto v = get(key, snap);
        if (v.has_value()) {
            return std::optional<typename _DocDef::Type>(EmplaceByReturn(
                    [&]{return _DocDef::from_binary(unmove(v->begin()), v->end());}
            ));
        } else {
            return {};
        }
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

    leveldb::DB& get_level_db() const {return *_dbinst;}

    static constexpr KeyspaceID system_table = std::numeric_limits<KeyspaceID>::max();

    ///Get persistent variable
    /**
     * The database supports section with variables. They are intended to store any
     *  non-table information, such a configuration, user options, etc.
     *
     * @param var_name variable name
     * @return value. If variable is not set, returns empty
     */
    std::string get_variable(std::string_view var_name) {
        RawKey k(system_table, system_table, system_table, Blob(var_name));
        auto r = get(k);
        if (r.has_value()) return std::string(*r);
        else return {};

    }
    ///Sets persistent variable
    /**
     * The database supports section with variables. They are intended to store any
     *  non-table information, such a configuration, user options, etc.
     *
     * @param b batch
     * @param var_name name of variable
     * @param value value
     * @note variable becomes visible after commiting batch
     */
    void set_variable(Batch &b, std::string_view var_name, std::string_view value) {
        RawKey key(system_table, system_table, system_table, Blob(var_name));
        if (value.empty()) b.Delete(key);
        else b.Put(key, to_slice(value));
    }
    ///Sets persistent variable
    /**
     * The database supports section with variables. They are intended to store any
     *  non-table information, such a configuration, user options, etc.
     *
     * @param var_name name of variable
     * @param value value
     */
    void set_variable(std::string_view var_name, std::string_view value) {
        Batch b;
        set_variable(b,var_name, value);
        commit_batch(b);
    }

    std::vector<std::pair<std::string, std::string> > list_variables() const {
        std::unique_ptr<leveldb::Iterator> iter(_dbinst->NewIterator({}));
        iter->Seek(RawKey(system_table, system_table, system_table));

        std::vector<std::pair<std::string, std::string> > out;
        while (iter->Valid()) {
            auto k = iter->key();
            if (k.size() >= sizeof(KeyspaceID)*3) {
             auto [tmp, tmp2, tmp3, var_name] = Row::extract<KeyspaceID,KeyspaceID,KeyspaceID, Blob>(to_string(k));
             if (tmp != system_table || tmp2 != system_table || tmp3 != system_table) break;
             out.push_back({std::string(var_name), std::string(to_string(iter->value()))});
            }
            iter->Next();
        }
        return out;
    }

protected:

    std::unique_ptr<leveldb::DB> _dbinst;
    leveldb::WriteOptions _write_opts;

    std::map<std::string, std::pair<KeyspaceID, Purpose>, std::less<> > _table_map;
    std::stack<KeyspaceID> _free_ids;
    KeyspaceID _min_free_id = 0;
    mutable std::shared_mutex _mx;

};

inline thread_local std::string Database::global_buffer_for_get;

}


#endif /* SRC_DOCDB_DATABASE_H_ */

