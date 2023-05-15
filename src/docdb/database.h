#pragma once
#ifndef SRC_DOCDB_DATABASE_H_
#define SRC_DOCDB_DATABASE_H_

#include "iterator.h"
#include "keyvalue.h"

#include <memory>
#include <leveldb/db.h>
#include <limits>
#include <string>
#include <optional>
#include <shared_mutex>
#include <stack>
#include <vector>
#include <map>



namespace docdb {

class Database;

using PDatabase = std::shared_ptr<Database>;
using PSnapshot = std::shared_ptr<const leveldb::Snapshot>;
using Batch = leveldb::WriteBatch;


///Database base
class Database: public std::enable_shared_from_this<Database> {
public:
    ///Initialize database
    /**
     * @param dbinst leveldb instance
     */
    Database(leveldb::DB *dbinst);

    ///Find table by name
    /**
     * @param v name of table
     * @return keyspace id for given table, if exist, otherwise returns no value
     */
    std::optional<KeyspaceID> find_table(std::string_view v) const;
    ///Retrieves name of the table from the id
    /**
     * @param id keyspace id
     * @return name of table if exists
     */
    std::optional<std::string> name_from_id(KeyspaceID id) const;

    ///Retrieves snapshot of list of tables
    /**
     * @return a map object which contains a name as key and keyspace id as value
     */
    std::map<std::string, KeyspaceID, std::less<> > list() const;

    ///Create or open the table
    /**
     * @param name name of table
     * @return keyspace id for given table. The table is automatically create if not exists
     */
    KeyspaceID open_table(std::string_view name);
    ///Delete table
    /**
     * @param name of table to delete
     */
    void delete_table(std::string_view name);
    ///Deletes all rows in given table
    /**
     * @param id keyspace id of table
     */
    void clear_table(KeyspaceID id);

    ///rescan for all tables in database and create local index
    /**
     * This is done automatically when database is constructed, however if the
     * local index is not in sync, this will recreate the index (by default,
     * index should be update in normal condition, so you don't need to call this
     * function manually)
     *
     */
    void scan_tables();

    ///Snapshot current database state
    PSnapshot make_snapshot();

    ///Create iterator instance
    /**
     * @param cache set true to cache results for late access. In most cases this is
     * not needed
     * @param snap use snapshot
     * @return iterator
     */
    std::unique_ptr<leveldb::Iterator>  make_iterator(bool cache = false, const PSnapshot &snap = {});

    std::unique_ptr<leveldb::Iterator>  make_iterator(bool cache = false);


    static PDatabase create(leveldb::DB *db);


    bool get(std::string_view key, std::string &result);

    bool get(std::string_view key, std::string &result, const PSnapshot &snap);

    std::uint64_t get_index_size(std::string_view key1, std::string_view key2);

    ///Commits the batch and clear the batch
    void commit_batch(Batch &batch);


protected:
    static constexpr KeyspaceID system_table = std::numeric_limits<KeyspaceID>::max();

    std::unique_ptr<leveldb::DB> _dbinst;
    leveldb::WriteOptions _write_opts;

    std::map<std::string, KeyspaceID, std::less<> > _table_map;
    std::stack<KeyspaceID> _free_ids;
    KeyspaceID _min_free_id = 0;
    mutable std::shared_mutex _mx;

};

class DatabaseError: public std::exception {
public:

    DatabaseError(leveldb::Status st);
    const leveldb::Status &get_status() {return _st;}
    const char *what() const noexcept override {return _msg.c_str();}
protected:
    leveldb::Status _st;
    std::string _msg;

};


}


#endif /* SRC_DOCDB_DATABASE_H_ */

