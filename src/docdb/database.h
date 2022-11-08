#pragma once
#ifndef SRC_DOCDB_DATABASE_H_
#define SRC_DOCDB_DATABASE_H_

#include "table_handle.h"

#include "rdifc.h"

#include <leveldb/db.h>
#include <memory>
#include <shared_mutex>

namespace docdb {


enum class OpenMode {
    ///open existing table
    /** This mode fails, if table doesn't exist */
    open_existing,
    ///Open or create table
    /**
     * opens table, if doesn't exists, it is created, this mode is defaultr
     */
    open_or_create,
    
    ///Create temporary table
    /**
     * Creates temporary table, which is destroyed when table is closed
     * Note that table is still allocated in leveldb keyspace and it is
     * stored in this. Can be useful for large temporary tables which
     * doesn't fit to the memory. However when table is closed, it
     * is destroyed. Note that destroying such a table can take some amount
     * of time
     * 
     * If the application crashes, temporary tables are destroyed during
     * initialization unless read only flag is set
     */
    create_temporary
};




class DatabaseCommon: public IReadAccess {
public:

    ///maximum tables can be stored in single leveldb instance
    /**
     * This is hardcoded, because keyspaceID is 8 bit, so it can hold number 0-255
     * Keep in mind, that one keyspace is always reserved, so you can create max_tables-1 tables at all 
     */
    static constexpr std::size_t max_tables = 256;
    static constexpr KeyspaceID root_db = 0xFF;

    ///flag marks table temporary
    static constexpr std::uint8_t flag_erase_on_close = 0x1;

    struct TableInfo {
        ///table's assigned keyspace id
        KeyspaceID id;
        ///table's internal flags
        std::uint8_t flags;
        ///reserved space for internal usage
        std::uint8_t __reserved;
        ///aproximate size of the table
        std::uint64_t size;
        ///name of the table
        std::string name;
        ///metadata of the table (internal structure)
        std::string metadata;
    };
    
    
    std::vector<TableInfo> list(const std::string_view prefix = {}, bool skip_temporary = true) const;

    
};

class Snapshot;
using PSnapshot = std::shared_ptr<Snapshot>;

class Database: public DatabaseCommon, public std::enable_shared_from_this<Database> {

    Database(leveldb::DB *dbinst);
    
    ///Open table
    /**
     * @param name name of table.
     * @param mode 
     * @return pointer to table handle, or nullptr, if table doesn't exists and create is false;
     */
    PTableHandle open(const std::string_view &name, OpenMode mode = OpenMode::open_or_create);

    
    ///Creates temporary table
    /**
     * @return it is the same as open() with flag OpenMode::create_temporary. There is
     * only one benefit, you don't need to specify name. All tables
     * must have names, so in this case, name of the table is random
     * 
     * (it always starts with "__temp" followed by random characters. The prefix
     * "__temp" doesn't declares temporary nature of the table, it is defined to 
     * avoid choosing same prefix by a user and risk potentional naming collisions    
     * 
     */
    PTableHandle create_temporary();

    ///Erase existing table
    /**
     * @param name name of table
     * 
     * @exception TableAlreadyOpened cannot erase opened table
     */
    void erase(const std::string_view &name);
    
    
    ///List of tables in the database
    /**
     * @param prefix prefix to include. This allows to add namespaces (so you can
     * list a single namespace)
     * 
     * @param skip_temporary skip all temporary tables, and also tables marked to be erased, 
     * or tables being currently erased.
     * 
     * @return list of tables, each item is TableInfo
     */
    std::vector<TableInfo> list(const std::string_view prefix = {}, bool skip_temporary = true) const;

    
    virtual bool find(const docdb::KeyView &key, std::string &value) const override;
    
    virtual docdb::PIterator iterate() const override;

    
    PSnapshot snapshot() const;
    
protected:
    std::unique_ptr<leveldb::DB> _dbinst;
    
    leveldb::WriteOptions _write_opts;
    
    std::shared_mutex _mx;
    std::array<TableHandle *, max_tables> _open_tables;
    std::array<std::uint8_t, max_tables/8> _free_bitmap;
           
    
    PTableHandle create_table_handle_lk(KeyspaceID id, std::string_view name, std::uint8_t flags);
    
    KeyspaceID alloc_keyspace_lk();
    void free_keyspace(KeyspaceID id);
    void close(KeyspaceID id, const KeyView &kv, bool erase);

    class OpenTable;
    
    static void checkStatus(leveldb::Status st);

    void erase_lk(KeyspaceID id, const KeyView &kv);
    void erase_range(const KeyView &from, const KeyView &to);
    void erase_range_lk(const KeyView &from, const KeyView &to);
    void compact_range(const KeyView &from, const KeyView &to);
    
    void init();
    virtual void getApproximateSizes(const leveldb::Range* range, int n, uint64_t* sizes);

};

class LevelDBUnexpectedStatusException: public std::exception {
public:
    LevelDBUnexpectedStatusException(leveldb::Status st):_st(st) {};
    virtual const char *what() const noexcept override;
    const leveldb::Status &status() const {return _st;}
protected:
    leveldb::Status _st;
    mutable std::string _whatmsg;
};

class TooManyTablesException: public std::exception {
public:
    virtual const char *what() const noexcept override;
};

class TableAlreadyOpened: public std::exception {
public:
    TableAlreadyOpened(std::string name):_name(std::move(name)) {}
    virtual const char *what() const noexcept override;
    const std::string &name() const {return _name;}
    
protected:
    std::string _name;
    mutable std::string _whatmsg;
};


using PDatabase = std::shared_ptr<Database>;






}



#endif /* SRC_DOCDB_DATABASE_H_ */
