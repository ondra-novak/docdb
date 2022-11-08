#pragma once
#ifndef SRC_DOCDB_SNAPSHOT_H_
#define SRC_DOCDB_SNAPSHOT_H_
#include "database.h"


#include <memory>



namespace docdb {

///Snapshot of the whole database
class Snapshot:public DatabaseCommon,  public std::enable_shared_from_this<Snapshot> {
public:
   
    ///Open table from snapshot. Table must exists, otherwise function returns null
    /**
     * @param name name of table to open
     * @return handle to table, or nullptr if doesn't exists
     */
    PTableHandle open(std::string_view name);
    
    
    virtual bool find(const docdb::KeyView &key, std::string &value) const override;
    
    virtual docdb::PIterator iterate() const override;

    
    Snapshot(std::shared_ptr<Database> ref, leveldb::DB *db, const leveldb::Snapshot *snp);
    virtual ~Snapshot();

    Snapshot(Snapshot &&other);
    Snapshot(const Snapshot &) = delete;
    Snapshot &operator=(Snapshot &&other);
    Snapshot &operator=(const Snapshot &) = delete;
    
protected:
    
    virtual void getApproximateSizes(const leveldb::Range* range, int n, uint64_t* sizes) const override;

    
    PTableHandle create_table_handle_lk(KeyspaceID id, std::string_view name);
    
    
    friend class Database;
    
    
    std::shared_ptr<Database> _ref;
    leveldb::DB *_db;
    leveldb::ReadOptions _opt;
    
    
    PTableHandle create_snapshot_handle();
    
    class OpenTable;
};

 

class TableIsReadOnlyException: public std::exception {
public:
    virtual const char *what() const noexcept override {
        return "Can't write to the table. It is opened in read only mode.";
        
    }
};


}



#endif /* SRC_DOCDB_SNAPSHOT_H_ */
