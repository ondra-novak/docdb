#include "database.h"

#include "snapshot.h"

#include <random>

namespace docdb {

Database::Database(leveldb::DB *dbinst):_dbinst(dbinst)
,_write_opts({})
,_open_tables({})
,_free_bitmap({})
{
    init();
}


class Database::OpenTable: public TableHandle {
public:
    OpenTable(PDatabase owner, KeyspaceID id, std::uint8_t flags, std::string_view name)
        :_owner(std::move(owner))
        ,_key(root_db)
        ,_keyid(id)
        ,_flags(flags)
    {
        _key.append(name);
    }

    virtual docdb::KeyspaceID keyspace() const override  {return _key.keyspace();}
    virtual std::string_view name() const override {return _key.get_key_content();}
    virtual std::string metadata() const override {
        std::string v;
        if (_owner->find(_key, v)) {        
            _flags = v[1];
            return v.substr(2);
        } else {
            return std::string();
        }
    }
    virtual leveldb::DB* database() override {return _owner->_dbinst.get();}
    virtual void metadata(Batch batch,
            const std::string_view &metadata) override {
        Key value(_keyid);
        value.push_back(_flags);
        value.append(metadata);
        batch.put(_key, ValueView(value));
    }
    virtual void commit(leveldb::WriteBatch &batch) override {
        checkStatus(_owner->_dbinst->Write(_owner->_write_opts, &batch));
    }


    virtual ~OpenTable() {
        std::lock_guard _(_mx);
        _owner->close(_keyid, _key.view(), (_flags & flag_erase_on_close) != 0);
    }
    
    virtual void observe(UpdateObserver obs) override {
        std::lock_guard _(_mx);
        _observers.push_back(std::move(obs));
    }
    
    virtual void update(Batch batch, const KeyView &key, const std::string_view *data, const void *source_data) override {
        std::lock_guard _(_mx);
        _observers.erase(std::remove_if(_observers.begin(), _observers.end(),[&](UpdateObserver &h){
            return !h(batch,key, data,source_data);
        }), _observers.end());
    }
    virtual void erase(const KeyView &from, const KeyView &to) override {
        _owner->erase_range(from, to);
    }
    virtual void compact(const KeyView &from, const KeyView &to) override {
        _owner->compact_range(from, to);
    }
    virtual IReadAccess &read_access() override {
        return *_owner;
    }
    virtual void destoy_on_close(bool x) override {
        leveldb::WriteBatch batch;        
        std::string d = metadata();
        if (x) _flags|=flag_erase_on_close;
        else _flags&=~flag_erase_on_close;
        metadata(batch, d);
        commit(batch);
    }

    virtual std::shared_ptr<docdb::TableHandle> snapshot() override {
        PSnapshot sh(_owner->snapshot());
        return sh->create_table_handle_lk(_keyid, name());
    }

protected:
    PDatabase _owner;
    Key _key;
    KeyspaceID _keyid;
    std::mutex _mx;
    std::vector<UpdateObserver> _observers;
    mutable std::uint8_t _flags;
    
    
    
};

PTableHandle Database::create_table_handle_lk(KeyspaceID id, std::string_view name, std::uint8_t flags) {
    if (_open_tables[id] != nullptr) throw TableAlreadyOpened(std::string(name));
    PTableHandle h = std::make_shared<OpenTable>(shared_from_this(), id, flags, name);
    _open_tables[id] = h.get();
    return h;
}

KeyspaceID Database::alloc_keyspace_lk() {
    auto iter = std::find_if(_free_bitmap.begin(), _free_bitmap.end(), [](std::uint8_t x){return x != 0xFF;});
    if (iter == _free_bitmap.end()) {
        throw TooManyTablesException();
    }
    auto &a = *iter;
    KeyspaceID id = std::distance(_free_bitmap.begin(), iter) * 8;
    for (int i = 0; i < 8; i++) {
        std::uint8_t mask = 1 << i;
        if ((a & mask) == 0) {
          a |= mask;
          return id + i;
        }
    }
    throw TooManyTablesException(); //should never called
            
}

void Database::free_keyspace(KeyspaceID id) {
    int ofs = id >> 3;
    std::uint8_t mask = 1 << (id & 0x7);
    _free_bitmap[ofs] &= ~mask;
}

void Database::close(KeyspaceID id, const KeyView &kv, bool erase) {
    std::lock_guard _(_mx);    
    if (erase) {
        erase_lk(id, kv);
    }    
    _open_tables[id] = nullptr;
}

PTableHandle Database::open(const std::string_view &name, OpenMode mode) {
    std::lock_guard _(_mx);
    Key k(root_db, name);
    std::string v;
    leveldb::Status s = _dbinst->Get(leveldb::ReadOptions{}, k, &v);
    if (s.IsNotFound() || v.size()<2) {
        if (mode == OpenMode::open_existing) return nullptr;
        KeyspaceID new_id = alloc_keyspace_lk();
        Key v(new_id);
        std::uint8_t flags(mode == OpenMode::create_temporary?flag_erase_on_close:std::uint8_t(0));  
        v.push_back(flags);
        leveldb::WriteOptions opts = {};
        opts.sync = true;
        _dbinst->Put(opts, k, v);
        return create_table_handle_lk(new_id, name, flags);
    } else if (s.ok()) {
        KeyspaceID id = v[0];
        std::uint8_t flags(v[1]);
        return create_table_handle_lk(id, name, flags);
    } else {
        throw LevelDBUnexpectedStatusException(std::move(s));
    }
}

void Database::erase(const std::string_view &name) {
    auto table = open(name,OpenMode::open_existing);
    if (table) table->destoy_on_close(true);    
}

void Database::checkStatus(leveldb::Status st) {
    if (!st.ok()) throw LevelDBUnexpectedStatusException(std::move(st));
}

const char* LevelDBUnexpectedStatusException::what() const noexcept {
    if (_whatmsg.empty()) _whatmsg = _st.ToString();
    return _whatmsg.c_str();
}

const char* TooManyTablesException::what() const noexcept {
    return "Too many tables - all slots (256 slots) are allocated";
}

const char* TableAlreadyOpened::what() const noexcept {
    if (_whatmsg.empty()) {
        _whatmsg = "Table already opened: ";
        _whatmsg.append(_name);
    }
    return _name.c_str();
}


void Database::erase_range(const KeyView &from, const KeyView &to) {
    std::lock_guard _(_mx);
    erase_range_lk(from, to);
}
void Database::erase_range_lk(const KeyView &from, const KeyView &to) {
    std::unique_ptr<leveldb::Iterator> iter(_dbinst->NewIterator({}));
    iter->Seek(from);
    leveldb::WriteBatch batch;
    while (iter->Valid()) {
        KeyView cur(iter->key());
        if (cur >= to) break;
        batch.Delete(cur);
        iter->Next();
        if (batch.ApproximateSize()>65536) {
            _dbinst->Write({}, &batch);
            batch.Clear();
        }
    }
    _dbinst->Write({}, &batch);    
}

void Database::compact_range(const KeyView &from, const KeyView &to) {
    leveldb::Slice sfrom(from);
    leveldb::Slice sto(to);
   _dbinst->CompactRange(&sfrom, &sto);
}

void Database::erase_lk(KeyspaceID id, const KeyView &kv) {
    Key k1(id);
    Key k2(id+1);
    erase_range_lk(k1.view(), k2.view());
    _dbinst->Delete({}, kv);
    free_keyspace(id);

}

PTableHandle Database::create_temporary() {
    std::string name = "__temp";
    std::random_device rnd;
    for (int i =0; i < 16; i++) {
        char c = rnd() & 0xFF;
        name.push_back(c);
    }
    return open(name, OpenMode::create_temporary);
}

std::vector<Database::TableInfo> DatabaseCommon::list(const std::string_view prefix, bool skip_temporary) const {
    std::vector<Database::TableInfo> out;
    PIterator iter (iterate());
    Key startKey(root_db);
    startKey.append(prefix);
    //seek for first table def
    iter->Seek(startKey);
    //repeat till valid
    while (iter->Valid()) {
        //retrieve key, it is in form <0xFF><name>
        KeyView k = iter->key();
        //retrieve value, it is in form <keyspaceId><flags><metadata>        
        KeyView v = iter->value();
        //retrieve keyspace
        KeyspaceID id = v.keyspace();
        
        std::string_view content = v.get_key_content(); 
        std::uint8_t flags = content[0];
        auto metadata = content.substr(1);
        
        if (!skip_temporary || ((flags & flag_erase_on_close) == 0)) {
            out.push_back(TableInfo{
                id,flags,static_cast<KeyspaceID>(id+1),0,std::string(k.get_key_content()),std::string(metadata)                        
            });
        }
        //go next
        iter->Next();
    }
    
    
    std::vector<leveldb::Range> ranges;
    std::vector<std::uint64_t> sizes;
    ranges.reserve(out.size());
    sizes.resize(out.size());
    for (const auto &item: out) {
        leveldb::Slice beg(reinterpret_cast<const char *>(&item.id),1);
        leveldb::Slice end(reinterpret_cast<const char *>(&item.__reserved),1);
        ranges.push_back(leveldb::Range(beg, end));
    }
    getApproximateSizes(ranges.data(), ranges.size(), sizes.data());
    auto iter2 = sizes.begin();
    for (auto &item: out) {
        item.size = *iter2;
        ++iter2;
    }
    return out;
    
}

PSnapshot Database::snapshot() const {
    return std::make_shared<Snapshot>(
            const_cast<Database *>(this)->shared_from_this(),
            _dbinst.get(), _dbinst->GetSnapshot());
}

void Database::init() {
    //reserve root_db's id in free bitmap
    _free_bitmap[(root_db/8)] |= (1<<(root_db & 0x7));
    //list of tables to be erased
    std::vector<std::string> _deldbs;
    //create iterator, iterate all tables
    std::unique_ptr<leveldb::Iterator> iter (_dbinst->NewIterator({}));
    //start key is begin of root_db
    //there is list of tables, until end of keyspace
    Key startKey(root_db);
    //seek for first table def
    iter->Seek(startKey);
    //repeat till valid
    while (iter->Valid()) {
        //retrieve key, it is in form <0xFF><name>
        KeyView k = iter->key();
        //retrieve value, it is in form <keyspaceId><flags><metadata>        
        KeyView v = iter->value();
        //retrieve keyspace
        auto id = v.keyspace();
        //reserve the id in free bitmap
        _free_bitmap[(id/8)]= (1<<(id & 0x7));
        //retrieve flags and check, whether flag erase on close is enabled
        if (v.get_key_content()[0] & flag_erase_on_close) {
            //if does, put table to list tables to be erased
            _deldbs.push_back(std::string(k.get_key_content()));
        }
        //go next
        iter->Next();
    }    
    //erase all tables marked to be erased
    for (const auto &dels : _deldbs) {
        erase(dels);
    }
}

bool Database::find(const docdb::KeyView &key, std::string &value) const {
    leveldb::Status st = _dbinst->Get({},key, &value);
    if (st.ok()) return true;
    if (st.IsNotFound()) return false;
    throw LevelDBUnexpectedStatusException(std::move(st));
}

PIterator Database::iterate() const {
    return PIterator(_dbinst->NewIterator({}));
}

void Database::getApproximateSizes(const leveldb::Range *range, int n,
            uint64_t *sizes) {
    return _dbinst->GetApproximateSizes(range, n, sizes);
}

}

