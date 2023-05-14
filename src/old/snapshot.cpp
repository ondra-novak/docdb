
#include "snapshot.h"
namespace docdb {


class Snapshot::OpenTable: public TableHandle {
public:
    OpenTable(PSnapshot owner, KeyspaceID id, std::string_view name)
        :_owner(std::move(owner)) 
        ,_key(root_db, name)
        ,_keyid(id) {}

    virtual docdb::KeyspaceID keyspace() const override {return _keyid;}
    virtual std::string metadata() const override {
        std::string s;
        if (_owner->find(_key, s)) {
            return s.substr(2);
        } else {
            return std::string();
        }
    }
    virtual leveldb::DB* database() override {
        return _owner->_db;
    }
    virtual void metadata(Batch batch, const std::string_view &metadata) override {
        throw TableIsReadOnlyException();
    }
    virtual void update(Batch batch, const docdb::KeyView &key,
            const std::string_view *data, const void *source_data) override {
        throw TableIsReadOnlyException();
    }
    virtual void erase(const docdb::KeyView &from, const docdb::KeyView &to)
            override {
        throw TableIsReadOnlyException();
    }
    virtual void commit(leveldb::WriteBatch &batch) override {
        throw TableIsReadOnlyException();
    }
    virtual std::string_view name() const override {
        return _key.get_key_content();
    }
    virtual void compact(const docdb::KeyView &from, const docdb::KeyView &to)
            override {
        throw TableIsReadOnlyException();
    }
    virtual void destoy_on_close(bool x) override {
        throw TableIsReadOnlyException();
    }
    virtual std::shared_ptr<docdb::TableHandle> snapshot() override {
        return _owner->create_table_handle_lk(_keyid, name());  
    }
    virtual void observe(docdb::UpdateObserver x) override {
        //as there is nothing to observe, one event can be. 
        //event of destroying table can be observed, so register such observer
        _observers.push_back(std::move(x));
    }
    virtual IReadAccess &read_access() override {
        return *_owner;
    }

protected:
    
    PSnapshot _owner;
    Key _key;
    KeyspaceID _keyid;
    std::vector<docdb::UpdateObserver> _observers;
};

PTableHandle Snapshot::open(std::string_view name) {
    Key k(root_db, name);
    std::string s;
    if (find(k,s)) {
        KeyView v((std::string_view(s)));
        return create_table_handle_lk(v.keyspace(), name);
    } else {
        return nullptr;
    }
}


bool Snapshot::find(const docdb::KeyView &key, std::string &value) const {
    leveldb::Status st = _db->Get(_opt,key, &value);
    if (st.ok()) return true;
    if (st.IsNotFound()) return false;
    throw LevelDBUnexpectedStatusException(std::move(st));
}

docdb::PIterator Snapshot::iterate() const {
    return PIterator(_db->NewIterator(_opt));
}

Snapshot::~Snapshot() {
    if (_opt.snapshot) {
        _db->ReleaseSnapshot(_opt.snapshot);
    }
}

Snapshot::Snapshot(Snapshot &&other)
    :_ref(std::move(other._ref))
    ,_db(other._db)
    ,_opt(other._opt)
{
    other._opt.snapshot = nullptr;
 
}

Snapshot& Snapshot::operator =(Snapshot &&other) {
    if (&other != this) {
        if (_opt.snapshot) _db->ReleaseSnapshot(_opt.snapshot);
        _ref =std::move(other._ref);
        _db = other._db;
        _opt = other._opt;
        other._opt.snapshot = nullptr;
    }
    return *this;
}

PTableHandle Snapshot::create_table_handle_lk(KeyspaceID id, std::string_view name) {
    return std::make_shared<OpenTable>(shared_from_this(),id,name);
}

Snapshot::Snapshot(std::shared_ptr<Database> ref, leveldb::DB *db, const leveldb::Snapshot *snp)
:_ref(ref)
,_db(db)
{
    _opt.snapshot = snp;
}

void Snapshot::getApproximateSizes(const leveldb::Range *range, int n, uint64_t *sizes) const {
    return static_cast<IReadAccess *>(_ref.get())->getApproximateSizes(range,n,sizes);
}

}
