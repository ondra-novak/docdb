#include "index.h"

#include "storage.h"



namespace docdb {

EmitFn::EmitFn(Batch &b, KeySet &ks, KeyspaceID kid, Storage::DocID docId, bool del)
        :_b(b),_ks(ks),_kid(kid),_docid(docId),_del(del) {}


void EmitFn::operator ()(Key &key, Row &value) const {
    key.change_kid(_kid);
    key.append(_docid);
    if (_del) {
        _b.Delete(key);
    } else {
        _b.Put(key, value);
    }
    std::string_view tmp =key;
    std::string_view key_value = tmp.substr(sizeof(KeyspaceID), tmp.length()-sizeof(KeyspaceID)-sizeof(_docid));
    _ks.append(static_cast<std::uint32_t>(key_value.length()));
    _ks.append(RemainingData(key_value));
}

Index::Instance::Instance(KeyspaceID kid, Storage &source,
        std::size_t revision, Indexer &&indexFn)
:IndexView(source.get_db(), kid)
,_source(source)
,_indexFn(std::move(indexFn))
,_revision(revision)
{

}

void Index::Instance::init() {
    std::string v;
    if (!_db->get(RawKey(_kid), v)) {
        _start_id = 1;
    } else {
        auto [lastId, rev] = BasicRow::extract<DocID, std::size_t>(v);
        if (rev == _revision) _start_id = lastId;
        else {
            _start_id = 1;
        }
    }

}

void Index::Instance::update(const PSnapshot &snap) {

    std::unique_lock lk(_mx);
    Batch b;
    DocID curdoc;
    std::string buffer;
    EmitFn::KeySet ks;
    std::vector<std::string_view> modified_keys;

    StorageView snapshot = _source.open_snapshot(snap);

    auto iter = snapshot.scan_from(_start_id);
    while (iter.next()) {
        DocID id = iter.id();
        auto [prevId, data] = iter.doc();
        if (prevId) {
            auto pdoc = snapshot.get(prevId, buffer);
            if (pdoc.has_value()) {
                _indexFn(pdoc->doc_data, EmitFn(b,ks,_kid,prevId,true));
            }
        }
        _indexFn(iter.doc().doc_data,EmitFn(b,ks,_kid,id,false));
        if (!_cblist.empty()) {
            modified_keys.clear();
            std::string_view tmp = ks;
            while (!tmp.empty()) {
                auto [d, rm] = BasicRow::extract<std::uint32_t, RemainingData>(tmp);
                modified_keys.push_back(rm.substr(0,d));
                tmp = rm.substr(d);
            }
            std::sort(modified_keys.begin(), modified_keys.end());
            std::unique(modified_keys.begin(), modified_keys.begin());
            for (auto &fn: _cblist) {
                fn(b, modified_keys, snap);
            }
            ks.clear();
        }
        _db->commit_batch(b);
        _start_id = id+1;
    }
    b.Put(RawKey(_kid), BasicRow(_start_id, _revision));
    _db->commit_batch(b);

}

Index::Index(std::string_view name, Storage &source, std::size_t revision, Indexer &&indexFn, Direction dir)
    :IndexView(source.get_db(), name, dir)
{
    _inst = std::make_shared<Instance>(_kid, source, revision, std::move(indexFn));
    _inst->init();
    source.register_observer([wkinst = std::weak_ptr<Instance>(_inst)](const PSnapshot &snap){
        auto inst = wkinst.lock();
        if (!inst) [[unlikely]] return false;
        inst->update(snap);
        return true;
    }, _inst->get_start_id());

}

void Index::register_callback(UpdateCallback &&cb) {
}

void Index::init_from_db() {

}

}

