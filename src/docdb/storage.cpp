#include "storage.h"

#include <leveldb/write_batch.h>

namespace docdb {

Storage::DocID Storage::WriteBatch::put_doc(const std::string_view &doc, DocID replace_id) {
    auto id = _alloc_ids++;
    _buffer.clear();
    _buffer.add(replace_id, RemainingData(doc));
    _batch.Put(Key(_storage->_kid,id), to_slice(_buffer));
    return id;
}

void Storage::erase(Batch &b, DocID id) {
    b.Delete(Key(_kid,id));
}

Storage::DocID Storage::put(const std::string_view &doc, DocID replaced_id) {
    auto bp = bulk_put();
    DocID id = bp.put_doc(doc,replaced_id);
    bp.commit();
    return id;
}

void Storage::erase(DocID id) {
    Batch b;
    erase(b, id);
    _db->commit_batch(b);

}

void Storage::compact() {
    Batch b;
    Iterator iter = scan();
    while (iter.next()) {
        DocID p = iter.doc().old_rev;
        if (p) b.Delete(Key(_kid,p));
    }
    _db->commit_batch(b);
}

void Storage::register_observer(UpdateObserver &&cb) {
    std::lock_guard lk(_mx);
    _cblist.push_back(std::move(cb));
}

void Storage::register_observer(UpdateObserver &&cb, DocID id) {
    std::unique_lock lk(_mx);
    while (id<_next_id) {
        id=_next_id;
        PSnapshot snap=_db->make_snapshot();
        lk.unlock();
        cb(snap);
        lk.lock();
    }
    _cblist.push_back(std::move(cb));
}

void Storage::notify() noexcept {
    if (_cblist.empty()) return;
    PSnapshot snap = _db->make_snapshot();
    _cblist.erase(std::remove_if(_cblist.begin(), _cblist.end(), [&](auto &fn){
        return !fn(snap);
    }),_cblist.end());
}

}
