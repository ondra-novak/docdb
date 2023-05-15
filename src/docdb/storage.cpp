#include "storage.h"

#include <leveldb/write_batch.h>

namespace docdb {

Storage::DocID Storage::WriteBatch::put_doc(const std::string_view &doc, DocID replace_id) {
    auto id = _alloc_ids++;
    _buffer.clear();
    _buffer.add(replace_id, RemainingData(doc));
    _batch.Put(_storage->key(id), to_slice(_buffer));
    return id;
}

void Storage::erase(Batch &b, DocID id) {
    b.Delete(key(id));
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
        DocID p = iter.get_prev_docid();
        if (p) b.Delete(key(p));
    }
    _db->commit_batch(b);
}

Storage::DocID Storage::find_revision_id() {
    Storage::Iterator iter = scan(Direction::backward);
    if (iter.next()) {
        return iter.get_docid()+1;
    } else {
        return 1;
    }

}

Storage::DocID Storage::Iterator::get_docid() const {
    KeyspaceID _dummy;
    DocID docId;
    Key::parse(this->key(), _dummy, docId);
    return docId;
}

Storage::DocID Storage::Iterator::get_prev_docid() const {
    DocID docId;
    Value::parse(this->value(), docId);
    return docId;
}

std::string_view Storage::Iterator::get_doc() const {
    DocID docId;
    RemainingData data;
    Value::parse(this->value(), docId, data);
    return data;
}

}
