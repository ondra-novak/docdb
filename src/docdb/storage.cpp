#include "storage.h"

#include <leveldb/write_batch.h>

namespace docdb {

Storage::DocID Storage::WriteBatch::put_doc(const std::string_view &doc, DocID replace_id) {
    auto id = _alloc_ids++;
    _buffer.clear();
    _buffer.add(replace_id, RemainingData(doc));
    Put(_storage->key(id), to_slice(doc));
    return id;
}

void Storage::erase(Batch &b, DocID id) {
    b.Delete(key(id));
}

Storage::DocID Storage::put(const std::string_view &doc) {
    auto bp = bulk_put();
    DocID id = bp.put_doc(doc);
    bp.commit();
    return id;
}

void Storage::erase(DocID id) {
    Batch b;
    erase(b, id);
    _db->commit_batch(b);

}


Storage::DocID Storage::find_revision_id() {
    Iterator iter = scan(Direction::backward);
    if (iter.next()) {
        return key2docid(iter.key())+1;
    } else {
        return 1;
    }

}

}
