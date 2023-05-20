#include "check.h"
#include "../docdb/doc_storage.h"
#include "../docdb/doc_index.h"
#include "../docdb/aggregate.h"

#include "memdb.h"

void test1() {

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    using Storage = docdb::DocumentStorage<docdb::BasicRowDocument>;
    using Index = docdb::DocumentIndex<Storage>;
    using SumAndCountAggr = docdb::AggregateIndex<Index, docdb::BasicRowDocument>;

    Storage storage(db, "test_storage");
    Index index(storage, "test_index", 1, [](Index::Emit &emit, const docdb::BasicRowView &doc) {
        auto [txt, val] = doc.get<std::string_view, double>();
        docdb::Key k(txt);
        docdb::BasicRow v(val);
        emit(k,v);
    });




}


int main() {

}
