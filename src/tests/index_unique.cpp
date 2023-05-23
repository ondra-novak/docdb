#include "check.h"
#include "memdb.h"

#include "../docdb/doc_storage.h"
#include "../docdb/doc_index.h"



void test1() {

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    using Storage = docdb::DocumentStorage<docdb::RowDocument>;
    using Index = docdb::DocumentIndex<Storage, docdb::RowDocument, docdb::IndexType::unique_enforced_single_thread>;

    Storage storage(db, "test_storage");
    Index index(storage, "test_index", 1, [](Index::Emit &emit, const docdb::Row &doc, const Index::DocMetadata &mt) {
        CHECK(!mt.deleting || doc.is_view());
        auto [txt] = doc.get<std::string_view>();
        emit({txt},{txt.length()});
    });

    Storage::DocID d1,d2,d3,d4;
        {
            d1 = storage.put("hello");
            d2 = storage.put("world");
            d3 = storage.put("bar");
            d4 = storage.put("foo");
        }
        CHECK_EQUAL(d1,1);
        CHECK_EQUAL(d2,2);
        CHECK_EQUAL(d3,3);
        CHECK_EQUAL(d4,4);

        CHECK_EXCEPTION(std::runtime_error, storage.put("hello")) //duplicate key;


}


int main() {



    test1();
}
