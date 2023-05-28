#include "check.h"
#include "memdb.h"

#include "../docdb/storage.h"
#include "../docdb/indexer.h"


void test1() {


    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    using Storage = docdb::Storage<docdb::FixedRowDocument<std::string_view, int> >;
    using Index = docdb::Indexer<Storage, [](auto emit, const auto &doc) {
            auto [txt, val] = doc.get();
            emit(txt,val);
        },1,docdb::IndexType::unique,docdb::FixedRowDocument<int> >;

    Storage storage(db, "test_storage");
    Index index(storage, "test_index");

    docdb::DocID id = storage.put({"aaa", 5});
    storage.put({"bbb", 3});
    CHECK_EXCEPTION(docdb::DuplicateKeyException, storage.put({"aaa", 2}));
    id = storage.put({"aaa", 12}, id);


    {
        docdb::Batch b;
        id = storage.put(b, {"aaa", 13}, id);
        id = storage.put(b, {"aaa", 15}, id);
        storage.put(b, {"aaa", 20}, id);
        CHECK_EXCEPTION(docdb::DuplicateKeyException, storage.put(b, {"aaa", 70}, id));
        db->commit_batch(b);
    }

    for (auto row: index.select_all()) {
        auto [k] = row.key.get<std::string_view>();
        auto [v] = row.value.get();
        std::cout << k <<  ": " << v << std::endl;
    }
}


int main() {



    test1();
}
