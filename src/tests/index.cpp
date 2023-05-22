#include "check.h"
#include "memdb.h"

#include "../docdb/doc_storage.h"
#include "../docdb/doc_index.h"



void test1() {

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    using Storage = docdb::DocumentStorage<docdb::BasicRowDocument>;
    using Index = docdb::DocumentIndex<Storage>;

    Storage storage(db, "test_storage");
    Index index(storage, "test_index", 1, [](Index::Emit &emit, const docdb::BasicRowView &doc) {
        auto [txt] = doc.get<std::string_view>();
        emit({txt},docdb::BasicRow{txt.length()});
    });

    Index index2(storage, "test_index_2", 1, [](Index::Emit &emit, const docdb::BasicRowView &doc) {
        auto [txt] = doc.get<std::string_view>();
        emit({txt.length()},docdb::BasicRow{txt});
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

        {
            auto iter = index.lookup({"world"});

            int fnd = 0;
            while (iter.next()) {
                CHECK_EQUAL(iter.id(), 2);
                auto docref = iter.doc();
                CHECK_EQUAL(*docref, "world");
                fnd++;
            }
            CHECK_EQUAL(fnd,1);
        }

        {
            auto iter = index2.lookup({std::size_t(3)});

            CHECK(iter.next());
            {
                CHECK_EQUAL(iter.id(),3);
                auto docref = iter.doc();
                CHECK_EQUAL(*docref, "bar");
            }
            CHECK(iter.next());
            {
                CHECK_EQUAL(iter.id(),4);
                auto docref = iter.doc();
                CHECK_EQUAL(*docref, "foo");
            }
            CHECK(!iter.next());
        }
        {
            typename Storage::DocID id = 2;
            auto lk = index.get({"world",2});
            CHECK(lk);
            auto docref = storage[id];
            CHECK_EQUAL(*docref, "world");
        }

}


int main() {



    test1();
}
