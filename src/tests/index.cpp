#include "check.h"
#include "memdb.h"

#include "../docdb/storage.h"
#include "../docdb/indexer.h"

using Storage = docdb::Storage<docdb::StringDocument>;
using Index_TextToLen = docdb::Indexer<Storage, [](auto emit, const std::string_view &doc) {
    emit(doc,doc.length());
}, 1>;

using Index_LenToText = docdb::Indexer<Storage, [](auto emit, const std::string_view &doc) {
    emit(doc.length(), doc);
}, 1>;


void test1() {

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));
    Storage storage(db, "test_storage");
    Index_TextToLen index1(storage, "text_to_len");
    Index_LenToText index2(storage, "len_to_text");

    docdb::DocID d1,d2,d3,d4;
    d1 = storage.put("hello");
    d2 = storage.put("world");
    d3 = storage.put("bar");
    d4 = storage.put("foo");
    CHECK_EQUAL(d1,1);
    CHECK_EQUAL(d2,2);
    CHECK_EQUAL(d3,3);
    CHECK_EQUAL(d4,4);

    {
        auto iter = index1.lookup({"world"});

        int fnd = 0;
        while (iter.next()) {
            CHECK_EQUAL(iter.id(), 2);
            auto docref = iter.doc();
            CHECK_EQUAL(*docref->document, "world");
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
                CHECK_EQUAL(*docref->document, "bar");
            }
            CHECK(iter.next());
            {
                CHECK_EQUAL(iter.id(),4);
                auto docref = iter.doc();
                CHECK_EQUAL(*docref->document, "foo");
            }
            CHECK(!iter.next());
        }
        {
            docdb::DocID id = 2;
            auto lk = index1[{"world",id}];
            CHECK(lk);
            auto docref = storage[id];
            CHECK_EQUAL(*docref->document, "world");
        }
        storage.put("world2",d2);
        {
            auto iter = index1.lookup("world");
            CHECK(!iter.next());
            auto iter2 = index1.lookup("world2");
            CHECK(iter2.next());
        }


}


int main() {



    test1();
}
