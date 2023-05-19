#include "check.h"
#include "memdb.h"

#include "../docdb/doc_storage.h"

void test1() {

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    using Storage = docdb::DocumentStorage<docdb::BasicRowDocument>;
    {
        Storage storage(db, "test_storage");

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

        std::string buffer;

        auto d = storage.get(d2);
        CHECK(d.has_value());
        CHECK_EQUAL(d.doc(),"world");
        d = storage.get(d4);
        CHECK(d.has_value());
        CHECK_EQUAL(d.doc(),"foo");
        d =storage.get(d3);
        CHECK(d.has_value());
        CHECK_EQUAL(d.doc(),"bar");

        auto d3_new = storage.put("baz", d3);
        d = storage.get(d3_new);
        CHECK(d.has_value());
        CHECK_EQUAL(d.doc(),"baz");
        CHECK_EQUAL(d.prev_id,d3);

        storage.compact();
        d = storage.get(d3);
        CHECK(!d.has_value());

        struct Res {
            Storage::DocID id;
            Storage::DocID prevId;
            std::string_view text;
        };

        Res res[] = {
                {1,0,"hello"},
                {2,0,"world"},
                {4,0,"foo"},
                {5,3,"baz"}
        };

        {
            Storage::Iterator iter = storage.scan_from(d1);
            const Res *pt = res;
            while (iter.next()) {
                auto doc = iter.doc();
                auto id = iter.id();
                auto prev_id = iter.prev_id();
                CHECK_EQUAL(id, pt->id);
                CHECK_EQUAL(prev_id, pt->prevId);
                CHECK_EQUAL(doc, pt->text);
                pt++;
            }
        }
    }
    {
        Storage storage(db, "test_storage");
        CHECK_EQUAL(storage.get_rev(), 6);
    }



}

void test_key(docdb::RawKey k) {

}

int main() {

    test_key({1,true,"ahoj"});



    test1();
}
