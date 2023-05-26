#include "check.h"
#include "memdb.h"

#include "../docdb/storage.h"

void test1() {

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    using Storage = docdb::Storage<docdb::StringDocument>;
    {
        Storage storage(db, "test_storage");

        docdb::DocID d1,d2,d3,d4;
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

        auto view = storage.get_snapshot();

        auto d = view[d2];
        CHECK(d.has_value());
        CHECK_EQUAL(d->content,"world");
        d = view[d4];
        CHECK(d.has_value());
        CHECK_EQUAL(d->content,"foo");
        d =view[d3];
        CHECK(d.has_value());
        CHECK_EQUAL(d->content,"bar");

        auto d3_new = storage.put("baz", d3);
        d = view[d3_new];
        CHECK(d.has_value());
        CHECK_EQUAL(d->content,"baz");
        CHECK_EQUAL(d->previous_id,d3);

#if 0
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
                auto doc = iter.value();
                auto id = iter.id();
                auto prev_id = iter.prev_id();
                CHECK_EQUAL(id, pt->id);
                CHECK_EQUAL(prev_id, pt->prevId);
                CHECK_EQUAL(*doc, pt->text);
                pt++;
            }
        }
#endif
    }
    {
        Storage storage(db, "test_storage");
        CHECK_EQUAL(storage.get_rev(), 6);
        for (auto x: storage.select_range(3,1,docdb::LastRecord::included)) {
            std::cout << x.id << std::endl;
        }
    }



}

void test_key(docdb::RawKey k) {

}

int main() {

    test_key({1,true,"ahoj"});



    test1();
}
