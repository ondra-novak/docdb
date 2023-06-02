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

        auto d = view.find(d1);
        CHECK(d.has_value());
        CHECK_EQUAL(d->content,"world");
        d = view.find(d4);
        CHECK(d.has_value());
        CHECK_EQUAL(d->content,"foo");
        d =view.find(d3);
        CHECK(d.has_value());
        CHECK_EQUAL(d->content,"bar");

        auto d3_new = storage.put("baz", d3);
        d = view.find(d3_new);
        CHECK(d.has_value());
        CHECK_EQUAL(d->content,"baz");
        CHECK_EQUAL(d->previous_id,d3);

        storage.compact();
        d = storage.find(d3);
        CHECK(!d.has_value());

        struct Res {
            docdb::DocID id;
            docdb::DocID prevId;
            std::string_view text;
        };

        Res res[] = {
                {1,0,"hello"},
                {2,0,"world"},
                {4,0,"foo"},
                {5,3,"baz"}
        };

        const Res *pt = res;
        for (const auto &row: storage.select_from(d1))
        {
            CHECK_EQUAL(row.id, pt->id);
            CHECK_EQUAL(row.previous_id, pt->prevId);
            CHECK_EQUAL(row.content, pt->text);
            pt++;
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
