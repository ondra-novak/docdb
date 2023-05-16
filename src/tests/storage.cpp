#include "check.h"
#include "memdb.h"

#include "../docdb/storage.h"

void test1() {

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    {
        docdb::Storage storage(db, "test_storage");

        docdb::Storage::DocID d1,d2,d3,d4;
        {
            auto batch = storage.bulk_put();
            d1 = batch.put_doc("hello");
            d2 = batch.put_doc("world");
            d3 = batch.put_doc("bar");
            d4 = batch.put_doc("foo");
            batch.commit();
        }
        CHECK_EQUAL(d1,1);
        CHECK_EQUAL(d2,2);
        CHECK_EQUAL(d3,3);
        CHECK_EQUAL(d4,4);

        std::string buffer;

        auto d = storage.get(d2, buffer);
        CHECK(d.has_value());
        CHECK_EQUAL(d->doc_data,"world");
        d = storage.get(d4, buffer);
        CHECK(d.has_value());
        CHECK_EQUAL(d->doc_data,"foo");
        d =storage.get(d3, buffer);
        CHECK(d.has_value());
        CHECK_EQUAL(d->doc_data,"bar");

        auto d3_new = storage.put("baz", d3);
        d = storage.get(d3_new, buffer);
        CHECK(d.has_value());
        CHECK_EQUAL(d->doc_data,"baz");
        CHECK_EQUAL(d->old_rev,d3);

        storage.compact();
        d = storage.get(d3, buffer);
        CHECK(!d.has_value());

        struct Res {
            docdb::Storage::DocID id;
            docdb::Storage::DocID prevId;
            std::string_view text;
        };

        Res res[] = {
                {1,0,"hello"},
                {2,0,"world"},
                {4,0,"foo"},
                {5,3,"baz"}
        };

        {
            docdb::Storage::Iterator iter = storage.scan_from(d1);
            const Res *pt = res;
            while (iter.next()) {
                auto [prev_id, data] = iter.doc();
                auto id = iter.id();
                CHECK_EQUAL(id, pt->id);
                CHECK_EQUAL(prev_id, pt->prevId);
                CHECK_EQUAL(data, pt->text);
                pt++;
            }
        }
    }
    {
        docdb::Storage storage(db, "test_storage");
        CHECK_EQUAL(storage.get_rev(), 6);
    }



}

void test_key(docdb::Key k) {

}

int main() {

    test_key({1,true,"ahoj"});



    test1();
}
