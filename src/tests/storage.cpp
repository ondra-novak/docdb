#include "check.h"
#include "memdb.h"

#include "../docdb/storage.h"

void test1() {

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

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

    docdb::Storage::DocDataBuffer dinfo;
    CHECK(storage.get(d2, dinfo));
    CHECK_EQUAL(dinfo.doc_data,"world");
    CHECK(storage.get(d4, dinfo));
    CHECK_EQUAL(dinfo.doc_data,"foo");
    CHECK(storage.get(d3, dinfo));
    CHECK_EQUAL(dinfo.doc_data,"bar");

    auto d3_new = storage.put("baz", d3);
    CHECK(storage.get(d3_new, dinfo));
    CHECK_EQUAL(dinfo.doc_data,"baz");
    CHECK_EQUAL(dinfo.old_rev,d3);

    storage.compact();
    CHECK(!storage.get(d3, dinfo));

    {
        docdb::Storage::Iterator iter = storage.scan(d1);
        while (iter.next()) {
            std::cout << iter.get_docid() << "," << iter.get_prev_docid() << "," << iter.get_doc() << std::endl;
        }
    }



}


int main() {
    test1();
}
