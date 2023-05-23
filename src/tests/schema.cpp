#include "check.h"
#include <iostream>
#include <docdb/schema.h>

#include "memdb.h"


constexpr auto myDatabaseSchema = docdb::create_storage<docdb::RowDocument>("test_storage",
        docdb::create_index<docdb::RowDocument>("aaa"),docdb::create_index<docdb::RowDocument>("bbb")
);

using Database = docdb::SchemaType_t<&myDatabaseSchema>;

void test1() {

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));
    Database storage = myDatabaseSchema.connect(db);

    docdb::DocID d1,d2,d3,d4;
    d1 = storage.put("hello");
    d2 = storage.put("world");
    d3 = storage.put("bar");
    d4 = storage.put("foo");

    CHECK_EQUAL(d1,1);
    CHECK_EQUAL(d2,2);
    CHECK_EQUAL(d3,3);
    CHECK_EQUAL(d4,4);

    auto d = storage.get(d2);
    CHECK(d.has_value());
    auto [doc1] = d->get<std::string_view>();
    CHECK_EQUAL(doc1,"world");
    d = storage.get(d4);
    CHECK(d.has_value());
    auto [doc2] = d->get<std::string_view>();
    CHECK_EQUAL(doc2,"foo");
    d =storage.get(d3);
    CHECK(d.has_value());
    auto [doc3] = d->get<std::string_view>();
    CHECK_EQUAL(doc3,"bar");

    auto d3_new = storage.put("baz", d3);
    d = storage.get(d3_new);
    CHECK(d.has_value());
    auto [doc4] = d->get<std::string_view>();
    CHECK_EQUAL(doc4,"baz");
    CHECK_EQUAL(d.prev_id(),d3);
/*
    storage.compact();
    d = storage.get(d3);
    CHECK(!d.has_value());
*/
    struct Res {
        Database::DocID id;
        Database::DocID prevId;
        std::string_view text;
    };

    Res res[] = {
            {1,0,"hello"},
            {2,0,"world"},
            {3,0,"bar"},
            {4,0,"foo"},
            {5,3,"baz"}
    };

    {
        Database::Iterator iter = storage.scan_from(d1);
        const Res *pt = res;
        while (iter.next()) {
            auto doc = iter.value();
            auto id = iter.id();
            auto prev_id = iter.prev_id();
            auto [txt] = doc->get<std::string_view>();
            CHECK_EQUAL(id, pt->id);
            CHECK_EQUAL(prev_id, pt->prevId);
            CHECK_EQUAL(txt, pt->text);
            pt++;
        }
    }

}


int main() {
    test1();
}
