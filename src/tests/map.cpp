#include <docdb/map.h>

#include "memdb.h"

#include "check.h"

int main() {
    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));
    docdb::Map<docdb::StringDocument> m(db, "test_map");

    void (*testfn)(docdb::Batch &, const docdb::Key &, const std::string_view &);


    m.register_transaction_observer([&](docdb::Batch &b, const docdb::Key &k, const std::string_view &doc, bool) {
        testfn(b, k, doc);
    });

    static bool called = false;


    auto testfn_Hello = [](docdb::Batch &b, const docdb::Key &k, const std::string_view &v) {
        called = true;
        auto [kv] = k.get<std::size_t>();
        CHECK_EQUAL(kv, 42);
        CHECK_EQUAL(v, "Hello");
    };

    testfn = testfn_Hello;

    m.put({std::size_t(42)},"Hello");
    CHECK(called);
    called = false;


    testfn = [](docdb::Batch &b, const docdb::Key &k, const std::string_view &v) {
        called = true;
        auto [kv] = k.get<std::size_t>();
        CHECK_EQUAL(kv, 56);
        CHECK_EQUAL(v, "World");
    };

    m.put({std::size_t(56)},"World");
    CHECK(called);
    called = false;

    {
        auto doc1 = m.get({std::size_t(42)});
        CHECK(doc1.has_value());
        CHECK_EQUAL(*doc1, "Hello");
        auto doc2 = m.get({std::size_t(56)});
        CHECK(doc2.has_value());
        CHECK_EQUAL(*doc2, "World");
    }

    {
        auto rs = m.select_all();
        auto iter = rs.begin();
        CHECK(iter != rs.end());
        auto [k1] = iter->key.get<std::size_t>();
        CHECK_EQUAL(k1, 42);
        CHECK_EQUAL(iter->value, "Hello");
        ++iter;
        CHECK(iter != rs.end());
        auto [k2] = iter->key.get<std::size_t>();
        CHECK_EQUAL(k2, 56);
        CHECK_EQUAL(iter->value, "World");
        ++iter;
        CHECK(iter == rs.end());
    }


    testfn = testfn_Hello;
    m.erase({std::size_t(42)});
    CHECK(called);

    {
        auto doc1 = m.get({std::size_t(42)});
        CHECK(!doc1.has_value());
        auto doc2 = m.get({std::size_t(56)});
        CHECK(doc2.has_value());
        CHECK_EQUAL(*doc2, "World");
    }



}
