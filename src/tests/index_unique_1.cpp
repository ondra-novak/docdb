#include "check.h"
#include "memdb.h"

#include "../docdb/storage.h"
#include "../docdb/indexer.h"

using Storage = docdb::Storage<docdb::StringDocument>;
using Index_TextToLen = docdb::Indexer<Storage, [](auto emit, const std::string_view &doc) {
    emit(doc,doc.length());
}, 1, docdb::IndexType::unique>;



void test1() {

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));
    Storage storage(db, "test_storage");
    Index_TextToLen index1(storage, "text_to_len");

    docdb::DocID d1,d2,d3,d4;
    d1 = storage.put("hello");
    d2 = storage.put("world");
    d3 = storage.put("bar");
    d4 = storage.put("foo");
    CHECK_EQUAL(d1,1);
    CHECK_EQUAL(d2,2);
    CHECK_EQUAL(d3,3);
    CHECK_EQUAL(d4,4);

    int fnd = 0;
    for(const auto &x: index1.lookup({"world"})) {
       CHECK_EQUAL(x.id, 2);
       auto [v] = x.value.get<std::size_t>();
       CHECK_EQUAL(v, 5);
       CHECK_EQUAL(*x.document()->content, "world");
       fnd++;
    }

    CHECK_EQUAL(fnd,1);

        {
            std::pair<std::string_view, std::size_t> results[] = {
                    {"bar",3},
                    {"foo",3},
                    {"hello",5},
                    {"world",5},
            };
            int r = 0;
            for (const auto &val: index1.select_all()) {
                auto [k1] = val.key.get<std::string_view>();
                auto [v1] = val.value.get<std::size_t>();
                CHECK_EQUAL(k1, results[r].first);
                CHECK_EQUAL(v1, results[r].second);
                ++r;
            }
        }

}


int main() {



    test1();
}
