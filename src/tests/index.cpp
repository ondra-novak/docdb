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

    int fnd = 0;
    for(const auto &x: index1 == "world") {
       CHECK_EQUAL(x.id, 2);
       auto [v] = x.value.get<std::size_t>();
       CHECK_EQUAL(v, 5);
       CHECK_EQUAL(index1.get_document(x)->content, "world");
       fnd++;
    }

    CHECK_EQUAL(fnd,1);

        {
            auto recordset = index2 == std::size_t(3);
            auto iter = recordset.begin();
            CHECK(iter != recordset.end());
            {
                auto &x = *iter;
                CHECK_EQUAL(x.id,3);
                auto docref = index2.get_document(x);
                CHECK_EQUAL(docref->content, "bar");
            }
            ++iter;
            CHECK(iter != recordset.end());
            {
                auto &x = *iter;
                CHECK_EQUAL(x.id,4);
                auto docref = index2.get_document(x);
                CHECK_EQUAL(docref->content, "foo");
            }
            ++iter;
            CHECK(iter == recordset.end());
        }
        {
            docdb::DocID id = 2;
            auto lk = index1.find({"world",id});
            CHECK(lk);
            auto docref = storage.find(id);
            CHECK_EQUAL(docref->content, "world");
        }
        storage.put("world2",d2);
        {
            auto rs1 = index1 == "world";
            CHECK(rs1.begin() == rs1.end());
            auto rs2 = index1 == "world2";
            CHECK(rs2.begin() != rs2.end());
        }


}


int main() {



    test1();
}
