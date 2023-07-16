#include "check.h"
#include "memdb.h"

#include "../docdb/storage.h"
#include "../docdb/indexer.h"

struct Index_TextToLenFn {
    static constexpr int revision = 1;
    template<typename Emit>
    void operator()(Emit emit, const std::string_view &doc) const {
        emit(doc,doc.length());
    }
};
struct Index_LenToTextFn {
    static constexpr int revision = 1;
    template<typename Emit>
    void operator()(Emit emit, const std::string_view &doc) const {
        emit(doc.length(), doc);
    }
};

using Storage = docdb::Storage<docdb::StringDocument>;
using Index_TextToLen = docdb::Indexer<Storage, Index_TextToLenFn>;

using Index_LenToText = docdb::Indexer<Storage, Index_LenToTextFn>;


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
       CHECK_EQUAL(index1.get_document(x)->document, "world");
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
                CHECK_EQUAL(docref->document, "bar");
            }
            ++iter;
            CHECK(iter != recordset.end());
            {
                auto &x = *iter;
                CHECK_EQUAL(x.id,4);
                auto docref = index2.get_document(x);
                CHECK_EQUAL(docref->document, "foo");
            }
            ++iter;
            CHECK(iter == recordset.end());
        }
        {
            auto [index1_view, storage_view] = docdb::make_snapshot(index1,storage);
            docdb::DocID id = 2;
            auto lk = index1_view.find({"world",id});
            CHECK(lk);
            auto docref = storage_view.find(id);
            CHECK_EQUAL(docref->document, "world");
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
