#include "check.h"
#include "memdb.h"

#include "../docdb/storage.h"
#include "../docdb/indexer.h"


template<typename Index, std::size_t N>
void check_result(const Index &index, const std::pair<std::string_view, int> (&vals)[N]) {
    std::size_t i = 0;
    for (const auto &row: index.select_all()) {
        CHECK_LESS(i , N);
        auto [k] = row.key.template get<std::string_view>();
        auto [v] = row.value.get();
        CHECK_EQUAL(k, vals[i].first);
        CHECK_EQUAL(v, vals[i].second);
        ++i;
    }
    CHECK_EQUAL(i, N);
}


struct IndexFn {
    static constexpr int revision = 1;
    template<typename Emit>
    void operator()(Emit emit, const auto &doc) const {
        auto [txt, val] = doc.get();
        emit(txt,val);
    };

};



void test1() {


    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    using Storage = docdb::Storage<docdb::FixedRowDocument<std::string_view, int> >;
    using Index = docdb::Indexer<Storage, IndexFn, docdb::IndexType::unique_hide_dup,docdb::FixedRowDocument<int> >;

    Storage storage(db, "test_storage");
    Index index(storage, "test_index");

    docdb::DocID id = storage.put({"aaa", 5});
    storage.put({"bbb", 3});
    storage.put({"aaa", 2});
    id = storage.put({"aaa", 12}, id);



    {
        auto b = db->begin_batch();
        storage.put(b, {"aaa", 13}, id);
        storage.put(b, {"aaa", 20}, id);
        storage.put(b, {"aaa", 70}, id);
        b.commit();
    }

    check_result(index, {
            {"aaa",2},
            {"bbb",3}
    });

    for (auto row: index.select("aaa")) {
        storage.erase(row.id);
    }

    check_result(index, {
            {"aaa",13},
            {"bbb",3}
    });


    for (auto row: index.select("aaa")) {
        storage.erase(row.id);
    }

    check_result(index, {
            {"aaa",20},
            {"bbb",3}
    });
}


int main() {



    test1();
}
