#include "check.h"
#include "../docdb/doc_storage.h"
#include "../docdb/doc_index.h"
#include "../docdb/aggregate.h"

#include "memdb.h"

static std::string_view words[] = {
        "feed",
        "well",
        "proud",
        "represent",
        "accurate",
        "recruit",
        "reservoir",
        "pity",
        "sigh",
        "venus",
        "review",
        "wolf",
        "world",
        "retired",
        "bitch",
        "regular",
        "stock",
        "fame",
        "notorious",
        "cellar",
        "forecast",
        "retire",
        "clearance",
        "seal",
        "comedy",
        "cylinder",
        "concentration",
        "falsify",
        "exile",
        "reward"
};

void test1() {

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    using Storage = docdb::DocumentStorage<docdb::StringDocument>;
    using Index = docdb::DocumentIndex<Storage>;
    using SumAndCountAggr = docdb::AggregateIndex<Index, docdb::BasicRowDocument>;

    Storage storage(db, "test_storage");
    Index index(storage, "test_index", 1, [](auto &emit, std::string_view doc){
        emit({doc.length()},docdb::BasicRow(1));
    });
    SumAndCountAggr aggr(index, "test_aggr", 1, [](auto &emit, const docdb::BasicRowView &key){
        auto [v] = key.get<std::size_t>();
        emit({v},{v});
    },SumAndCountAggr::AggregateFn([](auto &iter)->docdb::Value<docdb::BasicRowDocument>{
        double count = 0;
        if (iter.next()) {
            do {
                auto val = iter.value();
                auto [n] = val.template get<double>();
                count = count + n;
            }while (iter.next());
            docdb::BasicRow x(count);
            return docdb::BasicRowView(x);
        } else {
            return {};
        }
    }));
    for (auto c: words) {
        storage.put(c);
    }


    {
        auto s = aggr.scan();
        while (s.next()) {
            auto [v] = s.value().get<double>();
            auto [k] = s.key().get<std::size_t>();
            std::cout << k << ": " << v << std::endl;
        }
    }
    storage.put("aaa");
    auto g = index.lookup(docdb::BasicRow(std::size_t(13)));
    while (g.next()) {
        storage.erase(g.id());
    }
    {
        auto s = aggr.scan();
        while (s.next()) {
            auto [v] = s.value().get<double>();
            auto [k] = s.key().get<std::size_t>();
            std::cout << k << ": " << v << std::endl;
        }
    }







}


int main() {
    test1();
}
