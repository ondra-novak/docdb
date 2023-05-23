#include "check.h"
#include "../docdb/doc_storage.h"
#include "../docdb/doc_index.h"
#include "../docdb/aggregate.h"
#include "../docdb/stats.h"

#include "memdb.h"

static std::pair<std::string_view,int> words[] = {
        {"feed",56},
        {"well",73},
        {"proud",74},
        {"represent",24},
        {"accurate",63},
        {"recruit",8},
        {"reservoir",74},
        {"pity",93},
        {"sigh",46},
        {"venus",44},
        {"review",97},
        {"wolf",82},
        {"world",9},
        {"retired",72},
        {"bitch",48},
        {"regular",63},
        {"stock",45},
        {"fame",50},
        {"notorious",2},
        {"cellar",14},
        {"forecast",77},
        {"retire",68},
        {"clearance",73},
        {"seal",34},
        {"comedy",3},
        {"cylinder",43},
        {"concentration",10},
        {"falsify",36},
        {"exile",79},
        {"reward",99}
};

decltype(auto) operator<<(std::ostream &out, const docdb::StatData &data) {
    return (out << "count=" << data.count << ", sum=" << data.sum
               << ", sum^2=" << data.sum2 << ", min=" << data.min
               << ", max=" << data.max) << ", avg=" << data.sum/data.count;
}

void test1() {

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    using Storage = docdb::DocumentStorage<docdb::RowDocument>;
    using Index = docdb::DocumentIndex<Storage>;
    using SumAndCountAggr = docdb::AggregateIndex<Index, docdb::RowDocument>;

    Storage storage(db, "test_storage");
    Index index(storage, "test_index", 1, [](auto &emit, const docdb::Row &doc){
        auto [text,number] = doc.get<std::string_view, int>();
        emit({text.length()},{number});
    });
    SumAndCountAggr aggr(index, "test_aggr", 1, docdb::KeyReduce<std::size_t>(), docdb::Stats());

    for (auto c: words) {
        storage.put({c.first,c.second});
    }


    {
        auto s = aggr.scan();
        while (s.next()) {
            auto [v] = s.value().get<docdb::StatData>();
            auto [k] = s.key().get<std::size_t>();
            std::cout << k << ": " << v << std::endl;
        }
    }
    storage.put(docdb::Row{"aaa",2});
    auto g = index.lookup(13);
    while (g.next()) {
        storage.erase(g.id());
    }
    {
        auto s = aggr.scan();
        while (s.next()) {
            auto [v] = s.value().get<docdb::StatData>();
            auto [k] = s.key().get<std::size_t>();
            std::cout << k << ": " << v << std::endl;
        }
    }







}


int main() {
    test1();
}
