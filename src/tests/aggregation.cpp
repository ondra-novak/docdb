#include "check.h"
#include "../docdb/storage.h"
#include "../docdb/indexer.h"
#include "../docdb/aggregator.h"
#include "memdb.h"

#include "../docdb/aggregate_rows.h"

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



void test1() {

    using DocumentDef = docdb::FixedRowDocument<std::string_view, int>;
    using IndexDef = docdb::FixedRowDocument<int>;

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    using Storage = docdb::Storage<DocumentDef>;
    using Index = docdb::Indexer<Storage, [](auto emit, const auto &doc){
        auto [text,number] = doc.get();
        emit({text.length()},{number});
    },1,docdb::IndexType::multi, IndexDef>;
    using StatsAggregator = docdb::Aggregator<Index,
            docdb::reduceKey<std::size_t>(),
            docdb::aggregate_rows<docdb::Composite<int,
                    docdb::Count<int>, docdb::Sum<int>, docdb::Min<int>, docdb::Max<int>
                > >,1, docdb::UpdateMode::manual>;


    Storage storage(db, "test_storage");
    Index index(storage, "test_index");
    StatsAggregator aggr(index, "test_aggr");



    for (auto c: words) {
        storage.put({c.first,c.second});
    }

    aggr.update();

    for (const auto &x: aggr.select_all()) {
        auto [k] = x.key.get<std::size_t>();
        auto [count, sum, min, max] = x.value.get<int, int,int, int>();
        std::cout << k << ": count=" << count << ", sum=" << sum  << ", max=" << max <<", min=" << min << std::endl;
    }

    storage.put({"aaa",2});
    for (const auto &x:  index.lookup(std::size_t(13))) {
        storage.erase(x.id);
    }
    aggr.update();
    std::cout << "---------------" << std::endl;
    for (const auto &x: aggr.select_all()) {
        auto [k] = x.key.get<std::size_t>();
        auto [count, sum, min, max] = x.value.get<int, int,int, int>();
        std::cout << k << ": count=" << count << ", sum=" << sum  << ", max=" << max <<", min=" << min << std::endl;
    }
}
int main() {


    test1();
}
