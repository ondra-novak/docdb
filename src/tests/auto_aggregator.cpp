#include "check.h"
#include "../docdb/storage.h"
#include "../docdb/indexer.h"

#include "memdb.h"

#include <docdb/aggregator.h>
#include <docdb/aggregate_rows.h>

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


struct IndexFn {
    static constexpr int revision = 1;
    template<typename Emit, typename Doc>
    void operator()(Emit emit, const Doc &doc)const{
        auto [text,number] = doc.get();
        emit({text.length()},{number});
    }
};




static auto aggrSumFn = [sum = 0](const docdb::Row &row) mutable -> const int & {
    auto [v] = row.get<int>();
    return sum+=v;
};

inline constexpr std::wstring_view wgroup_concat_default_delimiter{L","};
template struct docdb::GroupConcat<>;
template struct docdb::GroupConcat<docdb::Blob>;
template struct docdb::GroupConcat<std::wstring_view,&wgroup_concat_default_delimiter>;

void test1() {

    using DocumentDef = docdb::FixedRowDocument<std::string_view, int>;
    using IndexDef = docdb::FixedRowDocument<int>;

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    using Storage = docdb::Storage<DocumentDef>;
    using Index = docdb::Indexer<Storage, IndexFn ,docdb::IndexType::multi, IndexDef>;
    using StatsAggregator = docdb::AggregateBy<std::tuple<std::size_t> >::Materialized<Index,
            docdb::AggregateRows<docdb::Composite<int,
                    docdb::Count<int>, docdb::Sum<int>, docdb::Min<int>, docdb::Max<int>
                > >, docdb::RowDocument, true>;


    Storage storage(db, "test_storage");
    Index index(storage, "test_index");
    StatsAggregator aggr(index, "test_aggr");


    for (auto c: words) {
        storage.put({c.first,c.second});
    }

    for (auto row: docdb::AggregateBy<std::tuple<std::size_t> >::make_recordset(index.select_all(), aggrSumFn)){
        std::cout << std::get<0>(row.key) << ": " << row.value << std::endl;
    }

    aggr.update();

    for (const auto &x: aggr.select_all()) {
        auto [k] = x.key.get<std::size_t>();
        auto [count, sum, min, max] = x.value.get<std::size_t, int,int, int>();
        std::cout << k << ": count=" << count << ", sum=" << sum  << ", max=" << max <<", min=" << min << std::endl;
    }

    storage.put({"aaa",2});
    for (const auto &x:  index.select(std::size_t(13))) {
        storage.erase(x.id);
    }
    aggr.update();
    std::cout << "---------------" << std::endl;
    for (const auto &x: aggr.select_all()) {
        auto [k] = x.key.get<std::size_t>();
        auto [count, sum, min, max] = x.value.get<std::size_t, int,int, int>();
        std::cout << k << ": count=" << count << ", sum=" << sum  << ", max=" << max <<", min=" << min << std::endl;
    }
}

struct IndexFn2 {
    static constexpr int revision = 1;
    template<typename Emit, typename Doc>
    void operator()(Emit emit, const Doc &doc)const{
        auto [number] = doc.get();
        emit({number},{number});
    }
};


void test2() {
    using DocumentDef = docdb::FixedRowDocument<int>;
    using IndexDef = docdb::FixedRowDocument<int>;

    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    using Storage = docdb::Storage<DocumentDef>;
    using Index = docdb::Indexer<Storage, IndexFn2 ,docdb::IndexType::unique, IndexDef>;
    using StatsAggregator = docdb::AggregateBy<std::tuple<docdb::ValueGroup<100> > >::Materialized<Index,
            docdb::AggregateRows<docdb::Composite<int,
                    docdb::Count<int>, docdb::Sum<int>, docdb::Min<int>, docdb::Max<int>
                > >, docdb::RowDocument, true >;

    int values[] = {125,978,188,758,584,256,336,121,745,985,335,251,142,
            258,155,192,669,122,228,126,124,365,252,564,236,981,345,233,
            123,457,114,946,368,159,658,751,581,185,678,916,672,638,285};

    Storage storage(db, "test_storage");
    Index index(storage, "test_index");
    StatsAggregator aggr(index, "test_aggr");


    for (int a: values) {
        storage.put(a);
    }

    for (auto row: docdb::AggregateBy<std::tuple<docdb::ValueGroup<100> > >::make_recordset(index.select_all(), aggrSumFn)){
        std::cout << std::get<0>(row.key) << ": " << row.value << std::endl;
    }

    aggr.update();

    for (const auto &x: aggr.select_all()) {
        auto [k] = x.key.get<int>();
        auto [count, sum, min, max] = x.value.get<std::size_t, int,int, int>();
        std::cout << k << ": count=" << count << ", sum=" << sum  << ", max=" << max <<", min=" << min << std::endl;
    }



}
int main() {


    test1();
    test2();
}
