#include "check.h"
#include "../docdb/storage.h"

#include "memdb.h"

#include <docdb/incremental_aggregator.h>

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

using DocumentDef = docdb::FixedRowDocument<std::string_view, int>;
using IndexDef = docdb::FixedRowDocument<int>;


struct AggrFunction {
    static constexpr std::size_t revision = 1;
    template<typename Emit>
    void operator()(Emit emit, const DocumentDef::Type &doc) const {
        auto [str, val] = doc.get();
        std::size_t key = str.length();
        auto cur_val_row = emit(key);
        if constexpr(emit.erase) {
            if (cur_val_row.has_value()) {
                auto [count,sum] = cur_val_row->template get<std::size_t, int>();
                count-=1;
                sum-=val;
                if (count) {
                    cur_val_row.put({count,sum});
                } else {
                    cur_val_row.erase();
                }
            }
        } else {
            auto [count,sum] = cur_val_row?cur_val_row->template get<std::size_t, int>():std::make_tuple<std::size_t, int>(0,0);
            count+=1;
            sum+=val;
            cur_val_row.put({count,sum});
        }
    }
};

void test1() {


    auto ramdisk = newRamdisk();
    auto db = docdb::Database::create(createTestDB(ramdisk.get()));

    using Storage = docdb::Storage<DocumentDef>;
    using Aggreg = docdb::IncrementalAggregator<Storage, AggrFunction>;


    Storage storage(db, "test_storage");
    Aggreg aggr(storage, "test_aggr");



    for (auto c: words) {
        storage.put({c.first,c.second});
    }


    for (const auto &x: aggr.select_all()) {
        auto [k] = x.key.get<std::size_t>();
        auto [count, sum] = x.value.get<std::size_t, int>();
        std::cout << k << ": count=" << count << ", sum=" << sum  << std::endl;
    }

    storage.put({"aaa",2});
    storage.erase(27);
    std::cout << "---------------" << std::endl;
    for (const auto &x: aggr.select_all()) {
        auto [k] = x.key.get<std::size_t>();
        auto [count, sum] = x.value.get<std::size_t, int>();
        std::cout << k << ": count=" << count << ", sum=" << sum  << std::endl;
    }
}
int main() {


    test1();
}
