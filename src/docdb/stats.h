#pragma once
#ifndef SRC_DOCDB_STATS_H_
#define SRC_DOCDB_STATS_H_

#include "serialize.h"

#include <cmath>
namespace docdb {

struct StatData {
    std::uint64_t count;
    double sum;
    double sum2;
    double min;
    double max;

};

template<>
class CustomSerializer<StatData> {
public:
    template<typename Iter>
    static Iter serialize(const StatData &val, Iter target) {
        return BasicRow::serialize_items(target, val.count, val.sum, val.sum2, val.max, val.min);
    }
    template<typename Iter>
    static StatData deserialize(Iter &at, Iter end) {
        StatData s;
        s.count = BasicRow::deserialize_item<std::uint64_t>(at, end);
        s.sum = BasicRow::deserialize_item<double>(at, end);
        s.sum2 = BasicRow::deserialize_item<double>(at, end);
        s.min = BasicRow::deserialize_item<double>(at, end);
        s.max = BasicRow::deserialize_item<double>(at, end);
        return s;
    }

};

struct Count {
    template<typename Emit, typename Iter>
    void operator()(Emit &emit, Iter &iter) {
        if (iter.next()) {
            std::size_t count = 0;
            do {
                ++count;
            } while (iter.next());
            emit(BasicRow(static_cast<double>(count)));
        }
    }
};

struct Sum {
    template<typename Emit, typename Iter>
    void operator()(Emit &emit, Iter &iter) {
        if (iter.next()) {
            Buffer<double,8> numbers;
            do {
                BasicRowView rw = iter.value();
                int p = 0;
                while (!rw.empty()) {
                    if (p >= numbers.size()) numbers.push_back(0.0);
                    auto [v,c] = rw.get<double, Blob>();
                    rw = c;
                    if (!std::isnan(v)) numbers[p] += v;
                    ++p;
                }
            } while (iter.next());
            BasicRow out;
            for (double x: numbers) out.append(x);
            emit(out);
        }
    }
};

struct Stats {
    template<typename Emit, typename Iter>
    void operator()(Emit &emit, Iter &iter) {
        if (iter.next()) {
            Buffer<StatData,8> numbers;
            do {
                BasicRowView rw = iter.value();
                int p = 0;
                while (!rw.empty()) {
                    auto [v,c] = rw.get<double, Blob>();
                    if (!std::isnan(v)) {
                        if (p >= numbers.size()) {
                            numbers.push_back(StatData{1,v,v,v,v*v});
                        } else {
                            StatData &d = numbers[p];
                            ++d.count;
                            d.max = std::max(d.max, v);
                            d.min = std::max(d.min, v);
                            d.sum += v;
                            d.sum2 += v*v;
                        }
                        rw = c;
                    }
                    ++p;
                }
            } while (iter.next());
            BasicRow out;
            for (StatData x: numbers) out.append(x);
            emit(out);
        }
    }
};

struct StatsOfStats {
    template<typename Emit, typename Iter>
    void operator()(Emit &emit, Iter &iter) {
        if (iter.next()) {
            Buffer<StatData,8> numbers;
            do {
                BasicRowView rw = iter.value();
                int p = 0;
                while (!rw.empty()) {
                    auto [v,c] = rw.get<StatData, Blob>();
                    if (p >= numbers.size()) {
                        numbers.push_back(v);
                    } else {
                        StatData &d = numbers[p];
                        d.count += v.count;
                        d.max = std::max(d.max, v.max);
                        d.min = std::max(d.min, v.min);
                        d.sum += v.sum;
                        d.sum2 += v.sum2;
                    }
                    rw = c;
                    ++p;
                }
            } while (iter.next());
            BasicRow out;
            for (StatData x: numbers) out.append(x);
            emit(out);
        }
    }
};

struct KeyIdent {
    template<typename Emit>
    void operator()(Emit &emit, const BasicRowView &key) {
        emit(key, key);
    }
};



}


#endif /* SRC_DOCDB_STATS_H_ */
