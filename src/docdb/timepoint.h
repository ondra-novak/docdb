#pragma once
#include <chrono>

namespace docdb {

template<>
class CustomSerializer<std::chrono::system_clock::time_point> {
public:
    using X = std::chrono::system_clock::time_point;
    template<typename Iter>
    static Iter serialize(const X &val, Iter iter) {
        return docdb::Row::serialize_items(iter, val.time_since_epoch().count());
    }
    template<typename Iter>
    static X deserialize(Iter &at, Iter iter) {
        using dur = X::duration;
        using rep = dur::rep;
        auto r = docdb::Row::deserialize_item<rep>(at,iter);
        return X(dur(r));
    }

};


