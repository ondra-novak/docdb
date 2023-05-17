#pragma once
#ifndef SRC_DOCDB_SERIALIZE_H_
#define SRC_DOCDB_SERIALIZE_H_

#include "concepts.h"

namespace docdb {

///Simple string document - document is a single string
struct StringDocument {
    using Type = std::string;
    template<typename Iter>
    static void to_binary(const Type &src, Iter insert) {
        for (auto c: src) {
            *insert = c;
            ++insert;
        }
    }
    template<typename Iter>
    static Type from_binary(Iter beg, Iter end) {
        const char *c = &(*beg);
        auto dist = std::distance(beg, end);
        return std::string_view(c, dist);
    }
};

static_assert(DocumentDef<StringDocument>);

}




#endif /* SRC_DOCDB_SERIALIZE_H_ */
