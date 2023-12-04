#pragma once
#ifndef SRC_DOCDB_TYPES_H_
#define SRC_DOCDB_TYPES_H_
#include <cstdint>


namespace docdb {

using DocID = std::uint64_t;
using KeyspaceID = std::uint8_t;

template<class T> T& unmove(T&& t) { return static_cast<T&>(t); }

template<typename Fn>
class EmplaceByReturn {
public:
    using RetType = std::invoke_result_t<Fn>;
    EmplaceByReturn(Fn &&fn):_fn(std::forward<Fn>(fn)) {}
    explicit operator RetType() const {return _fn();}
protected:
    Fn _fn;
};

template<typename Fn>
EmplaceByReturn(Fn fn) -> EmplaceByReturn<Fn>;


}





#endif /* SRC_DOCDB_TYPES_H_ */
