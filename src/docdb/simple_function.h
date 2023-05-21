#pragma once
#ifndef SRC_DOCDB_SIMPLE_FUNCTION_H_
#define SRC_DOCDB_SIMPLE_FUNCTION_H_

#include "concepts.h"


namespace docdb {

template<auto> struct SimpleMemberCallHelper;
template<typename T, typename R, typename ... Args, R (T::*x)(Args ... args)>
struct SimpleMemberCallHelper<x> {
    using Type = T;
    using RetVal = R;
    static RetVal call(void *ctx, Args ... args) {
        auto ptr = reinterpret_cast<Type *>(ctx);
        return (ptr->*x)(std::forward<Args>(args)...);
    }
};

///Simple function wrapper
/**
 * This function occupies fixed space, just two pointers. It is able to
 * call standard function with a context pointer or member function without
 * context (expect the pointer to an object)
 *
 * @tparam R return value
 * @tparam Args arguments
 *
 * Wrapper is used to call member functions "&Type::fn". Use member_fn. It
 * is equivalent to function [this](args...){...}
 */
template<typename R, typename ... Args>
struct SimpleFunction {
public:
    using Fn = R (*)(void *, Args ...);

    SimpleFunction() = default;

    ///Wrap member function call. The member function is carried as template argument
    /**
     * @tparam x param member function pointer
     * @param ptr pointer to object
     * @return SimpleFunction instance
     */
    template<auto x>
    CXX20_REQUIRES(requires {typename SimpleMemberCallHelper<x>::Type;})
    static SimpleFunction member_fn(typename SimpleMemberCallHelper<x>::Type *ptr) {
        return {&SimpleMemberCallHelper<x>::call,ptr};
    }
    SimpleFunction(Fn fn, void *ctx):_fn(fn),_ctx(ctx) {}

    ///Call function
    template<typename ... A>
    CXX20_REQUIRES(std::is_convertible_v<A, Args> && ...)
    R operator()(A && ... args) const {
        return _fn(_ctx, std::forward<Args>(args)...);
    }
    ///Returns true, if function is set
    operator bool() const {
        return _fn != nullptr;
    }
protected:
    Fn _fn = nullptr;
    void *_ctx = nullptr;

};


}




#endif /* SRC_DOCDB_SIMPLE_FUNCTION_H_ */
