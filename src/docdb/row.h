#pragma once
#ifndef SRC_DOCDB_ROW_H_
#define SRC_DOCDB_ROW_H_

#include "buffer.h"
#include "serialize.h"

#include <algorithm>
#include <variant>
#include <ieee754.h>
#include <cstdint>

namespace docdb {

using RowBuffer = Buffer<char, 40>;

template<typename T>
struct ConstructVariantHelper {

    template<int index>
    struct DoIt{
        static constexpr int val = index;
    };
};


class Row {
public:

    Row() = default;
    template<typename ... Args>
    Row(const Args & ... args) {
        serialize_items(back_inserter(), args...);
    }

    operator leveldb::Slice() const {
        return {_data.data(),_data.size()};
    }

    operator std::string_view() const {
        return {_data.data(),_data.size()};
    }

    ///Retrieve internal mutable buffer
    /**
     * This function works also in read-only mode. In this case,
     * it replaces content with mutable copy
     *
     * @return mutable buffer
     */
    RowBuffer &mutable_buffer() {
        return _data;

    }

    ///Retrieve mutable pointer
    /**
     * Mutable pointer allows to modify content of internal buffer.
     * This function works, even if the row is in readonly mode. In
     * this case, it switches the read-write mode (copies the content)
     * @return mutable pointer
     */
    char *mutable_ptr() {
        return _data.data();
    }

    ///Retrieve size of internal buffer
    std::size_t size() const {
        return _data.size();
    }

    ///Determines, whether internal buffer is empty
    bool empty() const {
        return _data.empty();
    }

    ///Clears content and switches to read-write mode
    void clear() {
        _data.clear();
    }

    void resize(std::size_t sz, char init = 0) {
        _data.resize(sz, init);
    }

    RowBuffer::const_iterator begin() const {
        return _data.begin();
    }
    RowBuffer::const_iterator end() const {
        return _data.end();
    }

    ///Serialize items into binary container
    /**
     * @param iter output iterator
     * @param val value to serialize
     * @param args additional values
     * @return position of iterator after serialization
     */
    template<typename Iter, typename X, typename ... Args>
    static Iter serialize_items(Iter iter, const X &val, const Args & ... args) {
        if constexpr(IsTuple<X>) {
            iter = std::apply([&](const auto & ... args){
                return serialize_items(iter, args...);
            }, val);
        } else if constexpr(IsVariant<X>) {
            *iter++ = static_cast<std::uint8_t>(val.index());
            iter = std::visit([&](const auto &v){
                return serialize_items(iter, v);
            },val);
        } else if constexpr(IsStdArray<X>) {
            for (const auto &x: val) iter = serialize_items(iter, x);
        } else if constexpr(std::is_same_v<X, char>) {
            *iter++=val;
        } else if constexpr(std::is_null_pointer_v<X> || std::is_same_v<X, std::monostate>) {
            //empty;
        } else if constexpr(std::is_same_v<X, Row> || std::is_base_of_v<Blob, X>) {
            iter = std::copy(val.begin(), val.end(), iter);
        } else if constexpr(std::is_same_v<bool, X>) {
            *iter++ = val?static_cast<char>(1):static_cast<char>(0);
        } else if constexpr(std::is_enum_v<X>) {
            *iter++ = static_cast<std::uint8_t>(val);
        } else if constexpr(std::is_unsigned_v<X> || std::is_same_v<X, wchar_t>) {
            X v = val;
            for (std::size_t i = 0; i < sizeof(X); i++) {
                int shift = 8*(sizeof(X)-i-1);
                *iter++=static_cast<char>((v >> shift) & 0xFF);
            }
        } else if constexpr(std::is_integral_v<X>) {
            static_assert(!std::is_unsigned_v<X>);
            using UVal = std::make_unsigned_t<X>;
            UVal v = static_cast<UVal>(val) ^ (UVal(1) << (sizeof(UVal)*8-1));
            iter =  serialize_items(iter, v);
        } else if constexpr(std::is_convertible_v<X, double>) {
            double d = val;
            ieee754_double hlp{d};
            std::uint64_t bin_val = (std::uint64_t(hlp.ieee.negative) << 63)
                    | (std::uint64_t(hlp.ieee.exponent) << 52)
                    | (std::uint64_t(hlp.ieee.mantissa0) << 32)
                    | (std::uint64_t(hlp.ieee.mantissa1));
            std::uint64_t mask = hlp.ieee.negative?~std::uint64_t(0):(std::uint64_t(1)<<63);
            iter = serialize_items(iter, bin_val ^ mask);
        } else if constexpr(std::is_base_of_v<LocalizedString, X>) {
            auto &f = std::use_facet<std::collate<char> >(val.get_locale());
            std::string out = f.transform(val.begin(), val.end());
            iter = serialize_items(iter, out);
        } else if constexpr(std::is_base_of_v<LocalizedWString, X>) {
            auto &f = std::use_facet<std::collate<wchar_t> >(val.get_locale());
            auto out = f.transform(val.begin(), val.end());
            iter = serialize_items(iter, out);
        } else if constexpr(std::is_array_v<std::remove_reference_t<X> >) {
            auto beg = std::begin(val);
            auto end = std::end(val);
            auto sz = std::distance(beg,end);
            if (sz > 0) {
                auto last = beg;
                std::advance(last, sz-1);
                if (*last == 0) end = last;
            }
            if constexpr(sizeof(val[0]) == 1) {
                iter = std::copy(beg, end, iter);
                *iter++ = '\0';
            } else {
                using Type = decltype(val[0]);
                for (auto i = beg; i != end; ++i) {
                    iter = serialize_items(iter, *i);
                }
                iter = serialize_items(iter, Type{});
            }
        } else if constexpr(std::is_same_v<std::decay_t<X>, const char *>) {
            const char *c = val;
            do {
                *iter++ = *c;
            } while (*c++);
        } else if constexpr(std::is_convertible_v<X, std::string_view>) {
            iter = std::copy(std::begin(val), std::end(val), iter);
            *iter++ = '\0';
        } else if constexpr(std::is_convertible_v<X, std::wstring_view>) {
            for (auto c: val) iter = serialize_items(iter, c);
            serialize_items(iter, wchar_t(0));
        } else if constexpr(IsContainer<X>) {
            std::uint16_t count = std::min<std::size_t>(0xFFFF,std::distance(val.begin(), val.end()));
            iter =serialize_items(iter, count);
            auto x = val.begin();
            for (std::uint16_t i = 0; i < count; ++i) {
                iter = serialize_items(iter, *x);
                std::advance(x,1);
            }
        } else if constexpr(IsOptional<X>) {
            if (val.has_value()) {
                *iter++ = '\x01';
                iter = serialize_items(iter, val.value());
            } else {
                *iter++ = '\0';
            }
        } else {
            iter = CustomSerializer<X>::serialize(val, iter);
        }
        return serialize_items(iter, args...);
    }

    template<typename Iter>
    static Iter serialize_items(Iter iter) {return iter;}


    ///Append additional variables
    template<typename ... Args>
    void append(const Args & ... args) {
        serialize_items(back_inserter(), args ...);
    }

    ///Retrieve values
     /***
      * @tparam Items types of items how they were serialized
      * @return tuple of values.
      *
      * @note you need to pass exact types
      */
     template <typename... Items>
     auto get() const {
         return extract<Items...>(std::string_view(*this));
     }

     ///Helper class to deserialize_tuple/2
     template<IsTuple Tup, typename Iter, std::size_t ... Is>
     static Tup deserialize_tuple(Iter &at, Iter end, std::index_sequence<Is...>) {
         return {deserialize_item<typename std::tuple_element_t<Is, Tup> >(at, end) ...};
     }


     ///Deserialize to tuple
     /**
      * @tparam Tup tuple (std::tuple<A,B,C>)
      * @tparam Iter iterator
      * @param at begin of serialized sequence
      * @param end end of serialized sequence
      * @return result (deserialized tuple);
      */
     template<IsTuple Tup, typename Iter>
     static Tup deserialize_tuple(Iter &at, Iter end) {
         return deserialize_tuple<Tup>(at, end, std::make_index_sequence<std::tuple_size_v<Tup> >{});
     }

     ///Deserialize single item
     /**
      * @tparam T type of item
      * @param at reference to iterator pointing at
      *  location where to
      * start the serialization. The function updates the iterator
      * @param end end iterator (where the binary content ends)
      * @return deserialized value
      */
     template<typename T, typename Iter>
     static T deserialize_item(Iter & at, Iter end) {
         std::size_t sz = end - at;
         if constexpr(IsTuple<T>) {
             return deserialize_tuple<T>(at, end);
         } else if constexpr(IsVariant<T>) {
             if (sz == 0) return T();
             std::uint8_t index = *at++;
             return number_to_constant<0, std::variant_size_v<T>-1 >(index, [&](auto c){
                 if constexpr(!c.valid) {
                     throw std::invalid_argument("ROW: Variant index out of range (corrupted row?)");
                     return T();
                 } else {
                     using Type = std::variant_alternative_t<c.value,T>;
                     return T(deserialize_item<Type>(at, end));
                 }
             });
        } else if constexpr(IsStdArray<T>) {
            T var;
            using ItemType = typename T::value_type;
            for (auto &x: var) x = deserialize_item<ItemType>(at, end);
            return var;
        } else if constexpr(std::is_same_v<T, char>) {
            if (at == end) return ' ';
            return *at++;
        } else if constexpr(std::is_null_pointer_v<T> || std::is_same_v<T, std::monostate>) {
             return T();
         } else if constexpr(std::is_base_of_v<Blob, T>) {
             T var(at, sz);
             at = end;
             return T(var);
         } else if constexpr(std::is_base_of_v<Row, T>) {
             auto v = deserialize_item<Blob>(at, end);
             return T(v);
         } else if constexpr(std::is_same_v<bool, T>) {
             if (sz == 0) return false;
             bool var = *at != 0;
             ++at;
             return var;
         } else if constexpr(std::is_enum_v<T>) {
             if (sz == 0) return T();;
             std::uint8_t x = static_cast<std::uint8_t>(*at);
             ++at;
             return static_cast<T>(x);
         } else if constexpr(std::is_unsigned_v<T> || std::is_same_v<T, wchar_t>) {
             T var = 0;
             auto cnt = std::min<std::size_t>(sizeof(T), sz);
             for (std::size_t i = 0; i < cnt; i++) {
                 var = (var << 8) | std::uint8_t(*at);
                 ++at;
             }
             return var;
         } else if constexpr(std::is_integral_v<T>) {
             static_assert(!std::is_unsigned_v<T>);
             using UVal = std::make_unsigned_t<T>;
             UVal v = deserialize_item<UVal>(at, end);
             return static_cast<T>(v ^ (UVal(1) << (sizeof(UVal)*8-1)));;
         } else if constexpr(IsOptional<T>) {
             bool has_value = *at != '\0';
             ++at;
             if (has_value) {
                 return T(deserialize_item<typename T::value_type>(at, end));
             } else {
                 return T();
             }
         } else if constexpr(std::is_convertible_v<double,T>) {
             auto bin_val = deserialize_item<std::uint64_t>(at, end);
             std::uint64_t mask = (std::uint64_t(1)<<63);
             auto mask2 = (bin_val & mask) != 0?mask:~std::uint64_t(0);;
             bin_val ^= mask2;
             ieee754_double hlp;
             hlp.ieee.negative = (bin_val & mask)?1:0;
             hlp.ieee.exponent = bin_val >> 52;
             hlp.ieee.mantissa0 = bin_val >> 32;
             hlp.ieee.mantissa1 = bin_val;;
             return T(hlp.d);
         } else if constexpr(std::is_same_v<const char *, T>) {
             const char *x = at;
             while (x != end && *x) ++x;
             T out = at;
             at = x+1;
             return out;
         } else if constexpr(std::is_convertible_v<std::string_view, T>) {
             const char *x = at;
             while (x != end && *x) ++x;
             std::string_view var(at, x-at);
             at = x+1;
             return T(var);
         } else if constexpr(std::is_convertible_v<std::string, T>) {
             const char *x = at;
             while (x != end && *x) ++x;
             std::string var(at, x-at);
             at = x+1;
             return T(var);
         } else if constexpr(std::is_convertible_v<std::wstring, T>) {
             std::wstring out;
             auto z = deserialize_item<wchar_t>(at, end);
             while (z) {
                 out.push_back(z);
                 z = deserialize_item<wchar_t>(at, end);
             }
             return out;

         }  else if constexpr(IsContainer<T>) {
             std::uint16_t count = deserialize_item<std::uint16_t>(at, end);
             T out;
             if constexpr(HasReserveFunction<T>) {
                 out.reserve(count);
             }
             for (std::uint16_t i = 0; i < count; ++i) {
                 out.push_back(deserialize_item<typename T::value_type>(at,end));
             }
             return out;
         }else {
             return CustomSerializer<T>::deserialize(at, end);
         }
     }


     ///Exctract items from the binary string
     /**
      * @tparam Items type of items to extract, or std::tuple<Items...>
      * @param src  binary string
      * @return extracted items returned ast std::tuple<Items...>
      */
     template <typename ... Items>
     static auto extract(std::string_view src)  {
         const char *iter = src.data();
         const char *end = src.data()+src.size();
         return extract<Items...>(iter,end);
     }

     ///Extracat items from binary sequence
     /**
      * @tparam Items list of items, or std::tuple<Items...>
      * @tparam Iter Iterator type (can be deduced)
      * @param iter starting iterator, this value is updated after extraction
      * @param end end of binary sequence
      * @return extracted items returned ast std::tuple<Items...>
      */
     template<typename ... Items, typename Iter>
     static auto extract(Iter &iter, Iter end) {
         if constexpr(IsTuple1Arg<Items...>) {
             return deserialize_item<Items...>(iter, end);
         } else {
             return deserialize_tuple<std::tuple<Items...> >(iter,end);
         }
     }


     int operator<(const Row &other) const {return std::string_view(*this) < std::string_view(other);}
     int operator>(const Row &other) const {return std::string_view(*this) > std::string_view(other);}
     int operator==(const Row &other) const {return std::string_view(*this) == std::string_view(other);}
     int operator!=(const Row &other) const {return std::string_view(*this) != std::string_view(other);}
     int operator<=(const Row &other) const {return std::string_view(*this) <= std::string_view(other);}
     int operator>=(const Row &other) const {return std::string_view(*this) >= std::string_view(other);}


protected:
    auto back_inserter() -> decltype(std::back_inserter(std::declval<RowBuffer &>())) {
       return std::back_inserter(_data);
    }

    RowBuffer _data;

};



///Fixed type which is base class for FixedRow or FixedKey

template<typename T, typename ... Args>
class FixedType: public T {
public:

    FixedType() = default;
    FixedType(const Args & ... args): T(args...) {}

    auto get() const {
        return T::template get<Args...>();
    }

    static auto extract(std::string_view src) {
        return T::template extract<Args...>(src);
    }
    template<typename Iter>
    static auto extract(Iter &at, Iter end) {
        return T::template extract<Args..., Iter>(at, end);
    }
};

template<typename ... Args>
using FixedRow = FixedType<Row, Args...>;


template<typename T>
struct _TRowDocument {
    using Type = T;
    template<typename Iter>
    static T from_binary(Iter beg, Iter end) {
        T out;
        auto &buf = out.mutable_buffer();
        std::copy(beg, end, std::back_inserter(buf));
        return out;
    }
    template<typename Iter>
    static auto to_binary(const T &row, Iter iter) {
        return std::copy(row.begin(), row.end(), iter);
    }
};

using RowDocument = _TRowDocument<Row>;

template<typename ... Args>
using FixedRowDocument = _TRowDocument<FixedRow<Args...> >;


}



#endif /* SRC_DOCDB_ROW_H_ */
