#pragma once
#ifndef SRC_DOCDB_ROW_H_
#define SRC_DOCDB_ROW_H_

#include "buffer.h"
#include "serialize.h"
#include <variant>

namespace docdb {


class RowView : public std::string_view {
public:
    using std::string_view::string_view;
    RowView(const std::string_view &x):std::string_view(x) {}
};

using RowBuffer = Buffer<char, 32>;

class Row {
public:

    Row() = default;
    Row(const RowView &x):_data(x) {}
    template<typename ... Args>
    Row(const Args & ... args) {
        serialize_items(back_inserter(), args...);
    }

    operator leveldb::Slice() const {
       return std::visit([](const auto &x) -> leveldb::Slice {
            return {x.data(),x.size()};
        }, _data);
    }

    operator std::string_view() const {
       return std::visit([](const auto &x) -> std::string_view {
            return {x.data(),x.size()};
        }, _data);
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
        }
        else if constexpr(std::is_same_v<bool, X>) {
            *iter++ = val?static_cast<char>(1):static_cast<char>(0);
        } else if constexpr(std::is_unsigned_v<X>) {
            X v = val;
            for (std::size_t i = 0; i < sizeof(X); i++) {
                int shift = 8*(sizeof(X)-i-1);
                *iter++=static_cast<char>((v >> shift) & 0xFF);
            }
        } else if constexpr(std::is_convertible_v<X, double>) {
            double d = val;
            BinHelper<double> hlp{-d};
            iter  = std::copy(std::begin(hlp.bin), std::end(hlp.bin), iter);
        } else if constexpr(std::is_base_of_v<LocalizedString, X>) {
            auto &f = std::use_facet<std::collate<char> >(val.get_locale());
            auto out = f.transform(val.begin(), val.end());
            iter  = std::copy(out.begin(), out.end(), iter);
            *iter++ = '\0';
        } else if constexpr(std::is_array_v<std::remove_reference_t<X> >) {
            auto beg = std::begin(val);
            auto end = std::end(val);
            auto sz = std::distance(beg,end);
            if (sz > 0) {
                auto last = beg;
                std::advance(last, sz-1);
                if (*last == 0) end = last;
            }
            iter = std::copy(beg, end, iter);
            *iter++ = '\0';
        } else if constexpr(std::is_same_v<std::decay_t<X>, const char *>) {
            const char *c = val;
            do {
                *iter++ = *c;
            } while (*c++);
        } else if constexpr(std::is_base_of_v<Blob, X>) {
            iter = std::copy(val.begin(), val.end(), iter);
        } else if constexpr(std::is_convertible_v<X, std::string_view>) {
            iter = std::copy(std::begin(val), std::end(val), iter);
            *iter++ = '\0';
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

     ///Deserialize single item
     /**
      * @tparam T type of item
      * @param at reference to iterator pointing at location where to
      * start the serialization. The function updates the iterator
      * @param end end iterator (where the binary content ends)
      * @return deserialized value
      */
     template<typename T, typename Iter>
     static T deserialize_item(Iter & at, Iter end) {
         std::size_t sz = end - at;
         if constexpr(IsTuple<T>) {
             T result;
             auto assign_values = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                 ((std::get<Is>(result) = deserialize_item<typename std::tuple_element<Is, T>::type>(at, end)), ...);
             };
             assign_values(std::make_index_sequence<std::tuple_size_v<T> >{});
             return result;
         }else if constexpr(std::is_same_v<bool, T>) {
             if (sz == 0) return false;
             bool var = *at != 0;
             ++at;
             return var;
         } else if constexpr(std::is_unsigned_v<T>) {
             T var = 0;
             auto cnt = std::min<std::size_t>(sizeof(T), sz);
             for (std::size_t i = 0; i < cnt; i++) {
                 var = (var << 8) | (at[i]);
             }
             at+=cnt;
             return var;
         } else if constexpr(std::is_convertible_v<double,T>) {
             BinHelper<double> hlp;
             double var = 0;
             if (sz < sizeof(hlp)) return 0;
             for (std::size_t i = 0; i < sizeof(hlp); i++) hlp.bin[i] = at[i];
             var = T(-hlp.val);
             at += sizeof(hlp);
             return T(var);
         } else if constexpr(std::is_base_of_v<Blob, T>) {
             Blob var(at, sz);
             at = end;
             return T(var);
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
         } else {
             return CustomSerializer<T>::deserialize(at, end);
         }
     }


     ///Exctract items from the binary string
     /**
      * @tparam Items type of items to extract
      * @param src  binary string
      * @return extracted items
      */
     template <typename... Items>
     requires (sizeof...(Items) != 1 || !IsTuple<typename std::tuple_element<0, std::tuple<Items...> >::type>)
     static std::tuple<Items...> extract(std::string_view src)  {
         std::tuple<Items...> result;
         const char *iter = src.data();
         const char *end = src.data()+src.size();
         auto assign_values = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
             ((std::get<Is>(result) = deserialize_item<Items>(iter, end)), ...);
         };
         assign_values(std::index_sequence_for<Items...>{});
         return result;
     }

     template <typename Item>
     requires IsTuple<Item>
     static Item extract(std::string_view src)  {
         const char *iter = src.data();
         const char *end = src.data()+src.size();
         return deserialize_item<Item>(iter, end);
     }


protected:
    auto back_inserter() -> decltype(std::back_inserter(std::declval<RowBuffer &>())) {
        if (std::holds_alternative<RowBuffer>(_data)) {
            return std::back_inserter(std::get<RowBuffer>(_data));
        }
        throw std::invalid_argument("ROW: Can't append in t read-only mode. Use clear()");
    }

    std::variant<RowBuffer, RowView> _data;
};



}



#endif /* SRC_DOCDB_ROW_H_ */
