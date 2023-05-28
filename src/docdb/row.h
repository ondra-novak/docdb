#pragma once
#ifndef SRC_DOCDB_ROW_H_
#define SRC_DOCDB_ROW_H_

#include "buffer.h"
#include "serialize.h"
#include <variant>
#include <ieee754.h>

namespace docdb {


class RowView : public std::string_view {
public:
    using std::string_view::string_view;
    RowView(const std::string_view &x):std::string_view(x) {}
};

using RowBuffer = Buffer<char, 64>;

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

    ///Retrieve internal mutable buffer
    /**
     * This function works also in read-only mode. In this case,
     * it replaces content with mutable copy
     *
     * @return mutable buffer
     */
    RowBuffer &mutable_buffer() {
        if (!std::holds_alternative<RowBuffer>(_data)) {
            std::string_view s = (*this);
            _data = RowBuffer(s);
        }
        RowBuffer &v = std::get<RowBuffer>(_data);
        return v;

    }

    ///Retrieve mutable pointer
    /**
     * Mutable pointer allows to modify content of internal buffer.
     * This function works, even if the row is in readonly mode. In
     * this case, it switches the read-write mode (copies the content)
     * @return mutable pointer
     */
    char *mutable_ptr() {
        return mutable_buffer().data();
    }

    ///Retrieve size of internal buffer
    std::size_t size() const {
        return std::visit([](const auto &x){return x.size();},_data);
    }

    ///Determines, whether internal buffer is empty
    bool empty() const {
        return std::visit([](const auto &x){return x.empty();},_data);
    }

    ///Clears content and switches to read-write mode
    void clear() {
        if (!std::holds_alternative<RowBuffer>(_data)) {
            _data = RowBuffer();
        } else {
            std::get<RowBuffer>(_data).clear();
        }
    }

    ///Is view?
    /**
     * @retval true current value is view of other row, or string. It means
     * that row actually refers some outside buffer. To detach
     * from the buffer, call mutable_buffer() or clear();
     * @retval false current value is not view
     *
     */
    bool is_view() const {
        return std::holds_alternative<RowView>(_data);
    }

    /**
     * @retval true current value is mutable
     * @retval false current value is views
     */
    bool is_mutable() const {
        return std::holds_alternative<RowBuffer>(_data);
    }

    ///Resize the buffer
    /**
     * @param sz new size. If the size is larger than current size,
     * the Row will be switched to read-write mode;
     * @param init initialization value
     */
    void resize(std::size_t sz, char init = 0) {
        if (std::holds_alternative<RowView>(_data)) {
            RowView &view = std::get<RowView>(_data);
            if (sz < view.size()) {
                std::string_view x = view;
                _data = RowView(x.substr(0, sz));
            } else {
                RowBuffer &newbuf = mutable_buffer();
                newbuf.resize(sz,init);
            }
        } else if (std::holds_alternative<RowBuffer>(_data)) {
            RowBuffer &buff = std::get<RowBuffer>(_data);
            buff.resize(sz, init);
        }
    }

    auto begin() const {
        return std::visit([](const auto &x){return x.begin();}, _data);
    }
    auto end() const {
        return std::visit([](const auto &x){return x.end();}, _data);
    }
    RowView view() const {
        return RowView(*this);
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
        } else if constexpr(std::is_null_pointer_v<X> || std::is_same_v<X, std::monostate>) {
            //empty;
        } else if constexpr(std::is_same_v<X, Row> || std::is_same_v<X, RowView> || std::is_base_of_v<Blob, X>) {
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
                serialize_items(iter, Type{});
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
         } else if constexpr(IsVariant<T>) {
             if (sz == 0) return T();
             std::uint8_t index = *at++;
             ByteToIntegralType<ConstructVariantHelper<T>::template DoIt> c;
             return c.visit([&](auto c){
                 if constexpr(c.val >= std::variant_size_v<T>) {
                     throw std::invalid_argument("ROW: Variant index out of range (corrupted row?)");
                     return T();
                 } else {
                     using Type = std::variant_alternative_t<c.val,T>;
                     return T(deserialize_item<Type>(at, end));
                 }
             }, index);
         } else if constexpr(std::is_null_pointer_v<T> || std::is_same_v<T, std::monostate>) {
             return T();
         } else if constexpr(std::is_base_of_v<Blob, T> || std::is_base_of_v<RowView, T>) {
             T var(at, sz);
             at = end;
             return T(var);
         } else if constexpr(std::is_base_of_v<Row, T>) {
             auto v = deserialize_item<RowView>(at, end);
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

     int operator<(const Row &other) const {return std::string_view(*this) < std::string_view(other);}
     int operator>(const Row &other) const {return std::string_view(*this) > std::string_view(other);}
     int operator==(const Row &other) const {return std::string_view(*this) == std::string_view(other);}
     int operator!=(const Row &other) const {return std::string_view(*this) != std::string_view(other);}
     int operator<=(const Row &other) const {return std::string_view(*this) <= std::string_view(other);}
     int operator>=(const Row &other) const {return std::string_view(*this) >= std::string_view(other);}


protected:
    auto back_inserter() -> decltype(std::back_inserter(std::declval<RowBuffer &>())) {
        if (std::holds_alternative<RowBuffer>(_data)) {
            return std::back_inserter(std::get<RowBuffer>(_data));
        }
        throw std::invalid_argument("ROW: Can't append in t read-only mode. Use clear()");
    }

    std::variant<RowBuffer, RowView> _data;

};




template<typename T, typename ... Args>
class FixedT: public T {
public:

    FixedT() = default;
    FixedT(const RowView &view):T(view) {}
    FixedT(const Args & ... args): T(args...) {}

    auto get() const {
        return T::template get<Args...>();
    }

    template<std::size_t n>
    auto getN() const {
        using TType = decltype(
           ([]<std::size_t ... Is>(std::index_sequence<Is...>) {
                return std::make_tuple(std::declval<typename std::tuple_element<Is, T>::type>()...);
            })(std::make_index_sequence<n>{})
        );
        return T::template get<TType>();
    }
};

template<typename ... Args>
using FixedRow = FixedT<Row, Args...>;


template<typename T>
struct _TRowDocument {
    using Type = T;
    template<typename Iter>
    static T from_binary(Iter beg, Iter end) {
        if constexpr(std::is_convertible_v<Iter, const char *>) {
            const char *bptr = beg;
            const char *eptr = end;
            return RowView(bptr, eptr-bptr);
        } else {
            T out;
            auto &buf = out.mutable_buffer();
            std::copy(beg, end, std::back_inserter(buf));
            return out;
        }
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
