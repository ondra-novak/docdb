#pragma once
#ifndef SRC_DOCDB_SERIALIZE_H_
#define SRC_DOCDB_SERIALIZE_H_

#include "concepts.h"

#include "buffer.h"
#include <locale>
#include <string_view>
#include <tuple>
namespace docdb {

///Simple string document - document is a single string
struct StringDocument {
    using Type = std::string_view;
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

template<typename T>
union BinHelper {
    T val;
    char bin[sizeof(T)];
};


template<typename T>
class LocalizedBasicString: public std::basic_string_view<T> {
public:
    LocalizedBasicString(const std::basic_string_view<T> &str, const std::locale &loc)
        :std::basic_string_view<T>(str),_loc(loc) {}
    const std::locale &get_locale() const  {return _loc;}
protected:
    std::locale _loc;
};

///Any binary blob
/**
 * Deserializing blob causes that all data from current position till end are
 * considered as blob.
 * Serializing blob cause that all data are directly appended to the key
 * or value, with no separator and terminator
 *
 * It is recommended to use blob as the very last field of the row.
 * However you can use blob to collapse remaining items into single field
 * (like a tuple) and parse it later
 */
class Blob: public std::string_view {
public:
    using std::string_view::string_view;
    Blob(const std::string_view &other):std::string_view(other) {}
};



using LocalizedString = LocalizedBasicString<char>;
using LocalizedWString = LocalizedBasicString<wchar_t>;

template<typename X>
class CustomSerializer {
public:
    template<typename Iter>
    static Iter serialize(const X &val, Iter target) {
        static_assert(std::is_same_v<X, std::nullptr_t>, "Custom serializer is not defined for this type");
    }
    template<typename Iter>
    static X deserialize(Iter &at, Iter end) {
        static_assert(std::is_same_v<X, std::nullptr_t>, "Custom deserializer is not defined for this type");
        return {};
    }
};

template<typename ... Args> struct TupleInspect {

};

using BasicRowBuffer = Buffer<char, 32>;


template<typename Base>
class BasicRowBasicView: public Base  {
public:
    using Base::Base;
    BasicRowBasicView(const Base &x):Base(x) {}
    BasicRowBasicView(Base &&x):Base(std::move(x)) {}

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

    operator leveldb::Slice() const {return {this->data(),this->size()};}
};


using BasicRowView = BasicRowBasicView<std::string_view>;


///A row serialized from basic types
/**
 * Allows to construct and read multi-value row from basic types. It
 * is required to deserialize the row that caller knows types and
 * order how the row has been serialized
 *
 * @note supported types: bool, unsigned, double, string. The string
 * must not contain zero character. You can define own serialization
 * by declaring specialization of CustomSerializer
 */
class BasicRow: public BasicRowBasicView<BasicRowBuffer > {
public:

    using BasicRowBasicView<BasicRowBuffer>::BasicRowBasicView;

    ///Convert BasicRow na BasicRowView
    operator BasicRowView() const {
        return BasicRowView(data(), size());
    }

    ///Construct content of basic row from various variables
    /**
     * @param args values to be serialized
     */
    template<typename ... Args>
    BasicRow(const Args & ... args) {
        serialize_items(std::back_inserter(*this), args...);
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
            *iter++ = val?static_cast<char>(0):static_cast<char>(1);
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
        serialize_items(std::back_inserter(*this), args ...);
    }


};

///Defines document type, which is stored as BasicRow;
struct BasicRowDocument {
    using Type = BasicRowView;
    using ConstructType = BasicRow;
    template<typename Iter>
    static void to_binary(const Type &src, Iter insert) {
        std::copy(src.begin(), src.end(), insert);
    }
    template<typename Iter>
    static Type from_binary(Iter beg, Iter end) {
        const char *c = &(*beg);
        auto dist = std::distance(beg, end);
        return Type(c, dist);
    }

};


}




#endif /* SRC_DOCDB_SERIALIZE_H_ */
