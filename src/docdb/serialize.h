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
    static Iter to_binary(const Type &src, Iter insert) {
        for (auto c: src) {
            *insert = c;
            ++insert;
        }
        return insert;
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
    constexpr Blob(const std::string_view &other):std::string_view(other) {}
};


template<std::size_t count>
using FixedBlob = std::span<unsigned char, count>;
template<std::size_t count>
using FixedString = std::span<char, count>;
template<typename T>
DOCDB_CXX20_CONCEPT(IsFixedString, requires(T x) {
    (T::extent != std::dynamic_extent && sizeof(typename T::element_type) == 1);
    {x.data()} -> std::same_as<typename T::pointer>;
});

template<typename T, std::size_t sz>
static int is_convertible_to_array(const std::array<T,sz> &val);


template<typename T>
DOCDB_CXX20_CONCEPT(IsStdArray, requires(T x){
    typename T::value_type;
    {x.size()};
    {x.begin()};
    {x.end()};
    {x.operator[](0)} -> std::same_as<typename T::value_type &>;
    {is_convertible_to_array(x)};
});

using LocalizedString = LocalizedBasicString<char>;
using LocalizedWString = LocalizedBasicString<wchar_t>;

template<typename X>
class CustomSerializer {
public:
    template<typename Iter>
    static Iter serialize(const X &, Iter ) {
        static_assert(std::is_same_v<X, std::nullptr_t>, "Custom serializer is not defined for this type");
    }
    template<typename Iter>
    static X deserialize(Iter &, Iter ) {
        static_assert(std::is_same_v<X, std::nullptr_t>, "Custom deserializer is not defined for this type");
        return {};
    }
};

///Converts string to prefix searchable string for a Key
/**
 * Allows to search all string strarting with the specified prefix. To successfuly
 * search for prefix, the string must be last column in the Key.
 * @param prefix prefix to search.
 * @return Return value is Blob as the Blob can be used only for prefix search of binary string. You
 * need to pass this value to Key constructor
 *
 */
constexpr Blob prefix(const std::string_view &prefix) {return Blob(prefix);}

inline Blob prefix(const std::wstring_view &prefix) {
    std::string_view binary(reinterpret_cast<const char *>(prefix.data()), prefix.size()*sizeof(wchar_t));
    return Blob(binary);
}

}






#endif /* SRC_DOCDB_SERIALIZE_H_ */
