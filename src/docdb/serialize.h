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



///Retrieved value
/**
 * Value cannot be retrieved directly, you still need to test whether
 * the key realy exists in the database. You also need to keep internal
 * serialized data as the some document still may reffer to serialized
 * area. This helps to speed-up deserialization, but the document cannot
 * be separated from its serialized version. So to access the document
 * you need to keep this structure.
 *
 * @tparam _ValueDef Defines format of value/document
 * @tparam Buffer type of buffer
 */
template<DocumentDef _ValueDef, typename Buffer = std::string>
class Document {
public:

    using ValueType = typename _ValueDef::Type;

    ///Construct object using function
    /**
     * This is adapted to leveldb's Get operation
     *
     * @param rt function receives buffer, and returns true if the buffer
     * was filled with serialized data, or false, if not
     */
    template<typename Rtv>
    CXX20_REQUIRES(std::same_as<decltype(std::declval<Rtv>()(std::declval<Buffer &>())), bool>)
    Document(Rtv &&rt) {
        _found = rt(_buff);
    }

    ///Creates empty value
    Document():_found(false) {}

    ///Serializes document into buffer
    Document(const ValueType &val):_found(true) {
        _ValueDef::to_binary(val, std::back_inserter(_buff));
    }

    Document(const Document &other):_found(other.found), _buff(other.buff) {}
    Document(Document &&other):_found(other.found), _buff(std::move(other.buff)) {
        if (other._inited) {
            ::new(&_storage) ValueType(std::move(other._storage));
            _inited = true;
        }
    }

    Document &operator=(const Document &other) {
        if (this != &other) {
            _found = other._found;
            _buff = other._buff;
            if (_inited) {
                _storage.~ValueType();
                _inited =false;
            }
        }
    }
    Document &operator=(Document &&other) {
        if (this != &other) {
            _found = other._found;
            _buff = std::move(other._buff);
            if (_inited) {
                _storage.~ValueType();
                _inited =false;
            }
            if (other._inited) {
                ::new(&_storage) ValueType(std::move(other._storage));
                _inited = true;
            }
        }
        return *this;
    }

    ~Document() {
        if (_inited) _storage.~ValueType();
    }

    ///Deserialized the document
    const ValueType &operator*() const {
        return get_parse();
    }
    ///Tests whether value has been set
    bool has_value() const {
        return _found;
    }

    operator bool() const {return has_value();}

    const Buffer &get_serialized() const {return _buff;}

    const ValueType *operator->() const {
        return &get_parse();
    }

protected:
    bool _found;
    mutable bool _inited =false;
    Buffer _buff;
    union {
        mutable ValueType _storage;
    };

    ValueType &get_parse() const {
        if (!_inited) {
            ::new(&_storage) ValueType(_ValueDef::from_binary(_buff.begin(), _buff.end()));
            _inited =true;
        }

        return _storage;
    }

};


}






#endif /* SRC_DOCDB_SERIALIZE_H_ */
