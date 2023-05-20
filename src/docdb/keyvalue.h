/*
 * key.h
 *
 *  Created on: 14. 5. 2023
 *      Author: ondra
 */

#ifndef SRC_DOCDB_KEY_H_
#define SRC_DOCDB_KEY_H_
#include "serialize.h"

#include <locale>
#include <string>
#include <string_view>
#include <leveldb/slice.h>
#include <concepts>

namespace docdb {

using KeyspaceID = std::uint8_t;




///contains key in raw form including keyspaceID
/**
 * This key is used to adress data in underlying database engine
 */
class RawKey: public BasicRow {
public:

    template<typename ... Args >
    RawKey(KeyspaceID id, const Args & ...  args):BasicRow(id, args...) {}


    ///Consider current key as prefix. Calculates key for prefix end
    /**
     * If prefix is abcd, you need keys abcdXXXXX including abcd. The
     * end key will be abce, which is returned. If the last byte is 0xFF,
     * then it is removed and previous byte is increased
     *
     * (similar example in case, that only a-z are allowed.
     *
     *  xyz -> xyzXXXX, end key is xz, because xyz cannot be transformed to xy(z+1)
     *
     * @return
     */
    RawKey prefix_end() const {
        RawKey out = *this;
        while (!out.empty()) {
            auto c = static_cast<unsigned char>(out.back());
            out.pop_back();
            if (c < 0xFF) {
                out.push_back(c+1);
                break;
            }
        }
        return out;
    }

    KeyspaceID get_kid() {
        auto [kid] = BasicRow::get<KeyspaceID>();
        return kid;
    }

    void change_kid(KeyspaceID kid) {
        if constexpr(sizeof(kid) == 1) {
            (*this)[0] = kid;
        } else {
            for (int i = 0; i < sizeof(kid);i++) {
                (*this)[i] = static_cast<char>(kid >> (8*(sizeof(kid)-1-i)));
            }
        }
    }

    void clear() {
        resize(sizeof(KeyspaceID));
    }
};

///Use StringPrefix for a key to search by prefix
/**
 * Key k(_kid, StringPrefix("abc"));
 * generates a key which is able to search for all prefixes of "abc"
 */
using StringPrefix = Blob;

template<typename X>
class TempAppend {
public:
    TempAppend(X &str):_str(str),_len(_str.size()) {}
    ~TempAppend() {_str.resize(_len);}
protected:
    X &_str;
    std::size_t _len;
};

template<typename X>
TempAppend(X &) -> TempAppend<X>;



///Key object used at public interface.
/**
 * This object is created by user containing a key, where keyspace id is
 * just reserved, and must be changed by change_kid method.
 */
class Key: public RawKey {
public:
    template<typename ... Args>
    Key(const Args & ... args):RawKey(0, args...) {}
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
class Value {
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
    Value(Rtv &&rt) {
        _found = rt(_buff);
    }

    ///Creates empty value
    Value():_found(false) {}

    ///Serializes document into buffer
    Value(const ValueType &val):_found(true) {
        _ValueDef::to_binary(val, std::back_inserter(_buff));
    }

    ///Deserialized the document
    ValueType operator*() const {
        return _ValueDef::from_binary(_buff.begin(), _buff.end());
    }
    ///Tests whether value has been set
    bool has_value() const {
        return _found;
    }

    const Buffer &get_serialized() const {return _buff;}

protected:
    bool _found;
    Buffer _buff;
};


}

#endif /* SRC_DOCDB_KEY_H_ */
