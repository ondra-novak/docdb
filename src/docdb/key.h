/*
 * key.h
 *
 *  Created on: 14. 5. 2023
 *      Author: ondra
 */

#ifndef SRC_DOCDB_KEY_H_
#define SRC_DOCDB_KEY_H_
#include "serialize.h"
#include "row.h"
#include "types.h"


namespace docdb {






///contains key in raw form including keyspaceID
/**
 * This key is used to adress data in underlying database engine
 */
class RawKey: public Row {
public:


    template<typename ... Args >
    RawKey(KeyspaceID id, const Args & ...  args):Row(id, args...) {}

    RawKey(std::string_view data):Row(Blob(data)) {}


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
        auto &buff = out.mutable_buffer();
        while (!buff.empty()) {
            auto c = static_cast<unsigned char>(buff.back());
            buff.pop_back();
            if (c < 0xFF) {
                buff.push_back(c+1);
                break;
            }
        }
        return out;
    }

    KeyspaceID get_kid() const {
        auto [kid] = Row::get<KeyspaceID>();
        return kid;
    }

    void change_kid(KeyspaceID kid) {
        if (Row::size() < sizeof(KeyspaceID)) {
            throw std::invalid_argument("Invalid Key object instance");
        }
        char *ptr = mutable_ptr();
        if constexpr(sizeof(kid) == 1) {
            ptr[0] = kid;
        } else {
            for (int i = 0; i < sizeof(kid);i++) {
                ptr[i] = static_cast<char>(kid >> (8*(sizeof(kid)-1-i)));
            }
        }
    }

    ///Get content of key (skip keyspaceid);
    template<typename ... Types>
    auto get() const {
        std::string_view me(*this);
        return this->extract<Types...>(me);
    }

    ///Get content of key (skip keyspaceid);
    template<typename ... Types>
    static auto extract(std::string_view me)  {
        me = me.substr(sizeof(KeyspaceID));
        return Row::extract<Types...>(me);
    }

    auto size() const {
        return Row::size() - sizeof(KeyspaceID);
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
    Key(const RawKey &raw_key):RawKey(raw_key) {}

    static Key from_string(const std::string_view &str_key) {
        return RawKey(str_key);
    }
};

template<typename ... Args>
using FixedKey = FixedT<Key, Args...>;



}

#endif /* SRC_DOCDB_KEY_H_ */
