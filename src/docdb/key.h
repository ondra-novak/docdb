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
    ///Get content of key (skip keyspaceid);
    template<typename ... Types, typename Iter>
    static auto extract(Iter &at, Iter end)  {
        at+=sizeof(KeyspaceID);
        return Row::extract<Types...>(at, end);
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


///Key object used at public interface.
/**
 * This object is created by user containing a key, where keyspace id is
 * just reserved, and must be changed by set_kid method.
 *
 * Internal RawKey is inaccessible until you set the KID by set_kid() value. This
 * protects agains unintentionaly modify the keyspace 0 when the key is directly
 * converted to RawKey without setting the KID. So setting the KID is enforced
 */
class Key {
public:
    ///Construct the key
    /**
     * @param args columns
     */
    template<typename ... Args>
    Key(const Args & ... args):_v(0, args...) {}
    ///Construct the key by converting from RawKey
    /**
     * @param raw_key raw key object
     */
    Key(const RawKey &raw_key):_v(raw_key) {}

    ///Construct the key from a binary string
    /**
     * @param str_key binary string (it is not possible to use constructor )
     * @return key
     */
    static Key from_string(const std::string_view &str_key) {
        return RawKey(str_key);
    }

    ///Set Keyspace ID and return RawKey representation
    /**
     * @param kid new keyspaceid
     * @return refrence to updated RawKey instance
     *
     * @note function changes state of the object, doesn't create a copy
     */
    RawKey &set_kid(KeyspaceID kid) & {
        _v.change_kid(kid);
        return _v;
    }
    ///Set Keyspace ID and return RawKey representation
    /**
     * @param kid new keyspaceid
     * @return refrence to updated RawKey instance
     *
     * @note function changes state of the object, doesn't create a copy
     */
    RawKey &&set_kid(KeyspaceID kid) && {
        _v.change_kid(kid);
        return std::move(_v);
    }

    ///Extract columns of the key, return std::tuple<>
    template<typename ... Args>
    auto get() const {return _v.get<Args...>();}
    ///Extract columns of the key, return std::tuple<>
    template<typename ... Args>
    static auto extract(std::string_view binstr)  {return RawKey::extract<Args...>(binstr);}
    ///Extract columns of the key, return std::tuple<>
    template<typename ... Args, typename Iter>
    static auto extract(Iter &at, Iter end) {return RawKey::extract<Args...>(at, end);}

    ///Retrieve key's KID
    KeyspaceID get_kid() const {
        return _v.get_kid();
    }
    ///Convert key to binary string
    operator std::string_view const() {
        return _v;
    }

protected:
    RawKey _v;
};

template<typename ... Args>
using FixedKey = FixedType<Key, Args...>;



}

#endif /* SRC_DOCDB_KEY_H_ */
