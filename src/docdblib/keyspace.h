/*
 * keyspace.h
 *
 *  Created on: 16. 12. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDB_SRC_DOCDBLIB_KEYSPACE_H_
#define SRC_DOCDB_SRC_DOCDBLIB_KEYSPACE_H_

#include <string_view>
#include <string>
#include <leveldb/slice.h>
#include <imtjson/value.h>

using leveldb::Slice;

namespace docdb {

///Contains keyspace identifier - every object must allocate its keyspace
using KeySpaceID = unsigned char;
///Contains class identifier - used to identify class which controls specified keyspace
using ClassID = KeySpaceID;

template<typename T>
class KeyViewT {
public:
	static constexpr auto keyspaceid_size = sizeof(KeySpaceID);

	KeyViewT() {}
	KeyViewT(const T &keydata):keydata(keydata) {}
	KeyViewT(T &&keydata):keydata(std::move(keydata)) {}
	KeyViewT(const leveldb::Slice &slice):keydata(slice.data(), slice.size()) {}

	bool empty() const {return keydata.size() <= keyspaceid_size;}
	///retrieve size of key (including keyspaceid)
	std::size_t size() const {return keydata.size();}

	///retrieve size of key (excluding keyspaceid)
	std::size_t content_size() const {return keydata.size()-keyspaceid_size;}

	///retrieve content of key (excluding keyspaceid)
	std::string_view content() const {return std::string_view(keydata.data()+keyspaceid_size, content_size());}

	using iterator = typename T::const_iterator;
	///retrieve begin of the key (keyspace is excluded)
	iterator begin() const {return keydata.begin()+keyspaceid_size;}
	///retrieve end of the key
	iterator end() const {return keydata.end();}
	///extract up to specified bytes from the key
	/**
	 * @param iter iterator. It is updated once function returns
	 * @param count of bytes. Default value causes that rest of the key is extracted
	 * @return
	 */
	std::string_view extract_string(iterator &iter, std::size_t bytes = ~static_cast<std::size_t>(0));
	///extract JSON from the key
	/**
	 * @param iter iterator. It is updated once function returns
	 * @return extracted json, undefined if error
	 */
	json::Value extract_json(iterator &iter);
	///Return whole key for levelDB search
	operator leveldb::Slice() const {return leveldb::Slice(keydata.data(), keydata.size());}

	operator std::string_view() const {return keydata;}

protected:
	T keydata;
};

using KeyView = KeyViewT<std::string_view>;


class Key: public KeyViewT<std::string> {
public:

	using KeyViewT<std::string>::KeyViewT;
	///Create key in given keyspace
	Key(KeySpaceID keySpaceId, const std::string_view &key);
	///Create key in given keyspace
	Key(KeySpaceID keySpaceId, const std::string &key);
	///Create empty key for given keyspace
	explicit Key(KeySpaceID keySpaceId);
	///Create empty key for given keyspace and reserve bytes for append
	Key(KeySpaceID keySpaceId, std::size_t reserve);

	Key(const KeyView &kv);

	operator KeyView() const;

	KeySpaceID get_keyspace_id() const {return *(reinterpret_cast<const KeySpaceID *>(keydata.data()));}

	///Transfer key to diffe rent keyspace
	Key transfer(KeySpaceID id) {return Key(id, content());}

	///Create upper bound from this key (increase last byte by 1)
	/**
	 * @retval true success
	 * @retval false already on upper bound (can't increase more, for example 0xFF key cannot be put futher)
	 *
	 * @note result key can be from different keyspace. It is ok, if you
	 * exclude end of the range. However you cannot create upper bound of the last
	 * keyspace. This is handled elsewhere, so at an user level you will never
	 * need to handle such situation.
	 */
	bool upper_bound();

	static Key upper_bound(const KeyView &kv);

	static Key upper_bound(const Key &kv);

	///append string to key
	void append(const std::string_view &key);
	///append string to key
	void append(const std::string &key);

	///append json to key
	void append(const json::Value &key);

	void append(const std::initializer_list<json::Value> &key);

	///clear content of the key
	void clear();

	///push byte
	void push(unsigned char byte);

	///act as std::string
	void push_back(char byte);

	///pop byte
	unsigned char pop();

	///truncate the key
	void truncate(std::size_t sz);
	///truncate the key
	void truncate_content(std::size_t sz) {truncate(sz+keyspaceid_size);}


protected:

	std::string keydata;


};



}



#endif /* SRC_DOCDB_SRC_DOCDBLIB_KEYSPACE_H_ */
