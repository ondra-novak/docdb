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


class Key {
public:
	///Create key in given keyspace
	Key(KeySpaceID keySpaceId, const std::string_view &key);
	///Create key from json
	Key(KeySpaceID keySpaceId, const json::Value &key);
	///Create key from array
	Key(KeySpaceID keySpaceId, const std::initializer_list<json::Value> &key);
	///Create empty key for given keyspace
	explicit Key(KeySpaceID keySpaceId);
	///Create empty key for given keyspace and reserve bytes for append
	Key(KeySpaceID keySpaceId, std::size_t reserve);

	///Return whole key for levelDB search
	operator leveldb::Slice() const {return leveldb::Slice(keydata);}
	operator std::string_view() const {return keydata;}

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

	///append string to key
	void append(const std::string_view &key);

	///append json to key
	void append(const json::Value &key);

	void append(const std::initializer_list<json::Value> &key);

	///clear content of the key
	void clear();

	bool empty();

	///push byte
	void push(unsigned char byte);

	///pop byte
	unsigned char pop();

	///retrieve size of key (including keyspaceid)
	std::size_t size() const {return keydata.length();}

	///retrieve size of key (excluding keyspaceid)
	std::size_t content_size() const {return keydata.length()-keyspaceid_size;}

	///retrieve content of key (excluding keyspaceid)
	std::string_view content() const {return std::string_view(keydata.data()+keyspaceid_size, content_size());}

	///truncate the key
	void truncate(std::size_t sz);
	///truncate the key
	void truncate_content(std::size_t sz) {truncate(sz+keyspaceid_size);}

	using iterator = std::string::const_iterator;
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
protected:
	static constexpr auto keyspaceid_size = sizeof(KeySpaceID);

	std::string keydata;


};



}



#endif /* SRC_DOCDB_SRC_DOCDBLIB_KEYSPACE_H_ */
