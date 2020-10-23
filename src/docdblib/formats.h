/*
 * formats.h
 *
 *  Created on: 20. 10. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_FORMATS_H_
#define SRC_DOCDBLIB_FORMATS_H_

#include <string_view>
#include <imtjson/value.h>
#include <imtjson/binjson.h>

namespace docdb {

inline void json2string(const json::Value &v, std::string &out)  {
	v.serializeBinary([&](char c){out.push_back(c);},json::compressKeys);
}

inline json::Value string2json(const std::string_view &str) {
	std::size_t idx = 0;
	std::size_t len = str.length();
	return json::Value::parseBinary([&]{
		return idx<len?static_cast<int>(str[idx++]):-1;
	});
}

inline json::Value string2json(std::string_view &&str) {
	std::size_t idx = 0;
	std::size_t len = str.length();
	return json::Value::parseBinary([&]{
		return idx<len?static_cast<int>(str[idx++]):-1;
	});
	str = str.substr(idx);
}

template<int i>
struct Index2String_Push{
	static void push(std::uint64_t idx, std::string &out) {
		Index2String_Push<i-1>::push(idx >> 8, out);
		out.push_back(static_cast<char>(idx & 0xFF));
	}
};
template<>
struct Index2String_Push<0>{
	static void push(std::uint64_t idx, std::string &out) {}
};

inline void index2string(std::uint64_t idx, std::string &out) {
	Index2String_Push<9>::push(idx, out);
}

inline std::uint64_t string2index(const std::string_view &str) {
	std::uint64_t x = 0;
	for (const auto &c: str) {
		x = (x << 8) | static_cast<unsigned int>(c);
	}
	return x;
}


}




#endif /* SRC_DOCDBLIB_FORMATS_H_ */
