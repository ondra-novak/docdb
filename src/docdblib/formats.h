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
#include "../imtjson/src/imtjson/array.h"

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

static constexpr unsigned int index_byte_length = 9;


inline void index2string(std::uint64_t idx, std::string &out) {
	Index2String_Push<index_byte_length>::push(idx, out);
}


inline std::uint64_t string2index(const std::string_view &str) {
	std::uint64_t x = 0;
	for (const auto &c: str) {
		x = (x << 8) | static_cast<unsigned int>(c);
	}
	return x;
}


namespace codepoints {

	using Type = unsigned char;

	static constexpr Type undefined = 0;
	///null value
	static constexpr Type null = 1;
	///false value
	static constexpr Type false_value = 2;
	///true value
	static constexpr Type true_value = 3;
	///negative number written in its float form (absolute value)
	static constexpr Type neg_number = 4;
	///positive number written in its float form (absolute value)
	static constexpr Type pos_number = 5;
	///string terminated by zero
	static constexpr Type stringz = 6;
	///doc id follows - used in doc-key map
	static constexpr Type doc = 0x40;
	///combine with code point to mark sequence as array
	static constexpr Type array_prefix = 0x10;
	///arbitrary json (binary stored)
	static constexpr Type json = 0x20;

}

inline void jsonkey2string(const json::Value &v, std::string &out, codepoints::Type prefix = 0) {
	switch (v.type()) {
	case json::undefined:out.push_back(prefix|codepoints::undefined);break;
	case json::null: out.push_back(prefix|codepoints::null);break;
	case json::boolean: out.push_back(prefix|(v.getBool()?codepoints::true_value:codepoints::false_value));break;
	case json::number: {
			double numb = v.getNumber();
			out.push_back(prefix|(numb<0?codepoints::neg_number:codepoints::pos_number));
			numb = std::abs(numb);
			out.append(reinterpret_cast<const char *>(&numb), sizeof(numb));
		}break;
	case json::string:
		out.push_back(prefix|codepoints::stringz);
		for (const char &c: v.getString()) {
			if (c == 0) out.append("\xC0\x80"); else out.push_back(c);
		}
		out.push_back('\0');
		break;
	case json::array: {
		codepoints::Type pfx = codepoints::array_prefix;
		for (json::Value item: v) {
			jsonkey2string(item, out, pfx);
			pfx = 0;
		}
		}break;
	default:
		out.push_back(prefix|codepoints::json);
		v.serializeBinary([&](char c){out.push_back(c);});
		break;
	}
}


inline json::Value string2jsonkey(std::string_view &&key) {
	if (key.empty()) return json::Value();
	codepoints::Type t = key[0];
	key = key.substr(1);
	bool startArray = (t & codepoints::array_prefix) != 0;
	json::Value r;
	switch (t & ~codepoints::array_prefix){
	case codepoints::undefined: break;
	case codepoints::null: r = nullptr;break;
	case codepoints::false_value: r = false; break;
	case codepoints::true_value: r = true; break;
	case codepoints::neg_number: {
			double n = *reinterpret_cast<const double *>(key.data(),sizeof(double));
			key = key.substr(sizeof(double));
			r = -n;
		} break;
	case codepoints::pos_number: {
			double n = *reinterpret_cast<const double *>(key.data(),sizeof(double));
			key = key.substr(sizeof(double));
			r = n;
		} break;
	case codepoints::stringz: {
			std::string_view str(key.data());
			key = key.substr(str.length()+1);
			r = str;
		} break;
	case codepoints::json: {
			int p = 0;
			r = json::Value::parseBinary([&]{
				return key[p++];
			});
			key = key.substr(p);
		} break;
	}

	if (startArray) {
		json::Array a;
		a.push_back(r);
		while (!key.empty()) {
			json::Value itm = string2jsonkey(std::move(key));
			a.push_back(itm);
		}
		r = a;
	}
	return r;
}

}




#endif /* SRC_DOCDBLIB_FORMATS_H_ */
