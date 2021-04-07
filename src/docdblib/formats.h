/*
 * formats.h
 *
 *  Created on: 20. 10. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_FORMATS_H_
#define SRC_DOCDBLIB_FORMATS_H_

#include <cmath>
#include <string_view>
#include <imtjson/value.h>
#include <imtjson/binjson.h>
#include <imtjson/binjson.tcc>
#include <imtjson/array.h>

namespace docdb {

inline void json2string(const json::Value &v, std::string &out)  {
	v.serializeBinary([&](char c){out.push_back(c);},json::compressKeys);
}

inline void json2string(const std::initializer_list<json::Value> &v, std::string &out)  {
	auto wr = [&](char c){out.push_back(c);};
	using FnType = decltype(wr);
	class Ser: public json::BinarySerializer<FnType> {
	public:
		using json::BinarySerializer<FnType>::BinarySerializer;
		void serializeList(const std::initializer_list<json::Value> &v) {
			serializeInteger(v.size(), json::opcode::array);
			for (json::Value x: v) serialize(x.stripKey());
		}
	};
	Ser ser(std::move(wr),0);
	ser.serializeList(v);
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
	json::Value res = json::Value::parseBinary([&]{
		return idx<len?static_cast<int>(str[idx++]):-1;
	});
	str = str.substr(idx);
	return res;
}

template<int i>
struct Index2String_Push{
	template<typename Cont>
	static void push(std::uint64_t idx, Cont &out) {
		Index2String_Push<i-1>::push(idx >> 8, out);
		out.push_back(static_cast<char>(idx & 0xFF));
	}
};
template<>
struct Index2String_Push<0>{
	template<typename Cont>
	static void push(std::uint64_t , Cont &) {}
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

	///end of key or array
	static constexpr Type end = 0;
	///null value
	static constexpr Type null = 1;
	///false value
	static constexpr Type false_value = 2;
	///true value
	static constexpr Type true_value = 3;
	///number - base
	static constexpr Type number_value = 4;  //4-19
	///string terminated by zero (zero inside is escaped)
	static constexpr Type stringz = 20;
	///combine with code point to mark sequence as array
	static constexpr Type array_prefix = 0x20;
	///arbitrary json (binary stored)
	static constexpr Type json = 0x40;
	///doc id follows - used in doc-key map
	static constexpr Type doc = 0x80;
}

namespace _numbers {


	template<typename Fn>
	void decompose_number(double num, Fn &&fn) {
		bool neg = std::signbit(num);
		int exp = num == 0? 0: std::ilogb(num)+1;
		std::uint64_t m = static_cast<std::uint64_t>(std::abs(num) / std::scalbn(1.0, exp-64));
		exp+=1024;
		if (neg) {
			exp = (~exp) & 0x7FF;
			m = ~m;
		} else {
			exp = exp | 0x800;
		}
		fn(static_cast<unsigned char>(exp >> 8));
		fn(static_cast<unsigned char>(exp & 0xFF));
		for (unsigned int i = 0; i < 8; i++) {
			unsigned char c = (m >> ((7-i)*8) & 0xFF);
			fn(c);
		}
	}

	template<typename Fn>
	void decompose_number_int(std::int64_t numb, Fn &&fn) {
		bool neg;
		int exp;
		uint64_t m;
		if (numb < 0) {
			neg = true;
			m = static_cast<uint64_t>(-numb);
		} else if (numb > 0) {
			neg = false;
			m = static_cast<uint64_t>(numb);
		} else {
			neg = false;
			m = 0;
		}
		if (m) {
			unsigned int shift = 0;
			while ((m & (static_cast<std::uint64_t>(1) << 63)) == 0) {
				m <<= 1;
				shift++;
			}
			exp = 64-shift;
		} else {
			exp = 0;
		}
		exp+=1024;
		if (neg) {
			m = ~m;
			exp = (~exp) & 0x7FF;
		} else {
			exp = exp | 0x800;
		}
		fn(static_cast<unsigned char>(exp >> 8));
		fn(static_cast<unsigned char>(exp & 0xFF));
		for (unsigned int i = 0; i < 8; i++) {
			unsigned char c = (m >> ((7-i)*8) & 0xFF);
			fn(c);
		}
	}

	template<typename Fn>
	void decompose_number_uint(std::uint64_t numb, Fn &&fn) {
		int exp;
		uint64_t m;
		m = static_cast<uint64_t>(numb);
		if (m) {
			unsigned int shift = 0;
			while ((m & (static_cast<std::uint64_t>(1) << 63)) == 0) {
				m <<= 1;
				shift++;
			}
			exp = 64-shift;
		} else {
			exp = 0;
		}
		exp+=1024;
		exp = exp | 0x800;
		fn(static_cast<unsigned char>(exp >> 8));
		fn(static_cast<unsigned char>(exp & 0xFF));
		for (unsigned int i = 0; i < 8; i++) {
			unsigned char c = (m >> ((7-i)*8) & 0xFF);
			fn(c);
		}
	}

	struct MaskTables {
		uint64_t maskTables[65];
		MaskTables() {
			for (int i = 0; i < 64; i++) {
				maskTables[i] = (~static_cast<std::uint64_t>(0)) >> i;
			}
			maskTables[64] = 0;
		}
	};


	template<typename Fn>
	json::Value compose_number(Fn &&fn) {
		static MaskTables mt;

		unsigned char e1 = fn();
		unsigned char e2 = fn();
		bool neg = (e1 & 0x8) == 0;
		int exp = static_cast<int>(e1 & 0xF) << 8 | e2;
		std::uint64_t m = 0;
		for (unsigned int i = 0; i < 8; i++) m = (m << 8) | (fn() & 0xFF);
		double sign;
		if (neg) {
			exp = ~exp & 0x7FF;
			m = ~m;
			sign = -1.0;
		} else {
			sign = 1.0;
			exp = exp & 0x7FF;
		}
		exp-=1024;
		if (exp >= 0 && exp <= 64 && (m & mt.maskTables[exp]) == 0) {
			if (neg) {
				if (exp < 64) {
					std::intptr_t v = m >> (64-exp);
					return -v;
				}
			} else {
				return m >> (64-exp);
			}
		}

		return std::scalbln(sign*m, exp-64);
	}


}

template<typename T>
inline void jsonkey2string(const json::Value &v, T &out, codepoints::Type flags = 0) {
	switch (v.type()) {
	case json::undefined:
	case json::null: out.push_back(flags|codepoints::null);break;
	case json::boolean: out.push_back(flags|(v.getBool()?codepoints::true_value:codepoints::false_value));break;
	case json::number: {
			unsigned char buff[10];
			unsigned int p = 0;
			auto fn = [&](unsigned char c) {buff[p++] = c;};
			if (v.flags() & json::numberUnsignedInteger) {
				_numbers::decompose_number_uint(v.getUInt(), fn);
			} else if (v.flags() & json::numberInteger) {
				_numbers::decompose_number_int(v.getInt(), fn);
			} else {
				_numbers::decompose_number(v.getNumber(), fn);
			}
			buff[0] = (codepoints::number_value+buff[0]) | flags;
			for (unsigned char c: buff) {
				out.push_back(static_cast<char>(c));
			}
		}break;
	case json::string:
		out.push_back(flags|codepoints::stringz);
		for (const char &c: v.getString()) {
			switch (c) {
				case 0:
				case 1: out.push_back('\x01');out.push_back(c);break;
				default: out.push_back(c);break;
			}
		}
		out.push_back('\0');
		break;
	case json::array:
		if (v.empty()) {
			out.push_back(codepoints::array_prefix|codepoints::end);
		} else {
			codepoints::Type pfx = codepoints::array_prefix;
			for (json::Value item: v) {
				jsonkey2string(item, out, pfx);
				pfx = 0;
			}
			out.push_back(codepoints::end);

		}break;
	default:
		out.push_back(flags|codepoints::json);
		v.serializeBinary([&](char c){out.push_back(c);});
		break;
	}
}

template<typename T>
inline void jsonkey2string(const std::initializer_list<json::Value> &v, T &out) {
	codepoints::Type pfx = codepoints::array_prefix;
	for (json::Value item: v) {
		jsonkey2string(item, out, pfx);
		pfx = 0;
	}
	out.push_back(codepoints::end);
}

inline json::Value string2jsonkey(std::string_view &&key, codepoints::Type t) {
	json::Value r;
	switch (t & ~codepoints::array_prefix){
	case codepoints::end: return json::Value();
	case codepoints::null: r = nullptr;break;
	case codepoints::false_value: r = false; break;
	case codepoints::true_value: r = true; break;
	case codepoints::number_value:
	case codepoints::number_value+1:
	case codepoints::number_value+2:
	case codepoints::number_value+3:
	case codepoints::number_value+4:
	case codepoints::number_value+5:
	case codepoints::number_value+6:
	case codepoints::number_value+7:
	case codepoints::number_value+8:
	case codepoints::number_value+9:
	case codepoints::number_value+10:
	case codepoints::number_value+11:
	case codepoints::number_value+12:
	case codepoints::number_value+13:
	case codepoints::number_value+14:
	case codepoints::number_value+15:{
		unsigned char c = static_cast<unsigned char>(t & ~codepoints::array_prefix)-codepoints::number_value;
		const unsigned char *p = reinterpret_cast<const unsigned char *>(key.data());
		key = key.substr(9);
		r = _numbers::compose_number([&]{
			auto ret = c;
			c = *p++;
			return ret;
		});
		break;
	}

	case codepoints::stringz: {
			std::size_t sz = 0;
			std::size_t ln = 0;
			std::size_t cnt = key.size();
			for (ln = 0; ln < cnt && key[ln]!=0 ; ln++) {
				sz++;
				if (key[ln] == 1) ln++;
			}
			json::String buff(sz, [&](char *c) {
				for (std::size_t i = 0; i < ln; i++) {
					if (key[i] == 1) i++;
					*c++ = key[i];
				}
				return sz;
			});
			key = key.substr(ln+1);
			r = json::Value(buff);
		} break;
	case codepoints::json: {
			int p = 0;
			r = json::Value::parseBinary([&]{
				return key[p++];
			});
			key = key.substr(p);
		} break;
	}

	return r;
}


inline json::Value string2jsonkey(std::string_view &&key) {
	if (key.empty()) return json::Value();
	codepoints::Type t = key[0];
	key = key.substr(1);
	bool startArray = (t & codepoints::array_prefix) != 0;
	json::Value r =string2jsonkey(std::move(key), t);
	if (startArray) {
		json::Array a;
		json::Value itm = r;
		while (itm.defined()) {
			a.push_back(itm);
			itm = string2jsonkey(std::move(key));
		}
		r = a;
	}
	return r;

}

inline json::Value extract_subkey(unsigned int index, std::string_view &&key) {
	if (key.empty()) return json::Value();
	codepoints::Type t = key[0];
	bool startArray = (t & codepoints::array_prefix) != 0;
	if (!startArray) {
		if (index == 0) return string2json(std::move(key));
	}
	key = key.substr(1);
	while (index > 0) {
		switch (t & ~codepoints::array_prefix){
			case codepoints::end:
			case codepoints::null:
			case codepoints::false_value:
			case codepoints::true_value: break;
			case codepoints::number_value:
			case codepoints::number_value+1:
			case codepoints::number_value+2:
			case codepoints::number_value+3:
			case codepoints::number_value+4:
			case codepoints::number_value+5:
			case codepoints::number_value+6:
			case codepoints::number_value+7:
			case codepoints::number_value+8:
			case codepoints::number_value+9:
			case codepoints::number_value+10:
			case codepoints::number_value+11:
			case codepoints::number_value+12:
			case codepoints::number_value+13:
			case codepoints::number_value+14:
			case codepoints::number_value+15: key = key.substr(9);break;
			case codepoints::stringz: {
				auto n = key.find('\0');
				key = key.substr(n+1);
			}break;
			case codepoints::json: {
				string2json(std::move(key));
			}break;
		}
		index--;
		if (key.empty()) return json::undefined;
		t = key[0];
		key = key.substr(1);
	}
	return string2jsonkey(std::move(key), t);
}

inline json::Value extract_subvalue(unsigned int index, std::string_view &&str) {
	if (str.empty()) return json::undefined;
	auto c = static_cast<unsigned char>(str[0]);
	if (c < (json::opcode::array+10) && c >= json::opcode::array) {
		c -= json::opcode::array;
		if (c <= index) return json::undefined;
		str = str.substr(1);
		while (index>0) {
			string2json(std::move(str));
			index--;
		}
		return string2json(std::move(str));
	} else{
		return string2json(std::move(str))[index];
	}
}

inline std::size_t guessKeySize(const json::Value &v) {
	switch (v.type()) {
	case json::undefined:
	case json::null:
	case json::boolean: return 1;
	case json::number: return 10;
	case json::string: return v.getString().length+2;
	case json::array: {
		std::size_t x = 0;
		for (json::Value item: v) {
			x = x + guessKeySize(item);
		}
		x++;
		return x;
	}
	default: {
		std::size_t x = 1;
		v.serializeBinary([&](char){++x;});
		return x;
	}}
}

inline std::size_t guessKeySize(const std::initializer_list<json::Value> &v) {
	std::size_t x = 0;
	for (json::Value item: v) {
		x = x + guessKeySize(item);
	}
	x++;
	return x;
}


}




#endif /* SRC_DOCDBLIB_FORMATS_H_ */
