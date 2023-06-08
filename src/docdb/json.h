#ifndef SRC_DOCDB_JSON_H_
#define SRC_DOCDB_JSON_H_


#include "structured_document.h"
#include <cmath>
#include <stdexcept>
namespace docdb {

namespace _details {

template<typename Iter>
void text_to_iter(std::string_view text, Iter &iter) {
    std::copy(text.begin(), text.end(), iter);
}

template<typename OutputIterator>
OutputIterator encodeJsonChar(wchar_t character, OutputIterator output) {
    switch (character) {
        case '"':
            text_to_iter("\\\"", output);
            return output;
        case '\\':
            text_to_iter("\\\\", output);
            return output;
        case '/':
            text_to_iter("\\/", output);
            return output;
        case '\b':
            text_to_iter("\\b", output);
            return output;
        case '\f':
            text_to_iter("\\f", output);
            return output;
        case '\n':
            text_to_iter("\\n", output);
            return output;
        case '\r':
            text_to_iter("\\r", output);
            return output;
        case '\t':
            text_to_iter("\\t", output);
            return output;
        default:
            switch ((character >= 32) + (character >= 128)
                    + (character >= 0x10000)) {
                default:
                case 2: {
                    char buff[8];
                    std::size_t n = std::snprintf(buff, sizeof(buff), "\\u%04X",
                            character);
                    text_to_iter( { buff, n }, output);
                    return output;
                }
                case 1:
                    *output = static_cast<char>(character);
                    ++output;
                    return output;
                case 3: {
                    // Character falls outside the BMP, so encode as surrogate pair
                    wchar_t highSurrogate = 0xD800
                            + ((character - 0x10000) >> 10);
                    wchar_t lowSurrogate = 0xDC00
                            + ((character - 0x10000) & 0x3FF);
                    return encodeJsonChar(lowSurrogate,
                            encodeJsonChar(highSurrogate, output));
                }
            }
    }
}
template<typename Iter>
wchar_t decodeJsonChar2(Iter &at, Iter end) {
    if (at == end) return -1;
    char c = *at;
    ++at;
    if (c != '\\') return c;
    if (at == end) return -1;
    char d = *at;
    ++at;
    switch (d)  {
        case 'n': return '\n';
        case 'r': return '\r';
        case 'b': return '\b';
        case 'f': return '\f';
        case 't': return '\t';
        case 'u': {
            wchar_t n = 0;
            for (int i = 0; i < 4; i++) {
                if (at == end) return -1;   
                char z = *at;
                if (z >= '0' && z <= '9') n = (n * 16) + (z - '0');
                else if (z >= 'A' && z <='F')  n = (n * 16) + (z - 'A' + 10);
                else if (z >= 'a' && z <='f')  n = (n * 16) + (z - 'a' + 10);
                else return -1;                
                ++at;
            }
            return n;
        }
        default: return d;
    }
}
template<typename Iter>
wchar_t decodeJsonChar(Iter &at, Iter end) { 
    wchar_t x = decodeJsonChar2(at, end);
    wchar_t y;
    if (x >= 0xD800 && x < 0xDC00) {
        y = decodeJsonChar2(at,end);
        if (y >=0xDC00 && y < 0xE000) {
            return (((x & 0x3FF) << 10) | (y & 0x3FF)) + 0x10000;
        } else {
            return -1;
        }
    } else if (x >=0xDC00 && x < 0xE000) {
        y = decodeJsonChar2(at,end);
        if (y >= 0xD800 && y < 0xDC00) {
            return (((y & 0x3FF) << 10) | (x & 0x3FF)) + 0x10000;
        } else {
            return -1;
        }
    } else return x;        
}



template<typename Iter>
void string_to_iter(std::string_view text, Iter &iter) {
    *iter = '"';
    ++iter;
    auto b = text.begin();
    auto e = text.end();
    while (b != e) {
        iter = encodeJsonChar(utf8Towchar(b, e), iter);
    }
    *iter = '"';
    ++iter;
}

template<typename Iter>
void wstring_to_iter(std::wstring_view text, Iter &iter) {
    *iter = '"';
    ++iter;
    auto b = text.begin();
    auto e = text.end();
    while (b != e) {
        iter = encodeJsonChar(*b, iter);
        ++b;
    }
    *iter = '"';
    ++iter;
}

}


template<typename Iter>
inline Iter Structured::to_json(Iter iter) const {

    return std::visit([&](const auto &val){
        using Type = std::decay_t<decltype(val)>;
        if constexpr(std::is_same_v<Type, bool>) {
            _details::text_to_iter(val?"true":"false", iter);
        } else if constexpr(std::is_null_pointer_v<Type> || std::is_same_v<Type, Undefined>) {
            _details::text_to_iter("null", iter);
        } else if constexpr(std::is_same_v<Type, double>) {
            if (std::isfinite(val)) {
                _details::text_to_iter(std::to_string(val), iter);
            } else if (val < 0) {
                _details::text_to_iter("\"-∞\"", iter);
            } else if (val > 0) {
                _details::text_to_iter("\"∞\"", iter);
            } else if (std::isnan(val)) {
                _details::text_to_iter("null", iter);
            }
       } else if constexpr(std::is_same_v<Type, std::intmax_t>) {
           _details::text_to_iter(std::to_string(val), iter);
       } else if constexpr(std::is_same_v<Type, std::string>){
           _details::string_to_iter(val, iter);
       } else if constexpr(std::is_same_v<Type, std::wstring>) {
           _details::wstring_to_iter(val, iter);
       } else if constexpr(std::is_same_v<Type, StructArray>) {
           *iter = '[';
           ++iter;
           if (!val.empty()) {
               auto b = val.begin();
               auto e = val.end();
               iter = b->to_json(iter);
               ++b;
               while (b != e) {
                   *iter = ',';
                   ++iter;
                   iter = b->to_json(iter);
                   ++b;
               }
           }
           *iter = ']';
           ++iter;
       } else if constexpr(std::is_same_v<Type, StructKeypairs>) {
           *iter = '{';
           ++iter;
           if (!val.empty()) {
               auto b = val.begin();
               auto e = val.end();
               _details::string_to_iter(b->first, iter);
               *iter = ':';
               ++iter;
               iter = b->second.to_json(iter);
               ++b;
               while (b != e) {
                   *iter = ',';
                   ++iter;
                   _details::string_to_iter(b->first, iter);
                   *iter = ':';
                   ++iter;
                   iter = b->second.to_json(iter);
                   ++b;
               }
           }
           *iter = '}';
           ++iter;

       }
        return iter;
    }, *this);

}

class JsonParseException: public std::exception {
public:
    enum class Type {
        eof,
        number,
        character,
        keyword,
        comma,
        key,
        collon,
        dupkey,
        unknown
    };

    JsonParseException(Type type):_type(type) {}
    const char *what() const noexcept override {
        switch (_type) {
            default: return "Generic parse error";
            case Type::eof: return "Unexpected EOF";
            case Type::number: return "Invalid number format";
            case Type::character: return "Invalid character encoding";
            case Type::keyword: return "Unknown keyword. Expected 'true', 'false','null'";
            case Type::comma: return "Expecting ','";
            case Type::key: return "Expecting key (as string)";
            case Type::collon: return "Expecting ':'";
            case Type::dupkey: return "Duplicate keys";
            case Type::unknown: return "Unknown character";
        }
    }
protected:
    Type _type;



};

template<typename Iter>
inline Structured Structured::from_json(Iter &at, Iter end, int flags) {
    if (at == end) throw JsonParseException(JsonParseException::Type::eof);
    char c = *at;
    ++at;
    while (std::isspace(c)) {
        if (at == end) throw JsonParseException(JsonParseException::Type::eof);
        c = *at;
        ++at;
    }    
    switch (c) {
        case '+':
        case '-':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':  {
                    bool dot = false;
                    std::string buff;
                    char *tmp;
                    buff.push_back(c);
                    while (at != end) {
                        c = *at;                        
                        if (c == ',' || c == ']' || c == '}' || std::isspace(c) ) break;
                        dot |= c == '.';
                        buff.push_back(c);
                        ++at;
                   }
                   if (dot) {
                        double d = strtod(buff.c_str(), &tmp);
                        if (*tmp) throw JsonParseException(JsonParseException::Type::number);
                        return d;
                   } else {
                        auto x = strtoll(buff.c_str(), &tmp, 10);
                        if (*tmp) throw JsonParseException(JsonParseException::Type::number);
                        return static_cast<std::intmax_t>(x);        
                   }
            }
        case '"': {
            if (flags & flagWideStrings) {
                std::wstring out;
                while (at != end && *at != '"') {
                    wchar_t w = _details::decodeJsonChar(at,end);
                    if (w == -1) throw JsonParseException(JsonParseException::Type::character);
                    out.push_back(w);                    
                }
                if (at == end) throw JsonParseException(JsonParseException::Type::eof);
                ++at;
                return out;
            } else {
                std::string out;
                auto iter = std::back_inserter(out);
                while (at != end && *at != '"') {
                    wchar_t w = _details::decodeJsonChar(at,end);
                    if (w == -1) throw JsonParseException(JsonParseException::Type::character);
                    iter = wcharToUtf8(w, iter);
                }
                if (at == end) throw JsonParseException(JsonParseException::Type::eof);
                ++at;
                return out;
            }
        }
        case 't': if (*at == 'r') {
                    ++at;
                    if (*at == 'u') {
                        ++at;
                        if (*at == 'e') {
                            ++at;
                            return true;
                        }
                    }
            }
            throw JsonParseException(JsonParseException::Type::keyword);
        case 'f': if (*at == 'a') {
                    ++at;
                    if (*at == 'l') {
                        ++at;
                        if (*at == 's') {
                            ++at;
                            if (*at == 'e') {
                                ++at;
                            return false;
                            }
                        }
                    }
            }
            throw JsonParseException(JsonParseException::Type::keyword);
        case 'n': if (*at == 'u') {
                    ++at;
                    if (*at == 'l') {
                        ++at;
                        if (*at == 'l') {
                            ++at;
                            return true;
                        }
                    }
            }
            throw JsonParseException(JsonParseException::Type::keyword);
        case '[': {
            Structured::Array out;
            while (at != end && std::isspace(*at)) ++at;
            if (*at == ']') {
                ++at;
                return out;
            }
            do {
                out.push_back(from_json(at,end,flags));
                while (at != end && std::isspace(*at)) ++at;
                char c = *at;                   
                ++at;
                if (c == ']') return out;
                if (c != ',') throw JsonParseException(JsonParseException::Type::eof);                 
            } while (true);
        }
        case '{': {
            Structured::KeyPairs out;
            while (at != end && std::isspace(*at)) ++at;
            if (*at == '}') {
                ++at;
                return out;
            }
            do {
                auto s = from_json(at,end,0);
                if (!std::holds_alternative<std::string>(s)) {
                    throw JsonParseException(JsonParseException::Type::key);                 
                }
                while (at != end && std::isspace(*at)) ++at;
                char c = *at;                   
                ++at;
                if (c != ':') throw JsonParseException(JsonParseException::Type::collon);                 
                auto v = from_json(at,end,flags);                
                if (!out.emplace(std::move(std::get<std::string>(s)), std::move(v)).second) {
                    throw JsonParseException(JsonParseException::Type::dupkey);                 
                }
                while (at != end && std::isspace(*at)) ++at;
                c = *at;                   
                ++at;
                if (c == '}') return out;
                if (c != ',') throw JsonParseException(JsonParseException::Type::eof);                 
            } while (true);
        }
        default:
            break;
        
    }
    throw JsonParseException(JsonParseException::Type::unknown);                 

}

}



#endif /* SRC_DOCDB_JSON_H_ */
