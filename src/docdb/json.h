#ifndef SRC_DOCDB_JSON_H_
#define SRC_DOCDB_JSON_H_

#include "structured_document.h"

#include <cmath>
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

template<typename Iter>
inline Structured Structured::from_json(Iter &at, Iter end) {

}

}



#endif /* SRC_DOCDB_JSON_H_ */
