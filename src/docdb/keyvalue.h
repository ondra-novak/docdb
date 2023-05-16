/*
 * key.h
 *
 *  Created on: 14. 5. 2023
 *      Author: ondra
 */

#ifndef SRC_DOCDB_KEY_H_
#define SRC_DOCDB_KEY_H_
#include <locale>
#include <string>
#include <string_view>
#include <leveldb/slice.h>
#include <concepts>

namespace docdb {

using KeyspaceID = std::uint8_t;

template<typename T>
union BinHelper {
    T val;
    char bin[sizeof(T)];
};

template<typename T>
class LocalizedBasicString: public std::basic_string_view<T> {
public:
    LocalizedBasicString(const std::basic_string_view<T> &str, const std::locale &loc)
        :std::basic_string_view<T>(str),_loc(loc) {}
    const std::locale &get_locale() const  {return _loc;}
protected:
    std::locale _loc;
};

class RemainingData: public std::string_view {
public:
    using std::string_view::string_view;
    RemainingData(const std::string_view &other):std::string_view(other) {}
};

using LocalizedString = LocalizedBasicString<char>;
using LocalizedWString = LocalizedBasicString<wchar_t>;

template<typename X>
class CustomSerializer {
    static void serialize(const X &val, std::string &target) {
        static_assert("Custom serializer is not defined for this type");
    }
    static std::string_view deserialize(const std::string_view &src, X &val) {
        static_assert("Custom deserializer is not defined for this type");
        return {};
    }
};


class Value: public std::string {
public:

    using std::string::string;


    template<typename X, typename ... Args>
    void add(const X &val, const Args & ... args) {
        if constexpr(std::is_same_v<bool, X>) {
            this->push_back(val?'0':'1');
        } else if constexpr(std::is_unsigned_v<X>) {
            X v = val;
            for (int i = 0; i < sizeof(X); i++) {
                int shift = 8*(sizeof(X)-i-1);
                this->push_back(static_cast<char>((v >> shift) & 0xFF));
            }
        } else if constexpr(std::is_convertible_v<X, double>) {
            double d = val;
            BinHelper<double> hlp{-d};
            for (char c: hlp.bin) this->push_back(c);
        } else if constexpr(std::is_base_of_v<LocalizedString, X>) {
            auto &f = std::use_facet<std::collate<char> >(val.get_locale());
            auto out = f.transform(val.begin(), val.end());
            for (char c: out) this->push_back(c);
            this->push_back('\0');
        } else if constexpr(std::is_base_of_v<RemainingData, X>) {
            append(val);
        } else if constexpr(std::is_convertible_v<X, std::string_view>) {
            std::string_view s(val);
            for (char c: s) this->push_back(c);
            this->push_back('\0');
        } else {
            CustomSerializer<X>::serialize(val, *this);
        }
        add(args...);
    }

    void add() {};

    template<typename X, typename ... Args>
    static int parse(const std::string_view &s, X &var,  Args &... args) {
        std::string_view nxt;
        if (s.empty()) return 0;

        if constexpr(std::is_same_v<bool, X>) {
            var = s[0] != '0';
            nxt = s.substr(1);
        } else if constexpr(std::is_unsigned_v<X>) {
            var = 0;
            auto cnt = std::min<std::size_t>(sizeof(X), s.size());
            for (int i = 0; i < cnt; i++) {
                var = (var << 8) | (s[i]);
            }
            nxt = s.substr(cnt);
        } else if constexpr(std::is_convertible_v<double,X>) {
            BinHelper<double> hlp;
            if (s.size() < sizeof(hlp)) return 0;
            for (int i = 0; i < sizeof(hlp); i++) hlp.bin[i] = s[i];
            var = X(-hlp.val);
            nxt = s.substr(sizeof(hlp));
        } else if constexpr(std::is_convertible_v<RemainingData, X>) {
            var = RemainingData(s.data(), s.size());
            nxt = {};
        } else if constexpr(std::is_convertible_v<std::string_view, X>) {
            auto pos = s.find('\0');
            if (pos == s.npos) {
                var = X(s);
                nxt = {};
            } else {
                var = s.substr(0,pos);
                nxt = s.substr(pos+1);
            }
        } else {
            nxt = CustomSerializer<X>::deserialize(s,var);
        }
        if (nxt.empty()) return 0;
        return 1 + parse(nxt, args...);

    }
    static int parse(const std::string_view &s) {
        return 0;
    }



};


class Key: public Value {
public:

    template<typename ... Args >
    Key(KeyspaceID id, const Args & ...  args)
    {
        add(id, args...);
    }


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
    Key prefix_end() const {
        Key out = *this;
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

    static KeyspaceID get_kid(const std::string_view &key) {
        KeyspaceID kid;
        parse(key, kid);
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


   operator const leveldb::Slice() const {return {this->data(), this->size()};}
};

///Use StringPrefix for a key to search by prefix
/**
 * Key k(_kid, StringPrefix("abc"));
 * generates a key which is able to search for all prefixes of "abc"
 */
using StringPrefix = RemainingData;

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

}



#endif /* SRC_DOCDB_KEY_H_ */
