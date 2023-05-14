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

class Value: public std::string {
public:

    using std::string::string;


    template<typename ... Args>
    void add(bool b, const Args & ... args) {
        this->push_back(b?'0':'1');
        add(args...);
    }

    template<typename ... Args>
    void add(double v, const Args & ... args) {
        BinHelper<double> hlp{-v};
        for (char c: hlp.bin) this->push_back(c);
        add(args...);
    }

    template<std::unsigned_integral UInt, typename ... Args>
    void add(UInt v, const Args & ... args) {
        for (int i = 0; i < 8; i++) {
            int shift = 8*(8-i);
            this->push_back(static_cast<char>((v >> shift) & 0xFF));
        }
        add(args...);
    }

    template<typename ... Args>
    void add(const std::string_view &s, const Args & ... args) {
        for (char c: s) this->push_back(c);
        this->push_back('\0');
        add(args...);
    }

    template<typename ... Args>
    void add(const LocalizedString &s, const Args & ... args) {
        auto &f = std::use_facet<std::collate<char> >(s.get_locale());
        auto out = f.transform(s.begin(), s.end());
        for (char c: out) this->push_back(c);
        this->push_back('\0');
        add(args...);
    }

    void add(const RemainingData &data) {
        append(data);
    }

    void add() {};

    template<typename ... Args>
    static int deserialize(const std::string_view &s, bool &b,  Args &... args) {
        if (s.empty()) return 0;
        b = s[0] != '0';
        return 1 + deserialize(s.substr(1), args...);
    }

    template<typename ... Args>
    static int deserialize(const std::string_view &s, double &v,  Args &... args) {
        BinHelper<double> hlp{-v};
        if (s.size() < sizeof(hlp)) return 0;
        for (int i = 0; i < sizeof(hlp); i++) hlp.bin[i] = s[i];
        v = -hlp.val;
        return 1 + deserialize(s.substr(8), args...);
    }

    template<std::unsigned_integral UInt, typename ... Args>
    static int deserialize(const std::string_view &s, UInt &v,  Args &... args) {
        if (s.size() < sizeof(v)) return 0;
        v = 0;
        for (int i = 0; i < sizeof(v); i++) v = (v << 8) | static_cast<unsigned char>(s[i]);
        return 1 + deserialize(s.substr(8), args...);
    }

    template<typename ... Args>
    static int deserialize(const std::string_view &s, std::string_view &v,  Args &... args) {
        auto pos = s.find('\0');
        if (pos == s.npos) {
            v = s;
            return !v.empty();
        } else {
            return 1+deserialize(s.substr(pos+1), args...);
        }
    }
    template<typename ... Args>
    static int deserialize(const std::string_view &s, RemainingData &v) {
        v = RemainingData(s.data(), s.size());
        return 1;
    }

    static int deserialize(const std::string_view &s) {
        return 0;
    }


};


class Key: public Value {
public:

    Key(KeyspaceID id) {
        add(id);
    }

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
        deserialize(key, kid);
        return kid;
    }



   operator const leveldb::Slice() const {return {this->data(), this->size()};}
};



}



#endif /* SRC_DOCDB_KEY_H_ */
