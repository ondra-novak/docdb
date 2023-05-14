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

using LocalizedString = LocalizedBasicString<char>;
using LocalizedWString = LocalizedBasicString<wchar_t>;


class Key: public std::string {
public:

    Key(KeyspaceID id) {
        add(id);
    }

    template<typename ... Args >
    Key(KeyspaceID id, const Args & ...  args)
    {
        add(id, args...);
    }

    template<typename ... Args>
    void add(KeyspaceID id, const Args & ... args) {
        for (int i = 0; i < sizeof(KeyspaceID); i++) {
            this->push_back(static_cast<char>(id & 0xFF) >> ((sizeof(KeyspaceID) - i)*8));
        }
        add(args...);
    }

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

    void add() {};

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

   operator const leveldb::Slice() const {return {this->data(), this->size()};}


};



}



#endif /* SRC_DOCDB_KEY_H_ */
