#pragma once
#ifndef SRC_DOCDB_STRUCTURED_DOCUMENT_H_
#define SRC_DOCDB_STRUCTURED_DOCUMENT_H_

#include "row.h"

#include "utf8.h"
#include <algorithm>
#include <map>
#include <string>
#include <typeinfo>
#include <variant>
#include <vector>
namespace docdb {

class Structured;

using StructKeypairs = std::map<std::string, Structured, std::less<> >;

using StructArray = std::vector<Structured>;

struct Undefined {};

inline Undefined undefined;

using StructVariant = std::variant<
        Undefined,
        std::nullptr_t,
        bool,
        std::string,
        std::wstring,
        std::intmax_t,
        double,
        StructArray,
        StructKeypairs>;

class Structured:public StructVariant {
public:

    using KeyPairs = StructKeypairs;
    using Array = StructArray;

    using StructVariant::StructVariant;

    static const Structured not_defined;
    static const KeyPairs empty_keypair;
    static const Array empty_array;

    Structured(const std::initializer_list<Structured> &list)
        :Structured(buildValue(list.begin(), list.end())) {}


    bool defined() const {return !std::holds_alternative<Undefined>(*this);}

    bool operator==(std::nullptr_t) const {
        return std::holds_alternative<Undefined>(*this) || std::holds_alternative<std::nullptr_t>(*this);
    }

    operator bool() const {return !operator==(nullptr);}

    const Structured &operator[](const char *name) {
        return operator[](std::string_view(name));
    }
    const Structured &operator[](const std::string_view &name) {
        if (std::holds_alternative<StructKeypairs>(*this)) {
            const StructKeypairs &kp = std::get<StructKeypairs>(*this);
            auto iter = kp.find(name);
            if (iter != kp.end()) return iter->second;

        }
        return not_defined;
    }

    const Structured &operator[](std::size_t index) {
        if (std::holds_alternative<StructArray>(*this)) {
            const StructArray&arr = std::get<StructArray>(*this);
            if (index < arr.size()) return arr[index];
        }
        return not_defined;
    }

    template<typename T>
    T get() const {
        return std::visit([&](const auto &v) -> const T {
            using U = std::decay_t<decltype(v)>;
            if constexpr(std::is_convertible_v<U, T>) {
                return U(v);
            } else {
                throw std::bad_cast();
            }
        }, *this);
    }

    template<typename T>
    bool contains() const {
        return std::visit([&](const auto &v){
            using U = std::decay_t<decltype(v)>;
            return std::is_convertible_v<U, T>;
        }, *this);
    }

    const KeyPairs &keypairs() const {
        if (std::holds_alternative<KeyPairs>(*this)) return std::get<KeyPairs>(*this);
        else return empty_keypair;
    }

    const Array &array() const {
        if (std::holds_alternative<Array>(*this)) return std::get<Array>(*this);
        else return empty_array;
    }
    KeyPairs &keypairs() {
        if (!std::holds_alternative<KeyPairs>(*this)) {
            *this = KeyPairs();
        }
        return std::get<KeyPairs>(*this);
    }

    Array &array()  {
        if (!std::holds_alternative<Array>(*this)) {
            *this = Array();
        }
        return std::get<Array>(*this);
    }

    template<std::convertible_to<std::string> Key>
    void set(Key &&key, Structured &&val) {
        if (std::holds_alternative<KeyPairs>(*this)) {
            auto &x = std::get<KeyPairs>(*this);
            auto iter = x.emplace(std::string(std::forward<Key>(key)), std::move(val));
            if (!iter.second) {
                iter.first->second = std::move(val);
            }
        } else {
            throw std::bad_cast();
        }

    }
    template<std::convertible_to<std::string> Key>
    void set(Key &&key, const Structured &val) {
        if (std::holds_alternative<KeyPairs>(*this)) {
            auto &x = std::get<KeyPairs>(*this);
            auto iter = x.emplace(std::string(std::forward<Key>(key)), val);
            if (!iter.second) {
                iter.first->second = val;
            }
        } else {
            throw std::bad_cast();
        }
    }

    void set(std::size_t index, Structured &&val) {
        if (std::holds_alternative<Array>(*this)) {
            auto &x = std::get<Array>(*this);
            if (x.size()<=index) {
                x.resize(index);
                x.push_back(std::move(val));
            } else {
                x[index] = std::move(val);
            }
        } else {
            throw std::bad_cast();
        }

    }

    void set(const KeyPairs &keypairs) {
        if (!std::holds_alternative<KeyPairs>(*this)) {
            *this = KeyPairs();
        }
        auto &x = std::get<KeyPairs>(*this);
        for (const auto &y: keypairs) {
            if (!y.second.defined()) {
                x[y.first] = y.second;
            } else {
                x.erase(y.first);
            }
        }
    }

    void set(std::size_t index, const Structured &val) {
        if (std::holds_alternative<Array>(*this)) {
            auto &x = std::get<Array>(*this);
            if (x.size()<=index) {
                x.resize(index);
                x.push_back(val);
            } else {
                x[index] = val;
            }
        } else {
            throw std::bad_cast();
        }
    }

    void push_back(Structured &&val) {
        if (std::holds_alternative<Array>(*this)) {
            auto &x = std::get<Array>(*this);
            x.push_back(std::move(val));
        } else {
            throw std::bad_cast();
        }
    }
    void push_back(const Structured &val) {
        if (std::holds_alternative<Array>(*this)) {
            auto &x = std::get<Array>(*this);
            x.push_back(val);
        } else {
            throw std::bad_cast();
        }
    }
    void pop_back() {
        if (std::holds_alternative<Array>(*this)) {
            auto &x = std::get<Array>(*this);
            x.pop_back();
        } else {
            throw std::bad_cast();
        }
    }
    const Structured &back() const {
        if (std::holds_alternative<Array>(*this)) {
            auto &x = std::get<Array>(*this);
            return x.back();
        } else {
            throw std::bad_cast();
        }
    }
    const Structured &front() const {
        if (std::holds_alternative<Array>(*this)) {
            auto &x = std::get<Array>(*this);
            return x.front();
        } else  {
            throw std::bad_cast();
        }
    }

    bool operator == (const Structured &other) const {
        return std::visit([](const auto &a, const auto &b){
            using A = std::decay_t<decltype(a)>;
            using B = std::decay_t<decltype(b)>;
            if constexpr(std::is_same_v<A,B>) {
                if constexpr(std::is_same_v<A, Undefined> || std::is_same_v<A, std::nullptr_t>) {
                    return true;
                } else {
                    return a == b;
                }
            } else {
                return false;
            }
        }, *this, other);
    }

    std::string to_string() const {
        return std::visit([this](const auto &v) -> std::string{
            using T = std::decay_t<decltype(v)>;
            if constexpr(std::is_same_v<T, Undefined>) {return "[undefined]";}
            else if constexpr(std::is_same_v<T, bool>) {return v?"true":"false";}
            else if constexpr(std::is_same_v<T, std::string>) {return v;}
            else if constexpr(std::is_same_v<T, std::wstring>) {
                std::string out;
                for (auto c: v) wcharToUtf8(c, std::back_inserter(out));
                return out;
            } 
            else if constexpr(std::is_same_v<T,std::intmax_t> || std::is_same_v<T,double>) {
                return std::to_string(v);
            }
            else { 
                return to_json();
            }
        }, *this);
    }

    std::wstring to_wstring() const {
        return std::visit([this](const auto &v) -> std::wstring{
            using T = std::decay_t<decltype(v)>;
            if constexpr(std::is_same_v<T, Undefined>) {return L"[undefined]";}
            else if constexpr(std::is_same_v<T, bool>) {return v?L"true":L"false";}
            else if constexpr(std::is_same_v<T, std::string>) {
                std::wstring out;
                auto iter = v.begin();
                auto end = v.end();
                while (iter != end) {
                    out.push_back(utf8Towchar(iter,end));
                }
                return out;
            }
            else if constexpr(std::is_same_v<T, std::wstring>) {return v;}        
            else if constexpr(std::is_same_v<T,std::intmax_t> || std::is_same_v<T,double>) {
                return std::to_wstring(v);
            }
            else { 
                std::wstring out;
                std::string z = to_json();
                auto iter = z.begin();
                auto end = z.end();
                while (iter != end) {
                    out.push_back(utf8Towchar(iter,end));
                }
                return out;
            }
        }, *this);
    }

    template<typename Iter>
    Iter     to_json(Iter iter) const;


    static constexpr int flagWideStrings = 1;


    template<typename Iter>
    static Structured from_json(Iter &at, Iter end, int flags = 0);



    std::string to_json() const {
        std::string s;
        to_json(std::back_inserter(s));
        return s;
    }

    static Structured from_json(std::string_view text, int flags = 0) {
        auto b = text.begin();
        return from_json(b,text.end(), flags);
    }


protected:
    template<typename Iter>
    static Structured buildValue(Iter begin, Iter end);



};

inline const Structured Structured::not_defined;
inline const Structured::KeyPairs Structured::empty_keypair;
inline const Structured::Array Structured::empty_array;


template<typename Iter>
Structured Structured::buildValue(Iter begin, Iter end) {
    if (std::all_of(begin, end,[&](const Structured &v){
        if (std::holds_alternative<StructArray>(v)) {
            const StructArray &a = std::get<StructArray>(v);
            if (a.size() == 2 && std::holds_alternative<std::string>(a[0])) {
                return true;
            }
        }
        return false;
    })) {
        StructKeypairs r;
        for (Iter iter = begin; iter != end; ++iter) {
            const StructArray &a = std::get<StructArray>(*iter);
            r.emplace(std::get<std::string>(a[0]), a[1]);
        }
        return Structured(std::move(r));
    }
    else {
        return Structured(StructArray(begin, end));

    }
}

template<typename VariantType, typename T, std::size_t index = 0>
constexpr std::size_t variant_index_calc() {
    static_assert(std::variant_size_v<VariantType> > index, "Type not found in variant");
    if constexpr (index == std::variant_size_v<VariantType>) {
        return index;
    } else if constexpr (std::is_same_v<std::variant_alternative_t<index, VariantType>, T>) {
        return index;
    } else {
        return variant_index_calc<VariantType, T, index + 1>();
    }
}

template<typename VariantType, typename T>
constexpr int variant_index = variant_index_calc<VariantType, T>();


struct StructuredDocument {
    using Type = Structured;

    static int getByteCount(std::uint64_t number) {
        int byteCount = 1 + (number > 0xFFULL)
                          + (number > 0xFFFFULL)
                          + (number > 0xFFFFFFULL)
                          + (number > 0xFFFFFFFFULL)
                          + (number > 0xFFFFFFFFFFULL)
                          + (number > 0xFFFFFFFFFFFFULL)
                          + (number > 0xFFFFFFFFFFFFFFULL);
        return byteCount;
    }


    template<typename Iter>
    static Iter to_binary(const Structured &val, Iter iter) {
        const StructVariant &vval = val;
        unsigned char index = static_cast<char>(vval.index()) << 4;
        return std::visit([&](const auto &v){
            using Type  = std::decay_t<decltype(v)>;
            if constexpr(std::is_same_v<Type, bool>) {
                index |= v?1:0;
                *iter = index;
                ++iter;
                return iter;
            } else if constexpr(std::is_same_v<Type, Undefined>
                        || std::is_same_v<Type, std::nullptr_t>) {
                *iter = index;
                ++iter;
                return iter;
            } else if constexpr(std::is_same_v<Type, std::intmax_t>) {
                std::intmax_t n = (v<0);
                index |= n << 3;
                std::uint64_t uv = (1-(n<<1))*v;
                return uint_to_binary(index, uv, iter);
            } else if constexpr(std::is_same_v<Type, double>) {
                *iter = index;
                ++iter;
                return Row::serialize_items(iter, v);
            } else if constexpr(std::is_same_v<Type, std::string>) {
                return string_to_binary(index, v, iter);
            } else if constexpr(std::is_same_v<Type, std::wstring>) {
                return wstring_to_binary(index, v, iter);
            } else if constexpr(std::is_same_v<Type, Structured::KeyPairs>) {
                iter = uint_to_binary(index, v.size(), iter);
                for (const auto &[key, val]: v) {
                    iter = string_to_binary(0, key, iter);
                    iter = to_binary(val, iter);
                }
                return iter;
            } else {
                static_assert(std::is_same_v<Type, Structured::Array>);
                iter = uint_to_binary(index, v.size(), iter);
                for (const auto &val: v) {
                    iter = to_binary(val, iter);
                }
                return iter;
            }


        }, val);
    }


    template<typename Iter>
    static Structured from_binary(Iter &at, Iter end) {
        if (at == end) return undefined;
        unsigned char code = *at++;
        int index = code >> 4;
        int extra = code & 0xF;
        return number_to_constant<0,std::variant_size_v<StructVariant>-1 >(index, [&](auto bidx) -> Structured {
            if constexpr(!bidx.valid) {
                return undefined;
            } else {
                using Type = std::variant_alternative_t<bidx.value, StructVariant>;
                if constexpr(std::is_same_v<Type, bool>) {
                    return extra != 0;
                } else if constexpr(std::is_same_v<Type, std::nullptr_t> || std::is_same_v<Type, Undefined>) {
                    return Type();
                } else if constexpr(std::is_same_v<Type, std::string>) {
                    return string_from_binary(extra, at, end);
                } else if constexpr(std::is_same_v<Type, std::wstring>) {
                    return wstring_from_binary(extra, at, end);
                } else if constexpr(std::is_same_v<Type, std::intmax_t>) {
                    std::intmax_t v = uint_from_binary(extra, at, end);
                    if (extra & 0x08) v = -v;
                    return v;
                } else if constexpr(std::is_same_v<Type, double>) {
                    return Row::deserialize_item<double>(at, end);
                } else if constexpr(std::is_same_v<Type, Structured::KeyPairs>) {
                    Structured::KeyPairs out;
                    std::size_t count = uint_from_binary(extra, at,end);
                    for (std::size_t i = 0; i < count; ++i) {
                        if (at == end) break;
                        char sz = *at;
                        at++;
                        std::string key = string_from_binary(sz, at,end);
                        if (at == end) break;
                        out.emplace(std::move(key), from_binary(at, end));
                    }
                    return out;
                } else {
                    static_assert(std::is_same_v<Type, Structured::Array>);
                    Structured::Array out;
                    std::size_t count = uint_from_binary(extra, at,end);
                    for (std::size_t i = 0; i < count; ++i) {
                        if (at == end) break;
                        out.push_back(from_binary(at, end));
                    }
                    return out;
                }
            }
        });

    }

    template<typename N, typename Iter>
    static Iter uint_to_binary(unsigned char index, N uv, Iter iter) {
        return number_to_constant<0,7>(getByteCount(uv),[&](auto a){
            if constexpr(!a.valid) {
                return iter;
            } else {
                *iter = index | (a.value-1);
                ++iter;
                for (int i = 0; i < a.value; i++) {
                    *iter = uv & 0xFF;
                    ++iter;
                    uv >>= 8;
                }
            }
            return iter;
        });
    }
    template<typename Iter>
    static Iter string_to_binary(unsigned char index, const std::string_view &val, Iter iter) {
        iter = uint_to_binary(index, val.size(), iter);
        for (char c: val) *iter++ = c;
        return iter;
    }
    template<typename Iter>
    static Iter wstring_to_binary(unsigned char index, const std::wstring_view &val, Iter iter) {
        *iter = index;
        ++iter;
        for (wchar_t c: val) iter = wcharToUtf8(c, iter);
        *iter = 0;
        ++iter;
        return iter;
    }

    template<typename Iter>
    static std::uint64_t uint_from_binary(int extra, Iter &at, Iter iter) {
        return number_to_constant<0,7>((extra & 0x7),[&](auto a)->std::uint64_t{
            if constexpr(!a.valid) {
                return 0;
            } else {
                constexpr auto count = a.value+1;
                std::uint64_t v = 0;
                for (int i = 0; i < count; i++)  {
                    v = v | (static_cast<std::uint64_t>(*at) << (i * 8));
                    ++at;
                }
                return v;
            }
        });
    }

    template<typename Iter>
    static std::string string_from_binary(int extra, Iter &at, Iter iter) {
        std::size_t sz = uint_from_binary(extra, at, iter);
        std::string out;
        out.reserve(sz);
        for (std::size_t i = 0; i < sz && at != iter; ++i) {
            out.push_back(*at);
            ++at;
        }
        return out;
    }
    template<typename Iter>
    static std::wstring wstring_from_binary(int extra, Iter &at, Iter iter) {
        std::wstring out;
        while (at != iter && *at) {
            out.push_back(utf8Towchar(at, iter));
        }
        at++;
        return out;

    }
};

}



#endif /* SRC_DOCDB_STRUCTURED_DOCUMENT_H_ */
