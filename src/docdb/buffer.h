#pragma once
#ifndef SRC_DOCDB_BUFFER_H_
#define SRC_DOCDB_BUFFER_H_


#include <leveldb/slice.h>
#include <string_view>
#include <span>

namespace docdb {

///BufferBase - used as temporary space to build keys or values
/**
 * BufferBase is base class for Buffer<>
 *
 * @tparam T type of object.
 * @tparam small_buffer_size size of static area for small buffer which are not allocated
 *
 * @note type T should be trivially constructible. If not, note that object allocates whole capacity and call constructor for every item before it is even used
 */
template<typename T, std::size_t small_buffer_size>
class BufferBase { // @suppress("Miss copy constructor or assignment operator")
public:

    using value_type = T;
    using view_type = std::conditional_t<std::is_trivial_v<T> && std::is_standard_layout_v<T>,
            std::basic_string_view<T>, std::span<T> >;

    using const_iterator = const T *;
    using iterator = const_iterator;

    operator view_type() const {return {_begin, _len};}

    constexpr BufferBase():_begin(_static),_len(0),_capa(sizeof(_static)) {}

    constexpr BufferBase(const view_type &string) {
        alloc_and_copy(string.begin(), string.end());
    }
    template<std::size_t N>
    constexpr BufferBase(const char (&string)[N]) {
        if constexpr((std::is_same_v<T, char> || std::is_same_v<T, wchar_t>) && N>0) {
            if (string[N-1] == T(0)) {
                alloc_and_copy(string, string+N-1);
                return;
            }
        }
        alloc_and_copy(std::begin(string), std::end(string));
    }

    constexpr BufferBase(const T *data, std::size_t count) {
        alloc_and_copy(data, data+count);
    }
    constexpr BufferBase(const BufferBase &other) {
        alloc_and_copy(other.begin(), other.end());
    }
    template<std::size_t N>
    constexpr BufferBase(const BufferBase<T,N> &other) {
        alloc_and_copy(other.begin(), other.end());
    }

    template<std::size_t N>
    constexpr BufferBase(BufferBase<T,N> &&other) {
        move_in(other);
    }

    constexpr BufferBase(BufferBase &&other) {
        move_in(other);
    }

    constexpr ~BufferBase() {
        release();
    }

    template<std::size_t N>
    constexpr BufferBase &operator=(const BufferBase<T, N> &other) {
        return operator_assign(other);
    }

    constexpr BufferBase &operator=(const BufferBase &other) {
        return operator_assign(other);    }

    template<std::size_t N>
    constexpr BufferBase &operator=(BufferBase<T, N> &&other) {
        return operator_assign(other);
    }

    constexpr BufferBase &operator=(BufferBase &&other) {
        return operator_assign(std::move(other));
    }

    constexpr void push_back(T val) {
        if (_len == _capa) extend();
        _begin[_len] = val;
        ++_len;
    }

    constexpr void append(const view_type &data) {
        if (_len + data.size() > _capa) extend_to(_len+data.size());
        std::copy(data.begin(), data.end(), _begin+_len);
        _len+=data.size();
    }
    constexpr void clear() {
        _len = 0;
    }

    constexpr const T &back() const {
        return _begin[_len-1];
    }

    constexpr void pop_back() {
        _len--;
    }

    constexpr const T &operator[](std::size_t x) const {return _begin[x];}
    constexpr T &operator[](std::size_t x) {return _begin[x];}

    constexpr void resize(std::size_t x) {
        if (x > _capa) extend_to(x);
        _len = x;
    }

    constexpr void resize(std::size_t x, const T &init) {
        if (x > _capa) extend_to(x);
        if (x > _len) {
            std::fill(_begin+_len, _begin+x, init);
        }
        _len = x;
    }

    constexpr void reserve(std::size_t x) {
        if (x > _capa) extend_to_2(x);
    }

    constexpr const T *data() const {return _begin;}
    constexpr T *data() {return _begin;}

    constexpr std::size_t length() const {return _len;}
    constexpr std::size_t size() const {return _len;}
    constexpr bool empty() const {return _len == 0;}
    constexpr const T *begin() const {return _begin;}
    constexpr const T *end() const {return _begin+_len;}
    constexpr T *begin() {return _begin;}
    constexpr T *end() {return _begin+_len;}

    constexpr view_type slice(std::size_t from) {
        if (from > _len) return {};
        return {_begin+from, _len-from};
    }

    constexpr view_type slice(std::size_t from, std::size_t count) {
        if (from > _len) return {};
        if (from+count > _len) count = _len - from;
        return {_begin+from, count};
    }

    constexpr std::size_t capacity() const {return _capa;}
    constexpr bool operator<(const view_type &other) const {
        return operator view_type() < other;
    }
    constexpr bool operator>(const view_type &other) const {
        return operator view_type() > other;
    }
    constexpr bool operator<=(const view_type &other) const {
        return operator view_type() <= other;
    }
    constexpr bool operator>=(const view_type &other) const {
        return operator view_type() >= other;
    }
    constexpr bool operator==(const view_type &other) const {
        return operator view_type() == other;
    }
    constexpr bool operator!=(const view_type &other) const {
        return operator view_type() != other;
    }

protected:
    template<typename X, std::size_t N>
    friend class BufferBase;

    static const auto adj_small_buffer_size = std::max<std::size_t>(small_buffer_size, 1);

    T *_begin = nullptr;
    std::size_t _len = 0;
    std::size_t _capa = 0;
    T _static[adj_small_buffer_size] = {};

    constexpr bool is_local() const {
        return _begin == _static;
    }
    template<typename Iter>
    constexpr void alloc_and_copy(Iter from, Iter to) {
        std::size_t sz = std::distance(from, to);
        if (sz <= small_buffer_size) {
            _begin = _static;
            _capa = sizeof(_static);
        } else {
            _begin = new T[sz];
            _capa = sz;
        }
        std::copy(from, to, _begin);
        _len = sz;
    }

    constexpr void release() {
        if (_begin != _static) {
            delete[] _begin;
        }
    }

    constexpr void extend() {
        std::size_t newcapa = _capa *3 / 2;
        extend_to_2(newcapa);
    }

    constexpr void extend_to(std::size_t c) {
        std::size_t newcapa = _capa;
        while (newcapa < c ){
            newcapa = _capa *3 / 2;
        }
        extend_to_2(newcapa);
    }

    constexpr void extend_to_2(std::size_t newcapa) {
        T *new_begin = new T[newcapa];
        std::copy(begin(), end(), new_begin);
        release();
        _begin = new_begin;
        _capa = newcapa;
    }
    template<size_t N>
    constexpr void move_in(BufferBase<T, N> &other) {
        if (other.is_local()) {
            alloc_and_copy(other.begin(), other.end());
            other._len = 0;
        } else {
            _begin = other._begin;
            _len = other._len;
            _capa = other._capa;
            other._capa = N;
            other._len = 0;
            other._begin = other._static;
        }
    }

    template<std::size_t N>
    constexpr BufferBase &operator_assign(BufferBase<T, N> &&other) {
        if (this != &other){
            if (other.is_local()) {
                _len = other.size();
                if (_len <= _capa) {
                    std::copy(other.begin(), other.end(), _begin);
                } else {
                    release();
                    alloc_and_copy(other.begin(), other.end());
                }
            } else {
                release();
                _begin = other._begin;
                _len = other._len;
                _capa = other._capa;
                other._capa = N;
                other._len = 0;
                other._begin = other._static;
            }
        }
        return *this;
    }

    template<std::size_t N>
    constexpr BufferBase &operator_assign(const BufferBase<T, N> &other) {
        if (this != &other) {
            _len = other.size();
            if (_len <= _capa) {
                std::copy(other.begin(), other.end(), _begin);
            } else {
                release();
                alloc_and_copy(other.begin(), other.end());
            }
        }
        return *this;
    }

};

///Buffer - used as temporary space to build keys or values
/**
 * Buffer works as string.
 *
 * @tparam T type of object.
 * @tparam small_buffer_size size of static area for small buffer which are not allocated.
 * This value can be 0, which means, that everything will be allocated, however, this can
 * be useful, to enforce movability using pointer
 *
 * @note type T should be trivially constructible. If not, note that object allocates whole capacity and call constructor for every item before it is even used
 */
template<typename T, std::size_t small_buffer_size>
class Buffer: public BufferBase<T, small_buffer_size> {
public:
    using BufferBase<T, small_buffer_size>::BufferBase;
};

template<std::size_t small_buffer_size>
class Buffer<char, small_buffer_size>: public BufferBase<char, small_buffer_size> {
public:
    using BufferBase<char, small_buffer_size>::BufferBase;

    Buffer(const leveldb::Slice &slc): BufferBase<char, small_buffer_size>(slc.data(), slc.size()) {}
    operator leveldb::Slice() const {return {this->_begin, this->_len};}
};


}


inline leveldb::Slice to_slice(const std::string_view &v) {
    return {v.data(), v.size()};
}

inline std::string_view to_string(const leveldb::Slice &slice) {
    return {slice.data(), slice.size()};
}



template<typename Stream, std::size_t N>
Stream &operator << (Stream &s, const docdb::Buffer<char, N> &buffer) {
    return s << buffer.operator std::string_view();
}


#endif /* SRC_DOCDB_BUFFER_H_ */
