#include "key.h"

namespace docdb {


Key::Key(KeyspaceID id) {
    _k.push_back(id);
}
Key::Key():Key(0xFF) {}

Key::Key(KeyspaceID id, std::string_view content) {
    _k.reserve(content.size()+1);
    _k.push_back(id);
    _k.append(content);
}


void Key::add_raw(std::string_view content) {
    _k.append(content);
}

void Key::clear() {
    _k.resize(1);
}

template<typename T>
static void write(std::string &where, T what) {
    int pos = sizeof(T);
    while (pos) {
        --pos;
        where.push_back(static_cast<char>((what >> (pos * 8)) & 0xFF));
    }
}

template<typename T>
static void read(keycontent::iterator &iter, T &target) {
    for (int i = 0; i < sizeof(T);++i) {
        target = (target << 8) | static_cast<unsigned char>(*iter);
        ++iter;
    }
}

void Key::add(std::uint8_t x) {
    write(_k, x);
}

void Key::add(std::uint16_t x) {
    write(_k, x);
}

void Key::add(std::uint32_t x) {
    write(_k, x);
}

void Key::add(std::uint64_t x) {
    write(_k, x);
}


void Key::add(std::string_view text) {
    for (char c:text) {
        auto d = static_cast<unsigned char>(c);
        if (d<2) _k.push_back('\001');
        _k.push_back(c);
    }
    _k.push_back('\0');
}

void Key::keyspace(KeyspaceID id) {
    _k[0] = static_cast<char>(id);
}

std::uint8_t keycontent::read_uint8(iterator &iter) {
    std::uint8_t v;
    read(iter, v);
    return v;
}

std::uint16_t keycontent::read_uint16(iterator &iter) {
    std::uint16_t v;
    read(iter, v);
    return v;
}

std::uint32_t keycontent::read_uint32(iterator &iter) {
    std::uint32_t v;
    read(iter, v);
    return v;
}

std::uint64_t keycontent::read_uint64(iterator &iter) {
    std::uint64_t v;
    read(iter, v);
    return v;
}

void keycontent::read_string(std::string &out, iterator &iter) {
    while (*iter) {
        if (*iter == '\001') ++iter;
        out.push_back(*iter);
        ++iter;
    }    
}


}
