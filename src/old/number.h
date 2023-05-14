/*
 * number.h
 *
 *  Created on: 11. 11. 2022
 *      Author: ondra
 */

#ifndef SRC_DOCDB_NUMBER_H_
#define SRC_DOCDB_NUMBER_H_
#include <cmath>
#include <cstdint>
#include <cstdlib>

namespace docdb {

namespace _numbers {


    template<typename Iter>
    Iter decompose_number(double num, Iter iter) {
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
        *iter = (static_cast<unsigned char>(exp >> 8));
        ++iter;
        *iter = (static_cast<unsigned char>(exp & 0xFF));
        ++iter;
        for (unsigned int i = 0; i < 8; i++) {
            unsigned char c = (m >> ((7-i)*8) & 0xFF);
            *iter = c;
            ++iter;
        }
        return iter;
    }

    template<typename Iter>
    Iter decompose_number_int(std::int64_t numb, Iter iter) {
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
        *iter=static_cast<unsigned char>(exp >> 8);
        ++iter;
        *iter=static_cast<unsigned char>(exp & 0xFF);
        ++iter;
        for (unsigned int i = 0; i < 8; i++) {
            unsigned char c = (m >> ((7-i)*8) & 0xFF);
            *iter = c;
            ++iter;
        }
        return iter;
    }

    template<typename Iter>
    Iter decompose_number_uint(std::uint64_t numb, Iter iter) {
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
        *iter = (static_cast<unsigned char>(exp >> 8));
        ++iter;
        *iter = (static_cast<unsigned char>(exp & 0xFF));
        ++iter;
        for (unsigned int i = 0; i < 8; i++) {
            unsigned char c = (m >> ((7-i)*8) & 0xFF);
            *iter = c;
            ++iter;
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


    template<typename Iter>
    Iter compose_number(Iter iter, double &out) {
        static MaskTables mt;

        unsigned char e1 = *iter;
        ++iter;
        unsigned char e2 = *iter;
        ++iter;
        bool neg = (e1 & 0x8) == 0;
        int exp = static_cast<int>(e1 & 0xF) << 8 | e2;
        std::uint64_t m = 0;
        for (unsigned int i = 0; i < 8; i++) {
            m = (m << 8) | (*iter & 0xFF);
            ++iter;
        }
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

        out =std::scalbln(sign*m, exp-64);
        return iter;
    }


    
    template<typename Iter, typename UInt>
    inline Iter write_unsigned_int(Iter where, UInt what) {
        int pos = sizeof(UInt);
        while (pos) {
            --pos;
            *where = (static_cast<char>((what >> (pos * 8)) & 0xFF));
            ++where;
        }
        return where;
    }

    template<typename Iter, typename UInt>
    inline Iter read_unsigned_int(Iter iter, UInt &target) {
        for (int i = 0; i < sizeof(UInt);++i) {
            target = (target << 8) | static_cast<unsigned char>(*iter);
            ++iter;
        }
        return iter;
    }
    
    

    
    
}

}




#endif /* SRC_DOCDB_NUMBER_H_ */
