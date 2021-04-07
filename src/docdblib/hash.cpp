#include "hash.h"

namespace docdb {


Hash128::Hash128()
:state(0x6c62272e,0x07bb0142,0x62b82175,0x6295c58d) {}



const Hash128::uint128 Hash128::prime(0x00000000,0x01000000,0x00000000,0x0000013B);


void Hash128::update(const std::string_view &data) {
	for (char c: data) {
		state^=(static_cast<std::uint8_t>(c));
		state = state * prime;
	}
}

void Hash128::reset() {
	state = uint128(0x6c62272e,0x07bb0142,0x62b82175,0x6295c58d);
}

void Hash128::finish(std::uint8_t digest[16]) {
	int pos = 0;
	for (int i = 3; i >= 0; i--) {
		auto p = state.n[i];
		digest[pos++] = (static_cast<char>((p >>24)& 0xFF));
		digest[pos++] = (static_cast<char>((p >>16)& 0xFF));
		digest[pos++] = (static_cast<char>((p >>8)& 0xFF));
		digest[pos++] = (static_cast<char>(p& 0xFF));
	}
}

Hash128::uint128::uint128(std::uint32_t d, std::uint32_t c,
		std::uint32_t b, std::uint32_t a) {
	n[0] = a;
	n[1] = b;
	n[2] = c;
	n[3] = d;
}

static inline std::uint32_t multwc(std::uint32_t a, std::uint32_t b, std::uint32_t &cr, std::uint32_t i) {
	std::uint64_t x = a;
	x *= b; x += cr; x += i;
	cr = static_cast<std::uint32_t>((x >> 32)  & 0xFFFFFFFF);
	return static_cast<std::uint32_t>(x & 0xFFFFFFFF);
}

Hash128::uint128 Hash128::uint128::operator *(const uint128 &other) const {
	std::uint32_t cr = 0;
	uint128 res;
	res.n[0] = multwc(n[0],other.n[0], cr, 0);
	res.n[1] = multwc(n[1],other.n[0], cr, 0);
	res.n[2] = multwc(n[2],other.n[0], cr, 0);
	res.n[3] = multwc(n[3],other.n[0], cr, 0);
	cr = 0;
	res.n[1] = multwc(n[0],other.n[1], cr, res.n[1]);
	res.n[2] = multwc(n[1],other.n[1], cr, res.n[2]);
	res.n[3] = multwc(n[2],other.n[1], cr, res.n[3]);
	cr = 0;
	res.n[2] = multwc(n[0],other.n[2], cr, res.n[2]);
	res.n[3] = multwc(n[1],other.n[2], cr, res.n[3]);
	cr = 0;
	res.n[3] = multwc(n[0],other.n[3], cr, res.n[3]);
	return res;
}

Hash128::uint128& Hash128::uint128::operator ^=(const uint8_t &byte) {
	n[0] ^= byte;
	return *this;
}

}
