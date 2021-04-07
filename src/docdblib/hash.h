#ifndef DOCDB_HASH_INCLUDED_65464646
#define DOCDB_HASH_INCLUDED_65464646
#include <cstdint>
#include <string>

namespace docdb {


class Hash128 {
public:

	Hash128();
	void reset();
	void update(const std::string_view &data);
	void finish(std::uint8_t digest[16]);


protected:

	struct uint128 {
		uint128();
		uint128(std::uint32_t d, std::uint32_t c, std::uint32_t b, std::uint32_t a);
		uint128 operator*(const uint128 &other) const;
		uint128 &operator^=(const uint8_t &byte);
		std::uint32_t n[4];
	};

	uint128 state;
	static const uint128 prime;

};



}


#endif
