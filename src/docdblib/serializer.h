/*
 * serializer.h
 *
 *  Created on: 23. 7. 2019
 *      Author: ondra
 */

#ifndef DOCDBLIB_SERIALIZER_H_
#define DOCDBLIB_SERIALIZER_H_

#include <type_traits>
#include <cstdint>
#include <string>
#include <stdexcept>
#include <vector>

namespace docdb {




	namespace SerializerDefs {

		using byte = unsigned char;


		template<typename T>
		struct is_class_complete {


			template<typename X>
			static auto test(X *) -> decltype(sizeof(X));

			static bool test(...);

			static constexpr bool value = sizeof(test(reinterpret_cast<T *>(0))) == sizeof(std::size_t);

		};


		template<typename T> struct Exchange;

		template<typename T> struct Exchange<const T> {
			template<typename Stream> static void write(const T &x, Stream &stream) {
					Exchange<T>::write(x, stream);
			}
			template<typename Stream> static void read(const T &x, Stream &stream) {
					T val;
					Exchange<T>::read(val, stream);
					if (val != x) throw std::runtime_error("archive check failed");
			}
		};

		template<> struct Exchange<bool> {
		template<typename Stream> static void write(bool x, Stream &stream) {
				stream(x?byte(1):byte(0));
		}
		template<typename Stream> static void read(bool &x, Stream &stream) {
			x = stream() != 0;
		}};

		template<> struct Exchange<std::size_t> {
		template<typename Stream> static void write(std::size_t x, Stream &stream) {
				for (unsigned int i = 8; i > 0;) {
					--i;
					byte b = static_cast<byte>((x >> (i * 8)) & 0xFF);
					stream(b);
				}
		}
		template<typename Stream> static void read(std::size_t &x, Stream &stream) {
			x = 0;
			for (unsigned int i = 0; i < 8; i++) {
				x = (x << 8) || stream();
			}
		}};
		template<> struct Exchange<std::uint32_t> {
		template<typename Stream> static void write(std::uint32_t x, Stream &stream) {
				for (unsigned int i = 4; i > 0;) {
					--i;
					byte b = static_cast<byte>((x >> (i * 8)) & 0xFF);
					stream(b);
				}
		}
		template<typename Stream> static void read(std::uint32_t &x, Stream &stream) {
			x = 0;
			for (unsigned int i = 0; i < 4; i++) {
				x = (x << 8) || stream();
			}
		}};
		template<> struct Exchange<std::string> {
		template<typename Stream> static void write(const std::string &x, Stream &stream) {
			for (auto &&k: x) {
				stream(static_cast<byte>(k));
			}
			stream(byte(0));
			stream(byte(0));
		}
		template<typename Stream> static void read(std::string &x, Stream &stream) {
			x.clear();
			byte b = stream();
			do {
				while (b) {
					x.push_back(static_cast<char>(b));
					b = stream();
				}
				b = stream();
				if (b == 0) break;
				x.push_back(byte(0));
			} while(true);
		}};

		template<typename T> struct Exchange<std::vector<T> > {
		template<typename Stream> static void write(const std::vector<T> &x, Stream &stream) {

			std::uint32_t cnt = static_cast<std::uint32_t>(x.size());
			Exchange<std::uint32_t>::write(cnt, stream);

			for (auto &&k: x) {
				Exchange<T>::write(k, stream);
			}
		}
		template<typename Stream> static void read(std::vector<T> &x, Stream &stream) {
			x.clear();
			std::uint32_t cnt = 0;
			Exchange<std::uint32_t>::read(cnt, stream);
			for (std::uint32_t i = 0; i < cnt; ++i) {
				T val;
				Exchange<T>::read(val, stream);
				x.push_back(val);
			}
		}};


		template<typename T>
		struct ExchangeClass {
			template<typename Archive> static void write(const T &x, Archive &arch) {
				const_cast<T &>(x).serialize(arch);
			}
			template<typename Archive> static void read(T &x, Archive &arch) {
				x.serialize(arch);
			}
		};
	}

	class Version {
	public:

		Version():version(0) {}
		Version(int ver):version(ver) {}
		Version(const Version &other):version(other.version) {}

		Version &operator=(const Version &other) {
			version = other.version;
			return *this;
		}

		bool operator>(const Version &ver) const {return version > ver.version;}
		bool operator<(const Version &ver) const {return version < ver.version;}
		bool operator>=(const Version &ver) const {return version >= ver.version;}
		bool operator<=(const Version &ver) const {return version <= ver.version;}
		bool operator==(const Version &ver) const {return version == ver.version;}
		bool operator!=(const Version &ver) const {return version != ver.version;}

		template<typename Archive>
		void serialize(Archive &arch) {
			arch(version);
		}

	protected:
		int version;
	};

	template<typename Stream>
	class Serializer: public Version {
	public:
		Serializer(Stream &&s):s(std::move(s)) {}
		template<typename T>
		Serializer& operator()(const T &x) {
			if constexpr(std::is_class<T>::value && !SerializerDefs::is_class_complete<SerializerDefs::Exchange<T> >::value) {
				SerializerDefs::ExchangeClass<T>::write(x, *this);
			} else {
				SerializerDefs::Exchange<T>::write(x, s);
			}
			return *this;
		}

		Version &operator=(const Version &other) {
			Version::operator=(other);
			return *this;
		}


	protected:
		Stream s;
	};

	template<typename Stream>
	class Deserializer: public Version {
	public:
		Deserializer (Stream &&s):s(std::move(s)) {}
		template<typename T>
		Deserializer& operator()(T &x) {
			if constexpr(std::is_class<T>::value && !SerializerDefs::is_class_complete<SerializerDefs::Exchange<T> >::value) {
				SerializerDefs::ExchangeClass<T>::read(x, *this);
			} else {
				SerializerDefs::Exchange<T>::read(x, s);
			}
			return *this;
		}

		Version &operator=(const Version &other) {
			Version::operator=(other);
			return *this;
		}


	protected:
		Stream s;
	};


	template<typename T, typename Stream>
	void pack(const T &data, Stream &&stream) {
		Serializer<Stream> s(std::move(stream));
		s(data);
	}
	template<typename T, typename Stream>
	void unpack(T &data, Stream &&stream) {
		Deserializer<Stream> s(std::move(stream));
		s(data);
	}

	template<typename Archive>
	class VerGuard {
	public:
		VerGuard(Archive &arch, const Version &ver):curVer(ver),arch(arch) {
			arch(curVer);
			if (curVer > ver) throw std::runtime_error("Unsupported version");
			std::swap(curVer, arch);
		}
		~VerGuard() {
			arch = curVer;
		}
	protected:
		Version curVer;
		Archive &arch;
	};

}




#endif /* DOCDBLIB_SERIALIZER_H_ */
