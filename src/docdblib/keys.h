/*
 * keys.h
 *
 *  Created on: 23. 7. 2019
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_KEYS_H_
#define SRC_DOCDBLIB_KEYS_H_

#include <string_view>

#include "serializer.h"

/*  Section 1:
 *
 *  Database list
 *  key: database_name
 *
 *
 *
 */

namespace docdb {


using Handle = std::uint32_t;
using SeqID = std::size_t;
using RevID = std::size_t;
using Timestamp = std::size_t;

enum class DBSection {
	/** Simple index mapping name to handle. It is also used as list of active databases */
	handle_index = 0,
	/** for each handle, various database informations are stored */
	db_setup = 1,
	/**	for every update change, there is sequence index. Most updates are documents. This index contains just headers*/
	seq_index_hdr = 2,
	/** seq_index but contains data */
	seq_index_data = 3,
	/** index of documents where key is docid and list of available revision where first is recent one*/
	doc_index = 4
};


enum class UpdateType {
	document = 0
};

struct Rec_Handle_Index {
	struct EnumKey {
		static constexpr DBSection section = DBSection::handle_index;
		template<typename Archive>
		void serialize(Archive &arch) {
			arch(section);
		}
	};
	struct Key: EnumKey {

		std::string name;

		template<typename Archive>
		void serialize(Archive &arch) {
			EnumKey::serialize(arch);
			arch(name);
		}
	};
	struct Value {
		Handle handle;

		template<typename Archive>
		void serialize(Archive &arch) {
			arch(handle);
		}

	};
};


struct Rec_DBSetup {
	struct Key {
		static constexpr DBSection section = DBSection::db_setup;
		Handle handle;

		template<typename Archive>
		void serialize(Archive &arch) {
			arch(section);
			arch(handle);
		}
};
	struct Value {
		//TBD
		std::uint32_t max_revision_history;

		template<typename Archive>
		void serialize(Archive &arch) {
			VerGuard<Archive> _(arch, 0);
			arch(max_revision_history);
		}
	};
};

struct Rec_SeqIndex_Hdr {
	struct EnumKey {
		static constexpr DBSection section = DBSection::seq_index_hdr;
		Handle handle;

		template<typename Archive>
		void serialize(Archive &arch) {
			arch(section);
			arch(handle);
		}

	};

	struct Key: EnumKey {
		SeqID seq;

		template<typename Archive>
		void serialize(Archive &arch) {
			EnumKey::serialize(arch);
			arch(seq);
		}
	};

	struct DocumentHdr {
		static constexpr UpdateType update_type = UpdateType::document;
		std::string id;
		RevID rev;
		Timestamp timestamp;
		std::vector<RevID> history;
		std::vector<RevID> conflicts;
		bool deleted;
		bool blob;

		template<typename Archive>
		void serialize(Archive &arch) {
			arch(update_type);
			arch(id);
			arch(rev);
			arch(timestamp);
			arch(history);
			arch(conflicts);
			arch(deleted);
		}

	};

};
struct Rec_SeqIndex_Data {

	struct Key {
		static constexpr DBSection section = DBSection::seq_index_data;
		Handle handle;
		SeqID seq;

		template<typename Archive>
		void serialize(Archive &arch) {
			arch(section);
			arch(handle);
			arch(seq);
		}

	};

	struct DocumentData  {
		std::string payload;

		template<typename Archive>
		void serialize(Archive &arch) {
			arch(payload);
		}
	};
};

struct Rec_DocIndex {

	struct EnumKey {
		static constexpr DBSection section = DBSection::doc_index;
		Handle handle;

		template<typename Archive>
		void serialize(Archive &arch) {
			arch(section);
			arch(handle);
		}

	};
	struct Key : EnumKey {
		std::string id;

		template<typename Archive>
		void serialize(Archive &arch) {
			EnumKey::serialize(arch);
			arch(id);
		}
	};

	struct RevSeq {
		Timestamp timestamp;
		RevID revId;
		SeqID seqId;

		template<typename Archive>
		void serialize(Archive &arch) {
			arch(timestamp);
			arch(revId);
			arch(seqId);
		}
	};

	struct Value {
		std::vector<RevSeq> revisions;

		template<typename Archive>
		void serialize(Archive &arch) {
			arch(revisions);
		}
	};

};

template<typename T>
void pack_to_string(const T &t, std::string &buff) {
	buff.clear();
	pack(t, [&](unsigned char c){
		buff.push_back(static_cast<char>(c));
	});
}

template<typename T>
void unpack_string(const std::string_view &data, T &t) {
	std::size_t p = 0;
	std::size_t n = data.length();
	unpack(t, [&] {
		if (p < n) return data[p++]; else throw std::runtime_error("Unexcepted end of record");
	});
}

namespace SerializerDefs{
template<> struct Exchange<DBSection> {
template<typename Stream> static void write(DBSection x, Stream &stream) {
		stream(static_cast<byte>(x));
}
template<typename Stream> static void read(DBSection &x, Stream &stream) {
	x = static_cast<DBSection>(stream());
}};
template<> struct Exchange<UpdateType> {
template<typename Stream> static void write(UpdateType x, Stream &stream) {
		stream(static_cast<byte>(x));
}
template<typename Stream> static void read(UpdateType &x, Stream &stream) {
	x = static_cast<UpdateType>(stream());
}};

}

}



#endif /* SRC_DOCDBLIB_KEYS_H_ */
