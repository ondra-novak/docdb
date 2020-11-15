/*
 * filedb.cpp
 *
 *  Created on: 1. 11. 2020
 *      Author: ondra
 */

#include "filedb.h"

#include <imtjson/object.h>
#include "shared/toString.h"
namespace docdb {

FileDB::Upload FileDB::upload(const std::string_view &name,const std::string_view &content_type, DocRevision rev) {

	DocDB &db = this->updateDB?*this->updateDB:this->db;
	Document doc = db.get(name);
	if (doc.rev == rev) {
		return Upload(*this, name, content_type, rev, blockSize);
	} else {
		return Upload(*this);
	}

}


static std::string to_str(DocRevision rev) {
	std::string out;
	ondra_shared::unsignedToString(rev, [&](char c){out.push_back(c);}, 62, 4);
	return out;
}

FileDB::Upload::Upload(FileDB &db, const std::string_view &name, const std::string_view &content_type, DocRevision rev, std::size_t blockSize)
:db(db)
,name(name)
,ct(content_type)
,rev(rev)
,blockSize(blockSize)
,dataid(to_str(rev))
,conflict(false)
,commited(false)
{
}

FileDB::Upload::Upload(FileDB &db)
:db(db),conflict(true),commited(true)
{
}

void FileDB::Upload::write(std::string_view data) {
	db.update();
	if (!conflict) {
		block.append(data);
		while (block.size() > blockSize) {
			auto remain = block.size() - blockSize;
			block.resize(blockSize);
			db.put(name, dataid, seg, block);
			seg++;
			sz+=block.size();
			block.resize(0);
			if (remain) {
				data = data.substr(data.length()-remain);
				block.append(data);
			} else {
				break;
			}
		}
	}
}

void FileDB::Upload::write(json::BinaryView data) {
	write(std::string_view(reinterpret_cast<const char *>(data.data),data.length));
}

void FileDB::Upload::write(leveldb::Slice data) {
	write(std::string_view(data.data(),data.size()));
}

void FileDB::Upload::write(const std::string &data) {
	write(std::string_view(data));
}

void FileDB::Upload::write(const char *data, std::size_t size) {
	write(std::string_view(data,size));
}

bool FileDB::Upload::commit() {
	if (conflict) return false;

	DocDB &docdb = db.updateDB?*db.updateDB:db.db;


	if (!block.empty()) {
		db.put(name, dataid, seg, block);
		seg++;
		sz+=block.size();
	}

	Document doc;
	doc.rev = rev;
	doc.deleted = false;
	doc.content = json::Object
			("size", sz)
			("content_type", ct)
			("data_id", dataid)
			("file",true);
	doc.id = name;
	doc.timestamp = 0;
	commited = docdb.put(doc);
	conflict = !commited;
	return commited;
}

FileDB::Upload::~Upload() {
	try {
		if (!commited) {
			db.erase(name, dataid);
		}
	} catch (...) {

	}
}

bool FileDB::Upload::inConflict() const {
	return conflict;
}

void FileDB::map(const Document &doc, const EmitAttachFn &emit) {
	if (doc.content["file"].getBool()) {
		auto data_id = doc.content["data_id"].getString();
		if (!data_id.empty()) {
			emit(data_id);
		}
	}
}

}

