/*
 * attachments.cpp
 *
 *  Created on: 31. 10. 2020
 *      Author: ondra
 */

#include "attachments.h"

#include "formats.h"
#include <imtjson/binjson.tcc>
#include "changesiterator.h"
namespace docdb {



Attachments::Attachments(DocDB &db):UpdatableObject(db) {

	AttchKey key;
	std::string val;
	if (db.mapGet(key, val)) {
		seqId = string2index(val);
	}

}

void Attachments::put(const std::string_view &docid,
		const std::string_view &attid, const std::string_view data) {
	return put(docid, attid, 0, data);
}

void Attachments::put(const std::string_view &docid,
		const std::string_view &attid, std::size_t seg,
		const std::string_view data) {

	update();

	AttchKey key;
	key.append(docid);
	key.push_back(0);
	key.append(attid);
	key.push_back(0);
	Index2String_Push<8>::push(seg,key);

	db.mapSet(key,data);

}

std::optional<std::string> Attachments::get(const std::string_view &docid,
		const std::string_view &attid, std::size_t seg) {

	update();

	std::string res;

	AttchKey key;
	key.append(docid);
	key.push_back(0);
	key.append(attid);
	key.push_back(0);
	Index2String_Push<8>::push(seg,key);

	if (db.mapGet(key, res)) return res;
	else return std::optional<std::string>();

}

void Attachments::erase(const std::string_view &docid, const std::string_view &attid) {
	AttchKey key;
	key.append(docid);
	key.push_back(0);
	key.append(attid);
	key.push_back(0);
	db.mapErasePrefix(key);
}

void Attachments::storeState() {
	AttchKey key;
	std::string val;
	Index2String_Push<8>::push(seqId,val);
	db.mapSet(key, val);
}

SeqID Attachments::scanUpdates(ChangesIterator &&iter) {

	class Idents: public EmitAttachFn, public  std::vector<std::string>{
	public:
		virtual void operator()(const std::string_view &att_id) const {
			const_cast<Idents *>(this)->push_back(std::string(att_id));
		}
	};

	//contains list of identified attachments
	Idents idents;
	//new sequence number
	SeqID newseqid = 0;
	//temporary key
	AttchKey tmp;

	//iterate through new documents
	while (iter.next()) {
		//pick docid
		auto docid = iter.doc();
		//pick document
		auto doc = db.get(docid);
		//clear idents
		idents.clear();
		//map document to idents
		map(doc,idents);
		//remember new sequence id
		newseqid = iter.seq();

		//prepare list of attachments / segments
		AttchKey key;
		key.append(docid);
		key.push_back(0);
		//scan all attachments
		auto iter2 = db.mapScanPrefix(key,false);
		//until end
		while (iter2.next()) {
			//pick key
			auto k = iter2.key();
			//skip docid + ending zero
			k = k.substr(docid.length()+1);
			//find end of attachment id
			auto pos = k.find('\0');
			//if zero found
			if (pos != k.npos) {
				//extract attachment ident
				std::string_view ident = k.substr(0,pos);
				//search all idents
				if (std::find(idents.begin(), idents.end(), ident) == idents.end()) {
					//if not found, use temporary key to create erase key
					tmp.set(iter2.key());
					//erase record
					db.mapErase(tmp);
				}
			} else {
				//also erase invalid keys
				tmp.set(iter2.key());
				db.mapErase(tmp);
			}
		}

	}
	//report newseqid
	return newseqid;


}

}
