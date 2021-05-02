/*
 * attachments.cpp
 *
 *  Created on: 31. 10. 2020
 *      Author: ondra
 */

#include "attachments.h"

#include "formats.h"
#include <imtjson/binjson.tcc>
#include <imtjson/parser.h>
#include <imtjson/operations.h>
#include "changesiterator.h"
namespace docdb {

std::size_t Attachments::cfgMinSegment = 10000;
std::size_t Attachments::cfgMaxSegment = 50000;

AttachmentView::AttachmentView(DB db, const std::string_view &name)
:jmap(db, static_cast<ClassID>(KeySpaceClass::attachments), name)
{
}

AttachmentView::Download::Download(JsonMapView &&jmap, Metadata &&mdata)
:jmap(std::move(jmap))
,mdata(std::move(mdata))
{}

void AttachmentView::Metadata::parse(json::Value jmetadata) {
	ctx = jmetadata[0].toString();
	hash = jmetadata[1].toString();
	auto seg = jmetadata[2];
	SegID s;
	for (json::Value c: seg) {
		s = s + c.getUInt();
		segments.push(s);
	}
}

json::Value AttachmentView::Metadata::compose() {
	json::Array segArr;
	SegID curSeg = 0;
	while (!segments.empty()) {
		SegID nx = segments.front();
		segments.pop();
		SegID dff = nx-curSeg;
		segArr.push_back(dff);
		curSeg = nx;
	}
	return {ctx, hash, segArr};
}


AttachmentView::Download AttachmentView::open(const json::Value &docId, const std::string_view &attId) const {
	JsonMapView map (jmap, jmap.getDB().getSnapshot()); //create snapshot
	json::Value res = map.lookup({docId, attId}); //search for attachment metadata
	if (res.defined()) {
		Metadata mdata;
		mdata.parse(res);
		return Download(std::move(map), std::move(mdata));
	} else {
		return Download(std::move(map), {});
	}
}


bool AttachmentView::Download::exists() const {
	return mdata.segments.empty();
}

std::string_view AttachmentView::Download::getContentType() const {
	return mdata.ctx.str();
}

std::string_view AttachmentView::Download::getHash() const {
	return mdata.hash.str();
}

AttachmentView::SegID AttachmentView::Download::segID() const {
	if (mdata.segments.empty()) return 0;
	else return mdata.segments.front();

}

std::string_view AttachmentView::Download::read() {
	if (!putBackData.empty()) {
		return std::move(putBackData);
	} else if (mdata.segments.empty()) {
		return std::string_view();
	} else {
		SegID seg = mdata.segments.front();
		mdata.segments.pop();
		Key k = jmap.createKey(seg);
		if (jmap.getDB().get(k, buffer)) {
			return buffer;
		} else {
			return std::string_view();
		}
	}
}

void AttachmentView::Download::putBack(std::string_view data) {
	putBackData = data;
}



std::string_view AttachmentView::Iterator::docId() const {
	return key(0).getString();
}

std::string_view AttachmentView::Iterator::attId() const {
	return key(1).getString();
}

AttachmentView::Metadata AttachmentView::Iterator::metadata() const {
	Metadata m;
	m.parse(value());
	return m;
}

AttachmentView::Iterator AttachmentView::scan(const json::Value &docId, bool backward) {
	return jmap.prefix({docId}, backward);
}

AttachmentView::Iterator AttachmentView::scan(bool backward) {
	return jmap.prefix(json::array, backward);
}

Attachments::Attachments(const DocStoreViewBase &docStore, const std::string_view &name, std::size_t revision, AttachmentIndexFn &&indexFn)
:AttachmentView(docStore.getDB(), name)
,source(&docStore)
,indexFn(std::move(indexFn))
,minSegment(cfgMinSegment)
,maxSegment(cfgMaxSegment)
,revision(revision)
{
	loadMetadata();
}

Attachments::Attachments(DB db, const std::string_view &name, std::size_t revision, AttachmentIndexFn &&indexFn)
:AttachmentView(db, name)
,source(nullptr)
,indexFn(std::move(indexFn))
,minSegment(cfgMinSegment)
,maxSegment(cfgMaxSegment)
,revision(revision)
{
	loadMetadata();
}

void Attachments::setSource(const DocStoreViewBase &docStore) {
	source = &docStore;
}

void Attachments::run_gc() {
	if (source) run_gc(*source);
}

class AttachSet: public AttachmentEmitFn {
public:

	virtual void operator()(std::string_view attachId) {
		strList.append(attachId);
		sizes.push_back(attachId.length());
	}

	void clear() {
		strList.clear();
		sizes.clear();
	}

	void getAttachments(std::vector<std::string_view> &out) {
		out.clear();
		std::size_t pos = 0;
		for (auto c: sizes) {
			out.push_back(std::string_view(strList.data()+pos, c));
			pos = pos + c;
		}
		std::sort(out.begin(), out.end());
	}

protected:
	std::string strList;
	std::vector<std::size_t> sizes;

};

bool Attachments::run_gc(const DocStoreViewBase &source) {
	std::unique_lock m(lock, std::try_to_lock);
	if (!m.owns_lock()) {
		scheduleUpdate = true;
		return false;
	}

	Batch b;
	run_gc_lk(b, source);
	jmap.getDB().commitBatch(b);

	return true;
}

std::vector<std::string> Attachments::missing(const Document &doc) const {
	std::vector<std::string> m;
	class Emit: public AttachmentEmitFn {
	public:
		const Attachments &o;
		std::vector<std::string> &m;
		const Document &doc;

		Emit(const Attachments &o, std::vector<std::string> &m, const Document &doc):o(o),m(m),doc(doc) {}
		virtual void operator()(std::string_view attachId) {
			auto iter = o.jmap.find({doc.id, attachId});
			if (!iter.next()) m.push_back(std::string(attachId));
		}
	};

	Emit emit(*this,m,doc);
	indexFn(doc, emit);
	return m;
}


Attachments::Upload::Upload(Attachments &owner, std::string_view docId, std::size_t minSegment, std::size_t maxSegment)
:owner(owner),minSegment(minSegment),maxSegment(maxSegment),docId(docId)
{
	owner.lockGC();
}

Attachments::Upload::~Upload() {
	Batch batch;
	while (!stored_segments.empty()) {
		SegID q = stored_segments.front();
		stored_segments.pop();
		batch.Delete(owner.jmap.createKey(q));
	}
	owner.jmap.getDB().commitBatch(batch);
	owner.unlockGC();
}



void Attachments::Upload::open(std::string_view attId, std::string_view content_type) {
	if (opened) close();
	metadata.ctx = json::StrViewA(content_type);
	this->attId = attId;
	segment.clear();
	hashfn.reset();
}

void Attachments::Upload::write(std::string_view data) {
	if (!opened) return;
	if (segment.empty() && data.length() >= minSegment) {
		writeRaw(data);
	} else {
		segment.append(data);
		if (segment.length() >= minSegment) {
			if (segment.length() <= maxSegment) {
				writeRaw(std::string_view(segment.data(), maxSegment));
				segment.erase(0, maxSegment);
			} else {
				writeRaw(segment);
				segment.clear();
			}
		}
	}
}


void Attachments::Upload::writeRaw(std::string_view data) {
	if (data.empty()) return;
	hashfn.update(data);
	SegID seg = owner.allocSegment();
	Batch wr;
	wr.Put(owner.jmap.createKey(seg), leveldb::Slice(data.data(), data.length()));
	metadata.segments.push(seg);
	stored_segments.push(seg);
	owner.jmap.getDB().commitBatch(wr);
}


json::String Attachments::Upload::close() {
	if (!opened) return json::String();
	if (!segment.empty()) {
		writeRaw(segment);
		segment.clear();
	}
	std::uint8_t digest[16];
	hashfn.finish(digest);
	metadata.hash = json::base64url->encodeBinaryValue(json::BinaryView(digest, 16)).toString();
	json::Value key = {docId, attId};
	json::Value oldmdj = owner.jmap.lookup(key);
	if (oldmdj.defined()) {
		Metadata old_metadata;
		old_metadata.parse(oldmdj);
		while (!old_metadata.segments.empty()) {
			batch.Delete(owner.jmap.createKey(old_metadata.segments.front()));
			old_metadata.segments.pop();
		}
	}
	owner.jmap.set(batch, key, metadata.compose());
	return metadata.hash;
}

void Attachments::Upload::commit() {
	if (opened) close();
	owner.onCommit(batch, stored_segments);
}

Attachments::SegID Attachments::allocSegment() {
	std::lock_guard _(lock);
	return segId++;
}

void Attachments::lockGC() {
	std::lock_guard _(lock);
	uploadLock++;
}

void Attachments::unlockGC() {
	std::lock_guard _(lock);
	uploadLock--;
	if (uploadLock<=0) {
		Batch b;
		uploadLock = 0;
		if (source) {
			run_gc_lk(b, *source);
		}
		updateMetadata(b);
	}
}

void Attachments::run_gc_lk(Batch &b, const DocStoreViewBase &source) {
	AttachSet emitFn;
	std::vector<std::string_view> attchSet;

	DocStore::ChangesIterator iter =  source.scanChanges(seqId);
	while (iter.next()) {
		emitFn.clear();
		Document doc = iter.get();
		indexFn(doc, emitFn);
		emitFn.getAttachments(attchSet);

		auto alist = scan(doc.id, false);
		while (alist.next()) {
			auto attId = alist.attId();
			auto f = std::lower_bound(attchSet.begin(), attchSet.end(), attId);
			if (f == attchSet.end() || *f != attId) {
				erase(b, doc.id, attId);
			}
		}
	}

}

void Attachments::updateMetadata(Batch &b) {
	//key: null - metadata
	//key: true - pending writes
	//key: number - segment
	//key: {}  - directory

	//when metadata are updated, no more pending writes, clean them
	pendingWrites.clear();
	//store final state
	jmap.set(b,nullptr, {segId, seqId, revision});
	//clear pending writes
	jmap.set(b,true,pendingWrites);
}

void Attachments::loadMetadata() {
	//load map state
	json::Value res = jmap.lookup(nullptr);
	//if map state defined
	if (res.defined()) {
		//retrieve last segment id
		segId = res[0].getUIntLong();
		//retrieve last sequence id
		seqId = res[1].getUIntLong();
		//retrieve map revision
		auto r = res[2].getUIntLong();
		//if revision is different, we will need to run garbage collector from the beginning
		if (r != revision) {
			//so reset sequence id
			seqId = 0;
		}

		//retrieve pending writes
		//when application crashes during write,written segmnets are not referenced.
		//if attachment is closed, written segments are referenced.
		//But, there is also segId used to allocate new segment ID, which is commited once all writes are closed, because
		//there can be pending writes that was not commited yet
		//Each commit is stored to pendingWrites array.
		//With last commit, pending writes are cleard when metadata are stored
		//Explorer now penidng writes, if there are any
		json::Value pending = jmap.lookup(true);
		if (pending.type() == json::array && !pendingWrites.empty()) {
			//sort writes (they are not sorted by default)
			pending = pending.sort([](json::Value a, json::Value b){return a.getIntLong() - b.getIntLong();});
			//walk throu it
			auto b = pending.begin();
			auto e = pending.end();
			Batch batch;
			//iterate over segments
			while (b != e) {
				SegID s = (*b).getUIntLong();
				//check for holes in pending writes
				while (segId < s) {
					//erase these holes
					jmap.erase(batch, s);
					segId++;
				}
				b++;
				segId++;
			}
			//it is time to update metadata
			updateMetadata(batch);
			//commit new state
			jmap.getDB().commitBatch(batch);
		}
	}
}

void Attachments::erase(const json::Value & docId, std::string_view attId) {
	Batch b;
	erase(b, docId, attId);
}

void Attachments::erase(Batch &b, const json::Value &docId, std::string_view attId) {
	json::Value key = {docId, attId};
	auto f = jmap.lookup(key);
	if (f.defined()) {
		Metadata mtd;
		mtd.parse(f);;
		mtd.eraseSegments(jmap,b);
		jmap.erase(b, key);
		jmap.getDB().commitBatch(b);
	}
}

void Attachments::purgeDoc(std::string_view docId) {
	Batch b;
	Iterator iter = scan(docId, false);
	while (iter.next()) {
		auto mtd = iter.metadata();
		mtd.eraseSegments(jmap,b);
		jmap.erase(b, iter.key());
	}
	jmap.getDB().commitBatch(b);
}

void AttachmentView::Metadata::eraseSegments(const JsonMap &jmap, Batch &b) {
	while (!segments.empty()) {
		SegID s = segments.front();
		segments.pop();
		b.Delete(jmap.createKey(s));
	}
}

Attachments::Upload Attachments::upload(std::string_view docId) {
	return Upload(*this, docId, minSegment, maxSegment);
}


void Attachments::onCommit(Batch &b, std::queue<SegID> &segments) {
	std::lock_guard _(lock);
	while (!segments.empty()) {
		auto s = segments.front();
		segments.pop();
		pendingWrites.push_back(s);
	}

	jmap.set(b, true, pendingWrites);

}


Attachments::Upload::Upload(Upload &&other)
:owner(other.owner)
,minSegment(other.minSegment)
,maxSegment(other.maxSegment)
,docId(std::move(other.docId))
,attId(std::move(other.attId))
,segment(std::move(other.segment))
,batch(std::move(other.batch))
,metadata(std::move(other.metadata))
,hashfn(std::move(other.hashfn))
,stored_segments(std::move(other.stored_segments))
,opened(std::move(other.opened))
{
	owner.lockGC();
}

}
