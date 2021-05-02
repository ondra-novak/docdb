/*
 * attachments.h
 *
 *  Created on: 31. 10. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_ATTACHMENTS_H_
#define SRC_DOCDBLIB_ATTACHMENTS_H_

#include <queue>
#include <imtjson/string.h>
#include <imtjson/array.h>
#include "json_map.h"

#include "doc_store.h"
#include "iterator.h"
#include "hash.h"

namespace docdb {

///Attachments are stored in separate keyspace
/**
 * Attachments can be used to store any arbitrary data along with documents, including binary data, such as images or videos.
 *
 * Attachments are stored as they are as binary blobs. You can split attachment into segments which allows easy manipulation with each attachment
 *
 * Single documents can have multiple attachments, each attachment is identified by its id
 *
 */

class AttachmentView {
public:

	using SegID = std::uint64_t;

	///Create attachment view, This class only R/O access to the attachment table
	/**
	 * @param db database object
	 * @param name name of the view
	 */
	AttachmentView(DB db, const std::string_view &name);

	struct Metadata {
		json::String ctx;
		json::String hash;
		std::queue<SegID> segments;

		void parse(json::Value jmetadata);
		json::Value compose();
		void eraseSegments(const JsonMap &jmap, Batch &b);
	};



	///This class allows to download attachment per segments
	class Download {
	public:

		///Create instance - you should use open() command to retrieve this instance
		Download(JsonMapView &&jmap, Metadata &&metaData);

		///Returns true, if attachment exists
		/**
		 * @retval false attachment cannot be opened, rest of functions will return empty data
		 * @retval true attachment opened, and you can read
		 */
		bool exists() const;
		///Retrieve content type
		std::string_view getContentType() const;
		///Retrieve hash
		std::string_view getHash() const;
		///Retrieve seqID of this segment
		SeqID segID() const;
		///Read segment
		/**
		 * @return returns the whole segment, unless there are data put back. In this case, the put-back-data are read first. Function returns
		 * empty string, if there are no more data to read
		 */
		std::string_view read();
		///Put back segment or part of segment, so it can be read later
		/**
		 * @param data data to be read later
		 *
		 * Main purpose of this function is to store unprocessed part of the segment back to the instance so it can be processed later by
		 * next read() call. The argument should contain part of data returned by previous read() function. However it is possible
		 * to put back complete different data, you also need to ensure, that data remains valid until they are procesed complete. Note that
		 * function should be called once before next read(), otherwise it resets state of previous call
		 */
		void putBack(std::string_view data);

	protected:
		JsonMapView jmap;
		Metadata mdata;
		std::string buffer;

		std::string_view putBackData;
		mutable json::Value keyData;

	};



	///Iterates over attachments
	class Iterator: public JsonMapView::Iterator {
	public:
		using JsonMapView::Iterator::Iterator;

		std::string_view docId() const;
		std::string_view attId() const;
		Metadata metadata() const;
	};

	///Open attachment
	/**
	 *
	 * @param docId document id
	 * @param attId attachment id
	 * @return Download instance. Function will not fail, when attachment not found. You should call exists() to check, that attachment exists
	 */
	Download open(const json::Value &docId, const std::string_view &attId) const;

	///Scan for all attachments for given document
	/**
	 * @param docId document id
	 * @param backward scan backward
	 * @return iterator
	 */
	Iterator scan(const json::Value &docId, bool backward = false);

	///Scan for all attachments for all documents
	/**
	 * @param backward scan backward
	 * @return iterator
	 */
	Iterator scan(bool backward = false);


protected:
	JsonMap jmap;

};

///This is emit function for the attachment map
/**
 * Function is called to check, whether attachments are still referenced by documents. For each document function emits list of
 * attachments (their IDs). Attachments not listed in this list are erased from the database.
 *
 * It is expected, that each attachment is somehow referenced by the document, and this function is used to collect these references
 */
class AttachmentEmitFn {
public:
	virtual ~AttachmentEmitFn() {}
	virtual void operator()(std::string_view attachId) = 0;

};

///Function defines, how reference to the attachment is stored within the document.
/**
 * For each document, it should emit all references to the attachments. Attachments missing from the reference are removed
 */
using AttachmentIndexFn = std::function<void(const Document &doc, AttachmentEmitFn &)>;


///Allows to store attachments with documents
/**
 * Attachments are stored in separate keyspace.
 * Attachment can be any arbitrary binary content. Large content is split into segments. This allows easy streaming attachment without need to read whole attachment
 * to the memory.
 *
 * The class uses JsonMap, which is additionaly split to several parts
 * - metadata - occupies keys null and true
 * - segment map - occupies number keys
 * - directory map - occupies two collumns array, which contains document ID and attachment ID
 *
 * To work with attachments, you need to define format, how attachment are referenced within documents. This format is then implementas as function, which must be
 * passed to the constructor of the object. The purpose of this function (AttachmentIndexFn) is to enumerate all active attachments referenced in the document. When
 * attachment is dereferenced, it is eventually removed from the database.
 *
 * To store attachment, you need to acquire an instance of the Upload class. You can use this class to store multiple attachments per document. Note that only one thread can upload
 * the attachments, multiple pending uploads can cause update conflict. Once all attachments are uploaded, you need to commit upload and then modify original document
 * to store references to newly created attachments. Once the document is updated, you can drop the instance of the Update class, which finalizes all writes.
 *
 * This allows an atomic update of the attachment. By holding instance of Update prevents to garbage collector to run. Once document is updated, attachments are already uploaded
 * and ready to be downloaded. Once you drop the instance of the Update class, the garbage collector eventualy removes any not referenced attachments or any old version
 * of the attachments left in the database
 *
 *
 */
class Attachments: public AttachmentView {
public:

	///Minimum segment size. Writes below this size are combined into large segment
	/** This value affects only newly created instances */
	static std::size_t cfgMinSegment; /*=10000*/
	///Maximum segment size. Writes above this size are split into multiple segments
	/** This value affects only newly created instances */
	static std::size_t cfgMaxSegment; /*=50000*/

	Attachments(const DocStoreViewBase &docStore, const std::string_view &name, std::size_t revision, AttachmentIndexFn &&indexFn);
	Attachments(DB db, const std::string_view &name, std::size_t revision, AttachmentIndexFn &&indexFn);

	///Change document source
	void setSource(const DocStoreViewBase &docStore);

	void run_gc();

	///Update from document source
	/**
	 * @param source source database
	 * @retval true processed
	 * @retval false delayed, upload lock is held (note function will not schedule
	 * 			automatic update after lock is release, this need to have source set permanetly
	 */
	bool run_gc(const DocStoreViewBase &source);


	///list missing attachments (attachments refered by the document but not stored)
	std::vector<std::string> missing(const Document &doc) const;

	///Upload class allows you atomicaly update attachments
	/**
	 * You can create instance using the function upload(). You can upload multiple attachments. Each attachment can be split to
	 * multiple segments. Once all attachments are uploaded, you can update source document and commit everything to the database.
	 *
	 * During upload operation, uploaded data are not visible until committed.
	 *
	 * While you keeping this instance, no attachment can be automatically removed
	 */
	class Upload {
	public:

		Upload (Attachments &owner, std::string_view docId , std::size_t minSegment, std::size_t maxSegment);
		///Drops Upload instance
		/**
		 * Unlocks garbage collector, so it can remove no longer referenced attachments
		 * Any not commited writes are deleted
		 */
		~Upload();


		Upload (Upload &&other);

		///Initialize writing of the attachment
		/**
		 * Function must be called before write(). You can write only one attachment at once.
		 * Previously opened attachment is closed
		 *
		 * @param attId attachment ID
		 */
		void open(std::string_view attId, std::string_view content_type);

		///Write data segment
		void write(std::string_view data);

		///closes current attachment
		/**
		 * @return attachment's hash
		 */
		json::String close();

		///Commit all writes
		/**
		 * Commit is need to make attachments available for download. After commit(), the document can be updated to refer newly uploaded attachments
		 */
		void commit();

		///Returns true, if attachment is opened
		bool isOpened() const { return opened; }
		///Returns currently opened attachment
		const std::string& getOpenedAttachment() const { return attId; }
		///Returns current document
		const std::string& getDocID() const {return docId;}


	protected:

		Attachments &owner;
		std::size_t minSegment;
		std::size_t maxSegment;
		std::string docId;
		std::string attId;
		std::string segment;
		Batch batch;
		Metadata metadata;
		Hash128 hashfn;
		std::queue<SegID> stored_segments;
		bool opened = false;

		void writeRaw(std::string_view data);
	};

	///Erase attachment manually
	void erase(const json::Value &docId, std::string_view attId);

	///Erase attachment manually
	void erase(Batch &b, const json::Value &docId, std::string_view attId);

	///Purge document from the database
	void purgeDoc(std::string_view docId);

	Upload upload(std::string_view docId);

	std::size_t getMaxSegment() const {
		return maxSegment;
	}

	void setMaxSegment(std::size_t maxSegment) {
		this->maxSegment = maxSegment;
	}

	std::size_t getMinSegment() const {
		return minSegment;
	}

	void setMinSegment(std::size_t minSegment) {
		this->minSegment = minSegment;
	}




protected:

	const DocStoreViewBase *source;
	AttachmentIndexFn indexFn;
	std::size_t minSegment;
	std::size_t maxSegment;
	std::size_t revision;

	std::mutex lock;
	unsigned int uploadLock;
	SeqID seqId = 0;
	SegID segId = 0;
	json::Array pendingWrites;
	bool scheduleUpdate = false;

	SegID allocSegment();
	void lockGC();
	void unlockGC();
	void updateMetadata(Batch &b);
	void run_gc_lk(Batch &b, const DocStoreViewBase &source);

	void onCommit(Batch &b, std::queue<SegID> &segments);


	void loadMetadata();
};

}


#endif /* SRC_DOCDBLIB_ATTACHMENTS_H_ */
