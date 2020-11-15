/*
 * attachments.h
 *
 *  Created on: 31. 10. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_ATTACHMENTS_H_
#define SRC_DOCDBLIB_ATTACHMENTS_H_
#include "docdb.h"
#include "updatableobject.h"
#include "iterator.h"

namespace docdb {


///Function is used to generate list of attachment used in the document
class EmitAttachFn {
public:
	virtual void operator()(const std::string_view &att_id) const = 0;
	virtual ~EmitAttachFn() {}
};
///Access and controls attachments
/**
 * Attachments are not part of core-db, however, they can be accessed through separate object.
 * Attachments are stored in same database under different section.
 *
 * To store attachment along with document, you need to create attchment identification first.
 * The identification should be stored in the document. The attachment's content is stored
 * under same identification.
 *
 * During object update, unreferenced attachments are collected and deleted. The function
 * is similar to AbstractView. There must be a function which enumerates identifications
 * of referenced attachments. No longer referenced attachments are removed
 *
 * Note: attachments are bound to the documents. They are not automatically deduplicated.
 * If you store same content with multiple documents, the content is duplicated once
 * per document.
 *
 */
class Attachments: public UpdatableObject {
public:

	///Creates attachment container
	/**
	 * @param db reference to database
	 */
	Attachments(DocDB &db);

	///Put content to the database
	/**
	 * @param docid document id
	 * @param attid attachment identification - binary string not supported (it is terminated by 0)
	 * @param data data to put
	 */
	void put(const std::string_view &docid,
			 const std::string_view &attid,
			 const std::string_view data);

	///Put segmented content to the database
	/**
	 * @param docid document id
	 * @param attid attachment identification - binary string not supported (it is terminated by 0)
	 * @param seg number of segment. Segments are numbered from 0 to n, each segment can have different size.
	 * @param data data of this segment
	 */
	void put(const std::string_view &docid,
			 const std::string_view &attid,
			 std::size_t seg,
			 const std::string_view data);

	///Gets single segment from the database
	/**
	 * @param docid document id
	 * @param attid attechment identification
	 * @param seqid segment number. If segment was not used during store, it defaults to 0
	 * @return if segment exists, it returns content, if not, return value is empty
	 */
	std::optional<std::string> get(const std::string_view &docid,
			 const std::string_view &attid,
			 std::size_t seg = 0);


	///Streams out all segments through th function
	/**
	 * @param docid document id
	 * @param attid attachment id
	 * @param fn function to use
	 * @retval true at least one segment found
	 * @retval false no segments found
	 */
	template<typename Fn>
	bool stream_out(const std::string_view &docid, const std::string_view &attid, Fn &&fn);


	///Erases all segments of the attachment
	/**
	 * @param docid document id
	 * @param attid attachment identification
	 */
	void erase(const std::string_view &docid, const std::string_view &attid);

	///Function maps signe document to list of attachments
	/**
	 * @param doc document
	 * @param emit function which emits single attachment identification.
	 *
	 * If document doesn't contain attachments, it don't need to call emit()
	 */
	virtual void map(const Document &doc, const EmitAttachFn &emit) = 0;

protected:

	virtual void storeState() override;
	virtual SeqID scanUpdates(ChangesIterator &&iter) override;
	using AttchKey = DocDB::AttachmentKey;




};

template<typename Fn>
inline bool docdb::Attachments::stream_out(const std::string_view &docid,
		const std::string_view &attid, Fn &&fn) {

	AttchKey key;
	key.append(docid);
	key.push_back(0);
	key.append(attid);
	key.push_back(0);
	auto iter = db.mapScanPrefix(key,false);
	bool ok = false;
	while (iter.next()) {
		fn(iter.value());
		ok = true;
	}
	return ok;
}


}


#endif /* SRC_DOCDBLIB_ATTACHMENTS_H_ */
