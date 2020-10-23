/*
 * document.h
 *
 *  Created on: 23. 10. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_DOCUMENT_H_
#define SRC_DOCDBLIB_DOCUMENT_H_
#include <cstdint>
#include <string>
#include <imtjson/value.h>
namespace docdb {

using DocRevision = std::uint64_t;
using SeqID = std::uint64_t;
using Timestamp = std::uint64_t;

struct DocumentBase {
	///Document ID
	/** Document Identifier is any arbitrary string - must be unique */
	std::string id;
	/** Document content. If the content is undefined, the document is either deleted or not-exists */
	json::Value content;
	/** last write timestamp */
	Timestamp timestamp = 0;

	bool valid() const {return content.defined();}

};

///Contains document to be processed or modified
struct Document: DocumentBase {
	///Document revision
	/** Contains last document revision. If the document doesn't exists, it contains zero. If
	 * the document is deleted, it contains different value
	 */
	DocRevision rev = 0;
};

///Contains document to be replicated (should not be modified)
struct DocumentRepl: DocumentBase {
	///Document revision list
	/** contains list of revisions, first revision is most recent */
	json::Value revisions;
};


}



#endif /* SRC_DOCDBLIB_DOCUMENT_H_ */
