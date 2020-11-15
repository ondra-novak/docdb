/*
 * filedb.h
 *
 *  Created on: 1. 11. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_FILEDB_H_
#define SRC_DOCDBLIB_FILEDB_H_

#include <optional>
#include "attachments.h"
#include <imtjson/binary.h>

namespace docdb {


///Implements a file database above document database.
/**
 * Allows to store file which appear as standard document with attachment.
 */
class FileDB: public Attachments {
public:
	using Attachments::Attachments;

	class Upload {
	public:

		Upload(FileDB &db, const std::string_view &name, const std::string_view &content_type, DocRevision rev, std::size_t blockSize);
		Upload(FileDB &db);
		~Upload();
		Upload(Upload &&other);
		void write(json::BinaryView data);
		void write(leveldb::Slice data);
		void write(std::string_view data);
		void write(const std::string &data);
		void write(const char *data, std::size_t size);
		bool commit();
		bool inConflict() const;

	protected:
		FileDB &db;
		std::string name;
		std::string ct;
		DocRevision rev;
		std::size_t blockSize;
		std::string dataid;
		unsigned int seg = 0;
		std::size_t sz = 0;

		bool conflict;
		bool commited;

		std::string block;

	};

	Upload upload(const std::string_view &name,const std::string_view &content_type, DocRevision rev = 0);


	std::size_t getMaxBlockSize() const {
		return blockSize;
	}

	void setMaxBlockSize(std::size_t blockSize) {
		this->blockSize = blockSize;
	}

	class Download {
	public:

		Download(FileDB &db, const std::string_view &docName, const std::string_view &attid);

	};


protected:
	std::size_t blockSize = 262144;

	virtual void map(const Document &doc, const EmitAttachFn &emit) override;

};





}



#endif /* SRC_DOCDBLIB_FILEDB_H_ */
