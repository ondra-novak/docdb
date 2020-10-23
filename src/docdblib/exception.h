/*
 * exception.h
 *
 *  Created on: 23. 10. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_EXCEPTION_H_
#define SRC_DOCDBLIB_EXCEPTION_H_
#include <leveldb/status.h>
#include <exception>


namespace docdb {

class DocDBException: public virtual std::exception {
public:
};


class LevelDBException: public DocDBException {
public:

	LevelDBException(leveldb::Status &&st);
	const leveldb::Status status;
	virtual const char *what() const noexcept override;

	static void checkStatus(leveldb::Status &&st) ;
	static bool checkStatus_Get(leveldb::Status &&st) ;

protected:
	mutable std::string msg;

};

}

#endif /* SRC_DOCDBLIB_EXCEPTION_H_ */

