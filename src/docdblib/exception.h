/*
 * exception.h
 *
 *  Created on: 23. 10. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_EXCEPTION_H_
#define SRC_DOCDBLIB_EXCEPTION_H_
#include <docdblib/keyspace.h>
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

class DatabaseOpenError: public DocDBException {
public:
	DatabaseOpenError(int code, const std::string &name);

	virtual const char *what() const noexcept override;
	int getCode() const;
protected:
	mutable std::string msg;
	int code;
	std::string name;

};

class TooManyKeyspaces: public DocDBException {
public:
	TooManyKeyspaces(const std::string &dbName);
	virtual const char *what() const noexcept override;
protected:
	mutable std::string msg;
	std::string name;

};

class CantWriteToSnapshot: public DocDBException {
public:
	virtual const char *what() const noexcept override;

};

class DocumentIDCantBeEmpty: public DocDBException {
public:
	virtual const char *what() const noexcept override;
};


class KeyspaceAlreadyLocked: public DocDBException{
public:
	KeyspaceAlreadyLocked(KeySpaceID id);
	virtual const char *what() const noexcept override;
protected:
	mutable std::string msg;
	KeySpaceID id;
};

}

#endif /* SRC_DOCDBLIB_EXCEPTION_H_ */

