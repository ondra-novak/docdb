/*
 * exception.cpp
 *
 *  Created on: 23. 10. 2020
 *      Author: ondra
 */


#include "exception.h"

#include <algorithm>
#include <sstream>
namespace docdb {


LevelDBException::LevelDBException(leveldb::Status &&st)
	:status(std::move(st))
{
}

const char* LevelDBException::what() const noexcept {
	if (msg.empty()) {
		msg = status.ToString();
	}
	return msg.c_str();
}

bool LevelDBException::checkStatus_Get(leveldb::Status &&st) {
	if (st.ok()) return true;
	if (st.IsNotFound()) return false;
	throw LevelDBException(std::move(st));
}


void LevelDBException::checkStatus(leveldb::Status &&st)  {
	if (!st.ok()) throw LevelDBException(std::move(st));
}

DatabaseOpenError::DatabaseOpenError(int code, const std::string &name)
	:code(code),name(name)
{
}

const char* DatabaseOpenError::what() const noexcept {
	if (msg.empty()) {
		msg.append("Failed to open DB: ");
		msg.append(name);
		msg.append(" error: ");
		msg.append(strerror(code));
	}
	return msg.c_str();
}

int DatabaseOpenError::getCode() const {
	return code;
}

TooManyKeyspaces::TooManyKeyspaces(const std::string &dbName)
:name(dbName)
{
}

const char* TooManyKeyspaces::what() const noexcept {
	if (msg.empty()) {
		msg.append("Failed to allocate keyspace - there are too many keyspaces - DB: ");
		msg.append(name);
	}
	return msg.c_str();
}

const char *CantWriteToSnapshot::what() const noexcept  {
	return "Can't write to snapshot";
}

const char *DocumentIDCantBeEmpty::what() const noexcept {
	return "Document's ID can't be empty";
}

const char* KeyspaceAlreadyLocked::what() const noexcept {
	if (msg.empty()) {
		std::ostringstream out;
		out << "Keyspace is already locked: " << id;
		msg = out.str();
	}
	return msg.c_str();
}

KeyspaceAlreadyLocked::KeyspaceAlreadyLocked(KeySpaceID id)
:id(id) {}

}


