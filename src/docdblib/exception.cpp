/*
 * exception.cpp
 *
 *  Created on: 23. 10. 2020
 *      Author: ondra
 */


#include "exception.h"

#include <algorithm>
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

}


