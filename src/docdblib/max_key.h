/*
 * max_key.h
 *
 *  Created on: 25. 4. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDB_SRC_DOCDBLIB_MAX_KEY_H_
#define SRC_DOCDB_SRC_DOCDBLIB_MAX_KEY_H_

namespace docdb {

///Contains value which is recognized as maximum key value. This value is always ordered as last value. You can use it for search
extern const json::Value MAX_KEY_VALUE;

}



#endif /* SRC_DOCDB_SRC_DOCDBLIB_MAX_KEY_H_ */
