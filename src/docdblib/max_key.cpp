/*
 * max_key.cpp
 *
 *  Created on: 25. 4. 2021
 *      Author: ondra
 */

#include <imtjson/wrap.h>

namespace docdb {

enum _TMaxKeyValue {
	MaxKeyValue
};

json::Value MAX_KEY_VALUE = json::makeValue(MaxKeyValue, nullptr);

}

