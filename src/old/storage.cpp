/*
 * storage.cpp
 *
 *  Created on: 8. 11. 2022
 *      Author: ondra
 */


#include "storage.h"

namespace docdb {

template class Storage<std::uint8_t>;
template class Storage<std::uint16_t>;
template class Storage<std::uint32_t>;
template class Storage<std::uint64_t>;

}

