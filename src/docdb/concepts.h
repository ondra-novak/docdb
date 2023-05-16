#pragma once
#ifndef SRC_DOCDB_CONCEPTS_H_
#define SRC_DOCDB_CONCEPTS_H_

#include <concepts>
#include <type_traits>


namespace docdb {


template<typename T, typename U>
concept same_or_reference_of = std::is_same_v<std::remove_reference_t<T>, U>;


}



#endif /* SRC_DOCDB_CONCEPTS_H_ */
