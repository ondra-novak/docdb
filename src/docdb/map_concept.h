#pragma once
#ifndef SRC_DOCDB_MAP_CONCEPT_H_
#define SRC_DOCDB_MAP_CONCEPT_H_


#include "database.h"
#include "concepts.h"


namespace docdb {


template<typename T>
CXX20_CONCEPT(MapViewType , requires(T x) {
    {x.get_db() } -> std::same_as<const PDatabase &>;
    {x.scan()} -> std::convertible_to<typename T::Iterator>;
});

template<typename T>
CXX20_CONCEPT(MapType , requires(T x) {
    MapViewType<T>;
    {x.register_observer(std::declval<SimpleFunction<bool, Batch &, const BasicRowView &> >())} -> std::same_as<std::size_t>;
    {x.unregister_observer(std::declval<std::size_t>())};
    {x.rescan_for(std::declval<SimpleFunction<bool, Batch &, const BasicRowView &> >())};
});


}






#endif /* SRC_DOCDB_MAP_CONCEPT_H_ */
