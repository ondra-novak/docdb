#ifndef SRC_DOCDB_AGGREGRATOR_SOURCE_CONCEPT_H_
#define SRC_DOCDB_AGGREGRATOR_SOURCE_CONCEPT_H_

#include "database.h"

namespace docdb {

template<typename T>
DOCDB_CXX20_CONCEPT(AggregatorSource, requires(T x) {
    typename T::ValueType;
    {x.get_db()} -> std::convertible_to<PDatabase>;
    {x.register_transaction_observer([](Batch &b, const Key& key, bool erase){})};
    {x.rescan_for([](Batch &b, const Key& key, bool erase){})};
    {x.select(std::declval<Key>()) } -> std::derived_from<RecordSetBase>;
    {x.update() };
});

}




#endif /* SRC_DOCDB_AGGREGRATOR_SOURCE_CONCEPT_H_ */
