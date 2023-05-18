#pragma once
#ifndef SRC_DOCDB_DOC_STORAGE_CONCEPT_H_
#define SRC_DOCDB_DOC_STORAGE_CONCEPT_H_

#include "database.h"


namespace docdb {


template<typename T>
concept DocumentStorageViewType = requires(T x) {
    {x.get_db() } -> std::same_as<const PDatabase &>;
    {x[std::declval<typename T::DocID>()].doc() } -> std::same_as<typename T::DocType>;
    {x.scan()} -> std::convertible_to<typename T::Iterator>;
    {x.scan_from(std::declval<typename T::DocID>())} -> std::convertible_to<typename T::Iterator>;
};

template<typename T>
concept DocumentStorageType = requires(T x) {
    DocumentStorageViewType<T>;
    {x.register_observer([](Batch &, const typename T::Update &){return true;})};
    {T::Update::old_doc}->std::convertible_to<const typename T::DocType *>;
    {T::Update::new_doc}->std::convertible_to<const typename T::DocType *>;
    {T::Update::old_doc_id}->std::convertible_to<const typename T::DocID>;
    {T::Update::old_old_doc_id}->std::convertible_to<const typename T::DocID>;
    {T::Update::new_doc_id}->std::convertible_to<const typename T::DocID>;
};


}




#endif /* SRC_DOCDB_DOC_STORAGE_CONCEPT_H_ */
