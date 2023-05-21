#pragma once
#ifndef SRC_DOCDB_DOC_STORAGE_CONCEPT_H_
#define SRC_DOCDB_DOC_STORAGE_CONCEPT_H_

#include "database.h"
#include "concepts.h"



namespace docdb {


template<typename T>
CXX20_CONCEPT(DocumentStorageViewType , requires(T x) {
    {x.get_db() } -> std::same_as<const PDatabase &>;
    {*x[std::declval<typename T::DocID>()] } -> std::same_as<typename T::DocType>;
    {x.scan()} -> std::convertible_to<typename T::Iterator>;
    {x.scan_from(std::declval<typename T::DocID>())} -> std::convertible_to<typename T::Iterator>;
});

template<typename T>
CXX20_CONCEPT(DocumentStorageType , requires(T x) {
    DocumentStorageViewType<T>;
    {x.register_observer(std::declval<SimpleFunction<bool, Batch &, const typename T::Update &> >())} -> std::same_as<std::size_t>;
    {x.unregister_observer(std::declval<std::size_t>())};
    {x.rescan_for(std::declval<SimpleFunction<bool, Batch &, const typename T::Update &> >())};
    {T::Update::old_doc}->std::convertible_to<const typename T::DocType *>;
    {T::Update::new_doc}->std::convertible_to<const typename T::DocType *>;
    {T::Update::old_doc_id}->std::convertible_to<const typename T::DocID>;
    {T::Update::old_old_doc_id}->std::convertible_to<const typename T::DocID>;
    {T::Update::new_doc_id}->std::convertible_to<const typename T::DocID>;
});


}




#endif /* SRC_DOCDB_DOC_STORAGE_CONCEPT_H_ */
