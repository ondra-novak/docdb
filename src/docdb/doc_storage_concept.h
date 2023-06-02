#pragma once
#ifndef SRC_DOCDB_DOC_STORAGE_CONCEPT_H_
#define SRC_DOCDB_DOC_STORAGE_CONCEPT_H_

#include "database.h"
#include "concepts.h"



namespace docdb {

using DocID = std::uint64_t;

template<typename T>
DOCDB_CXX20_CONCEPT(DocumentStorageViewType , requires(T x) {
    {x.get_db() } -> std::convertible_to<PDatabase>;
    {x.find(std::declval<DocID>())->content } -> std::convertible_to<const typename T::DocType &>;
    {x.find(std::declval<DocID>()) } -> std::convertible_to<bool>;
    {x.select_all()} -> std::derived_from<RecordSetBase>;
    {x.select_from(std::declval<DocID>())} -> std::derived_from<RecordSetBase>;
});

template<typename T>
DOCDB_CXX20_CONCEPT(DocumentStorageType , requires(T x) {
    DocumentStorageViewType<T>;
    {x.register_transaction_observer(std::declval<std::function<void(Batch &, const typename T::Update &)> >())} -> std::same_as<void>;
    {x.rescan_for(std::declval<std::function<void(Batch &, const typename T::Update &)> >(), std::declval<DocID>())};
    {x.get_rev()} -> std::same_as<DocID>;
    {T::Update::old_doc}->std::convertible_to<const typename T::DocType *>;
    {T::Update::new_doc}->std::convertible_to<const typename T::DocType *>;
    {T::Update::old_doc_id}->std::convertible_to<DocID>;
    {T::Update::old_old_doc_id}->std::convertible_to<DocID>;
    {T::Update::new_doc_id}->std::convertible_to<DocID>;
});


}




#endif /* SRC_DOCDB_DOC_STORAGE_CONCEPT_H_ */
