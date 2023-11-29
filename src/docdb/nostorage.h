
#ifndef SRC_DOCDB_VIRTUAL_STORAGE_H_
#define SRC_DOCDB_VIRTUAL_STORAGE_H_
#include "database.h"
#include "storage_view.h"

#include <optional>

namespace docdb {


///Simulates storage, but doesn't store anything
/**
 * This object still can be used to update indexes.
 * @tparam _DocDef document definition
 */
template<typename _DocType>
class NoStorage {
public:


    using DocType = _DocType;
    ///Create no-storage
    /**
     * @param db database
     * @param revision_var_name variable name, where _next_id is stored
     */
    NoStorage(PDatabase db, std::string revision_var_name)
        :_db(db)
        ,_revision_var_name(std::move(revision_var_name)) {

        auto r = _db->get_variable(_revision_var_name);
        if (r.empty()) _next_id = 1;
        else{
            auto [id] = Row::extract<DocID>(r);
            _next_id.store(id);
        }

    }

    ///Create no-storage
    /**
     * @param db database
     * @param next_id next document ID
     *
     * @note in this variant, _next_id is not stored in the database
     */
    NoStorage(PDatabase db, DocID next_id)
        :_db(db)
        ,_next_id(next_id) {}

    using DocIDGenerator = std::function<DocID(unsigned int)>;


    ///Retrieve database pointer
    auto get_db() const {
        return _db;
    }

    ///Retrieve document
    /**
     * @param id document id
     * @return function always returns no-value as there is no way to find
     * document with this ID.
     */
    auto get(DocID id) -> std::optional<DocType> {
        return {};
    }


    using Update = IndexUpdate<DocType>;
    using IndexTransactionObserver = std::function<void(Batch &, const Update &)> ;

    ///Register indexer
    void register_transaction_observer(IndexTransactionObserver fn) {
        _observers.push_back(std::move(fn));
    }
    ///Should rescan storage and feed observer with updates from specified DocID
    /**
     * @param fn function of the observer
     * @param id starting document ID
     *
     * @note this function is empty as there are no documents.
     */
    void rescan_for(IndexTransactionObserver fn, DocID id) {
        //empty, you should overwrite
    }

    ///Index the document
    /**
     * @param b batch
     * @param doc document
     * @return document ID
     *
     * @note function updates all observers
     */
    DocID put(Batch &b, const DocType &doc) {
        DocID id = _next_id.fetch_add(1,std::memory_order_relaxed);
        for (const auto &obs: _observers) {
            obs(b, Update{
                &doc, nullptr, id, 0, 0
            });
        }
        if (!_revision_var_name.empty()) {
            _db->set_variable(b, _revision_var_name, Row(_next_id.load(std::memory_order_relaxed)));
        }
        return id;
    }

    ///Index the document
    /**
     * @param doc document
     * @return document ID
     *
     * @note function updates all observers and commits final batch
     */
    DocID put(const DocType &doc) {
        Batch b;
        DocID id = put(b, doc);
        _db->commit_batch(b);
        return id;
    }

    ///Erase document from the index
    /**
     * @param b batch
     * @param id id of document
     * @param doc document content. It must be consistent with document stored along the
     * same id, as the function updates all observers, giving them this ID and document.
     * Observers will probably retrieve keys from the document.
     */
    void erase(Batch &b, DocID id, const DocType &doc) {
        for (const auto &obs: _observers) {
            obs(b, Update{
                nullptr, &doc, 0, id, 0
            });
        }
    }

    ///Erase document from the index
    /**
     * @param id id of document
     * @param doc document content. It must be consistent with document stored along the
     * same id, as the function updates all observers, giving them this ID and document.
     * Observers will probably retrieve keys from the document.
     */
    void erase(DocID id, const DocType &doc) {
        Batch b;
        erase(b, id, doc);
        _db->commit_batch(b);
    }

    ///Retrieve next documen ID which defines upper bound of rescan
    DocID get_rev() const {
        return _next_id.load(std::memory_order_relaxed);
    }

protected:

    std::vector<IndexTransactionObserver> _observers;
    PDatabase _db;
    std::atomic<DocID> _next_id;
    std::string _revision_var_name;

};


}





#endif /* SRC_DOCDB_VIRTUAL_STORAGE_H_ */
