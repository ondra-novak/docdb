/*
 * index.h
 *
 *  Created on: 14. 5. 2023
 *      Author: ondra
 */

#ifndef SRC_DOCDB_INDEX_H_
#define SRC_DOCDB_INDEX_H_
#include "key.h"

#include "view.h"
#include <string_view>
#include "storage.h"


namespace docdb {

class Storage;

///Object which helps to build index
class Indexer {
public:
    Indexer(KeyspaceID kid):_kid(kid) {}

    ///Calculate index
    template<typename ... Args>
    Key key(const Args & ... args) {return Key(_kid, args...);}

    virtual void put(const Key &key, std::string_view value) = 0;
    virtual void erase(const Key &key) = 0;
    virtual bool get(const Key &key, std::string &value) = 0;

    virtual ~Indexer() = default;
protected:
    KeyspaceID _kid;
};

using EmitFn = std::function<void(std::string_view, std::string_view)>;



class Index: public View {
public:

    using View::View;
    using IndexFn = std::function<void(Storage::DocID docId, const std::string_view &value, Indexer &indexer)>;


    using DocWithIdIndex = std::function<void(std::string_view, EmitFn &)>;

    ///Update index for any arbitrary document format
    /**
     * @param storage storage
     * @param rev index revision
     * @param indexFn index function
     * @return id of last processed document + 1
     */
    Storage::DocID update_index(const Storage &storage, std::uint64_t rev, IndexFn indexFn);
    ///Update index for document, which has docid of document being updated
    /**
     * The document has following format: <replaced_docid><content>
     * for the first document, replaced_docid
     * @param storage
     * @param rev
     * @param indexFn
     * @return
     */
    Storage::DocID update_index(const Storage &storage, std::uint64_t rev, DocWithIdIndex indexFn);


    class IndexBuilder;
};

}


#endif /* SRC_DOCDB_INDEX_H_ */
