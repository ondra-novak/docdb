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



/// Index allows to generate keys from documents and search these keys
/**
 * The index allows to store keys per document. When document is
 * updated, it deletes keys of old documents and creates new keys
 *
 *
 *
 */
class Index: public View {
public:

    Index(PDatabase db, std::string_view name, Direction dir = Direction::forward, PSnapshot snap = {});
    Index(PDatabase db, KeyspaceID kid, Direction dir = Direction::forward, PSnapshot snap = {});

};

}
#endif /* SRC_DOCDB_INDEX_H_ */
