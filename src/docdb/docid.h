#pragma once
#ifndef SRC_DOCDB_DOCID_H_
#define SRC_DOCDB_DOCID_H_
#include "database.h"


namespace docdb {


///Helps to record latest docid
/**
 * This solves problem of overlapping batches. If there is multiple pending batches,
 * it can be hard to determine highest document ID has been written as the batches
 * can be commited in different order. This is solved by assign an unique key to
 * each batch, to the batch store its document ID under unique key. However there
 * is only limited count of unique keys. It uses serial number of the batch, which
 * increases over time. So the unique key is derived from the serial number and document
 * id is stored under this key. To determine highest stored document, just read all
 * possible unique keys and find maximum.
 */
class LastSeenDocID {
public:
    using KeyType = std::uint16_t;


    ///Retrieves max DocID stored in the database
    /**
     * @param db database
     * @param kid collection's keyspaceid
     * @return last stored document id
     */
    static DocID get_max_docid(const PDatabase &db, KeyspaceID kid)  {
        auto k1 = Database::get_private_area_key(kid, KeyType(0));
        auto k2 = Database::get_private_area_key(kid, KeyType(65535));
        RecordsetBase rs(db->make_iterator(), {
              k1,k2,FirstRecord::included, FirstRecord::excluded
        });
        DocID res =0;
        if (!rs.empty()) do {
                Row rw(RowView(rs.raw_value()));
                auto [id] = rw.get<DocID>();
                res = std::max(res, id);
            }
            while (rs.next());
        return res;
    }

    ///Records stored document id
    /**
     * @param b batch
     * @param kid keyspace id of collection
     * @param docid document id to store
     * @param mask mask for batch's revision id
     */
    static void store_DocID(Batch &b, KeyspaceID kid, DocID docid, KeyType mask) {
        auto kv = static_cast<KeyType>(b.get_revision()) & mask;
        auto key =  Database::get_private_area_key(kid, KeyType(0), kv);
        Row rw(docid);
        b.Put(key, rw);
    }

    static KeyType threads_to_mask(unsigned int threads) {
        threads = std::min(threads, 65536U);
        for (unsigned int i = 0; i < 16; i++) {
            threads |= threads >> 1;
        }
        return KeyType(threads);
    }
};




}



#endif /* SRC_DOCDB_DOCID_H_ */
