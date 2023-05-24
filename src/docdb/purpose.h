#pragma once
#ifndef SRC_DOCDB_PURPOSE_H_
#define SRC_DOCDB_PURPOSE_H_

namespace docdb {

///Defines purpose of keyspace
/**
 * It helps to manage keyspaces, to determine which keyspace carries documents and
 * which contains derived data.
 */
enum class Purpose: char {
    ///Keyspace is used as document storage
    storage = 'S',     ///< storage
    ///Keyspace is used as index with duplicated keys (docid is at the end of the key)
    index = 'I',       ///< index
    ///Keyspace is used as unique index (docid is at the beginning of the value)
    unique_index = 'U',///< unique_index
    ///Keyspace is unspecified map
    map = 'M',         ///< map
    ///Keyspace is materialized aggregation
    aggregation = 'A',  ///< aggregation
    ///Keyspace has no defined purpose - created by user
    undefined = '?'
};

}




#endif /* SRC_DOCDB_PURPOSE_H_ */
