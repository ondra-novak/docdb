/*
 * memdb.h
 *
 *  Created on: 15. 5. 2023
 *      Author: ondra
 */

#ifndef SRC_TESTS_MEMDB_H_
#define SRC_TESTS_MEMDB_H_

#include <docdb/database.h>
#include <leveldb/env.h>
#include <leveldb/helpers/memenv.h>

std::unique_ptr<leveldb::Env> newRamdisk() {
    return std::unique_ptr<leveldb::Env>(leveldb::NewMemEnv(leveldb::Env::Default()));
}

leveldb::DB *createTestDB(leveldb::Env *env) {
    leveldb::Options opts;
    opts.env = env;
    opts.create_if_missing = true;
    leveldb::DB *out;
    auto st = leveldb::DB::Open(opts, "testdb", &out);
    if (!st.ok()) throw docdb::DatabaseError(st);
    return out;

}



#endif /* SRC_TESTS_MEMDB_H_ */
