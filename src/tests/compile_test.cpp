#include <docdb/database.h>
#include <docdb/storage.h>
#include <docdb/concepts.h>
#include <docdb/serialize.h>
#include <docdb/doc_storage.h>

template class docdb::DocumentStorage<docdb::BinaryDocument>;

#include <iostream>
#include <cstdlib>


void test(docdb::RawKey &&, docdb::Value &&) {}
void test(docdb::RawKey &, docdb::Value &&) {}
void test(docdb::RawKey &&, docdb::Value &) {}
void test(docdb::RawKey &, docdb::Value &) {}



int main(int , char **) {
    docdb::RawKey k(0);
    docdb::Value v;
    test({1,2,3},{"a","b"});
    test({1,2,3},v);
    test(k,{"a","b"});
    test(k,v);
    return 0;
}
