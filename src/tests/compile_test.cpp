#include <docdb/buffer.h>
#include <docdb/database.h>
#include <docdb/concepts.h>
#include <docdb/serialize.h>
#include <docdb/doc_storage.h>
#include <docdb/doc_index.h>
#include <docdb/map.h>
#include <docdb/doc_index_unique.h>
#include <docdb/aggregate.h>


template class docdb::Buffer<char, 50>;


template class docdb::DocumentStorage<docdb::StringDocument>;
static_assert(docdb::DocumentStorageType<docdb::DocumentStorage<docdb::StringDocument> >);
template class docdb::DocumentStorage<docdb::BasicRowDocument>;
static_assert(docdb::DocumentStorageType<docdb::DocumentStorage<docdb::BasicRowDocument> >);
template class docdb::DocumentIndexView<docdb::DocumentStorage<docdb::BasicRowDocument> >;


#include <iostream>
#include <cstdlib>


void test(docdb::RawKey &&, docdb::BasicRow &&) {}
void test(docdb::RawKey &, docdb::BasicRow &&) {}
void test(docdb::RawKey &&, docdb::BasicRow &) {}
void test(docdb::RawKey &, docdb::BasicRow &) {}



int main(int , char **) {
    docdb::RawKey k(0);
    docdb::BasicRow v;
    test({1,2,3},{"a","b"});
    test({1,2,3},v);
    test(k,{"a","b"});
    test(k,v);
    return 0;
}
