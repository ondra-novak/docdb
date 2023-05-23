#include <docdb/buffer.h>
#include <docdb/database.h>
#include <docdb/concepts.h>
#include <docdb/serialize.h>
#include <docdb/doc_storage.h>
#include <docdb/doc_index.h>
#include <docdb/map.h>
#include <docdb/aggregate.h>


template class docdb::Buffer<char, 50>;


template class docdb::DocumentStorage<docdb::StringDocument>;
static_assert(docdb::DocumentStorageType<docdb::DocumentStorage<docdb::StringDocument> >);
template class docdb::DocumentStorage<docdb::RowDocument>;
static_assert(docdb::DocumentStorageType<docdb::DocumentStorage<docdb::RowDocument> >);
template class docdb::DocumentIndexView<docdb::DocumentStorage<docdb::RowDocument> >;


#include <iostream>
#include <cstdlib>



int main(int , char **) {
}
