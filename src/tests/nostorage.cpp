#include "../docdb/nostorage.h"
#include "../docdb/index_view.h"
namespace docdb {


static_assert(DocumentStorageType<NoStorage<RowDocument> >);



}

int main() {
    //TODO test
    return 0;
}
