#include <docdb/binops.h>
#include <iostream>

int main() {

    using namespace docdb;

    RecordsetStackT<DocID, std::nullptr_t>  calc;
    RecordsetStackT<DocID, std::nullptr_t>::Set res;
    for (const auto &item: calc.push({{1},{2},{3},{4},{5},{6},{7},{8},{9},{10}})
        .push_unsorted({{3},{5},{1},{6},{5}})
        .NOT()
        .AND()
        .push_unsorted({{3},{12},{0},{33}})
        .push_unsorted({{5},{12},{11},{33}})
        .XOR()
        .OR()) {
        std::cout << item.id << std::endl;
    }


}
