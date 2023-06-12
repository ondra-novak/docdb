#include <docdb/binops.h>
#include <iostream>

int main() {

    using namespace docdb;

    SetCalculator<DocID> calc;
    SetCalculator<DocID>::Set res;
    calc.push({1,2,3,4,5,6,7,8,9,10})
        .push_unsorted({3,5,1,6,5})
        .NOT_AND()
        .push_unsorted({3,12,0,33})
        .push_unsorted({5,12,11,33})
        .XOR()
        .OR()
        .pop(res);


    for (auto id: res) {
        std::cout << id << std::endl;
    }


}
