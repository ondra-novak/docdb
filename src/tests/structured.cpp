
#include <docdb/structured_document.h>

#include <docdb/row.h>
#include <iostream>

#include <map>
int main() {
    docdb::Structured doc = {
            {"aaa","bbb"},
            {"true", true},
            {"false", false},
            {"neg", -15},
            {"double",12.25},
            {"xyz", 12},
            {"pole",{1,2,3,4,5}},
    };

    std::string bin;
    docdb::StructuredDocument::to_binary(doc, std::back_inserter(bin));
    auto beg = bin.begin();
    auto end = bin.end();
    docdb::Structured out = docdb::StructuredDocument::from_binary(beg, end);

    bool equal = out == doc;
    std::cout << equal << std::endl;
    std::cout << bin << std::endl;
}






