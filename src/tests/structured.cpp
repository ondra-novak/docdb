
#include <docdb/structured_document.h>
#include <docdb/json.h>
#include "check.h"

#include <docdb/row.h>
#include <iostream>

using Document = docdb::StructuredDocument<>;

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
            {"unicode",L"Příšerně žluťoučký kůň úpěl ďábelské kódy"},
            {"null1",std::wstring(L"znak\0nula",9)},
            {"null2",std::string("znak\0nula",9)}
    };

    std::string bin;
    Document::to_binary(doc, std::back_inserter(bin));
    auto beg = bin.begin();
    auto end = bin.end();
    docdb::Structured out = Document::from_binary(beg, end);

    bool equal = out == doc;
    CHECK(equal);
    CHECK_EQUAL(out["neg"].as<int>(), -15);

    std::string x = doc.to_json();
    docdb::Structured jout = docdb::Structured::from_json(x);
    std::string y = jout.to_json();
    CHECK_EQUAL(x, y);
}






