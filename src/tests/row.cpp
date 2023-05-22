#include "check.h"
#include <docdb/row.h>

int main() {

    docdb::Row x(docdb::RowView("hello"));
    CHECK_EQUAL(std::string_view(x) ,"hello");

    docdb::Row y({true,"hello","world",42});

    {
    auto [a,b,c,d] = y.get<bool, std::string, std::string, int>();

    CHECK(a);
    CHECK_EQUAL(b,"hello");
    CHECK_EQUAL(c,"world");
    CHECK_EQUAL(d,42);
    }

    docdb::Row z = docdb::RowView(std::string_view(y));
    {
    auto [a,b,c,d] = z.get<bool, std::string, std::string, int>();

    CHECK(a);
    CHECK_EQUAL(b,"hello");
    CHECK_EQUAL(c,"world");
    CHECK_EQUAL(d,42);
    }

}
