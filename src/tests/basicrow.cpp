#include "check.h"
#include <docdb/serialize.h>

int main() {

    static_assert(std::is_array_v<std::remove_reference_t<decltype("ahoj")> >);

    docdb::BasicRow row1("Pavel","Nowak",47,"M");

    {
        auto [fname, lname, age, sex] = row1.get<std::string_view, std::string_view, int, std::string_view>();

        CHECK_EQUAL(fname,"Pavel");
        CHECK_EQUAL(lname,"Nowak");
        CHECK_EQUAL(age,47);
        CHECK_EQUAL(sex,"M");
    }


    using RowFormat = std::tuple<std::string, const char *, unsigned int, std::string_view>;

    docdb::BasicRow row2(RowFormat("Marie", "Machalkova", 23, "F"));


    {
        auto [fname, lname, age, sex] = row2.get<RowFormat>();

        CHECK_EQUAL(fname,"Marie");
        CHECK_EQUAL(std::string_view(lname),"Machalkova");
        CHECK_EQUAL(age,23);
        CHECK_EQUAL(sex,"F");
    }


}
