#include "check.h"
#include <docdb/row.h>
#include <docdb/key.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <vector>

static_assert(docdb::DocumentDef<docdb::RowDocument> );

void test_basics() {

    docdb::Row y({true,"hello","world",42});

    {
    auto [a,b,c,d] = y.get<bool, std::string, std::string, int>();

    CHECK(a);
    CHECK_EQUAL(b,"hello");
    CHECK_EQUAL(c,"world");
    CHECK_EQUAL(d,42);
    }


    using MyVar = std::variant<int,std::string, bool>;
     MyVar val1(10), val2("ahoj"), val3(true);

    docdb::Row aa(val1, val2, val3);
    {
        auto [a,b,c] = aa.get<MyVar,MyVar,MyVar>();
        CHECK(std::holds_alternative<int>(val1));
        CHECK(std::holds_alternative<std::string>(val2));
        CHECK(std::holds_alternative<bool>(val3));
        CHECK_EQUAL(std::get<int>(a), 10);
        CHECK_EQUAL(std::get<std::string>(b), "ahoj");
        CHECK(std::get<bool>(c));
    }




}

void test_wide_chars() {

    docdb::Row aw(L"hello world",42);
    auto [txt, val] = aw.get<std::wstring, int>();
    CHECK(txt == L"hello world");
    CHECK(val == 42);
}



void test_collation() {
    constexpr std::string_view words[] = {
            "hrad","chamrad","mamlas","brak"
    };
    std::vector<docdb::Row> rows;
    std::locale czech("cs_CZ.UTF-8");
    std::size_t i = 0;
    for (auto c: words) {
        rows.push_back(docdb::Row(docdb::LocalizedString(c, czech),i));
        i++;
    }
    std::sort(rows.begin(), rows.end());
    std::vector<std::string_view> results;
    for (auto c: rows) {
        auto [txt,idx] = c.get<std::string_view,std::size_t>();
        results.push_back(words[idx]);
    }
    CHECK_EQUAL(results[0], "brak");
    CHECK_EQUAL(results[1], "hrad");
    CHECK_EQUAL(results[2], "chamrad");
    CHECK_EQUAL(results[3], "mamlas");

    std::vector<docdb::Row> numbers={1,32.5,-4.8,0,12e3,7e-4,-11e8, -0.5, -25.21584, 6, std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::signaling_NaN()};
    std::sort(numbers.begin(), numbers.end());
    std::vector<double> nres;
    for (auto &c: numbers) {
        auto [f] = c.get<double>();
        nres.push_back(f);
    }
    CHECK(!std::isfinite(nres[0]) && nres[0] < 0);
    CHECK_NEAR(nres[1], -1.1e+09, 1e01);
    CHECK_NEAR(nres[2], -25.2158, 1e-4);
    CHECK_NEAR(nres[3], -4.8, 1e-4);
    CHECK_NEAR(nres[4], -0.5, 1e-4);
    CHECK_NEAR(nres[5], 0, 1e-4);
    CHECK_NEAR(nres[6], 0.0007, 1e-4);
    CHECK_NEAR(nres[7], 1, 1e-4);
    CHECK_NEAR(nres[8], 6, 1e-4);
    CHECK_NEAR(nres[9], 32.5, 1e-4);
    CHECK_NEAR(nres[10], 12000, 1e-4);
    CHECK(!std::isfinite(nres[11]) && nres[11] > 0);
    CHECK(std::isnan(nres[12]));


}

void test_subrow() {
    docdb::Row rw1("ahoj","nazdar");
    docdb::Row rw2(42, rw1);

    auto [x,y,z] = rw2.get<int, std::string_view, std::string_view>();
    CHECK_EQUAL(x,42);
    CHECK_EQUAL(y,"ahoj");
    CHECK_EQUAL(z,"nazdar");

    auto [a,b] = rw2.get<int, docdb::Row>();
    CHECK_EQUAL(a, 42);
    auto [i,j] = b.get<std::string_view,std::string_view>();
    CHECK_EQUAL(i, "ahoj");
    CHECK_EQUAL(j, "nazdar");
}

void test_keys(){
    docdb::Key k(1,2,"aaa");
    docdb::Key k2(k);
    {
    auto [a,b,c] = k2.get<int,int,std::string_view>();
    CHECK_EQUAL(a,1);
    CHECK_EQUAL(b,2);
    CHECK_EQUAL(c,"aaa");
    }
    auto k3=k2.prefix_end();
    {
    auto [a,b,c] = k3.get<int,int,std::string_view>();
    CHECK_EQUAL(a,1);
    CHECK_EQUAL(b,2);
    CHECK_EQUAL(c,"aaa\001");
    }




}

void test_document() {

    {
        const char text[] = "ahoj\0nazdar";
        std::string_view textstr(text, sizeof(text));
        docdb::Row row = docdb::RowDocument::from_binary(textstr.begin(), textstr.end());
        auto [a,b] = row.get<std::string_view, std::string_view>();
        CHECK_EQUAL(a,"ahoj");
        CHECK_EQUAL(b,"nazdar");
    }

    {
        const char text[] = "ahoj\0nazdar";
        std::string textstr(text, sizeof(text));
        docdb::Row row = docdb::RowDocument::from_binary(textstr.begin(), textstr.end());
        auto [a,b] = row.get<std::string_view, std::string_view>();
        CHECK_EQUAL(a,"ahoj");
        CHECK_EQUAL(b,"nazdar");
    }

    {
        docdb::Row rw(1,2,3);
        std::string s;
        docdb::RowDocument::to_binary(rw, std::back_inserter(s));
        auto [a,b,c] = docdb::Row::extract<int,int,int>(s);
        CHECK_EQUAL(a,1);
        CHECK_EQUAL(b,2);
        CHECK_EQUAL(c,3);
    }
}

void test_container () {

    static_assert(docdb::IsContainer<std::vector<int> >);

    std::vector<int> x = {10,20,30,40};
    docdb::Row rw(x);

    auto [z] = rw.get<std::vector<int> >();
    CHECK_EQUAL(z.size(),4);
    for (int i = 0; i < 4; i++) {
        CHECK_EQUAL((i+1)*10, z[i]);
    }
}

int main() {
    test_basics();
    test_wide_chars();
    test_collation();
    test_subrow();
    test_keys();
    test_document();
    test_container();

}
