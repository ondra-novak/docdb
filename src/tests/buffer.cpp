#include "check.h"
#include <docdb/buffer.h>


using namespace docdb;

int main() {
    static constexpr Buffer<char, 30> ctest("aaaa");
    CHECK_EQUAL(ctest,"aaaa");

    Buffer<char, 20> test("ahoj");
    CHECK_EQUAL(test, "ahoj");
    Buffer<char, 10> test2(test);
    CHECK_EQUAL(test2, "ahoj");
    Buffer<char, 0> test3(test);
    CHECK_EQUAL(test3, "ahoj");
    Buffer<char, 30> test4(std::move(test));
    CHECK_EQUAL(test4, "ahoj");
    CHECK_EQUAL(test, "");

    Buffer<char, 20> test_large_1("largelargelargelargelargelargelargelarge");
    CHECK_EQUAL(test_large_1, "largelargelargelargelargelargelargelarge");
    Buffer<char, 20> test_large_2(std::move(test_large_1));
    CHECK_EQUAL(test_large_2, "largelargelargelargelargelargelargelarge");
    CHECK_EQUAL(test_large_1, "");

    std::string a;
    Buffer<char, 30> b;
    for (int i = 0; i < 100; i++) {
        a.push_back(i);
        b.push_back(i);
        CHECK_EQUAL(a,b);
    }


}
