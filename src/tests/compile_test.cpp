#include <docdb/database.h>
#include <docdb/storage.h>
#include <docdb/concepts.h>


#include <iostream>
#include <cstdlib>


void test(docdb::Key &&, docdb::Value &&) {}
void test(docdb::Key &, docdb::Value &&) {}
void test(docdb::Key &&, docdb::Value &) {}
void test(docdb::Key &, docdb::Value &) {}

int main(int , char **) {
    docdb::Key k(0);
    docdb::Value v;
    test({1,2,3},{"a","b"});
    test({1,2,3},v);
    test(k,{"a","b"});
    test(k,v);
    return 0;
}
