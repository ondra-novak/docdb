#include "check.h"
#include "memdb.h"

void test_keyspaces() {

    auto ramdisk = newRamdisk();
    docdb::KeyspaceID t1,t2;
    {
        auto db = docdb::Database::create(createTestDB(ramdisk.get()));
        auto ktbl1 = db->open_table("tbl1");
        auto ktbl2 = db->open_table("tbl2");
        auto ktbl3 = db->open_table("tbl1");
        CHECK_EQUAL(ktbl1, 0);
        CHECK_EQUAL(ktbl1, ktbl3);
        CHECK_NOT_EQUAL(ktbl1, ktbl2);
        t1 = ktbl1;
        t2 = ktbl2;
    }
    {
        auto db = docdb::Database::create(createTestDB(ramdisk.get()));
        auto ktbl1 = db->open_table("tbl1");
        auto ktbl2 = db->open_table("tbl2");
        auto ktbl3 = db->open_table("tbl1");
        CHECK_EQUAL(ktbl1, ktbl3);
        CHECK_NOT_EQUAL(ktbl1, ktbl2);
        CHECK_EQUAL(t1, ktbl1);
        CHECK_EQUAL(t2, ktbl2);
        db->delete_table("tbl1");
        auto m = db->list();
        CHECK(m.find("tbl1") == m.end());
        CHECK(m.find("tbl2") != m.end());
    }
    {
        auto db = docdb::Database::create(createTestDB(ramdisk.get()));
        auto ktbl1 = db->open_table("tbl1");
        CHECK_EQUAL(ktbl1, 0);
    }


}


int main() {
    test_keyspaces();


}
