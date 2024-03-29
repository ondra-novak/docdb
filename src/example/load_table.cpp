#include <iostream>
#include <docdb/storage.h>
#include <docdb/indexer.h>


#include "CSVReader.h"

static docdb::PDatabase open_db() {
    leveldb::Options opts;
    opts.create_if_missing = true;
    return docdb::Database::create("example_db", opts);
}

struct Customer {
    std::string id;
    std::string fname;
    std::string lname;
    std::string company;
    std::string city;
    std::string country;
    std::string phone1;
    std::string phone2;
    std::string email;
    std::string subsdate;
    std::string website;

    using FldPtr = std::string Customer::*;

    static constexpr FldPtr all_fields[] = {
            &Customer::id,
            &Customer::fname,
            &Customer::lname,
            &Customer::company,
            &Customer::city,
            &Customer::country,
            &Customer::phone1,
            &Customer::phone2,
            &Customer::email,
            &Customer::subsdate,
            &Customer::website
    };
};

struct CustomerDocument {
    using Type = Customer;
    template<typename Iter>
    static Type from_binary(Iter beg, Iter end) {
        Customer c;
        for (auto x: Customer::all_fields) {
            c.*x = docdb::Row::deserialize_item<std::string>(beg, end);
        }
        return c;
    }
    template<typename Iter>
    static Iter to_binary(const Customer &c, Iter iter) {
        for (auto x: Customer::all_fields) {
            iter = docdb::Row::serialize_items(iter, c.*x);
        }
        return iter;
    }

};

enum class IndexedField {
    name, company, city, country, phone, email, subsdate, website
};

struct PrimaryIndexFn {
    static constexpr int revision = 1;
    template<typename Emit>
    void operator()(Emit emit, const Customer &row) const {
        emit(row.id,{});
    }
};

struct OtherIndexFn {
    static constexpr int revision = 1;
    template<typename Emit>
    void operator()(Emit emit, const Customer &row) const {
        emit({IndexedField::name, row.fname, row.lname},{});
        emit({IndexedField::company, row.company},{});
        emit({IndexedField::city, row.city},{});
        emit({IndexedField::phone, row.phone1},{});
        emit({IndexedField::phone, row.phone2},{});
        emit({IndexedField::email, row.email},{});
        emit({IndexedField::subsdate, row.subsdate},{});
        emit({IndexedField::website, row.website},{});
    }
};

using Storage = docdb::Storage<CustomerDocument>;
using PrimaryIndex = docdb::Indexer<Storage,PrimaryIndexFn, docdb::IndexType::unique,docdb::StringDocument>;
using OtherIndex = docdb::Indexer<Storage, OtherIndexFn, docdb::IndexType::multi, docdb::StringDocument>;


void read_csv(std::istream &in, Storage &storage) {

    auto reader = [&]{return in.get();};
    CSVReader<decltype(reader)> csv(std::move(reader));

    auto mapping = csv.mapColumns<Customer>({
        {"Customer Id", &Customer::id},
        {"First Name", &Customer::fname},
        {"Last Name", &Customer::lname},
        {"Company", &Customer::company},
        {"City", &Customer::city},
        {"Country", &Customer::country},
        {"Phone 1", &Customer::phone1},
        {"Phone 2", &Customer::phone2},
        {"Email", &Customer::email},
        {"Subscription Date", &Customer::subsdate},
        {"Website", &Customer::website}
    });
    Customer c;
    while (csv.readRow(mapping, c)) {
        storage.put(c);
    }
}

int main() {

    auto db = open_db();
    Storage storage(db, "customers");
    PrimaryIndex primaryIndex(storage, "customers_primary");
    OtherIndex otherIndex(storage, "customers_by_fields");
    read_csv(std::cin, storage);




}
