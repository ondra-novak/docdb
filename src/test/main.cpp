/*
 * main.cpp
 *
 *  Created on: 23. 7. 2019
 *      Author: ondra
 */
#include <iostream>
#include <type_traits>

#include "../docdblib/docdb.h"
#include "../docdblib/dociterator.h"
#include "../imtjson/src/imtjson/object.h"
#include "testClass.h"

using json::Object;
using namespace docdb;

Document example_docs[] = {
		{"aaa","content_a"},
		{"xaq","content_b"},
		{"qwe","content_c"},
		{"qweopiq","content_d"},
		{"smqwq",1258},
		{"1778",12.5},
		{"eade",Object("a",1)("b",true)},
		{"qwewq",Object("a",2)("c","ewwew")},
		{"deceeq",Object("a",5)("c","1124")},
		{"piopo",Object("a",2)("c",false)},
		{"wdrw",Object("a",2)("c","dup")}
};


bool basic_test() {

	DocDB db(inMemory);
	TestSimple tst;

	tst.test("Insert documents","332699937539267857 332696639004383224 332697738516011435 332703236074152490 1139776946063571948 13752622716367829848 1947676425239093014 10612317509722477111 8715971956010370862 13782906422615416973 9477241505581034697 ") >> [&](std::ostream &out) {
		for (auto &x : example_docs) {
			db.put(x);
			out << x.rev << " ";
		}
	};
	tst.test("List documents","1778/13752622716367829848:12.5;"
			 "aaa/332699937539267857:content_a;"
			 "deceeq/8715971956010370862:{\"a\":5,\"c\":\"1124\"};"
			 "eade/1947676425239093014:{\"a\":1,\"b\":true};"
			 "piopo/13782906422615416973:{\"a\":2,\"c\":false};"
			 "qwe/332697738516011435:content_c;"
			 "qweopiq/332703236074152490:content_d;"
			 "qwewq/10612317509722477111:{\"a\":2,\"c\":\"ewwew\"};"
			 "smqwq/1139776946063571948:1258;"
			 "wdrw/9477241505581034697:{\"a\":2,\"c\":\"dup\"};"
			 "xaq/332696639004383224:content_b;") >> [&](std::ostream &out) {
		DocIterator iter = db.scan();
		while (iter.next()) {
			Document doc = iter.get();
			out << doc.id <<  "/" << doc.rev << ":" << doc.content.toString() << ";";
		}
	};
	tst.test("Update document","qwe/332696639004383224:content_b") >> [&](std::ostream &out) {
		Document doc = db.get("qwe");
		doc.content = "content_b";
		db.put(doc);
		doc = db.get("qwe");
		out << doc.id <<  "/" << doc.rev << ":" << doc.content.toString();
	};
	tst.test("Get for replicate","qwe/[332696639004383224,332697738516011435]:content_b") >> [&](std::ostream &out) {
		DocumentRepl doc = db.replicate("qwe");
		out << doc.id <<  "/" << doc.revisions << ":" << doc.content.toString();
	};
	tst.test("Put replicate","qwe/[222,111,332696639004383224,332697738516011435]:content_a") >> [&](std::ostream &out) {
		DocumentRepl doc {
			"qwe","content_a",0,{222,111,332696639004383224,332697738516011435}
		};
		db.put(doc);
		doc = db.replicate("qwe");
		out << doc.id <<  "/" << doc.revisions << ":" << doc.content.toString();
	};
	tst.test("Put same replicate","1qwe/[222,111,332696639004383224,332697738516011435]:content_a") >> [&](std::ostream &out) {
		DocumentRepl doc {
			"qwe","content_a",0,{222,111,332696639004383224,332697738516011435}
		};
		bool x = db.put(doc);
		doc = db.replicate("qwe");
		out << (x?1:0)<< doc.id <<  "/" << doc.revisions << ":" << doc.content.toString();
	};
	tst.test("Put conflict replicate","0qwe/[222,111,332696639004383224,332697738516011435]:content_a") >> [&](std::ostream &out) {
		DocumentRepl doc {
			"qwe","content_a",0,{333,111,332696639004383224,332697738516011435}
		};
		bool x = db.put(doc);
		doc = db.replicate("qwe");
		out << (x?1:0)<< doc.id <<  "/" << doc.revisions << ":" << doc.content.toString();
	};
	tst.test("Delete document","1smqwq/12638155314718423877:<undefined>") >> [&](std::ostream &out) {
		Document doc = db.get("smqwq");
		bool x = db.erase(doc.id, doc.rev);
		doc = db.get("smqwq");
		out << (x?1:0)<< doc.id <<  "/" << doc.rev << ":" << doc.content.toString();
	};

	return !tst.didFail();

}


int main(int argc, char **argv) {
	if (!basic_test()) return 1;
	return 0;
}
