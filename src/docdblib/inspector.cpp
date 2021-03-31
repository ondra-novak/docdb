/*
 * inspector.cpp
 *
 *  Created on: 29. 3. 2021
 *      Author: ondra
 */

#include <fstream>
#include "inspector.h"

#include <imtjson/array.h>
#include <imtjson/string.h>
#include <imtjson/serializer.h>
#include "../../../imtjson/src/imtjson/namedEnum.h"
#include "json_map_view.h"
#include "view.h"
namespace docdb {

using namespace json;

template<typename U, typename T>
T splitAt(const U &search, T &object) {
	auto s = T(search);
	auto l = object.size();
	auto sl = s.size();
	auto k = object.find(s);
	if (k > l) {
		T ret = object;
		object = object.substr(l);
		return ret;
	} else {
		T ret = object.substr(0,k);
		object = object.substr(k+sl);
		return ret;
	}
}


Inspector::Inspector(DB db):db(db) {
	// TODO Auto-generated constructor stub

}

static json::NamedEnum<KeySpaceClass> strClasses({
	{KeySpaceClass::document_index,"docdb"},
	{KeySpaceClass::filterView,"filter"},
	{KeySpaceClass::graveyard_index,"trash"},
	{KeySpaceClass::incremental_store,"inc_store"},
	{KeySpaceClass::jsonmap_view,"map"},
	{KeySpaceClass::view,"index"}
});

json::String getClassName(int clsid) {
	auto n =strClasses[static_cast<KeySpaceClass>(clsid)];
	if (n.empty()) {
		char buff[100];
		snprintf(buff,100,"user%02X",clsid);
		return buff;
	}else {
		return n;
	}
}

std::string decodeUrlEncode(std::string_view input) {
	std::string out;
	out.reserve(input.length()*3/2);
	char n=0;
	unsigned int s=0;
	for (char c: input) {
		if (s) {
			c = std::toupper(c);
			if (isxdigit(c)) {
				auto v =  (c < 'A'?c-'0':c-'A'+10);
				if (s==1) {
					n = v;
					s=2;
				} else {
					n = (n << 4) | v;
					s=0;
					out.push_back(n);
				}
			} else {
				s = 0;
				out.push_back(c);
			}
		} else if (c == '%') {
			s = 1;
		} else {
			out.push_back(c);
		}
	}
	return out;
}


bool Inspector::request(std::string_view method, std::string_view path, json::Value query, json::Value body, IOutput &&output) {



	if (path.substr(0,4) == "/db/") {

		std::string_view dbpath = path.substr(4);
		std::string_view clsname = splitAt("/", dbpath);
		if (clsname.empty()) {
			auto iter = db.listKeyspaces();
			Array res;
			while (iter.next()) {
				res.push_back(Value(object,{
					Value("id",iter.getID()),
					Value("class", getClassName(iter.getClass())),
					Value("name", iter.getName())
				}));
			}
			sendJSON(output, res);
			return true;
		} else {
			const KeySpaceClass *cls = strClasses.find(clsname);
			ClassID clsid;
			if (cls) {
				clsid = (int)(*cls);
			} else {
				if (clsname.compare(0,4,"user") == 0) {
					clsid = std::strtol(clsname.data()+4,nullptr,16);
				} else {
					return false;
				}
			}

			std::string_view dbname_enc=splitAt("/", dbpath);
			if (dbname_enc.empty()) {
				auto iter = db.listKeyspaces();
				Array res;
				while (iter.next()) {
					if (iter.getClass() == clsid) {
						res.push_back(Value(object,{
							Value("id",iter.getID()),
							Value("class", getClassName(iter.getClass())),
							Value("name", iter.getName())
						}));
					}
				}
				sendJSON(output, res);
				return true;
			} else {
				std::string dbname = decodeUrlEncode(dbname_enc);
				if (dbpath.empty()) {
					DB snp = db.getSnapshot(SnapshotMode::writeIgnore);
					if (query["raw"].defined()) {
						queryOnGenericMap(snp, clsid, dbname, query, output);
					} else {
						switch (clsid) {
						case int(KeySpaceClass::view): queryOnView(snp, dbname, query, output);break;
						case int(KeySpaceClass::jsonmap_view):queryOnJsonMap(snp, dbname, query, output);break;
						default: queryOnGenericMap(snp, clsid, dbname, query, output);break;
						}
					}
					return true;
				}  else if (dbpath == "info"){
					DB snp = db.getSnapshot(SnapshotMode::writeIgnore);
					KeySpaceID kid = db.allocKeyspace(clsid,  dbname);
					json::Value metadata = db.keyspace_getMetadata(kid);
					auto sz = db.getKeyspaceSize(kid);
					json::Value res(json::object,{
						json::Value("kid", kid),
						json::Value("metadata", metadata),
						json::Value("size", sz)
					});
					sendJSON(output, res);
					return true;


				} else {
					return false;
				}
			}
		}


	} else {
		if (path[0] == '/') {
			auto fname = path.substr(1);
			if (fname.empty()) fname = "index.html";
			auto iter = fileMap.find(fname);
			if (iter == fileMap.end()) return false;
			output.begin(200, iter->second.content_type);
			output.send(iter->second.content);
/*			auto ext = fname.substr(fname.rfind('.')+1);
			std::string_view ctx;
			if (ext == "html") ctx = "text/html;charset=utf-8";
			else if (ext == "js") ctx = "text/javascript;charset=utf-8";
			else if (ext == "css") ctx = "text/css;charset=utf-8";
			else if (ext == "png") ctx = "image/png";
			else if (ext == "jpg") ctx = "image/jpeg";
			else if (ext == "svg") ctx = "image/svg";
			else ctx = "application/octet-stream";

			std::ifstream file_in((std::string(fname)));
			if (!file_in) {
				return false;
			}

			output.begin(200, ctx);
			char buff[4096];
			bool rep = true;
			while (rep) {
				file_in.read(buff,4096);
				auto cnt = file_in.gcount();
				if (cnt) output.send(std::string_view(buff,cnt));
				rep = cnt == 4096;
			}
			*/
			return true;
		}


		return false; //zatim
	}


}

void Inspector::sendJSON(IOutput &output, json::Value c) {
	output.begin(200, "application/json");
	output.send(c.stringify().str());
}

static UInt offsetLimit(json::Value query, UInt &offset) {
	Value l = query["limit"];
	if (l.defined()) {
		auto limit = l.getUInt();
		if (limit == 0) offset = 0;
		return offset+limit;
	} else {
		return ~offset;
	}
}

void Inspector::queryOnJsonMap(DB snp, std::string_view name, json::Value query, IOutput &output) {
	JsonMapView view(snp,name);
	std::optional<JsonMapView::Iterator> iter;
	Value cmd;
	bool descending = query["descending"].getBool();
	if ((cmd = query["key"]).defined()) iter = view.find(Value::fromString(cmd.getString()));
	else if ((cmd = query["prefix"]).defined()) iter = view.prefix(Value::fromString(cmd.getString()),descending);
	else if ((cmd = query["start_key"]).defined()) {
		Value to = query["end_key"];
		cmd = Value::fromString(cmd.getString());
		bool include_upper = query["include_upper"].getBool();
		if (to.defined()) {
			iter = view.range(cmd, Value::fromString(to.getString()), include_upper);
		} else {
			iter = view.scan(cmd,descending);
		}
	} else {
		iter = view.scan(descending);
	}
	output.begin(200, "application/json");
	output.send("[");
	auto offset = query["offset"].getUInt();
	auto limit = offsetLimit(query,offset);
	std::string buff;
	auto appbuff = [&](char c){buff.push_back(c);};
	while (iter->next() && limit) {
		limit--;
		if (offset) {
			offset--;
		} else {
			buff.append("{\"key\":");
			iter->key().serialize(appbuff);
			buff.append(",\"value\":");
			iter->value().serialize(appbuff);
			buff.append("}");
			output.send(buff);
			buff.clear();
			buff.append(",\n");
		}
	}
	output.send("]");
}



void Inspector::queryOnView(DB snp, std::string_view name, json::Value query, IOutput &output) {
	std::optional<View::Iterator> iter;
	View view(snp,name);
	Value cmd;
	bool descending = query["descending"].getBool();
	if ((cmd = query["key"]).defined()) iter = view.find(Value::fromString(cmd.getString()));
	else if ((cmd = query["prefix"]).defined()) iter = view.prefix(Value::fromString(cmd.getString()),descending);
	else if ((cmd = query["start_key"]).defined()) {
		Value to = query["end_key"];
		cmd = Value::fromString(cmd.getString());
		bool include_upper = cmd["include_upper"].getBool();
		if (to.defined()) {
			iter = view.range(cmd, Value::fromString(to.getString()), include_upper);
		} else {
			iter = view.scan(cmd,std::string_view(),descending);
		}
	} else {
		iter = view.scan(descending);
	}
	output.begin(200, "application/json");
	output.send("[");
	auto offset = query["offset"].getUInt();
	auto limit = offsetLimit(query,offset);
	std::string buff;
	auto appbuff = [&](char c){buff.push_back(c);};
	while (iter->next() && limit) {
		limit--;
		if (offset) {
			offset--;
		} else {
			buff.append("{\"key\":");
			iter->key().serialize(appbuff);
			buff.append(",\"id\":");
			Value(iter->id()).serialize(appbuff);
			buff.append(",\"value\":");
			iter->value().serialize(appbuff);
			buff.append("}\n");
			output.send(buff);
			buff.clear();
			buff.push_back(',');
		}
	}
	output.send("]");

}

void Inspector::queryOnGenericMap(DB snp, ClassID clsid, std::string_view name, json::Value query, IOutput &output) {
	Value cmd;
	auto kid = snp.allocKeyspace(clsid, name);
	Key kfrom(kid);
	Key kto(kid);
	bool descending = query["descending"].getBool();
	if ((cmd = query["key"]).defined()) {
		kfrom.append(std::string_view(Value::fromString(cmd.getString()).getString()));
		kto = kfrom;
	}
	else if ((cmd = query["prefix"]).defined()) {
		kfrom.append(std::string_view(Value::fromString(cmd.getString()).getString()));
		kto = kfrom;
		if (descending) kfrom.upper_bound(); else kto.upper_bound();
	}
	else if ((cmd = query["start_key"]).defined()) {
		Value to = query["end_key"];
		cmd = Value::fromString(cmd.getString());
		kfrom.append(std::string_view(cmd.getString()));
		if (to.defined()) {
			kto.append(std::string_view(Value::fromString(to.getString()).getString()));
		} else {
			if (!descending) kto.upper_bound();
		}
	} else {
		if (descending) kfrom.upper_bound(); else kto.upper_bound();
	}
	auto iter = snp.createIterator({kfrom, kto, false, true});
	output.begin(200, "application/json");
	output.send("[");
	auto offset = query["offset"].getUInt();
	auto limit = offsetLimit(query,offset);
	std::string buff;
	auto appbuff = [&](char c){buff.push_back(c);};
	while (iter.next() && limit) {
		limit--;
		if (offset) {
			offset--;
		} else {
			buff.append("{\"key\":");
			Value(iter.key()).serialize(appbuff);
			buff.append(",\"value\":");
			Value(iter.value()).serialize(appbuff);
			buff.append("}\n");
			output.send(buff);
			buff.clear();
			buff.push_back(',');
		}
	}
	output.send("]");

}

} /* namespace docdb */
