/*
 * inspector.h
 *
 *  Created on: 29. 3. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDB_SRC_DOCDBLIB_INSPECTOR_H_
#define SRC_DOCDB_SRC_DOCDBLIB_INSPECTOR_H_
#include <map>

#include "db.h"

namespace docdb {







class Inspector {
public:
	Inspector(DB db);

	class IOutput {
	public:

		virtual void begin(int status, std::string_view contentType) = 0;
		virtual void send(const std::string_view &data) = 0;
		virtual void error(int status, const std::string_view &data) = 0;

	};




	bool request(std::string_view method, std::string_view path, json::Value query, json::Value body, IOutput &&output);


	template<typename Request, typename QueryParser, typename Response>
	bool simpleServerRequest(Request &req, const QueryParser &qp);
	template<typename Request, typename QueryParser>
	bool userverRequest(Request &req, const QueryParser &qp);

protected:
	DB db;

	struct FileInfo {
		std::string_view content_type;
		std::string_view content;
	};

	using FileMap = std::map<std::string_view, FileInfo>;
	static FileMap fileMap;

	void sendJSON(IOutput &output, json::Value c);
	void queryOnView(DB snp, std::string_view name, json::Value query, IOutput &output);
	void queryOnJsonMap(DB snp, std::string_view name, json::Value query, IOutput &output);
	void queryOnGenericMap(DB snp, ClassID clsid, std::string_view name, json::Value query, IOutput &output);
};



template<typename Request, typename QueryParser, typename Response>
bool Inspector::simpleServerRequest(Request &req, const QueryParser &qp) {

	class Output: public IOutput {
	public:
		std::optional<decltype(std::declval<Request>().sendResponse(""))> stream;
		Request req;

		Output(Request &req):req(std::move(req)) {}
		~Output() {
			if (stream.has_value()) stream->flush();
		}

		virtual void begin(int status, std::string_view contentType) {
			stream = req.sendResponse(Response(status).contentType(contentType));
		}
		virtual void send(const std::string_view &data) {
			stream->write(json::BinaryView(json::StrViewA(data)));
		}
		virtual void error(int status, const std::string_view &data) {
			req.sendErrorPage(status,"",data);
		}

	};

	json::Value body;
	auto method = req.getMethod();
	if (method != "GET") {
		try {
			body = json::Value::parse(req.getBodyStream());
		} catch (...) {

		}
	}

	return request(method,qp.getPath(), json::Value(json::object, qp.begin(), qp.end(),[&](const auto &x){
		return json::Value(x.first, x.second);
	}), body, Output(req));

}

template<typename Request, typename QueryParser>
bool Inspector::userverRequest(Request &req, const QueryParser &qp) {

	class Output: public IOutput {
	public:
		Request req;
		std::optional<decltype(std::declval<Request>()->send())> stream;

		Output(Request &req):req(std::move(req)) {}
		~Output() {
			if (stream.has_value()) {
				stream->flush();
				stream.reset();
			}
		}

		virtual void begin(int status, std::string_view contentType) {
			req->setContentType(contentType);
			stream = req->send();
		}
		virtual void send(const std::string_view &data) {
			stream->write(data);
		}
		virtual void error(int status, const std::string_view &data) {
			req->sendErrorPage(status,data);
		}

	};

	json::Value body;
	auto method = req->getMethod();
	if (method != "GET") {
		try {
			auto stream = req->getBody();
			body = json::Value::parse([&]{return stream.getChar();});
		} catch (...) {

		}
	}

	return request(method,qp.getPath(), json::Value(json::object, qp.begin(), qp.end(),[&](const auto &x){
		return json::Value(x.first, x.second);
	}), body, Output(req));

}


} /* namespace docdb */

#endif /* SRC_DOCDB_SRC_DOCDBLIB_INSPECTOR_H_ */
