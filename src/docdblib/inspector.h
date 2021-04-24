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



class InspectorServer;


///Inspects database using web interface
/** It is hard to inspect running database because database is locked, and cannot be openeded by other process. So if you
 * need to inspect dataabase, you need to install service to access database through the simple web interface.
 *
 * This class creates a web interface, however it doesn't offer web service as default. You need to plug the instance to an
 * existing web interface. If the application doesn't have any web interface, you can activate micro web service on a non-standard
 * port on IPv4. The micro web service offer only HTTP/1.0 interface.
 */
class Inspector {
public:

	///Initialize inspector, specify database
	Inspector(DB db);
	~Inspector();


	///Interface for network output. Must be implemented by a web server
	class IOutput {
	public:

		///Starts response
		/**
		 * @param status status code
		 * @param contentType content type of the body
		 *
		 * Called before response is generated
		 */
		virtual void begin(int status, std::string_view contentType) = 0;
		///Sends part of the body
		/** Body is send as stream of data blocks.
		 *
		 * @param data data to send
		 */
		virtual void send(const std::string_view &data) = 0;
		///Generates a error response
		/**
		 * @param status status code
		 * @param data description (can be put into body)
		 */
		virtual void error(int status, const std::string_view &data) = 0;
		virtual ~IOutput() {}

	};


	///process request
	/**
	 * @param method method (GET, PUT, POST, DELETE) - currently only GET is supported, but this can be changed in future version
	 * @param path uri, path without query (so strip anything after ?)
	 * @param query parsed query as JSON object ("key":"value")
	 * @param body content of body for PUT/POST method. Inspector always expects body in the format application/json
	 * @param output reference to IOutput instance
	 * @retval true request processed
	 * @retval false request was not recognized (server should send 404)
	 */
	bool request(std::string_view method, std::string_view path, json::Value query, json::Value body, IOutput &&output);


	///Process request initiated by a handler from the library: SimpleServer
	/**
	 * @tparam Request Request object (HTTPRequest)
	 * @tparam QueryParser Query parser object (QueryParser)
	 * @tparam Response Response object (HTTPResponse)
	 * @param req reference to request instance
	 * @param qp reference to instance of QueryParser
	 * @retval true request processed
	 * @retval false request was not recognized (server should send 404)
	 * @note return value can be passed as return value of the handler
	 */
	template<typename Request, typename QueryParser, typename Response>
	bool simpleServerRequest(Request &req, const QueryParser &qp);

	///Process the request initiated by a handler from the library: userver
	/**
	 *
	 * @tparam Request request object (PHttpServerRequest)
	 * @tparam QueryParser query parser object (QueryParser)
	 * @param req reference to instance of the request
	 * @param qp reference to query parser
	 * @retval true request processed
	 * @retval false request was not recognized (server should send 404)
	 * @note return value can be passed as return value of the handler
	 */
	template<typename Request, typename QueryParser>
	bool userverRequest(Request &req, const QueryParser &qp);

	///Starts a micro web service on specified port
	/**
	 * @param port port number
	 * @param localhost se true to bind server to localhost, set false to bind to all network interfaces
	 *
	 * If you need to stop this web service, just destroy the inspector's instance
	 */
	void startServer(int port, bool localhost = true);


	static std::string decodeUrlEncode(std::string_view input);



protected:
	DB db;
	std::shared_ptr<InspectorServer> server;

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
			req->setStatus(status);
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

namespace helper {

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

template<typename T, typename Fn>
T trim(const T &what, Fn &&fn) {
	auto end = what.length();
	auto start = end - end;
	while (start < end && fn(what[start])) {
		start++;
	}
	while (start < end && fn(what[end-1])) {
		end--;
	}
	return what.substr(start, end - start);
}


}


} /* namespace docdb */

#endif /* SRC_DOCDB_SRC_DOCDBLIB_INSPECTOR_H_ */
