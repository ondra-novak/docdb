/*
 * inspector_server.cpp
 *
 *  Created on: 1. 4. 2021
 *      Author: ondra
 */

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <imtjson/object.h>
#include <unistd.h>
#include <sstream>
#include <map>
#include <mutex>
#include <thread>
#include "inspector.h"

namespace docdb {

class InspectorServer {
public:

	InspectorServer(int port, bool localHost, Inspector &owner);
	~InspectorServer();

	void handle_conn(int c);
	void handle_conn2(int c);


protected:
	int mother_socket;
	std::thread listenThread;
	std::map<int, std::thread> openThreads;
	std::mutex lock;
	bool exit_now;
	Inspector &owner;

};


InspectorServer::InspectorServer(int port, bool localHost, Inspector &owner)
:owner(owner)
{

	mother_socket = socket(AF_INET, SOCK_STREAM|SOCK_CLOEXEC, IPPROTO_TCP);
	int flag = 1;
	::setsockopt(mother_socket, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char *>(&flag), sizeof(int));
	::setsockopt(mother_socket,IPPROTO_TCP,TCP_NODELAY,reinterpret_cast<char *>(&flag),sizeof(int));


	sockaddr_in sin = {};
	if (localHost) sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK); else sin.sin_addr.s_addr = INADDR_ANY;
	sin.sin_port = htons(port);
	sin.sin_family = AF_INET;
	if (::bind(mother_socket, reinterpret_cast<sockaddr *>(&sin), sizeof(sin))) {
		int e = errno;
		close(mother_socket);
		throw std::system_error(e, std::generic_category(),"bind failed");
	}
	if (::listen(mother_socket, SOMAXCONN)) {
		int e = errno;
		close(mother_socket);
		throw std::system_error(e, std::generic_category(),"listen failed");
	}
	exit_now = false;


	listenThread = std::thread([this]{
		while (!exit_now) {
			int c = accept4(mother_socket, nullptr, nullptr, SOCK_CLOEXEC);
			if (c < 0) {
				if (exit_now) break;
				std::this_thread::sleep_for(std::chrono::seconds(1));
			} else {
				std::lock_guard _(lock);
				std::thread th([c,this]{
					handle_conn(c);
				});
				openThreads.emplace(c,std::move(th));
			}
		}
	});
}

inline InspectorServer::~InspectorServer() {
	exit_now = true;
	shutdown(mother_socket,SHUT_RDWR);
	listenThread.join();
	std::vector<std::thread> waitlist;
	{
		std::lock_guard _(lock);
		for (auto &x: openThreads) waitlist.push_back(std::move(x.second));
	}
	for (auto &x: waitlist) x.join();
}

inline void InspectorServer::handle_conn(int c) {
	try {

		handle_conn2(c);


	} catch (...) {

	}
	std::lock_guard _(lock);
	::close(c);
	auto &me = openThreads[c];
	if (me.joinable())
		me.detach();
	openThreads.erase(c);
}

static void read_hdr_to_str(int sock, std::string &buff) {
	bool rep = true;
	while (rep) {
		auto ofs = buff.size();
		buff.resize(ofs+65536);
		int res =recv(sock, buff.data()+ofs, 65536, 0);
		if (res < 0) {
			int e = errno;
			throw std::system_error(e, std::generic_category(),"recv");
		}
		else if (res == 0) {
			throw std::runtime_error("Connection lost");
		}
		buff.resize(ofs+res);
		auto pos = buff.find("\r\n\r\n");
		rep = pos == buff.npos;
	}
}

static bool ucaseCompare(const std::string_view &a, const std::string_view &b) {
	auto la = a.length();
	auto lb = b.length();
	if (la != lb) return false;
	for (auto i = la-lb; i < la; i++) {
		if (toupper(a[i]) != toupper(b[i])) return false;
	}
	return true;
}

static const std::string_view crlf("\r\n");

inline void InspectorServer::handle_conn2(int c) {
	std::string buff;
	read_hdr_to_str(c, buff);
	std::string_view hdr(buff);
	auto fline = helper::splitAt(crlf, hdr);
	unsigned long body_len = 0;
	do {
		auto line = helper::splitAt(crlf, hdr);
		if (line.empty()) break;
		auto key = helper::trim(helper::splitAt(":", line),isspace);
		auto value = line;
		if (ucaseCompare(key, "Content-Length")) {
			body_len = std::strtoul(value.data(), nullptr, 10);
		}
	} while (true);

	auto method = helper::splitAt(" ", fline);
	auto uri = helper::splitAt(" ", fline);
	json::Value jsonBody;
	if (body_len) {
		std::string body (hdr);
		while (body.length() < body_len) {
			auto ofs = body.length();
			body.resize(body_len);
			int i = recv(c, body.data()+ofs, body.size()-ofs, 0);
			if (i < 0) {
				int e = errno;
				throw std::system_error(e, std::generic_category(), "recv");
			}
			if (i == 0) {
				throw std::runtime_error("Incomplete body");
			}
			body.resize(ofs+i);
		}
		jsonBody = json::Value::fromString(body);
	}

	class Output: public Inspector::IOutput {
	public:
		Output(int c):c(c) {}
		virtual void begin(int status, std::string_view contentType) {
			std::ostringstream hdr;
			hdr << "HTTP/1.0 " << status << " " << (status==200?"OK":"Error") << "\r\n"
			    << "Connection: close\r\n"
				<< "Content-Type: " << contentType << "\r\n"
				<< "\r\n";
			std::string s = hdr.str();
			send(s);
		}
		virtual void send(const std::string_view &data) {
			std::size_t pos = 0;
			while (pos < data.length()) {
				int r = ::send(c, data.data()+pos, data.length()-pos, MSG_NOSIGNAL);
				if (r < 1) {
					int e = errno;
					throw std::system_error(e, std::generic_category(), "send");
				}
				pos = pos + r;
			}
		}
		virtual void error(int status, const std::string_view &data) {
			begin(status, "text/plain");
			send(data);
		}
	protected:
		int c;
	};

	auto path = helper::splitAt("?", uri);
	json::Object query;
	while (!uri.empty()) {
		auto pair = helper::splitAt("&", uri);
		if (!pair.empty()) {
			auto key = helper::splitAt("=", pair);
			auto value = pair;
			auto dkey = Inspector::decodeUrlEncode(key);
			auto dvalue = Inspector::decodeUrlEncode(value);
			query.set(dkey, dvalue);
		}
	}

	owner.request(method, path, query, jsonBody, Output(c));
}

void Inspector::startServer(int port, bool localhost) {
	server = std::make_shared<InspectorServer>(port,localhost, *this);
}



}

