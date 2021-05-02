/*
 * observable.h
 *
 *  Created on: 5. 1. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_OBSERVABLE_H_
#define SRC_DOCDBLIB_OBSERVABLE_H_
#include <algorithm>
#include <memory>
#include <vector>

namespace docdb {

template<typename ... Args>
class Observable {
public:

	using Handle = std::size_t;

	template<typename Fn>
	auto addObserver(Fn &&fn) -> decltype(std::is_invocable<Handle, bool(Args...)>::value);
	void removeObserver(Handle h);
	template<typename ... XArgs>
	void broadcast(XArgs && ... args);
	bool empty() const {return !list.empty();}
	void clear() {list.clear();}

protected:

	Handle nxt = 1;
	class Observer {
	public:
		virtual bool exec(Args ... args) noexcept = 0;
		virtual ~Observer() {}
	};

	using PObserver = std::unique_ptr<Observer>;

	using ObserverList = std::vector<std::pair<Handle, PObserver> >;

	ObserverList list;
};

template<typename ... Args>
template<typename Fn>
inline auto Observable<Args ... >::addObserver(Fn &&fn)-> decltype(std::is_invocable<Handle, bool(Args...)>::value) {
	Handle h = nxt++;
	class Obs: public Observer {
	public:
		Obs(Fn &&fn):fn(std::forward<Fn>(fn)) {}
		virtual bool exec(Args ... args) noexcept override {
			return  fn(std::forward<Args>(args)...);
		}
	protected:
		std::remove_reference_t<Fn> fn;
	};

	list.push_back({h,PObserver(std::make_unique<Obs>(std::forward<Fn>(fn)))});
	return h;

}

template<typename ... Args>
inline void Observable<Args ...>::removeObserver(Handle h) {
	auto iter = std::remove_if(list.begin(), list.end(), [&](const auto &itm){
		return itm.first == h;
	});
	list.erase(iter, list.end());
}

template<typename ... Args>
template<typename ... XArgs>
inline void Observable<Args ...>::broadcast(XArgs &&... args) {
	auto iter = std::remove_if(list.begin(), list.end(), [&](const auto &itm){
		return !itm.second->exec(std::forward<XArgs>(args)...);
	});
	list.erase(iter, list.end());
}

}

#endif /* SRC_DOCDBLIB_OBSERVABLE_H_ */

