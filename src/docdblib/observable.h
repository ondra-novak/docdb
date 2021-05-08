/*
 * observable.h
 *
 *  Created on: 5. 1. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_OBSERVABLE_H_
#define SRC_DOCDBLIB_OBSERVABLE_H_
#include <imtjson/refcnt.h>
#include <algorithm>
#include <memory>
#include <vector>
#include <mutex>

namespace docdb {

class AbstractObservable: public json::RefCntObj {
public:
	using Handle = std::size_t;

	virtual void clear() = 0;
	virtual void removeObserver(Handle h) = 0;
	virtual bool empty() const = 0;

	virtual ~AbstractObservable() {};
};

using PAbstractObservable = json::RefCntPtr<AbstractObservable>;

typedef PAbstractObservable (*ObservableFactory)();

template<typename ... Args>
class Observable: public AbstractObservable {
public:


	template<typename Fn>
	auto addObserver(Fn &&fn) -> decltype(std::is_invocable<Handle, bool(Args...)>::value);
	virtual void removeObserver(Handle h) override;
	template<typename ... XArgs>
	void broadcast(XArgs && ... args);
	virtual bool empty() const override {
		std::lock_guard _(lock);
		return !list.empty();}
	virtual void clear() override {
		std::lock_guard _(lock);
		list.clear();
	}

	static ObservableFactory getFactory() {
		return []() -> PAbstractObservable{
			return PAbstractObservable(new Observable);
		};
	}

protected:

	mutable std::mutex lock;
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
using PObservable = json::RefCntPtr<Observable<Args...> >;

template<typename ... Args>
template<typename Fn>
inline auto Observable<Args ... >::addObserver(Fn &&fn)-> decltype(std::is_invocable<Handle, bool(Args...)>::value) {
	std::lock_guard _(lock);

	Handle h = nxt++;
	class Obs: public Observer {
	public:
		Obs(Fn &&fn):fn(std::forward<Fn>(fn)) {}
		virtual bool exec(Args  ... args) noexcept override {
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
	std::lock_guard _(lock);

	auto iter = std::remove_if(list.begin(), list.end(), [&](const auto &itm){
		return itm.first == h;
	});
	list.erase(iter, list.end());
}

template<typename ... Args>
template<typename ... XArgs>
inline void Observable<Args ...>::broadcast(XArgs &&... args) {
	std::lock_guard _(lock);

	auto iter = std::remove_if(list.begin(), list.end(), [&](const auto &itm){
		return !itm.second->exec(std::forward<XArgs>(args)...);
	});
	list.erase(iter, list.end());
}

}

#endif /* SRC_DOCDBLIB_OBSERVABLE_H_ */

