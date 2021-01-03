/*
 * observer.h
 *
 *  Created on: 26. 12. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_OBSERVER_H_
#define SRC_DOCDBLIB_OBSERVER_H_
#include <memory>


namespace docdb {



template<typename Lock, typename ... Args >
class Observerable {
public:

	Observerable(Lock &lock):lock(lock) {}

	template<typename Fn> std::size_t add(Fn &&fn);
	void remove(std::size_t h);
	void fire(const Args & ... args );

protected:
	class IObserver {
	public:
		virtual bool notify(const Args& ... args ) = 0;
	};

	using PObserver = std::unique_ptr<IObserver>;

	Lock &lock;
	std::size_t handle = 1;
	std::vector<std::pair<std::size_t, PObserver> > observers;

};

template<typename Lock, typename ... Args >
template<typename Fn>
inline std::size_t Observerable<Lock, Args ... >::add(Fn &&fn) {
	class Obs: public IObserver {
	public:
		Obs(Fn &&fn):fn(std::move(fn)) {}
		virtual bool notify(const Args & ... args  ) override {
			return fn(args...);
		}
	protected:
		std::remove_reference<Fn> fn;
	};
	std::unique_lock<Lock> _(lock);
	auto idx = handle++;
	observers.emplace_back({
		idx, std::make_unique<Obs>(std::forward<Fn>(fn))
	});
	return idx;
}

template<typename Lock, typename ... Args>
inline void docdb::Observerable<Lock, Args ... >::remove(std::size_t h) {
	std::unique_lock<Lock> _(lock);
	auto iter = std::lower_bound(observers.begin(), observers.end(), {
			h,nullptr
	}, [](const auto &a, const auto &b){return a.first < b.first;});
	if (iter != observers.end() && iter->first == h) {
		observers.erase(iter);
	}
}

template<typename Lock, typename ... Args >
inline void docdb::Observerable<Lock, Args>::fire(const Args &args, ...) {

}



}


#endif /* SRC_DOCDBLIB_OBSERVER_H_ */
