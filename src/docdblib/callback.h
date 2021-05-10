/*
 * callback.h
 *
 *  Created on: 10. 5. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDB_SRC_DOCDBLIB_CALLBACK_H_
#define SRC_DOCDB_SRC_DOCDBLIB_CALLBACK_H_
#include <algorithm>
#include <exception>

using std::__exception_ptr::exception_ptr;

namespace docdb {


template<typename Fn> class ICallbackT;
template<typename Fn> class CallbackT;

template<typename Ret, typename ... Args> class ICallbackT<Ret(Args...)> {
public:
	virtual Ret invoke(Args ... args) const = 0;
	virtual void move(void *ptr) = 0;
	virtual ~ICallbackT() {}

};

template<typename Fn, typename Ret, typename ... Args> class CallbackImpl1 {
public:
	CallbackImpl1 (Fn &&fn):fn(std::forward<Fn>(fn)) {}
	virtual Ret invoke(Args ... args) const override {
		return fn(std::forward<Args>(args)...);
	}
	virtual void move(void *ptr) override {
		new(ptr)CallbackImpl1(std::move(fn));
	}

	~CallbackImpl1() {

	}
protected:
	mutable std::remove_reference_t<Fn> fn;
};

template<typename Fn, typename DestructCallback, typename Ret, typename ... Args> class CallbackImpl2 {
public:
	CallbackImpl2(Fn &&fn, DestructCallback &&cfn, std::size_t callCount = 0):fn(std::forward<Fn>(fn)),cfn(std::forward<DestructCallback>(cfn)),callCount(callCount) {}
	virtual Ret invoke(Args ... args) const override {
		callCount++;
		return fn(std::forward<Args>(args)...);
	}
	~CallbackImpl2() {
			try {
				cfn(callCount.load());
			} catch (...) {

			}
	}
	virtual void move(void *ptr) override {
		new(ptr)CallbackImpl2(std::move(fn), std::move(cfn), callCount.load());
	}

protected:
	mutable std::remove_reference_t<Fn> fn;
	mutable std::remove_reference_t<DestructCallback> cfn;
	mutable std::atomic<std::size_t> callCount;
};



template<typename Ret, typename ... Args> class CallbackT<Ret(Args...)> {
public:
	using CBIfc = ICallbackT<Ret(Args...)>;


	template<typename Fn, typename = decltype(std::declval<Fn>()(std::declval<Args>()...))>
	CallbackT(Fn &&fn) {
		ptr = std::make_unique<CallbackImpl1<Fn, Ret, Args...> >(std::forward<Fn>(fn));
	}

	///Creates callback function which allows to defined reaction for situation when  callback is not called
	/**
	 * @param fn function to call
	 * @param cfn function called when callback is destroyed without calling
	 */
	template<typename Fn, typename DestructCallback,
				typename = decltype(std::declval<Fn>()(std::declval<Args>()...)),
				typename = decltype(std::declval<DestructCallback>()(std::declval<std::size_t>()))>
	CallbackT(Fn &&fn, DestructCallback &&cfn) {
		ptr = std::make_unique<CallbackImpl2<Fn, DestructCallback, Ret, Args...> >(
				std::forward<Fn>(fn), std::forward<DestructCallback>(cfn) );
	}

	CallbackT():ptr(nullptr) {}
	CallbackT(std::nullptr_t):ptr(nullptr) {}
	bool operator==(std::nullptr_t) const {return ptr == nullptr;}
	bool operator!=(std::nullptr_t) const {return ptr != nullptr;}
	CallbackT(CallbackT &&other):ptr(std::move(other.ptr)) {}
	CallbackT &operator=(CallbackT &&other) {
		ptr = std::move(other.ptr);return *this;
	}
	Ret operator()(Args ... args) const  {
		return ptr->invoke(std::forward<Args>(args)...);
	}
	void reset() {
		ptr = nullptr;
	}

protected:
	std::unique_ptr<CBIfc> ptr;
};

template<typename> class CallbackList;

template<typename Ret, typename ... Args> class CallbackList<Ret(Args...)> {
public:

	CallbackList() {}

	~CallbackList() {
		if (buffer) {
			clear();
			operator delete(buffer);
		}
	}

	CallbackList(CallbackList &&other):buffer(other.buffer),size(other.size),capacity(other.capacity) {
		other.buffer = nullptr;
		other.capacity = 0;
		other.size = 0;
	}

	void swap(CallbackList &&other) {
		std::swap(buffer, other.buffer);
		std::swap(size, other.size);
		std::swap(capacity, other.capacity);
	}

	CallbackList &operator=(CallbackList &&other) {
		swap(other);
		return *this;
	}

	void clear() {
		std::size_t p = 0;
		unsigned char *data = reinterpret_cast<unsigned char *>(buffer);
		while (p < size) {
			auto s = reinterpret_cast<const std::size_t *>(data+p);
			p+=sizeof(std::size_t);
			auto cb = reinterpret_cast<const ICallbackT<Ret(Args...)> *>(data+p);
			p+=*s;
			cb->~ICallbackT();
		}
		size = 0;
	}

	void broadcast(Args ... args) const {
		std::size_t p = 0;
		const unsigned char *data = reinterpret_cast<const unsigned char *>(buffer);
		while (p < size) {
			auto s = reinterpret_cast<const std::size_t *>(data+p);
			p+=sizeof(std::size_t);
			auto cb = reinterpret_cast<const ICallbackT<Ret(Args...)> *>(data+p);
			p+=*s;
			cb->invoke(std::forward<Args>(args)...);
		}
	}


	template<typename Fn, typename = decltype(std::declval<Fn>()(std::declval<Args>()...))>
	void add(Fn &&fn) {
		auto objsz = sizeof(CallbackImpl1<Fn, Ret, Args...>);
		auto needsz = objsz+sizeof(std::size_t);
		if (needsz + size < capacity) {
			auto newcap = std::max(capacity * 3/2, needsz);
			resize(newcap);
		}
		unsigned char *data = reinterpret_cast<unsigned char *>(buffer);
		auto s = reinterpret_cast<std::size_t *>(data+size);
		*s = objsz;
		new(data+size+sizeof(std::size_t)) CallbackImpl1<Fn,Ret, Args...>(std::forward<Fn>(fn));
		size += needsz;
	}

	template<typename Fn, typename DestructCallback,
				typename = decltype(std::declval<Fn>()(std::declval<Args>()...)),
				typename = decltype(std::declval<DestructCallback>()(std::declval<std::size_t>()))>
	void add(Fn &&fn, DestructCallback &&cfn) {
		auto objsz = sizeof(CallbackImpl1<Fn, Ret, Args...>);
		auto needsz = objsz+sizeof(std::size_t);
		if (needsz + size < capacity) {
			auto newcap = std::max(capacity * 3/2, needsz);
			resize(newcap);
		}
		unsigned char *data = reinterpret_cast<unsigned char *>(buffer);
		auto s = reinterpret_cast<std::size_t *>(data+size);
		*s = objsz;
		new(data+size+sizeof(std::size_t)) CallbackImpl2<Fn,Ret, Args...>(std::forward<Fn>(fn), std::forward<DestructCallback>(cfn));
		size += needsz;
	}

protected:

	void resize(std::size_t newcap) {
			void *newbuff = operator new(newcap);
			std::exception_ptr exception;
			std::size_t p = 0;
			std::size_t q = 0;
			unsigned char *data = reinterpret_cast<unsigned char *>(buffer);
			unsigned char *new_data = reinterpret_cast<unsigned char *>(newbuff);
			while (p < size) {
				auto s = reinterpret_cast<const std::size_t *>(data+p);
				p+=sizeof(std::size_t);
				auto cb = reinterpret_cast<ICallbackT<Ret(Args...)> *>(data+p);
				p+=*s;
				auto ns = reinterpret_cast<std::size_t *>(new_data+q);
				*ns = *s;
				try {
					cb->move(new_data+q+sizeof(std::size_t));
					q+=sizeof(std::size_t)+*s;
				} catch (...) {
					exception = std::current_exception();
				}
				cb->~ICallbackT();
			}
			capacity = newcap;
			size = q;
			operator delete(buffer);
			buffer = newbuff;
			if (exception) std::rethrow_exception(exception);
	}


	void *buffer = nullptr;
	std::size_t size = 0;
	std::size_t capacity = 0;


};
}




#endif /* SRC_DOCDB_SRC_DOCDBLIB_CALLBACK_H_ */
