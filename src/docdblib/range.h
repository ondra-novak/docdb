/*
 * range.h
 *
 *  Created on: 10. 8. 2019
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_RANGE_H_
#define SRC_DOCDBLIB_RANGE_H_

#include <string>
#include <string_view>

namespace docdb {

template<typename Key, typename Value>
class IteratorConcept {
public:

	///Returns true, if iterator is valid
	bool valid() const;
	///Returns current key
	const Key &getKey() const;
	///Returns current value
	const Value &getValue() const;
	///resets to begin
	void reset();
	///moves to next )
	void next();
};

template<typename Key, typename Value>
class UnderlyingIteratorConcept {
public:

	///Returns true, if iterator is valid
	bool valid() const;
	///Returns current key
	const Key &getKey() const;
	///Returns current value
	const Value &getValue() const;
	///resets to begin
	void seek(const Key &key);

	void seek_begin();

	void seek_end();

	void next();

	void prev();
};


template<typename UIter, bool backward>
class RangePrefix {
public:

	RangePrefix(std::string &&prefix):prefix(std::move(prefix)) {reset();}

	bool valid() {
		return uiter.valid() && uter.getKey().substr(0,prefix.length()) == prefix;
	}
	const std::string_view &getKey() const {return uiter.getKey();}
	const std::string_view &getValue() const {return uiter.getValue();}
	void reset() {
		if constexpr (backward) {
			if (prefix.empty()) {
				uiter.seek_end();
			}
			else {
				std::string nx = prefix;
				if (inc_key(nx)) {
					uiter.seek(nx);
					uiter.prev();
				} else {
					uiter.seek_end();
				}
			}
		} else {
			if (prefix.empty()) {
				uiter.seek_begin();
			} else {
				uiter.seek(prefix);
			}
		}
	}
	void next() {
		if constexpr (backward) {
			uiter.prev();
		} else {
			uiter.next();
		}
	}

	static bool inc_key(std::string &key) {
		if (key.empty()) return false;
		unsigned char c = key.pop_back();
		c++;
		if (c == 0) {
			return inc_key(key);
		}
		else
		{
			key.push_back(c);
			return true;

		}
	}

protected:
	std::string prefix;

	UIter &uiter;
};




}



#endif /* SRC_DOCDBLIB_RANGE_H_ */
