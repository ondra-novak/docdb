/*
 * kvstorage.h
 *
 *  Created on: 25. 7. 2019
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_KVSTORAGE_H_
#define SRC_DOCDBLIB_KVSTORAGE_H_

#include <memory>
#include <string_view>

namespace KeyValue {

class Base {
public:
	static const std::string_view end_position;


};

struct Item {
	std::string_view key;
	std::string_view value;
};


class IIterator: public Base {
public:


	virtual std::string_view getKey() const = 0;
	virtual std::string_view getValue() const = 0;

};


template<bool reversed>
class Iterator_Bi: public Base {
public:

	using difference_type =	std::ptrdiff_t;
	using value_type = Item;
	using pointer = void;
	using reference = const Item &;
	using iterator_category = std::bidirectional_iterator_tag;


	using PIter = std::shared_ptr<IIterator>;

	Iterator_Bi(PIter iter, const std::string_view &position)
			:iter(iter)
			,position(position) {}


	const Item &operator *() const {
		itm.value = iter->getValue();
		return itm;
	}

	Iterator_Bi &operator++() {
		itm.key = reversed?iter->prev():iter->next();
		return *this;
	}
	Iterator_Bi &operator--() {
		itm.key = reversed?iter->next():iter->prev();
		return *this;
	}

	int compare(const Iterator &other) const {
		bool e1 = is_end();
		bool e2 = other.is_end();
		int r;

		if (itm.key.data() == end_position.data()) {
			if (other.key.data() == end_position.data()) {
				r = 0;
			} else {
				r = 1;
			}
		} else if (other.key.data() == end_position.data()) {
			r = -1;
		} else {
			r = itm.key.compare(other.itm.key);
		}
		return reversed?-r:r;
	}

	bool operator==(const Iterator &other) const {return compare(other) == 0;}
	bool operator>=(const Iterator &other) const {return compare(other) >= 0;}
	bool operator<=(const Iterator &other) const {return compare(other) <= 0;}
	bool operator!=(const Iterator &other) const {return compare(other) != 0;}
	bool operator>(const Iterator &other) const {return compare(other) > 0;}
	bool operator<(const Iterator &other) const {return compare(other) < 0;}

	bool is_end() const {
		return itm.key.data() == end_position.data();
	}

protected:
	PIter iter;
	mutable Item itm;

};

class ISnapshot: public Base {
public:

	virtual bool exists(const std::string_view &key) = 0;
	virtual bool get(const std::string_view &key, std::string &value) = 0;




};



}
#endif /* SRC_DOCDBLIB_KVSTORAGE_H_ */
