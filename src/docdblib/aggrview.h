/*
 * reduceview.h
 *
 *  Created on: 10. 12. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDB_SRC_DOCDBLIB_AGGRVIEW_H_
#define SRC_DOCDB_SRC_DOCDBLIB_AGGRVIEW_H_
#include "abstractview.h"

namespace docdb {

///The class defines all functions availabale during mapKey function
/** During execution of mapKey, you can specify multiple mappings. Each mapping contains
 * consists from operation, result key and some paramateres, you can also specify a value, which
 * is then passed to calc() function
 * *
 */
class KeyMapping {
public:
	enum Exclude {
		no_exclude,
		exclude_end,
	};

	virtual ~KeyMapping() {}

	///Scan entire view and store aggregated result under specified result key
	/**
	 * @param result_key a key under the aggregated result will be stored.
	 * @param value a user value passed to the calc() function as an argument
	 *
	 * @note if the result key already exists, it is replaced (including the value)
	 */
	virtual void scan(const json::Value &result_key, const json::Value &value = json::Value()) const = 0;
	///Scan for prefix
	/**
	 * Scan the source view for specified prefix. It can be an array to retrieve all keys
	 * matching the array from the beginning. Or it can be string, which scans all string
	 * keys matching the begin of the key
	 *
	 * @param result_key a key under the aggregated result will be stored
	 * @param prefix prefix to scan. Default value: result_key
	 * @param value a user value passed to the calc() function as an argument
	 *
	 * @note if the result key already exists, it is replaced (including the value)
	 *
	 */
	virtual void scanPrefix(const json::Value &result_key, const json::Value &prefix = json::Value(), const json::Value &value = json::Value()) const = 0;
	///Find specified key
	/**
	 * Finds all rows from the source view matching specified key exactly.
	 *
	 * @param result_key a key under the aggregated result will be stored
	 * @param key key to find. Default value: result_key
	 * @param value a user value passed to the calc() function as an argument
	 *
	 * @note if the result key already exists, it is replaced (including the value)
	 */
	virtual void find(const json::Value &result_key ,const json::Value &key = json::Value(), const json::Value &value = json::Value()) const = 0;
	///Scan specified range for single aggregaged result
	/**
	 * Scans a range
	 *
	 * @param result_key a key under the aggregated result will be stored
	 * @param lower_bound lower bound of the range
	 * @param upper_bound upper bound of the range
	 * @param exclude specify whether to include or exclude upper_bound of the range
	 * @param value a user value passed to the calc() function as an argument
	 */
	virtual void scanRange(const json::Value &result_key, const json::Value &lower_bound, const json::Value &upper_bound, Exclude exclude = no_exclude, const json::Value &value = json::Value()) const = 0;
};

///Aggregate view is materialized derived view, which perform agregation over keys read from original view
/**
 * The aggregation function must be implemented by derived class.
 * The aggregation function receives iterator over keys and values, which must be aggregated
 * to a single value
 *
 * To achieve correct function, you need to attach this view to a source view before the source
 * view is updated.
 *
 */
class AbstractAggregateView: public AbstractUpdatableView, public IKeyUpdateObserver {
public:
	/**
	 * @param viewMap source view
	 * @param name name of this view
	 * @param groupLevel specify group level for multivalue keys. It specified how many
	 *  values (from the begining) is kept unique. For single value key, the groupLevel above
	 *  zero perform reduce over duplicated keys. For groupLevel=0, result is one big
	 *  aggregation over the source view
	 */
	AbstractAggregateView(AbstractUpdatableView &viewMap, const std::string &name, unsigned int groupLevel = 99);
	///Destructor
	~AbstractAggregateView();

	///Perform update triggered manually
	virtual void update();
	///Complete rebuild the view
	/**Invalidates content and regerates the view from the source view. Can take a long time */
	void rebuild();



	///Add observer
	virtual void addObserver(IKeyUpdateObserver *obs) override;
	///Remove observer
	virtual void removeObserver(IKeyUpdateObserver *obs)override;

	///Searches for single key
	/** Because there can be multiple records for single key, result is iterator
	 *
	 * @param key key to scan
	 * @param backward show results in descending order
	 * @return iterator
	 */
	ViewIterator find(const json::Value &key, bool backward = false);


	///Perform fast lookup for a value
	/**
	 * @param key key to lookup
	 * @return found value. If key doesn't exists, returns undefined value. If there are multiple results, it selects only one.
	 */
	json::Value lookup(const json::Value &key);

	///Scans whole view
	ViewIterator scan();

	///Scan for range
	/**
	 * @param from from key
	 * @param to to key
	 * @return iterator
	 *
	 * @note ordering is not strictly defined. Keys based on numbers are ordered, strings
	 * are ordered using binary ordering, but other json objects can have any arbitrary order.
	 */
	ViewIterator scanRange(const json::Value &from, const json::Value &to, bool exclude_end);


	///Scans for prefix
	/**
	 * @param prefix prefix to search
	 * @param backward enumerate backward
	 * @return iterator
	 *
	 * @note Prefix scan is defined if the part of the key is exact same. This can be achieved
	 * using strings, numbers and arrays (allows to search for all items starting by a item
	 * in the array), however objects wont work
	 */
	ViewIterator scanPrefix(const json::Value &prefix, bool backward);


protected:
	AbstractUpdatableView &viewMap;
	std::string name;
	unsigned int groupLevel;
	std::mutex lock;
	KeyUpdateObservableImpl keylistener;
	DocDB::ViewIndexKey keytmp;
	std::string tmpval;

	///Calculate aggregated value
	/**
	 * @param iter contains iterator of results which will be aggregated
	 * @param value custom value passed from mapping function (may be undefined)
	 * @return aggregated value. The function can return undefined, which causes, that key will be deleted
	 *
	 * @note function is not called for empty range. In this case, key is deleted without calling
	 * the reduce function
	 */
	virtual json::Value calc(ViewIterator &iter, const json::Value &) const = 0;

	///called when a key is updated
	virtual void updatedKey(WriteBatch &batch, const json::Value &key, bool deleted);


	class Translator: public IValueTranslator {
	public:
		Translator(AbstractAggregateView &owner):owner(owner) {}
		virtual json::Value translate(const std::string_view &key, const std::string_view &value) const override;
	protected:
		AbstractAggregateView &owner;
		mutable WriteBatch b;
		mutable std::string tmp;
	};

	Translator translator;

	enum class ScanCommand {
		//scan entire DB (reduce all)
		scan = 0,
		//scan prefix
		scanPrefix = 1,
		//find exact key
		find = 2,
		//scan given range
		scanRange = 3
	};



	///Maps the key from source key. Allows to change way how data are aggregated over the source view
	virtual void mapKey(const json::Value &key, const KeyMapping &mapping) const;


	class KeyMapImpl;

	///internal function
	ViewIterator scanSource(const json::Value &args, const std::string_view &result_key) const;

	void updateRange(MapIterator &&iter);
	void updateKey(const json::Value &key, bool pfx);
};

///Implements single aggregate view with externally defined function
class AggregateView: public AbstractAggregateView {
public:
	typedef json::Value (*AggrFn)(ViewIterator &iter,const json::Value &val);
	///Construct aggregate view
	/**
	 * @param viewMap source view
	 * @param name the unique identifier of this view
	 * @param groupLevel specifies grouping level for multicolumn keys. Exactly specified
	 * count of colums which defines the new key. Rest of columns are aggregated. For single colum key, this
	 * value must be 1, otherwise, all rows are aggregated into single row (results single row view)
	 * @param aggrFn Aggregate function - function (static function, can't use lamba function with clousure)
	 * called to perform aggregation. The function returns single value. It can return undefined to delete key
	 */
	AggregateView(AbstractView &viewMap, const std::string &name, unsigned int groupLevel, AggrFn aggrFn);
	virtual void release(const IKeyUpdateObservable *) {}
protected:
	AggrFn aggrFn;
	virtual json::Value calc(ViewIterator &iter, const json::Value &) const override;

};

///Implements advanced aggregation with key mapping feature
/** This allows to specify custom mapping function. More information is specified on
 * struct KeyTransform. You can aggregate over substring, or rename source key to
 * something different. The key mapping allows to specify search command, search range and
 * the final key.
 */
class AggregateViewWithKeyMap: public AggregateView {
public:
	typedef void (* MapKeyFn)(const json::Value &key, const KeyMapping &keyMapEmit);
	///Construct aggregate view
	/**
	 * @param viewMap source view
	 * @param name the unique identifier of this view
	 * @param mapKeyFn key-mapping function (must be static function without a clousure)
	 * @param aggrFn aggregate function (must be static function without a clousure)
	 */
	AggregateViewWithKeyMap(AbstractView &viewMap, const std::string &name, MapKeyFn mapKeyFn, AggrFn aggrFn);
public:
	MapKeyFn mapKeyFn;
	virtual void mapKey(const json::Value &key, const KeyMapping &keyMapEmit) const override;
};

///count unique keys
class AggregateCount: public AbstractAggregateView {
public:
	using AbstractAggregateView::AbstractAggregateView;
	virtual json::Value calc(ViewIterator &iter, const json::Value &) const override;
};

///sum values (double)
/** Slightly faster than AggregateReduce with operation sum, because all calculations are done above float numbers */
class AggregateSum: public AbstractAggregateView {
public:
	using AbstractAggregateView::AbstractAggregateView;
	virtual json::Value calc(ViewIterator &iter, const json::Value &) const override;
};
///sum values (integer)
/** Slightly faster than AggregateReduce with operation sum, because all calculations are done above integer numbers */
class AggregateIntegerSum: public AbstractAggregateView {
public:
	using AbstractAggregateView::AbstractAggregateView;
	virtual json::Value calc(ViewIterator &iter, const json::Value &) const override;
};


class AggregateReduce: public AbstractAggregateView {
public:
	typedef json::Value (*Operation)(const json::Value &a, const json::Value &b, unsigned int index);
	AggregateReduce(AbstractUpdatableView &viewMap, const std::string &name, unsigned int groupLevel, Operation op);
	virtual json::Value calc(ViewIterator &iter, const json::Value &) const override;

	static json::Value sum(const json::Value &a, json::Value &b, unsigned int index);
	static json::Value max(const json::Value &a, json::Value &b, unsigned int index);
	static json::Value min(const json::Value &a, json::Value &b, unsigned int index);

protected:
	Operation op;

};

}



#endif /* SRC_DOCDB_SRC_DOCDBLIB_AGGRVIEW_H_ */
