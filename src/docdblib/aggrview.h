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


protected:
	AbstractUpdatableView &viewMap;
	std::string name;
	unsigned int groupLevel;
	std::atomic<std::size_t> updated_rev, last_update_rev;
	std::mutex lock;
	KeyUpdateObservableImpl keylistener;
	DocDB::ReduceIndexKey keytmp;
	std::string tmpval;

	///Calculate aggregated value
	/**
	 * @param iter contains iterator of results which will be aggregated
	 * @return aggregated value. The function can return undefined, which causes, that key will be deleted
	 *
	 * @note function is not called for empty range. In this case, key is deleted without calling
	 * the reduce function
	 */
	virtual json::Value calc(ViewIterator &iter) const = 0;

	///called when a key is updated
	virtual void updatedKey(WriteBatch &batch, const json::Value &key, bool deleted);


	///Specifies transformation from the source view to target view
	struct KeyTransform {
		enum Command {
			//scan entire DB (reduce all)
			scan = 0,
			//scan prefix
			scanPrefix = 1,
			//find exact key
			find = 2
		};

		Command cmd;
		///key to find/scan - internally, if this field is undefined, it assumess keyScan=keyResult
		json::Value keyScan;
		///result key - note keyResult must be unique.
		json::Value keyResult;
	};

	///Maps the key from source key. Allows to change way how data are aggregated over the source view
	virtual KeyTransform mapKey(const json::Value &key) const;

	///internal function
	void updateRow(const KeyTransform &trns, ViewID &itmk, std::string &value, WriteBatch &b);

	///internal function
	ViewIterator scanSource(const KeyTransform &trns) const;
};

///Implements single aggregate view with externally defined function
class AggregateView: public AbstractAggregateView {
public:
	typedef json::Value (*AggrFn)(ViewIterator &iter);
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
protected:
	AggrFn aggrFn;
	virtual json::Value calc(ViewIterator &iter) const override;

};

///Implements advanced aggregation with key mapping feature
/** This allows to specify custom mapping function. More information is specified on
 * struct KeyTransform. You can aggregate over substring, or rename source key to
 * something different. The key mapping allows to specify search command, search range and
 * the final key.
 */
class AggregateViewWithKeyMap: public AggregateView {
public:
	typedef KeyTransform (* MapKeyFn)(const json::Value &key);
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
	virtual KeyTransform mapKey(const json::Value &key) const override;
};

///count unique keys
class AggregateCount: public AbstractAggregateView {
public:
	using AbstractAggregateView::AbstractAggregateView;
	virtual json::Value calc(ViewIterator &iter) const override;
};

///sum values (double)
/** Slightly faster than AggregateReduce with operation sum, because all calculations are done above float numbers */
class AggregateSum: public AbstractAggregateView {
public:
	using AbstractAggregateView::AbstractAggregateView;
	virtual json::Value calc(ViewIterator &iter) const override;
};
///sum values (integer)
/** Slightly faster than AggregateReduce with operation sum, because all calculations are done above integer numbers */
class AggregateIntegerSum: public AbstractAggregateView {
public:
	using AbstractAggregateView::AbstractAggregateView;
	virtual json::Value calc(ViewIterator &iter) const override;
};


class AggregateReduce: public AbstractAggregateView {
public:
	typedef json::Value (*Operation)(const json::Value &a, const json::Value &b, unsigned int index);
	AggregateReduce(AbstractUpdatableView &viewMap, const std::string &name, unsigned int groupLevel, Operation op);
	virtual json::Value calc(ViewIterator &iter) const override;

	static json::Value sum(const json::Value &a, json::Value &b, unsigned int index);
	static json::Value max(const json::Value &a, json::Value &b, unsigned int index);
	static json::Value min(const json::Value &a, json::Value &b, unsigned int index);

protected:
	Operation op;

};

}



#endif /* SRC_DOCDB_SRC_DOCDBLIB_AGGRVIEW_H_ */
