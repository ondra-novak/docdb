/*
 * replicator.h
 *
 *  Created on: 8. 4. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_REPLICATOR_H_
#define SRC_DOCDBLIB_REPLICATOR_H_
#include "attachments.h"
#include "doc_store.h"
#include "replication_ifc.h"

namespace docdb {


struct SolveConflictResult {
	///true, conflict was solved, false no solved, remaining fields are ignored
	bool solved;
	///true, if result is deleted documennt
	bool deleted;
	///contains merged result
	json::Value content;
};


struct ReplicateDefRO {
	std::string tablename;
	const DocStoreViewBase *table;
	const AttachmentView *attachments;
	std::function<bool(const DocumentRepl &)> filter;
};

struct ReplicateDef {
	std::string tablename;
	const DocStore *table;
	const Attachments *attachments;
	std::function<bool(const DocumentRepl &)> filter;
	std::function<SolveConflictResult(const DocumentRepl &local, const DocumentRepl &income)> conflictSolver;
};



class Replicator: public docdb::replication::ITarget {
public:

	Replicator(const std::initializer_list<ReplicateDefRO> &rodefs);
	Replicator(const std::initializer_list<ReplicateDef> &defs);
	Replicator(const std::initializer_list<ReplicateDefRO> &rodefs, const std::initializer_list<ReplicateDef> &defs);

	virtual void send(const std::string_view &tablename, const docdb::DocumentRepl &doc, std::function<void(const docdb::replication::TransferInfo&)>&&) override;
	virtual void release() override;
	virtual void replicate(const docdb::replication::ReplicationRequest &req, docdb::replication::ITarget &target) override;
	virtual void checkpoint(const std::string_view &tablename, const docdb::SeqID &id) override;
	virtual void got(const std::string_view &tablename, const std::string_view &docid, DocRevision revId, std::function<void(bool)> &&response) override;
	virtual void ping(std::function<void(bool)>&&) override;



	~Replicator();

protected:

	struct RoDef {
		const DocStoreViewBase *table;
		const AttachmentView *attachments;
		std::function<bool(const DocumentRepl &)> filter;
	};

	struct Def {
		const DocStore *table;
		const Attachments *attachments;
		std::function<bool(const DocumentRepl &)> filter;
		std::function<SolveConflictResult(const DocumentRepl &local, const DocumentRepl &income)> conflictSolver;
	};

	RoDef findRO(std::string_view tablename) const;

	struct PendingCnt {
		std::mutex mx;
		std::condition_variable cond;
		unsigned int lcnt = 0;
		bool stop = 0;

		void lock();
		void unlock();
		bool check();
		void wait();
	};

	PendingCnt pendingCounter;

	class TargetLock {
	public:
		ITarget &trg;
		TargetLock(ITarget &trg):trg(trg) {}
		~TargetLock() {trg.release();}
	};

	struct PendingLockGuard {
		Replicator &owner;
		PendingLockGuard(Replicator &owner):owner(owner) {owner.pendingCounter.lock();}
		PendingLockGuard(const PendingLockGuard &other):owner(other.owner) {owner.pendingCounter.lock();}
		~PendingLockGuard() {owner.pendingCounter.unlock();}
	};

	static void sendDoc(std::shared_ptr<TargetLock> tl, const std::string &tablename, const DocumentRepl doc, const AttachmentView *att, const PendingLockGuard &gr);

};



}



#endif /* SRC_DOCDBLIB_REPLICATOR_H_ */
