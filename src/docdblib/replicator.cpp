/*
 * replicator.cpp
 *
 *  Created on: 8. 4. 2021
 *      Author: ondra
 */

#include  "replicator.h"

namespace docdb {

void Replicator::send(const std::string_view &tablename, const DocumentRepl &doc, std::function<void(const replication::TransferInfo&)>&&) {
}

void Replicator::release() {
}


void Replicator::sendDoc(std::shared_ptr<TargetLock> tl, const std::string &tablename, const DocumentRepl doc, const AttachmentView *att, const PendingLockGuard &gr) {
	tl->trg.send(tablename, doc, [tl, gr, att, docId = doc.id](const replication::TransferInfo &ti){
		if (ti.state == replication::TransferState::attachment) {
			AttachmentView::Download dwn = att->open(docId, ti.attachName);
			auto rd = dwn.read();
			while (!rd.empty()) {
				ti.stream(rd);
				rd = dwn.read();
			}
			ti.stream(std::string_view());
		}
	});
}
void Replicator::replicate(const replication::ReplicationRequest &req, replication::ITarget &target) {
	try {
		auto tl = std::make_shared<TargetLock>(target);
		RoDef def = findRO(req.tablename);
		auto iter = def.table->scanChanges(req.since);
		while (iter.next()) {
			DocumentRepl doc = iter.replicate_get();
			if (def.filter == nullptr || def.filter(doc)) {
				tl->trg.got(req.target_table, doc.id, doc.revisions[0].getUIntLong(),
						[tl, doc, grd = PendingLockGuard(*this), target = std::string(req.target_table), att = def.attachments](bool got){
					if (!got) {
						sendDoc(tl, target, doc, att, grd);
					}
				});
			}
		}


	} catch (...) {
		target.error(req.tablename);

	}

}

void Replicator::checkpoint(const std::string_view &tablename, const SeqID &id) {
}

void Replicator::got(const std::string_view &tablename, const std::string_view &docid, DocRevision revId, std::function<void(bool)> &&response) {
}

void Replicator::ping(std::function<void(bool)> &&function) {
}

}
