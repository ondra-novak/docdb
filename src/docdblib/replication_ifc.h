/*
 * replication_ifc.h
 *
 *  Created on: 8. 4. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_REPLICATION_IFC_H_
#define SRC_DOCDBLIB_REPLICATION_IFC_H_

#include <string_view>
#include <memory>
#include "document.h"

namespace docdb {

namespace replication {

//do_you_have->yes/no->replicate->ok/conflict/attachments->send_attachment->ok/error->commit_attachment->ok/conflict






struct ReplicationRequest {
	///table name
	std::string_view tablename;
	///table name
	std::string_view target_table;
	///starting sequence id
	SeqID since;
	///set true, to allow continuous replication
	/** when true, the replication will be terminated by reaching final sequence id, It continues monitoring the database and transfers all changes made after.
	 *
	 * Monitoring can be stopped by other side by reporting error as response for send()
	 *
	 * */
	bool continuous;
};

///Specifies transfer state

enum class TransferState {
	///transfered - final state
	ok,
	///unsolvable conflict - final state
	/**If conflict happens, it must be solved on target side. The revision history must include both conflicted revisions. If conflict cannot be solved
	 * on the target, the document is rejected. Note that conflict can happen before attachments are transfered and after attachmants are transfered as well, because
	 * durng the transfer, target document can be updated as well
	 */
	conflict,
	///other error - final state
	error,
	///attachment required - intermediate state
	attachment,
	///stop replication
	/** non-error state, when other side don't want to continue in replication. This should stop replication and release target */
	stop

};

///Describes stransfer state
struct TransferInfo {
	///current transfer state
	TransferState state;
	///table name (of the document)
	std::string tableName;
	///document id
	std::string docid;
	///attachment id (for state attachment)
	std::string attachName;
	/** function to call to stream attachment
	 * Function can be called repeatedly, until whole attachment is transfered. To finalize transfer, simply destroy the stream. After
	 * attachment is transfered, the TransferInfo is updated through the same callback function
	 */
	std::function<void(std::string_view)> stream;
};

///Specifies replication target
/** It can be connected with database or with external connection. If you have two targets, you can initiate replication by calling replicate on one
 * interface and passing reference to other
 *
 * If the target is connection, operation is same, but other side must be able to search for the database and pass the request to their ITarget object
 *
 * The interface is simple, to be easy translated to connection stream, and asynchronous to avoid blocking calls and waiting. All responses are routed through callbacks
 */
class ITarget {
public:

	virtual ~ITarget() {}
	///initiate replication to the specified target
	/**
	 *  @param req Replication request
	 *  @param target pointer to target (database) where data will be replicated
	 *
	 */
	virtual void replicate(const ReplicationRequest &req, ITarget &target) = 0;
	///Asks target whether it has specified document and revision
	/**
	 * Function returns status through asynchronous callback.
	 *
	 *
	 * @param tablename name of keyspace
	 * @param docid id of the document
	 * @param revId revisionID. If the revision can be find in the revision history of the document, function return true, otherwise it returns false;
	 * @param response function called when response is available, and contains true or false depend on situation.
	 */
	virtual void got(const std::string_view &tablename, const std::string_view &docid, DocRevision revId, std::function<void(bool)> &&response) = 0;
	///Sends document to the target
	/**
	 * @param tablename name of the keyspace
	 * @param doc documentID
	 * @param control callback which can be caller (even repeatedly) during transfer, see description of TransferInfo
	 */
	virtual void send(const std::string_view &tablename, const DocumentRepl &doc, std::function<void(const TransferInfo &)> &&) = 0;

	///Record checkpoint
	/**
	 * Allows to target to record checkpoint. Especially for filtered replication there can be need to store checkpoint for bunch of skipped documents. This
	 * allows to reinitiate replication from specified sequence id in the future
	 * @param name name of keyspace (docstore)
	 * @param id sequence id.
	 */
	virtual void checkpoint(const std::string_view &tablename, const SeqID &id) = 0;

	///Release interface
	/**
	 * When replication is done, function is called to release ownership of the interface. It is expected, thet ownership is acquired by the function replicate();
	 * This function is called, where there is longer need to keep ownership. Every call of replicate() ends with release()
	 */
	virtual void release() = 0;

	///Check whether target is still alive (especially when on connection)
	/**
	 * @param function called with result
	 *
	 * when true is passed, then connection is alive and all operations should continue
	 * when false is passed, then connection has been terminated, and replication should stop
	 */
	virtual void ping(std::function<void(bool)> &&) = 0;


	///report error experienced during replication of the table - this is called by replicate, when specified table cannot be replicated
	/**
	 * @param tablename table which cannot be replicated
	 *
	 * (release() must be also called after error)
	 */
	virtual void error(const std::string_view &tablename) = 0;
};




}


}



#endif /* SRC_DOCDBLIB_REPLICATION_IFC_H_ */
