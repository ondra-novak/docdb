/*
 * updatableobject.cpp
 *
 *  Created on: 1. 11. 2020
 *      Author: ondra
 */


#include "updatableobject.h"
#include "changesiterator.h"

namespace docdb {

UpdatableObject::UpdatableObject(DocDB &db)
	:db(db)
	,seqId(0)
	,updateDB(&db)
{
}


void UpdatableObject::update() {
	if (updateDB) {
		update(*updateDB);
	}
}

void UpdatableObject::update(DocDB &updateDB) {

	if (seqId < updateDB.getLastSeqID()) {
		std::lock_guard _(wrlock);
		seqId = std::max(scanUpdates(updateDB.getChanges(seqId)),seqId);
		storeState();
	}
}

void UpdatableObject::rebuild() {
	seqId = 0;
	storeState();
	update();
}

UpdatableObject::~UpdatableObject() {
}


}

