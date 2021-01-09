/*
 * classes.h
 *
 *  Created on: 27. 12. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_CLASSES_H_
#define SRC_DOCDBLIB_CLASSES_H_

namespace docdb {

enum class KeySpaceClass {
	incremental_store=1,
	document_index=2,
	graveyard_index=3,
	view=4,
	filterView=5,
	jsonmap_view=6
};

}



#endif /* SRC_DOCDBLIB_CLASSES_H_ */
