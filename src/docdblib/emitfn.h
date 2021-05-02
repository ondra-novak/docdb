/*
 * emitfn.h
 *
 *  Created on: 2. 5. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDB_SRC_DOCDBLIB_EMITFN_H_
#define SRC_DOCDB_SRC_DOCDBLIB_EMITFN_H_


namespace docdb {

class EmitFn {
public:
	virtual ~EmitFn() {};
	virtual void operator()(const json::Value &key, const json::Value &value)  = 0;
};

using IndexFn = std::function<void(const Document &,  EmitFn &)>;


}


#endif /* SRC_DOCDB_SRC_DOCDBLIB_EMITFN_H_ */
