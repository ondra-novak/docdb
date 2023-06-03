#pragma once
#ifndef SRC_DOCDB_JOIN_H_
#define SRC_DOCDB_JOIN_H_

#include "index_view.h"

#include <algorithm>
namespace docdb {


class DocumentSet {
public:

    template<typename _ValueDef, typename IndexBase>
    DocumentSet(typename IndexViewGen<_ValueDef, IndexBase>::RecordSet &&rc) {
        for (auto row: rc) {
            _docset.push_back(row.id);
        }
        std::sort(_docset.begin(), _docset.end());
    }


    DocumentSet operator && (const DocumentSet &other) {
        std::vector<DocID> newset;
        if (!_docset.empty() && !other._docset.empty()) {
            std::set_intersection(_docset.begin(), _docset.end(),
                    other._docset.begin(), other._docset.end(),std::back_inserter(newset));
        }
        return newset;
    }

    DocumentSet operator || (const DocumentSet &other) {
        std::vector<DocID> newset;
        std::set_union(_docset.begin(), _docset.end(),
                other._docset.begin(), other._docset.end(),std::back_inserter(newset));
        return newset;
    }

    DocumentSet operator ^ (const DocumentSet &other) {
        std::vector<DocID> newset;
        std::set_symmetric_difference(_docset.begin(), _docset.end(),
                other._docset.begin(), other._docset.end(),std::back_inserter(newset));
        return newset;
    }

    bool empty() const {return _docset.empty();}

    std::size_t count() const {return _docset.size();}

    auto begin() const {
        return _docset.begin();
    }
    auto end() const {
        return _docset.end();
    }


protected:
    DocumentSet(std::vector<DocID> &&docset):_docset(std::move(docset)) {}

    std::vector<DocID> _docset;
};


template<typename _ValueDef1, typename IndexBase1, typename _ValueDef2, typename IndexBase2>
DocumentSet operator && (const typename IndexViewGen<_ValueDef1, IndexBase1>::RecordSet &rc1,
            const typename IndexViewGen<_ValueDef2, IndexBase2>::RecordSet &rc2) {
    return DocumentSet(rc1) && DocumentSet(rc2);
}

template<typename _ValueDef1, typename IndexBase1, typename _ValueDef2, typename IndexBase2>
DocumentSet operator || (const typename IndexViewGen<_ValueDef1, IndexBase2>::RecordSet &rc1,
            const typename IndexViewGen<_ValueDef1, IndexBase2>::RecordSet &rc2) {
    return DocumentSet(rc1) || DocumentSet(rc2);
}


template<typename _ValueDef1, typename IndexBase1, typename _ValueDef2, typename IndexBase2>
DocumentSet operator ^ (const typename IndexViewGen<_ValueDef1, IndexBase2>::RecordSet &rc1,
            const typename IndexViewGen<_ValueDef1, IndexBase2>::RecordSet &rc2) {
    return DocumentSet(rc1) ^ DocumentSet(rc2);
}

}




#endif /* SRC_DOCDB_JOIN_H_ */
