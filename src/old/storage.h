/*
 * storage.h
 *
 *  Created on: 8. 11. 2022
 *      Author: ondra
 */

#ifndef SRC_DOCDB_STORAGE_H_
#define SRC_DOCDB_STORAGE_H_

#include "table_handle.h"

#include "iterator.h"

#include "table.h"

#include <atomic>

namespace docdb {


///Simple storage where content is stored under automatick ID
/** Everytime item is stored, new ID is assigned. You can 
 * also delete items by id, or scan items
 * 
 * Table can be observed and connected with an index
 * 
 * @tparam IdType type of ID object
 */
template<typename IdType = std::uint64_t>
class Storage : public Table{
public:
    
    ///Construct from database handle
    Storage(PTableHandle th);
    
    ///Create snapshot
    Storage snapshot() const;
    
    
    ///Store data
    /**
     * @param data data to store
     * @return id of stored data
     */
    IdType store(const std::string_view &data);
    
    ///Store data put to batch
    /**
     * @param tx open transaction (batch)
     * @param data data
     * @return id of stored data
     * 
     * By reverting transaction doesn't release generated id 
     */
    IdType store(Batch tx, const std::string_view &data);
    
    ///Erase item
    void erase(IdType id);
    
    ///Erase item put change to batch
    void erase(Batch tx, IdType id);

    ///Find item by id
    /**
     * @param id id
     * @param data found ata
     * @retval true
     * @retval false not found
     */
    bool get(IdType id, std::string &data) const;
    
    ///Iterator
    class Iterator : public IteratorRaw {
    public:
        using IteratorRaw::IteratorRaw;
        
        IdType id() const {            
            auto k = IteratorRaw::key();
            IdType id;
            k.extract_untagged(id);
            return id;
        }
        
    };
     
    ///Scan from id to id
    /**
     * @param from from id
     * @param to to id
     * @param include_begin include begin (default true)
     * @param include_end include end (default false)
     * @return iterator
     */
    Iterator scan(IdType from, IdType to, bool include_begin = true, bool include_end = false) const;
    
    ///Scan from id in specified direction
    /**
     * @param from starting id
     * @param backward set true to scan backward
     * @param include_begin include specified id to result
     * @return iterator
     */
    Iterator scan_from(IdType from, bool backward = false, bool include_begin = true) const;
    
    ///Retrieve last generated id
    IdType get_last_id() const;
    
    
    
    
protected:
    std::atomic<IdType> _nextid = 0;


};



template<typename IdType>
inline Storage<IdType>::Storage(PTableHandle th)
:Table(th)
{
    Iterator iter(_th->read_access().iterate(),Key(_ks+1), Key(_ks),false,false);
    if (iter.next()) {
        _nextid = iter.id();
    } 
}

template<typename IdType>
inline IdType Storage<IdType>::store(Batch tx, const std::string_view &data) {
    Key k(_ks);
    IdType index = ++_nextid; 
    k.set_untagged(index);
    tx.put(k, ValueView(data));
    _th->update(tx, k, &data, nullptr);        
    return index;
}


template<typename IdType>
inline Storage<IdType> Storage<IdType>::snapshot() const {
    return Storage(_th->snapshot());
}

template<typename IdType>
inline IdType Storage<IdType>::store(const std::string_view &data) {
    WriteTx _(*this);
    return store(_,data);       
}

template<typename IdType>
inline bool Storage<IdType>::get(IdType id, std::string &data) const {
    Key k(_ks);
    k.set_untagged(id);
    return _th->read_access().find(k, data);
}

template<typename IdType>
inline typename Storage<IdType>::Iterator Storage<IdType>::scan(
        IdType from, IdType to, bool include_begin, bool include_end) const {
    
    Key kfrom(_ks);
    Key kto(_ks);
    kfrom.set_untagged(from);
    kto.set_untagged(to);
    Iterator iter(_th->read_access().iterate(), kfrom, kto, include_begin, include_end);
    return iter;
}

template<typename IdType>
inline typename Storage<IdType>::Iterator Storage<IdType>::scan_from(
        IdType from, bool backward, bool include_begin) const {
    Key kfrom(_ks);
    Key kto(backward?_ks:_ks+1);
    kfrom.set_untagged(from);
    Iterator iter(_th->read_access().iterate(), kfrom, kto, include_begin, false);
    return iter;
}

template<typename IdType>
inline void Storage<IdType>::erase(IdType id) {
    WriteTx _(*this);
    erase(_,id);       
}

template<typename IdType>
inline void Storage<IdType>::erase(Batch tx, IdType id) {
    Key k(_ks);
    k.set_untagged(id);
    tx.erase(k);
    _th->update(tx, k, nullptr, nullptr);
}

template<typename IdType>
inline IdType Storage<IdType>::get_last_id() const {
    return _nextid;
}

}
#endif /* SRC_DOCDB_STORAGE_H_ */
