#pragma once
#ifndef SRC_DOCDB_EXCEPTIONS_H_
#define SRC_DOCDB_EXCEPTIONS_H_

#include "types.h"

#include <leveldb/status.h>
#include <exception>


namespace docdb {

class exception: public std::exception {
public:
};



class ReferencedDocumentNotFoundException : public exception {
public:
    ReferencedDocumentNotFoundException(DocID id)
        :_id(id), _msg("Referenced document not found: ") {
        _msg.append(std::to_string(id));
    }
    DocID get_id() const {return _id;}
    const char *what() const noexcept override {return _msg.c_str();}
protected:
    DocID _id;
    std::string _msg;
};

class RecordNotFound: public exception {
public:
    virtual const char *what() const noexcept override {
        return "Record not found. (Access to an empty result of the operation 'find')";
    }
};

class DatabaseError: public exception {
public:

    DatabaseError(leveldb::Status st)
    :_st(std::move(st)),_msg(_st.ToString()) {}

    const leveldb::Status &get_status() {return _st;}
    const char *what() const noexcept override {return _msg.c_str();}
protected:
    leveldb::Status _st;
    std::string _msg;

};

class DuplicateKeyException: public exception {
public:
    DuplicateKeyException(std::string key, std::string message)
        :_key(std::move(key)), _message(std::move(message)) {}

    const std::string &get_key() const {return _key;}
    const char *what() const noexcept override {return _message.c_str();}
protected:
    std::string _key;
    std::string _message;

};

class DeadlockKeyException: public exception {
public:
    DeadlockKeyException(std::string key, std::string message)
        :_key(std::move(key)), _message(std::move(message)) {}

    const std::string &get_key() const {return _key;}
    const char *what() const noexcept override {return _message.c_str();}
protected:
    std::string _key;
    std::string _message;

};



}




#endif /* SRC_DOCDB_EXCEPTIONS_H_ */
