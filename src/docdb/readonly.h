#ifndef SRC_DOCDB_READONLY_H_
#define SRC_DOCDB_READONLY_H_

#include <leveldb/env.h>
#include <leveldb/helpers/memenv.h>
#include <sys/file.h>
#include <fcntl.h>


namespace docdb {

class CopyOnWriteEnv: public leveldb::Env {
public:

    using Status = leveldb::Status;
    using WritableFile = leveldb::WritableFile;
    using FileLock = leveldb::FileLock;
    using Logger = leveldb::Logger;
    using RandomAccessFile = leveldb::RandomAccessFile;
    using SequentialFile = leveldb::SequentialFile;

    class SharedLock: public FileLock {
    public:
        SharedLock(int fd):_fd(fd) {}
        int _fd;
    };

    ///Construct the environment
    /**
     * @param base_env base enviromnet, you can use Env::Default(). This pointer
     * is not deleted.
     *
     * @param no_lock disable shared lock. If this is 'false', SHARED LOCK is
     * held preventing database to be opened in read/write mode. Set this
     * true to disable such lock, so you can open locked database.
     */
    CopyOnWriteEnv(leveldb::Env *base_env, bool no_lock = false)
            :_base_env(base_env)
            ,_mem_env(leveldb::NewMemEnv(base_env))
            ,_no_lock(no_lock) {}

    virtual Status NewSequentialFile(const std::string &fname, SequentialFile **result) override {
        if (_mem_env->FileExists(fname)) {
            return _mem_env->NewSequentialFile(fname, result);
        } else {
            return _base_env->NewSequentialFile(redirect(fname), result);
        }
    }
    virtual void Schedule(void (*function)(void*), void *arg) override {
        _base_env->Schedule(function, arg);
    }
    virtual Status DeleteFile(const std::string &fname) override {
        return RemoveFile(fname);
    }
    virtual Status RemoveFile(const std::string &fname) override {
        if (_mem_env->FileExists(fname)) {
            return _mem_env->DeleteFile(fname);
        } else {
            std::lock_guard _(_mx);
            _deleted.push_back(fname);
            return Status::OK();
        }
    }
    virtual Status LockFile(const std::string &fname, FileLock **lock) override {
        if (_no_lock) {
            return _mem_env->LockFile(fname, lock);
        }
        int fd = ::open(fname.c_str(), O_RDWR|O_NOATIME);
        if (fd == -1) {
            return Status::IOError(fname, strerror(errno));
        }
        struct flock region = {
                F_RDLCK,SEEK_SET, 0,0
        };
        int r = fcntl(fd, F_SETLK, &region);
        if (r) {
            int err = errno;
            ::close(fd);
            if (err == EAGAIN || err == EACCES) return Status::IOError("LOCKED");
            else return Status::IOError(fname, strerror(err));
        }
        *lock = new SharedLock(fd);
        return Status::OK();
    }
    virtual Status NewAppendableFile(const std::string &fname, WritableFile **result) override {
        return Status::NotSupported("Read only environment");
    }
    virtual Status GetTestDirectory(std::string *path) override{
        return _mem_env->GetTestDirectory(path);
    }
    virtual Status NewLogger(const std::string &fname,leveldb::Logger **result) override {
        return _mem_env->NewLogger(fname, result);
    }
    virtual Status CreateDir(const std::string &dirname) override {
        return _mem_env->CreateDir(dirname);
    }
    virtual void StartThread(void (*function)(void*), void *arg) override {
        return _base_env->StartThread(function, arg);
    }
    virtual Status NewRandomAccessFile(const std::string &fname, RandomAccessFile **result) override {
        if (_mem_env->FileExists(fname)) {
            return _mem_env->NewRandomAccessFile(fname, result);
        } else {
            return _base_env->NewRandomAccessFile(redirect(fname), result);
        }
    }
    virtual Status RemoveDir(const std::string &dirname) override {
        return _mem_env->RemoveDir(dirname);
    }
    virtual Status GetFileSize(const std::string &fname, uint64_t *file_size) override {
        if (_mem_env->FileExists(fname)) {
            return _mem_env->GetFileSize(fname, file_size);
        }else {
            return _base_env->GetFileSize(redirect(fname), file_size);
        }
    }
    virtual Status NewWritableFile(const std::string &fname,WritableFile **result) override {
        return _mem_env->NewWritableFile(fname, result);
    }
    virtual bool FileExists(const std::string &fname) override {
        return _mem_env->FileExists(fname) || _base_env->FileExists(redirect(fname));
    }
    virtual Status RenameFile(const std::string &src,const std::string &target) override {
        if (_mem_env->FileExists(src)) {
            return  _mem_env->RenameFile(src, target);
        } else {
            auto fname = redirect(src);
            if (_base_env->FileExists(fname)) {
                std::lock_guard lk(_mx);
                _renamed.erase(src);
                _renamed[target] = fname;
                return Status::OK();
            } else {
                return Status::NotFound(src);
            }
        }
    }
    virtual void SleepForMicroseconds(int micros) override {
        _base_env->SleepForMicroseconds(micros);
    }
    virtual uint64_t NowMicros() override {
        return _base_env->NowMicros();
    }
    virtual Status DeleteDir(const std::string &dirname) override {
        return _mem_env->DeleteDir(dirname);
    }
    virtual Status UnlockFile(leveldb::FileLock *lock) override {
        SharedLock *lk = dynamic_cast<SharedLock *>(lock);
        if (lk) {
            struct flock region = {
                    F_UNLCK,SEEK_SET, 0,0
            };
            fcntl(lk->_fd, F_SETLK, &region);
            close(lk->_fd);
            delete lock;
            return Status::OK();
        }
        return _mem_env->UnlockFile(lock);
    }
    virtual Status GetChildren(const std::string &dir, std::vector<std::string> *result)override {
        std::vector<std::string> base_files;
        std::vector<std::string> mem_files;
        std::vector<std::string> to_del;
        std::vector<std::string> to_insert;
        {
            std::lock_guard lk(_mx);

            for (const auto &[f,g]: _renamed) {
                if (f.compare(0,dir.size(), dir) == 0) to_insert.push_back(f.substr(dir.size()+1));
                if (g.compare(0,dir.size(), dir) == 0) to_del.push_back(g.substr(dir.size()+1));
            }
            for (const auto &d: _deleted) {
                if (d.compare(0,dir.size(), dir) == 0) to_del.push_back(d.substr(dir.size()+1));
            }
        }
        _mem_env->GetChildren(dir, &mem_files);
        _base_env->GetChildren(dir, &base_files);
        std::sort(to_del.begin(), to_del.end());
        std::sort(to_insert.begin(), to_insert.end());
        std::sort(mem_files.begin(), mem_files.end());
        std::sort(base_files.begin(), base_files.end());

        std::vector<std::string> tmp1;
        std::set_difference(base_files.begin(), base_files.end(),
                to_del.begin(), to_del.end(), std::back_inserter(tmp1));
        base_files.clear();
        std::set_union(tmp1.begin(), tmp1.end(), to_insert.begin(), to_insert.end(),
                std::back_inserter(base_files));
        result->clear();
        std::set_union(base_files.begin(), base_files.end(), mem_files.begin(), mem_files.end(),
                std::back_inserter(*result));

        return Status::OK();


    }

protected:
    ///base environment
    leveldb::Env *_base_env;
    ///memory environment
    std::unique_ptr<leveldb::Env> _mem_env;
    ///mutex to protect internals
    std::mutex _mx;
    ///map of renamed base files (when base file is renamed, it should be accessible under new name)
    std::map<std::string, std::string> _renamed;
    ///list of deleted files (base files marked as deleted, so they should not appear in directory)
    std::vector<std::string> _deleted;
    ///true if locking is disabled
    bool _no_lock;

    std::string redirect(const std::string &fname) {
        std::lock_guard lk(_mx);
        auto iter = _renamed.find(fname);
        if (iter == _renamed.end()) return fname;
        else return iter->second;
    }


};




}

#endif /* SRC_DOCDB_READONLY_H_ */

