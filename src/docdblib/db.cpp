/*
 * db.cpp
 *
 *  Created on: 16. 12. 2020
 *      Author: ondra
 */


#include <filesystem>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>
#include "db.h"

#include <leveldb/comparator.h>

#include <imtjson/object.h>
#include <imtjson/array.h>
#include "exception.h"
#include "formats.h"
namespace docdb {

class DBCoreImpl: public DBCore {
public:
	DBCoreImpl(const std::string &path, const Config &cfg);

	virtual void commitBatch(Batch &b) override;
	virtual Iterator createIterator(const Key &from, const Key &to, bool exclude_begin, bool exclude_end) const override;
	virtual std::optional<std::string> get(const Key &key) const  override;
	virtual PDBCore getSnapshot(SnapshotMode mode) override;
	virtual void compact();
	virtual json::Value getStats() const;

protected:

	class Logger;
	class SnapshotIgn;
	class SnapshotErr;
	class SnapshotFwd;

	std::string name;
	std::unique_ptr<leveldb::DB> db;
	PCache cache;
	PEnv env;
	std::unique_ptr<Logger> logger;
	bool sync_writes;


};

class DBCoreImpl::Logger: public leveldb::Logger {
public:
	Logger(::docdb::Logger lg):lg(lg) {}
	::docdb::Logger lg;

	virtual void Logv(const char* format, va_list ap) {
		lg(format, ap);
	}
};

DBCoreImpl::DBCoreImpl(const std::string &path, const Config &cfg)
:name(path)
{
	if (!std::filesystem::is_directory(path)) {
		if (cfg.create_if_missing == false) throw DatabaseOpenError(ENOENT, path);
		std::error_code ec;
		if (!std::filesystem::create_directory(path, ec)) throw std::system_error(ec);
	}
	leveldb::Options opts;
	cache = cfg.block_cache;
	env = cfg.env;
	if (cfg.logger != nullptr) logger = std::make_unique<Logger>(cfg.logger);
	opts.block_cache = cfg.block_cache.get();
	if (cfg.env != nullptr) opts.env = cfg.env.get();
	opts.block_restart_interval = cfg.block_restart_interval;
	opts.block_size = cfg.block_size;
	opts.comparator = leveldb::BytewiseComparator();
	opts.compression = leveldb::kSnappyCompression;
	opts.create_if_missing = cfg.create_if_missing;
	opts.error_if_exists = cfg.error_if_exists;
	opts.info_log = logger.get();
	opts.max_file_size = cfg.max_file_size;
	opts.max_open_files = cfg.max_open_files;
	opts.paranoid_checks = cfg.paranoid_checks;
	opts.write_buffer_size = cfg.write_buffer_size;
	sync_writes = cfg.sync_writes;

	leveldb::DB *ptr;
	LevelDBException::checkStatus(leveldb::DB::Open(opts, path, &ptr));
	db = std::unique_ptr<leveldb::DB>(ptr);
}

class DBCoreImpl::SnapshotIgn: public DBCore {
public:
	SnapshotIgn(PDBCore core, leveldb::DB *leveldb);
	virtual ~SnapshotIgn();
	virtual void commitBatch(Batch &b) override;
	virtual Iterator createIterator(const Key &from, const Key &to, bool exclude_begin, bool exclude_end) const  override;
	virtual std::optional<std::string> get(const Key &key) const  override;
	virtual PDBCore getSnapshot(SnapshotMode mode = writeError)  override;
	virtual void compact() override {}
	virtual json::Value getStats() const override  {return core->getStats();}


protected:
	PDBCore core;
	leveldb::DB *leveldb;
	const leveldb::Snapshot *snapshot;
};

DBCoreImpl::SnapshotIgn::SnapshotIgn(PDBCore core, leveldb::DB *leveldb)
:core(core),leveldb(leveldb),snapshot(leveldb->GetSnapshot()) {}
DBCoreImpl::SnapshotIgn::~SnapshotIgn() {
	leveldb->ReleaseSnapshot(snapshot);
}

void DBCoreImpl::SnapshotIgn::commitBatch(Batch &) {}
Iterator DBCoreImpl::SnapshotIgn::createIterator(const Key &from, const Key &to, bool exclude_begin, bool exclude_end) const {
	leveldb::ReadOptions opt;
	opt.snapshot = snapshot;
	return Iterator(leveldb->NewIterator(opt), from, to, exclude_begin, exclude_end);
}
std::optional<std::string> DBCoreImpl::SnapshotIgn::get(const Key &key) const {
	leveldb::ReadOptions opts;
	std::string out;
	opts.snapshot = snapshot;
	if (LevelDBException::checkStatus_Get(leveldb->Get(opts, key, &out))) {
		return out;
	} else {
		return std::optional<std::string>();
	}
}
PDBCore DBCoreImpl::SnapshotIgn::getSnapshot(SnapshotMode)  {
	return PDBCore(this);
}

class DBCoreImpl::SnapshotErr: public DBCoreImpl::SnapshotIgn {
public:
	using DBCoreImpl::SnapshotIgn::SnapshotIgn;
	virtual void commitBatch(Batch &) override {
		throw CantWriteToSnapshot();
	}
	virtual void compact() {
		throw CantWriteToSnapshot();
	}
};

class DBCoreImpl::SnapshotFwd: public DBCoreImpl::SnapshotIgn {
public:
	using DBCoreImpl::SnapshotIgn::SnapshotIgn;
	virtual void commitBatch(Batch &b) override {
		core->commitBatch(b);
	}
	virtual void compact() {
		core->compact();
	}
};


PDBCore DBCoreImpl::getSnapshot(SnapshotMode mode) {
	switch(mode) {
		default:
		case writeError: return new SnapshotErr(this, db.get());
		case writeForward: return new SnapshotFwd(this, db.get());
		case writeIgnore: return new SnapshotIgn(this, db.get());
	}
}

std::optional<std::string> DBCoreImpl::get(const Key &key) const {
	std::string out;
	leveldb::ReadOptions opts;
	if (LevelDBException::checkStatus_Get(db->Get(opts, key, &out))) {
		return out;
	} else {
		return std::optional<std::string>();
	}
}

Iterator DBCoreImpl::createIterator(const Key &from, const Key &to, bool exclude_begin,
		bool exclude_end) const {
	leveldb::ReadOptions opts;
	return Iterator(db->NewIterator(opts),from, to, exclude_begin, exclude_end);
}

static leveldb::WriteOptions getWriteOptions(bool sync) {
	leveldb::WriteOptions opts;
	opts.sync = sync;
	return opts;
}

void DBCoreImpl::commitBatch(Batch &b) {
	auto opt = getWriteOptions(sync_writes);
	db->Write(opt, &b);
	b.Clear();
}

void DBCoreImpl::compact() {
	db->CompactRange(nullptr, nullptr);
}

json::Value DBCoreImpl::getStats() const {
	json::Object ret;
	std::string val;
	db->GetProperty("leveldb.approximate-memory-usage", &val);
	ret.set("memory_usage", val);
	val.clear();
	db->GetProperty("leveldb.stats", &val);
	auto splt = ondra_shared::StrViewA(val).split("\n");
	json::Array stats;
	while (!!splt) {
		ondra_shared::StrViewA line = splt();
		float level, files, size_mb, time_sec, read_mb, write_mb;
		if (std::sscanf(line.data,"%f %f %f %f %f %f", &level, &files, &size_mb, &time_sec, &read_mb, &write_mb) == 6) {
		  stats.push_back(json::Value(json::object,{
				json::Value("level",level),
				json::Value("files",files),
				json::Value("size_mb",size_mb),
				json::Value("time_sec",time_sec),
				json::Value("read_mb",read_mb),
				json::Value("write_mb",write_mb)
		  }));
		}
	}
	ret.set("levels", stats);
	return ret;

}


//<FF> <FF> <class> <name> - search for keyspace by name

Key DB::getKey(ClassID class_id, const std::string_view &name) {
	Key k(keyspaceManager);
	std::string val;
	k.append(std::string_view(reinterpret_cast<const char *>(keyspaceManager),sizeof(keyspaceManager)));
	k.append(std::string_view(reinterpret_cast<const char *>(class_id),sizeof(class_id)));
	k.append(name);
	return k;
}

//<FF> <id> - search for keyspace

Key DB::getKey(KeySpaceID id) {
	Key k(keyspaceManager);
	k.append(std::string_view(reinterpret_cast<const char *>(id),sizeof(keyspaceManager)));
	return k;
}

DB::DB(const std::string &path, const Config &cfg)
:core(new DBCoreImpl(path,cfg))
{

}

PCache DB::createCache(std::size_t size) {
	return PCache(leveldb::NewLRUCache(size));
}

KeySpaceID DB::allocKeyspace(ClassID class_id, const std::string_view &name) {
	std::lock_guard _(lock);
	Key k(getKey(class_id, name));
	std::string val;
	if (LevelDBException::checkStatus_Get(db->Get({}, k, &val))) {
		return *reinterpret_cast<const KeySpaceID *>(val.data());
	} else {
		KeySpaceID n = 0;
		auto iter = listKeyspaces();
		while (iter.next()) {
			KeySpaceID z = iter.getID();
			if (z > n) {
				break;
			} else {
				n = z+1;
			}
		}
		if (n == keyspaceManager) throw TooManyKeyspaces(this->name);
		val.clear();
		json2string({class_id, name, json::Value(nullptr)}, val);
		Batch b;
		Key k2=getKey(n);
		b.Put(k2, val);
		val.clear();
		val.append(k2.content());
		b.Put(k, val);
		commitBatch(b);
		return n;
	}
}


void DB::freeKeyspace(KeySpaceID id) {
	std::lock_guard _(lock);
	auto opts = getWriteOptions(this->sync_writes);;
	Key ct1(id);
	Key ct2(ct1);
	ct2.upper_bound();
	Iterator iter(db->NewIterator({}),ct1, ct2, false, true );
	while (iter.next()) {
		auto ctk = iter.key();
		db->Delete(opts, leveldb::Slice(ctk.data(), ctk.size()));
	}
	auto ks = getKeyspaceInfo(id);
	if (ks.has_value()) {
		auto k = getKey(ks->class_id, ks->name);
		db->Delete(opts, k);
	}
	Key mk(getKey(id));
	db->Delete(opts, mk);
}

static KeySpaceInfo infoFromJSON(KeySpaceID id, json::Value v) {
	return {
		id,
		static_cast<ClassID>(v[0].getUInt()),
		v[1].getString(),
		v[2]
	};
}


KeySpaceIterator DB::listKeyspaces() const {
	Key from(keyspaceManager);
	Key to(keyspaceManager);
	KeySpaceID zero = 0;
	from.append(std::string_view(reinterpret_cast<const char *>(zero),sizeof(zero)));
	to.append(std::string_view(reinterpret_cast<const char *>(keyspaceManager),sizeof(keyspaceManager)));
	return KeySpaceIterator(createIterator(from, to, false, true));
}

std::optional<KeySpaceInfo> DB::getKeyspaceInfo(KeySpaceID id) const {
	Key k(getKey(id));
	std::string val;
	if (LevelDBException::checkStatus_Get(db->Get({}, k, &val))) {
		return infoFromJSON(id, string2json(val));
	} else {
		return std::optional<KeySpaceInfo>();
	}


}

std::optional<KeySpaceInfo> DB::getKeyspaceInfo(ClassID class_id, const std::string_view &name) const {
	Key k (getKey(class_id, name));
	std::string val;
	if (LevelDBException::checkStatus_Get(db->Get({}, k, &val))) {
		KeySpaceID id = *reinterpret_cast<const KeySpaceID *>(val.data());
		return getKeyspaceInfo(id);
	} else {
		return std::optional<KeySpaceInfo>();
	}


}

void DB::storeKeyspaceMetadata(KeySpaceID id, const json::Value &data)  {
	Batch b;
	storeKeyspaceMetadata(id, data);
	commitBatch(b);
}

void DB::storeKeyspaceMetadata(Batch &b, KeySpaceID id, const json::Value &data) const {
	Key k(getKey(id));
	std::string val;
	LevelDBException::checkStatus(db->Get({}, k, &val));
	json::Array curData(string2json(val));
	curData[2] = data;
	val.clear();
	json2string(curData, val);
	b.Put(k, val);
}






KeySpaceIterator::KeySpaceIterator(Iterator &&iter)
	:Iterator(std::move(iter))
{
}

KeySpaceInfo KeySpaceIterator::getInfo() const {
	return infoFromJSON(getID(),string2json(this->value()));
}

KeySpaceID KeySpaceIterator::getID() const {
	return *reinterpret_cast<const KeySpaceID *>(Iterator::key().data()+sizeof(KeySpaceID));
}



}
