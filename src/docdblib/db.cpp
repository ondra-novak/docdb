/*
 * db.cpp
 *
 *  Created on: 16. 12. 2020
 *      Author: ondra
 */

#include <unordered_set>
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


static constexpr KeySpaceID keyspaceManager = ~KeySpaceID(0);

static Key getKey(ClassID class_id, const std::string_view &name);
static Key getKey(KeySpaceID id);


class DBCoreImpl: public DBCore {
public:
	DBCoreImpl(const std::string &path, const Config &cfg);

	virtual void commitBatch(Batch &b) override;
	virtual Iterator createIterator(const Iterator::RangeDef &rangeDef) const override;
	virtual bool get(const Key &key, std::string &val) const override;
	virtual PDBCore getSnapshot(SnapshotMode mode) const override;
	virtual void compact() override ;
	virtual json::Value getStats() const override;
	virtual KeySpaceID allocKeyspace(ClassID classId , const std::string_view &name) override;
	virtual bool freeKeyspace(ClassID class_id, const std::string_view &name) override;
	virtual void getApproximateSizes(const std::pair<Key,Key> *ranges, int nranges, std::uint64_t *sizes) override;
	virtual PAbstractObservable getObservable(KeySpaceID kid,  ObservableFactory factory) override;
	virtual bool keyspaceLock(KeySpaceID kid, bool unlock) override;

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
	std::mutex lock;

	using ObservableMap = std::unordered_map<KeySpaceID, PAbstractObservable >;
	using KeyspaceLockMap = std::unordered_set<KeySpaceID>;
	ObservableMap observableMap;
	KeyspaceLockMap lockMap;

};

class DBCoreImpl::Logger: public leveldb::Logger {
public:
	Logger(::docdb::Logger lg):lg(lg) {}
	::docdb::Logger lg;

	virtual void Logv(const char* format, va_list ap) {
		char buff[256];
		int n = vsnprintf(buff,sizeof(buff),format,ap);
		while (n && isspace(buff[n-1])) n--;
		if (n) {
			lg(std::string_view(buff,n));
		}
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
	virtual Iterator createIterator(const Iterator::RangeDef &rangeDef) const  override;
	virtual bool get(const Key &key, std::string &val) const override;
	virtual PDBCore getSnapshot(SnapshotMode mode = writeError) const  override;
	virtual void compact() override {}
	virtual json::Value getStats() const override  {return core->getStats();}
	virtual KeySpaceID allocKeyspace(ClassID  clsid, const std::string_view &name) override {
		Key k(getKey(clsid, name));
		std::string val;
		if (get(k, val)) return *reinterpret_cast<const KeySpaceID *>(val.data());
		else throw std::runtime_error("Keyspace not found");
	}
	virtual bool freeKeyspace(ClassID, const std::string_view &) override {return false;}
	virtual void getApproximateSizes(const std::pair<Key,Key> *ranges, int nranges, std::uint64_t *sizes) override {return core->getApproximateSizes(ranges, nranges, sizes);}
	virtual PAbstractObservable getObservable(KeySpaceID kid, ObservableFactory factory) {return core->getObservable(kid,factory);}
	virtual bool keyspaceLock(KeySpaceID , bool ) override {return false;};


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
Iterator DBCoreImpl::SnapshotIgn::createIterator(const Iterator::RangeDef &rangeDef) const {
	leveldb::ReadOptions opt;
	opt.snapshot = snapshot;
	return Iterator(leveldb->NewIterator(opt), rangeDef);
}
bool DBCoreImpl::SnapshotIgn::get(const Key &key, std::string &val) const {
	leveldb::ReadOptions opts;
	opts.snapshot = snapshot;
	return LevelDBException::checkStatus_Get(leveldb->Get(opts, key, &val));
}
PDBCore DBCoreImpl::SnapshotIgn::getSnapshot(SnapshotMode) const {
	return PDBCore(const_cast<SnapshotIgn *>(this));
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
	virtual bool freeKeyspace(ClassID , const std::string_view &) override {
		throw CantWriteToSnapshot();
	}
	virtual bool keyspaceLock(KeySpaceID , bool ) override {
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
	virtual KeySpaceID allocKeyspace(ClassID a , const std::string_view &b) override {
		return core->allocKeyspace(a,b);
	}
	virtual bool freeKeyspace(ClassID a, const std::string_view &b) override {
		return core->freeKeyspace(a,b);
	}
	virtual bool keyspaceLock(KeySpaceID kid, bool lock) override {
		return core->keyspaceLock(kid, lock);
	}
};


PDBCore DBCoreImpl::getSnapshot(SnapshotMode mode) const {
	PDBCore core (const_cast<DBCoreImpl *>(this));
	switch(mode) {
		default:
		case writeError: return new SnapshotErr(core, db.get());
		case writeForward: return new SnapshotFwd(core, db.get());
		case writeIgnore: return new SnapshotIgn(core, db.get());
	}
}

bool DBCoreImpl::get(const Key &key, std::string &out) const {
	leveldb::ReadOptions opts;
	return LevelDBException::checkStatus_Get(db->Get(opts, key, &out));
}

Iterator DBCoreImpl::createIterator(const Iterator::RangeDef &rangeDef) const {
	leveldb::ReadOptions opts;
	return Iterator(db->NewIterator(opts),rangeDef);
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

static Key getKey(ClassID class_id, const std::string_view &name) {
	Key k(keyspaceManager);
	std::string val;
	k.append(std::string_view(reinterpret_cast<const char *>(&class_id),sizeof(class_id)));
	k.append(name);
	return k;
}

//<FF> <id> - search for keyspace

static Key getKey(KeySpaceID id) {
	Key k(keyspaceManager);
	k.append(std::string_view(reinterpret_cast<const char *>(&keyspaceManager),sizeof(keyspaceManager)));
	k.append(std::string_view(reinterpret_cast<const char *>(&id),sizeof(id)));
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
	return core->allocKeyspace(class_id, name);
}

KeySpaceID DB::allocKeyspace(KeySpaceClass class_id, const std::string_view &name) {
	return core->allocKeyspace(static_cast<ClassID>(class_id), name);
}

bool DB::freeKeyspace(ClassID class_id, const std::string_view &name) {
	return core->freeKeyspace(class_id, name);
}

void DB::commitBatch(Batch &b) {
	core->commitBatch(b);
}

void DB::keyspace_putMetadata(KeySpaceID id, const json::Value &data) {
	Batch b;
	keyspace_putMetadata(b, id, data);
	commitBatch(b);
}

void DB::keyspace_putMetadata(Batch &b, KeySpaceID id,
		const json::Value &data) {
	std::string val;
	json2string(data, val);
	b.Put(getKey(id), val);
}

json::Value DB::keyspace_getMetadata(KeySpaceID id) const {
	std::string val;
	if (!core->get(getKey(id),val) || val.empty()) return json::Value();
	return string2json(val);
}

KeySpaceIterator DB::listKeyspaces() const {
	Key from(getKey(ClassID(0), std::string_view()));
	Key to(getKey(~ClassID(0), std::string_view()));
	return KeySpaceIterator(createIterator({from, to, false, true}));
}


KeySpaceIterator::KeySpaceIterator(Iterator &&iter)
	:Iterator(std::move(iter))
{
}


Iterator DB::createIterator(const Iterator::RangeDef &rangeDef) const {
	return core->createIterator(rangeDef);
}

bool DB::get(const Key &key, std::string &val) const {
	return core->get(key, val);
}

DB DB::getSnapshot(SnapshotMode mode) const {
	return DB(core->getSnapshot(mode));
}

json::Value DB::getStats() const {
	return core->getStats();
}

KeySpaceID DBCoreImpl::allocKeyspace(ClassID classId, const std::string_view &name) {
	std::lock_guard _(lock);
	Key k(getKey(classId, name));
	std::string val;
	if (get(k, val)) return *reinterpret_cast<const KeySpaceID *>(val.data());
	KeySpaceID n = 0;
	while (n < keyspaceManager) {
		Key z(getKey(n));
		if (!get(z, val)) break;
		n++;
	}
	if (n == keyspaceManager) throw TooManyKeyspaces(this->name);
	Batch b;
	val.clear();
	Key k2=getKey(n);
	b.Put(k2, val);
	val.clear();
	val.push_back(n);
	b.Put(k, val);
	commitBatch(b);
	return n;
}


bool DBCoreImpl::freeKeyspace(ClassID class_id, const std::string_view &name) {
	std::lock_guard _(lock);
	leveldb::WriteOptions opts;

	Key k(getKey(class_id, name));
	std::string val;
	if (!get(k, val)) return false;

	auto id = *reinterpret_cast<const KeySpaceID *>(val.data());

	if (lockMap.find(id) != lockMap.end())
		throw KeyspaceAlreadyLocked(id);

	Key ct1(id);
	Key ct2(ct1);
	ct2.upper_bound();
	Iterator iter(db->NewIterator({}),{ct1, ct2, false, true });
	while (iter.next()) {
		auto ctk = iter.key();
		db->Delete(opts, ctk);
	}

	db->Delete(opts, getKey(id));
	db->Delete(opts, k);
	observableMap.erase(id);
	return true;
}

KeySpaceID KeySpaceIterator::getID() const {
	return *reinterpret_cast<const KeySpaceID *>(this->value().data());
}

ClassID KeySpaceIterator::getClass() const {
	auto v = this->key();
	auto ctx = v.content();
	return *reinterpret_cast<const ClassID *>(ctx.data());
}

std::string_view KeySpaceIterator::getName() const {
	auto v = this->key();
	auto ctx = v.content();
	return ctx.substr(sizeof(ClassID));
}

thread_local std::string buffer;

void DB::clearKeyspace(KeySpaceID id) {
	Batch b;
	for (auto iter = createIterator({Key(id), Key(id+1), false, true}); iter.next();) {
		b.Delete(iter.key());
		if (b.ApproximateSize() > 64000) commitBatch(b);
	}
	commitBatch(b);

}

std::string &DB::getBuffer() {
	std::string &out = buffer;
	out.clear();
	return out;
}

void DBCoreImpl::getApproximateSizes(const std::pair<Key,Key> *ranges, int nranges, std::uint64_t *sizes) {
	leveldb::Range sl_ranges[nranges];
	for (int i = 0; i < nranges; i++) {
		sl_ranges[i].start = ranges[i].first;
		sl_ranges[i].limit = ranges[i].second;
	}
	db->GetApproximateSizes(sl_ranges, nranges, sizes);
}

std::uint64_t DB::getKeyspaceSize(KeySpaceID kid) const {
	std::pair<Key,Key> r{Key(kid), Key(kid+1)};
	std::uint64_t res;
	core->getApproximateSizes(&r, 1, &res);
	return res;

}

PAbstractObservable DBCoreImpl::getObservable(KeySpaceID kid, ObservableFactory factory) {
	std::lock_guard _(lock);
	auto &ret = observableMap[kid];
	if (ret == nullptr) ret = factory();
	return ret;
}

bool DBCoreImpl::keyspaceLock(KeySpaceID kid, bool lk) {
	std::lock_guard _(lock);
	if (lk) {
		auto res = lockMap.insert(kid);
		return res.second;
	} else {
		lockMap.erase(kid);
		return true;
	}
}

void DB::keyspaceLock(KeySpaceID kid, bool lock) {
	if (!core->keyspaceLock(kid, lock)) {
		if (lock) throw KeyspaceAlreadyLocked(kid);
	}
}

}

