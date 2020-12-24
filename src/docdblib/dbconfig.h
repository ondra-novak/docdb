/*
 * dbconfig.h
 *
 *  Created on: 16. 12. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDB_SRC_DOCDBLIB_DBCONFIG_H_
#define SRC_DOCDB_SRC_DOCDBLIB_DBCONFIG_H_

#include <functional>
#include <leveldb/cache.h>
#include <leveldb/env.h>
namespace docdb {

using PCache = std::shared_ptr<leveldb::Cache>;
using PEnv = std::shared_ptr<leveldb::Env>;
using Logger = std::function<void(const char *, va_list)>;

struct Config {

	  // If true, the database will be created if it is missing.
	  bool create_if_missing = true;

	  // If true, an error is raised if the database already exists.
	  bool error_if_exists = false;

	  // If true, the implementation will do aggressive checking of the
	  // data it is processing and will stop early if it detects any
	  // errors.  This may have unforeseen ramifications: for example, a
	  // corruption of one DB entry may cause a large number of entries to
	  // become unreadable or for the entire DB to become unopenable.
	  bool paranoid_checks = false;
	  // Use the specified object to interact with the environment,
	  // e.g. to read/write files, schedule background work, etc.
	  // Default: Env::Default()
	  PEnv env = nullptr;

	  // -------------------
	  // Parameters that affect performance

	  // Amount of data to build up in memory (backed by an unsorted log
	  // on disk) before converting to a sorted on-disk file.
	  //
	  // Larger values increase performance, especially during bulk loads.
	  // Up to two write buffers may be held in memory at the same time,
	  // so you may wish to adjust this parameter to control memory usage.
	  // Also, a larger write buffer will result in a longer recovery time
	  // the next time the database is opened.
	  size_t write_buffer_size = 4 * 1024 * 1024;
	  // Number of open files that can be used by the DB.  You may need to
	  // increase this if your database has a large working set (budget
	  // one open file per 2MB of working set).
	  int max_open_files = 1000;

	  // Control over blocks (user data is stored in a set of blocks, and
	  // a block is the unit of reading from disk).

	  // If non-null, use the specified cache for blocks.
	  // If null, leveldb will automatically create and use an 8MB internal cache.
	  PCache block_cache = nullptr;
	  // Approximate size of user data packed per block.  Note that the
	  // block size specified here corresponds to uncompressed data.  The
	  // actual size of the unit read from disk may be smaller if
	  // compression is enabled.  This parameter can be changed dynamically.
	  size_t block_size = 4 * 1024;
	  // Number of keys between restart points for delta encoding of keys.
	  // This parameter can be changed dynamically.  Most clients should
	  // leave this parameter alone.
	  int block_restart_interval = 16;

	  // Leveldb will write up to this amount of bytes to a file before
	  // switching to a new one.
	  // Most clients should leave this parameter alone.  However if your
	  // filesystem is more efficient with larger files, you could
	  // consider increasing the value.  The downside will be longer
	  // compactions and hence longer latency/performance hiccups.
	  // Another reason to increase this parameter might be when you are
	  // initially populating a large database.
	  size_t max_file_size = 2 * 1024 * 1024;

	  size_t bloom_filter_size = 16;

	  // Set true to perform synchronous writes (much worse performance)
	  bool sync_writes = false;

	  ///leveldb logger
	  Logger logger = nullptr;
};


}



#endif /* SRC_DOCDB_SRC_DOCDBLIB_DBCONFIG_H_ */
