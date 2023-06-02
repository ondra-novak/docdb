#include <docdb/database.h>
#include <docdb/storage.h>

#include <cmath>
#include <cstdio>
#include <iostream>
#include <iomanip>
#include <leveldb/env.h>
#include <sstream>
#include "ReadLine.h"

#include "parse_line.h"

#include <bits/getopt_core.h>
class Logger: public leveldb::Logger {
public:
    virtual void Logv(const char* format, std::va_list ap) override {
        vsnprintf(buff,sizeof(buff), format, ap);
        std::cerr << buff << std::endl;
    }
protected:
    char buff[1024];
};
class EmptyLogger: public leveldb::Logger {
public:
    virtual void Logv(const char* format, std::va_list ap) override {
    }
};

static Logger logger;
static EmptyLogger empty_logger;

static docdb::PDatabase open_db(std::string path, bool create_flag, bool verbose_mode) {
    leveldb::Options opts;
    opts.info_log = verbose_mode?static_cast<leveldb::Logger *>(&logger):static_cast<leveldb::Logger *>(&empty_logger);
    opts.create_if_missing = create_flag;
    opts.reuse_logs  = true;
    leveldb::DB *out;
    auto st = leveldb::DB::Open(opts, path, &out);
    if (!st.ok()) throw docdb::DatabaseError(st);
    return docdb::Database::create(out);
}

std::string convertNumberToString(std::uint64_t number, int width)
{
    std::string_view unit;

    if (number >= 10000000000ULL)
    {
        number /= 1000000000;
        unit = "G";
    }
    else if (number >= 10000000ULL)
    {
        number /= 1000000;
        unit = "M";
    }
    else if (number >= 10000ULL)
    {
        number /= 1000;
        unit = "k";
    } else {
        unit = " ";
    }

    std::ostringstream buff;
    buff << std::setw(width) << std::setfill(' ') << std::right << number << unit;
    return buff.str();
}

constexpr std::string_view purposeToText(docdb::Purpose x) {
    switch (x) {
        case docdb::Purpose::aggregation: return "Aggregation";
        case docdb::Purpose::index: return "Index";
        case docdb::Purpose::unique_index: return "Unique index";
        case docdb::Purpose::map: return "Map";
        case docdb::Purpose::storage: return "Storage";
        case docdb::Purpose::undefined: return "Undefined";
        default: return {};
    }
}

static std::size_t round_aprox(std::size_t count) {
    std::size_t aprox_div = static_cast<std::size_t>(std::pow(10,std::floor(std::log10(count))-1));
    if (aprox_div < 1) aprox_div = 1;
    count = aprox_div?((count + aprox_div/2) / aprox_div) * aprox_div:count;
    return count;

}

static void print_list_tables(const docdb::PDatabase &db) {
    auto l = db->list();
    int counter = 0;
    std::cout << "KID Type     Aprx.records    Size Name" << std::endl;
    std::cout << "--------------------------------------------------------" <<std::endl;

    std::string buff;
    for (const auto &row: l) {
        counter++;
        auto purpose_text = purposeToText(row.second.second);
        if (purpose_text.empty())  {
            buff = "Unknown '";
            buff.push_back(static_cast<char>(row.second.second));
            buff.append("'");
            purpose_text = buff;
        }
        int kid = row.second.first;
        docdb::RecordSetBase rc(db->make_iterator(),{
                docdb::RawKey(kid),
                docdb::RawKey(kid+1),
                docdb::FirstRecord::included,
                docdb::FirstRecord::excluded,
        });
        std::size_t count_rc  = rc.count_aprox(db,1000);
        std::size_t size = db->get_index_size(docdb::RawKey(kid), docdb::RawKey(kid+1));
        std::cout << std::setw(3) << std::right << kid << " "
                  << std::setw(13) << std::left << purpose_text << " "
                  << convertNumberToString(round_aprox(count_rc), 6) << " "
                  << convertNumberToString(size, 6) << " "
                  << row.first << std::endl;
    }
    std::cout << std::endl<< "Count collections: " << counter << std::endl;
}

static void print_db_info(const docdb::PDatabase &db) {
    auto &ldb = db->get_level_db();
    std::string out;
    std::string line;
    ldb.GetProperty("leveldb.sstables", &out);
    std::cout << "Level    Size Files" <<std::endl;
    std::cout << "-------------------" <<std::endl;
    std::istringstream input(out);
    std::size_t prev_level = -1;
    std::size_t prev_level_size = 0;
    std::size_t prev_level_count = 0;
    auto flush_out = [&] {
        if (prev_level != -1) {
            std::cout << " " <<prev_level << " "
                    << convertNumberToString(prev_level_size, 10) << " "
                    << std::setw(4) << std::right << prev_level_count << std::endl;
        }
    };
    while (true) {
        std::getline(input, line);
        if (line.empty()) break;
        if (line.compare(0,10,"--- level ") == 0) {
            flush_out();
            prev_level = std::strtoul(line.data()+10,nullptr,10);
            prev_level_size = 0;
            prev_level_count = 0;
        } else {
            auto sep1 = line.find(':');
            if (sep1 != line.npos) {
                prev_level_size += std::strtoul(line.data()+sep1+1,nullptr,10);
            }
            ++prev_level_count;
        }
    }
    flush_out();
    std::cout << std::endl;
}

struct Command {
    std::string name;
    void (*exec)(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args);
    void (*completion)(const docdb::PDatabase &db, const char *word, std::size_t word_size, const ReadLine::HintCallback &cb);
};

std::string make_printable(const std::string_view &s, bool space) {
    std::string out;
    for (signed char c: s) {
        switch (c) {
            case '\n': out.append("\\n");break;
            case '\r': out.append("\\r");break;
            case '\\': out.append("\\\\");break;
            case '\"': out.append("\\\"");break;
            case ' ': if (space) out.append("\\ "); else out.push_back(c);break;
            case 0: out.append("\\0");break;
            default:
                if (c >= 32) out.push_back(c);
                else {
                    out.append("\\x00");
                    snprintf(out.data()+out.size()-2,3,"%02X", static_cast<unsigned char>(c));
                }
        }
    }
    return out;
}



static bool exit_flag = false;

static void command_compact(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    db->compact();
}

static void command_quit(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    exit_flag= true;
}

static void command_use(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    //empty
}

static void command_levels(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    print_db_info(db);
}

static void command_list(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    print_list_tables(db);
}

auto get_kid(const docdb::PDatabase &db,const std::string &name) {
    auto tst = db->get_table_info(name);
    if (name.empty()) throw std::invalid_argument("No active collection. Type the command: 'use <collection>'");
    if (!tst.has_value()) throw std::runtime_error("Collection doesn't exists: " + name);
    return *tst;

}

static void command_erase(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    if (args.size() != 2) throw std::invalid_argument("Requires name of collection to erase");
    get_kid(db, args[1]);
    std::cout << "Do you really wish to erase collection: " << args[1] << std::endl;
    std::cout << "Please answer \"yes\": " <<  std::endl;
    std::string answer;
    std::getline(std::cin, answer);
    if (answer == "yes") {
        db->delete_table(args[1]);
    }

}

static void command_create(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    if (args.size() != 3) {
        throw std::invalid_argument("create <type> <name>");
    }
    for (unsigned char c = 32; c<=127; c++) {
        auto p = static_cast<docdb::Purpose>(c);
        if (purposeToText(p) == args[1]) {
            docdb::KeyspaceID id = db->open_table(args[2], p);
            std::cout << "Collection keyspace is: " << static_cast<int>(id)<< std::endl;
            return;
        }
    }
    throw std::invalid_argument("Unknown type");
}



static void empty_completion(const docdb::PDatabase &db, const char *word, std::size_t word_size, const ReadLine::HintCallback &cb) {

}

static void purpose_completion(const docdb::PDatabase &db, const char *word, std::size_t word_size, const ReadLine::HintCallback &cb) {
    for (char c = ' '; c < 127; c++) {
        auto str = purposeToText(static_cast<docdb::Purpose>(c));
        if(!str.empty()) {
            std::string s = make_printable(str, true);
            if (s.compare(0, word_size, word) == 0) cb(s);
            c++;
        }
    }
}

static void list_of_tables(const docdb::PDatabase &db, const char *word, std::size_t word_size, const ReadLine::HintCallback &cb) {
    auto l = db->list();
    std::string buff;
    for (const auto &x: l) {
        std::string buff = make_printable(x.first, true);
        if (buff.compare(0,word_size,word) == 0) {
            cb(buff);
        }
    }
}

struct Columns {
    std::string id;
    std::string key;
    std::string val;
};

struct ColumnSizes {
    std::size_t id = 0;
    std::size_t key = 0;
    std::size_t val = 0;;
};

template<typename Iter>
ColumnSizes calculate_sizes(Iter f, Iter t) {
    ColumnSizes s;
    while (f != t) {
        s.id = std::max(s.id,f->id.size());
        s.key = std::max(s.key,f->key.size());
        s.val = std::max(s.val,f->val.size());
        ++f;
    }
    return s;
}

void tabulize(std::string &s, std::size_t size, bool align_right) {
    if (s.length() <= size ) {
        if (align_right) {
            std::string tmp(size - s.length(), ' ');
            s.insert(0, tmp);
        } else {
            while (s.length()<size) s.push_back(' ');
        }
    } else {
        s.resize(size-3);
        s.append("...");
    }
}

template<typename Iter>
void makeShorter(const ColumnSizes &sz, const ColumnSizes &align, Iter f, Iter t) {
    while (f != t) {
        tabulize(f->id, sz.id, align.id != 0);
        tabulize(f->key, sz.key, align.key!= 0);
        tabulize(f->val, sz.val, align.val!= 0);
        ++f;
    }
}


struct RecordsetList {
    RecordsetList(docdb::PDatabase db, docdb::KeyspaceID id, docdb::Purpose p, docdb::Direction dir)
        :rc(db->make_iterator(), {
                docdb::RawKey(docdb::isForward(dir)?id:id+1),
                docdb::RawKey(docdb::isForward(dir)?id+1:id),
                docdb::isForward(dir)?docdb::FirstRecord::included:docdb::FirstRecord::excluded,
                docdb::isForward(dir)?docdb::LastRecord::excluded:docdb::LastRecord::included
        }), p(p), db(db), _dir(dir) {}
    RecordsetList(docdb::PDatabase db, docdb::KeyspaceID id, docdb::Purpose p, docdb::Direction dir, docdb::Row key)
        :rc(db->make_iterator(), {
                docdb::RawKey(id, key),
                docdb::RawKey(docdb::isForward(dir)?id+1:id),
                docdb::FirstRecord::included,
                docdb::isForward(dir)?docdb::LastRecord::excluded:docdb::LastRecord::included
        }), p(p), db(db), _dir(dir) {}
    RecordsetList(docdb::PDatabase db, docdb::Purpose p,  docdb::RawKey from, docdb::RawKey to)
        :rc(db->make_iterator(), {
                from, to,docdb::FirstRecord::included,docdb::LastRecord::excluded
        }), p(p), db(db), _dir(docdb::Direction::forward) {}

    docdb::RecordSetBase rc;
    docdb::Purpose p;
    docdb::PDatabase db;

    void print_page() {
        std::size_t count = 25;
        std::vector<Columns> _cols;

        list_ids.clear();
        list_keys.clear();

        while (!rc.empty() && count>0) {
            --count;
            switch (p) {
                case docdb::Purpose::storage: {
                    docdb::Key k((docdb::RowView(rc.raw_key())));
                    auto v = rc.raw_value();
                    auto [id] = k.get<docdb::DocID>();
                    auto [prev_id, doc] = docdb::Row::extract<docdb::DocID, docdb::Blob>(v);
                    _cols.push_back({
                        std::to_string(id),
                        std::to_string(prev_id),
                        make_printable(doc,false),
                    });
                    list_ids.push_back(id);
                    list_ids.push_back(prev_id);

                }break;;
                case docdb::Purpose::index: {
                    auto rw = rc.raw_key();
                    auto docidstr = rw.substr(rw.length()-sizeof(docdb::DocID));
                    auto [id] = docdb::Row::extract<docdb::DocID>(docidstr);
                    docdb::Key k(docdb::RowView(rw.substr(0, rw.size()-docidstr.size())));
                    auto val = rc.raw_value();
                    auto [key] = k.get<docdb::Blob>();
                    _cols.push_back({
                        std::to_string(id),
                        make_printable(key,false),
                        make_printable(val,false)
                    });
                    list_ids.push_back(id);
                    list_keys.push_back(std::string(key));
                } break;
                case docdb::Purpose::unique_index: {
                    docdb::Key k((docdb::RowView(rc.raw_key())));
                    auto [key] = k.get<docdb::Blob>();
                    auto v = rc.raw_value();
                    auto [id, doc] = docdb::Row::extract<docdb::DocID, docdb::Blob>(v);
                    _cols.push_back({
                        std::to_string(id),
                        make_printable(key,false),
                        make_printable(doc,false),
                    });
                    list_ids.push_back(id);
                    list_keys.push_back(std::string(key));
                }break;;
                case docdb::Purpose::private_area: {
                    docdb::Key k((docdb::RowView(rc.raw_key())));
                    auto [dummy, key] = k.get<docdb::KeyspaceID, docdb::Blob>();
                    auto v = rc.raw_value();
                    _cols.push_back({
                        std::string(),
                        make_printable(key,false),
                        make_printable(v,false),
                    });
                    list_keys.push_back(std::string(key));
                } break;
                default: {
                    docdb::Key k((docdb::RowView(rc.raw_key())));
                    auto [key] = k.get<docdb::Blob>();
                    auto v = rc.raw_value();
                    _cols.push_back({
                        std::string(),
                        make_printable(key,false),
                        make_printable(v,false),
                    });
                    list_keys.push_back(std::string(key));
                } break;
            }

            rc.next();
        }
        ColumnSizes szs = calculate_sizes(_cols.begin(), _cols.end());
        if (szs.key + szs.val + szs.id > 78) {
            auto remain = 78-szs.id;
            auto half = remain / 2;
            if (szs.key < half) {
                szs.val = remain - szs.key;
            } else if (szs.val < half) {
                szs.key = remain - szs.val;
            } else {
                szs.key = half;
                szs.val = remain - szs.key;
            }
        }
        ColumnSizes align;
        switch (p) {
            case docdb::Purpose::storage: align.id = 1; align.key = 1;break;
            default: align.id = 1;
        }
        makeShorter(szs, align, _cols.begin(), _cols.end());
        for (const auto &rw: _cols) {
            std::cout << rw.id << " " << rw.key << " " << rw.val << std::endl;
        }
        if (!rc.empty()) {
            std::size_t n;
            auto ccount = rc.get_offset(); // @suppress("Symbol shadowing")
            auto total = rc.aprox_size_in_bytes(db);
            auto processed1 = std::min(rc.aprox_procesed_bytes(db),total);
            auto processed2 = total - std::min(rc.aprox_remain_bytes(db), total);
            auto processed = (processed1+processed2)>>1;

            if (ccount < 1000 || total == 0 || processed == 0) {
                n = round_aprox(rc.count_aprox(db, 1000));
            } else{
                n =  static_cast<std::size_t>(static_cast<double>(ccount) * total / processed);
                if (n<ccount+1000 || total == 0) {
                    n = round_aprox(rc.count_aprox(db, 1000));
                } else {
                    n = round_aprox(n - ccount);
                }
            }


            std::cout << "... aprx. " << n << " record(s) follows. Press enter to load more." << std::endl;
        }
    }

    std::vector<docdb::DocID> list_ids;
    std::vector<std::string> list_keys;
    docdb::Direction _dir;
};

static std::unique_ptr<RecordsetList> cur_recordset;

static void check_table_selected(const std::string_view &name) {
    if (name.empty()) throw std::runtime_error("No collection selected. Use command 'use <collection>'");
}

static void command_iterate_from_first(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    check_table_selected(name);
    auto kid = get_kid(db, name);
    cur_recordset = std::make_unique<RecordsetList>(db, kid.first, kid.second, docdb::Direction::forward);
    cur_recordset->print_page();
}

static void command_iterate_from_last(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    check_table_selected(name);
    auto kid = get_kid(db, name);
    cur_recordset = std::make_unique<RecordsetList>(db, kid.first, kid.second, docdb::Direction::backward);
    cur_recordset->print_page();
}

static void command_document(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    if (args.size() != 2) throw std::invalid_argument("Usage: 'document <id>' - where id is document number");
    docdb::DocID id = std::strtoull(args[1].c_str(),nullptr,10);
    if (id == 0) throw std::invalid_argument("The document ID must be number greater than zero");
    auto list = db->list();
    auto iter = list.find(name);
    if (iter == list.end() || iter->second.second != docdb::Purpose::storage) {
        bool found = false;
        for (const auto &x: list) {
            if (x.second.second == docdb::Purpose::storage) {
                auto r =db->get_as_document<docdb::FoundRecord<docdb::DocRecordDef<docdb::StringDocument> > >(docdb::RawKey(x.second.first, id),{});
                if (r) {
                    found = true;
                    std::cerr << "Document from collection '" << x.first << "':" << std::endl<<std::endl;
                    std::cout << make_printable(r->content,false) << std::endl;
                    std::cerr << std::endl;
                }
            }
        }
        if (!found) std::cerr << "No document found" << std::endl;
    } else {
        auto r =db->get_as_document<docdb::FoundRecord<docdb::DocRecordDef<docdb::StringDocument> > >(docdb::RawKey(iter->second.first, id),{});
        if (r) {
            std::cout << make_printable(r->content,false) << std::endl;
        } else {
            std::cerr << "Document was not found in the collection: " << name << std::endl;
        }
    }
}

static void completion_current_ids(const docdb::PDatabase &db, const char *word, std::size_t word_size, const ReadLine::HintCallback &cb) {
    if (cur_recordset) {
        for (const auto &c: cur_recordset->list_ids) {
            std::string s= std::to_string(c);
            if (s.compare(0, word_size, word) == 0) cb(s);
        }
    }
}

static void completion_current_keys(const docdb::PDatabase &db, const char *word, std::size_t word_size, const ReadLine::HintCallback &cb) {
    if (cur_recordset) {
        if (cur_recordset->p == docdb::Purpose::storage) {
            for (const auto &c: cur_recordset->list_ids) {
                std::string s= std::to_string(c);
                if (s.compare(0, word_size, word) == 0) cb(s);
            }
        } else {
            for (const auto &c: cur_recordset->list_keys) {
                std::string s = make_printable(c, true);
                if (s.compare(0, word_size, word) == 0) cb(s);
            }
        }
    }
}

static void command_seek(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    if (args.size() !=2) {
        throw std::invalid_argument("Usage: seek <key|document_id>");
    }
    auto nfo = get_kid(db, name);
    docdb::Direction dir = cur_recordset?cur_recordset->_dir:docdb::Direction::forward;
    if (nfo.second == docdb::Purpose::storage) {
        docdb::DocID id = std::strtoull(args[1].c_str(),nullptr,10);
        if (id == 0) throw std::invalid_argument("The document ID must be number greater than zero");
        cur_recordset = std::make_unique<RecordsetList>(db, nfo.first, nfo.second, dir, docdb::Row(id));
    } else {
        cur_recordset = std::make_unique<RecordsetList>(db, nfo.first, nfo.second, dir, docdb::RowView(args[1]));
    }

    cur_recordset->print_page();



}

static void command_select(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    if (args.size() !=2) {
        throw std::invalid_argument("Usage: select <prefix>");
    }
    auto nfo = get_kid(db, name);
    if (nfo.second == docdb::Purpose::storage) {
        throw std::runtime_error("The command select cannot be used for Storage collection");
    }
    docdb::RawKey k(nfo.first,docdb::Blob(args[1]));
    cur_recordset = std::make_unique<RecordsetList>(db, nfo.second, k, k.prefix_end());
    cur_recordset->print_page();
}

static void command_rewind(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    if (cur_recordset) {
        cur_recordset->rc.reset();
        cur_recordset->print_page();
    } else {
        throw std::runtime_error("There is no opened recordset. Use first/last/seek/select command");
    }
}

static void command_backup(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    if (args.size() != 3) throw std::invalid_argument("Usafe: backup <DocID-from> <filename>");
    auto info = get_kid(db, name);
    if (info.second != docdb::Purpose::storage) throw std::runtime_error("Only Storage can be backed up");
    docdb::DocID id = std::strtoull(args[1].c_str(),nullptr,10);
    docdb::StorageView<docdb::StringDocument> storage(db,info.first,docdb::Direction::forward, {}, true);
    docdb::DocID ref = storage.get_last_document_id()+1;
    std::string fname = args[2] + "_" + std::to_string(ref);
    std::cout << "Backup_file: " << fname << std::endl;
    std::ofstream outf(fname, std::ios::out|std::ios::trunc|std::ios::binary);
    docdb::Row buffer;
    docdb::Row buffer_size;
    for (const auto &row : storage.select_from(id, docdb::Direction::forward)) {
        buffer.clear();
        buffer.append(row.id, row.previous_id, docdb::Blob(row.content));
        buffer_size.clear();
        buffer_size.append(buffer.size());
        std::string_view b(buffer);
        std::string_view bs(buffer_size);
        outf.write(bs.data(), bs.size());
        outf.write(b.data(), b.size());
    }
}

static void command_private(const docdb::PDatabase &db, std::string name, const std::vector<std::string> &args) {
    auto nfo = get_kid(db, name);
    std::cout << "Table's private area" << std::endl;
    cur_recordset = std::make_unique<RecordsetList>(db, docdb::Purpose::private_area,
            docdb::RawKey(docdb::Database::system_table, nfo.first),
            docdb::RawKey(docdb::Database::system_table, static_cast<docdb::KeyspaceID>(nfo.first+1))),
    cur_recordset->print_page();

}


static Command commands[] = {
        {"compact", command_compact, empty_completion},
        {"levels", command_levels, empty_completion},
        {"list", command_list, empty_completion},
        {"quit", command_quit, empty_completion},
        {"use", command_use, list_of_tables},
        {"erase", command_erase, list_of_tables},
        {"create", command_create,purpose_completion},
        {"first", command_iterate_from_first,empty_completion},
        {"last", command_iterate_from_last,empty_completion},
        {"document", command_document,completion_current_ids},
        {"seek", command_seek,completion_current_keys},
        {"select", command_select,completion_current_keys},
        {"rewind", command_rewind,empty_completion},
        {"backup", command_backup,completion_current_ids},
        {"private", command_private,completion_current_keys}

};


ReadLine::CompletionList genCompletionList(docdb::PDatabase db) {
    ReadLine::CompletionList out;
    for (const auto &c : commands) {
        out.push_back({ReadLine::Pattern(c.name+" (.*)"), ReadLine::HintGenerator([db, &c](const char *word, std::size_t word_size ,const std::cmatch &m, const ReadLine::HintCallback &cb) {
            c.completion(db, word, word_size,  cb);
        })});
    }
    out.push_back({"([a-zA-Z0-9]*)", [](const char *word, std::size_t word_size ,const std::cmatch &m, const ReadLine::HintCallback &cb){
        for (const auto &c : commands) {
            if (c.name.compare(0,word_size,word) == 0) cb(c.name);
        }
    }});
    return out;
}

void print_help(const char *arg0) {
    std::cout << "Usage: " << arg0 << " -hcd <database_path>\n\n"
            "-h           show help\n"
            "-c           create database if missing\n"
            "-d           show debug messages\n";

}


int main(int argc, char **argv) {

    bool create_flag = false;
    bool debug_mode = false;
    int opt;
    while ((opt = getopt(argc, argv, "hcd")) != -1) {
        switch (opt) {
            case 'h': print_help(argv[0]);
                      exit(0);
                      break;
            case 'c': create_flag = true;
                      break;
            case 'd': debug_mode = true;
                      break;
            default:
                      std::cerr << "Unknown option, use -h for help" << std::endl;
                      return 1;
        }
    }
    if (optind == argc) {
        std::cerr << "Missing name of database, use -h for help" << std::endl;
        return 1;
    }
    docdb::PDatabase db = open_db(argv[optind], create_flag, debug_mode);

    print_db_info(db);
    print_list_tables(db);
    std::string cur_table;

    ReadLine rl({"docdb:> ",0," "});
    rl.setCompletionList(genCompletionList(db));
    std::string line;
    while (!exit_flag && rl.read(line)) {
        std::istringstream ln(line);
        auto args = generateWordVector(ln);
        if (!args.empty()) {
            auto citer = std::find_if(std::begin(commands), std::end(commands), [&](const auto &cmd) {
                return cmd.name == args[0];
            });
            try {
                if (citer == std::end(commands)) {
                    std::cerr << "Unknown command: " << args[0] << std::endl;
                } else if (citer->name == "use") {
                    if (args.size() != 2) {
                        throw std::invalid_argument("use <collection_name>");
                    } else {
                        auto tbl = db->find_table(args[1]);
                        if (tbl.has_value()) {
                            cur_table = args[1];
                            rl.setPrompt("docdb:/"+cur_table+"> ");
                            cur_recordset.reset();
                        } else {
                            throw std::runtime_error("Collection doesn't exists: " + args[1]);
                        }
                    }
                } else {
                    citer->exec(db, cur_table, args);
                }
            } catch (std::invalid_argument &e) {
                std::cerr << "? " << e.what() << std::endl;
            } catch (std::exception &e) {
                std::cerr << "ERROR: " << e.what() << std::endl;
            }
        } else {
            if (cur_recordset) {
                cur_recordset->print_page();
            }
        }
    }
    cur_recordset.reset();


}
