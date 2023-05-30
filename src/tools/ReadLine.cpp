#include "ReadLine.h"

#include <readline/readline.h>
#include <readline/history.h>
#include <iostream>
#include <mutex>
#include <unistd.h>
#include <sys/types.h>
#include <pwd.h>
#include <dirent.h>
#include <sys/stat.h>


std::recursive_mutex ReadLine::gmx;
ReadLine *ReadLine::curInst = nullptr;

//THIS UGLY C-HYBRID FUNCTION IS BRIDGE BETWEEN UGLY C INTERFACE AND C++ INTERFACE
//function is super global
char **ReadLine::global_completion (const char *, int start, int end) {

    //all is static/global as the readline itself is not MT safe and
    //this function cannot be invoked in parallel

    static HintList _compl_tmp;
    static HintCallback _compl_cb = [](const std::string &sug){
        _compl_tmp.push_back(allocHintItem(sug));
    };

    if (!curInst) {
        return NULL;
    }

    _compl_tmp.clear();

    if (!curInst->onComplete(rl_line_buffer, start, end, _compl_cb)) {
        return NULL;
    }

    //return value - C list of strings
    char **list;
    //this is over
    rl_attempted_completion_over = 1;
    //this function contains bunch of C patterns - UNSAFE CODE!
    //we will use malloc and raw string pointers
    //there is no way how to overcome this
    //because readline library has C interface

    //if generated list of completions is empty
    //we must return nullptr - we cannot return empty list
    //because it sigfaults
    if (_compl_tmp.empty()) {
        return nullptr;

    //special case is when list of completions contains one item
    //we generate list of two items
    //where first item is our string
    //second item is NULL
    }else if (_compl_tmp.size() == 1) {
        list = reinterpret_cast<char **>(calloc(2,sizeof(char *)));
        list[0] = _compl_tmp[0].release();
        list[1] = nullptr;

    //if multiple matches
    //we must return list + 2 extra items
    //first item contains common part of all matches
    //other items are initialized from completion list
    //and last item is NULL
    } else {
        list = reinterpret_cast<char **>(calloc(_compl_tmp.size()+2,sizeof(char *)));

        //we must compute common part of all matches
        std::size_t common = std::numeric_limits<std::size_t>::max();
        for (const auto &x: _compl_tmp) {
            char *z1 = x.get();
            char *z2 = _compl_tmp[0].get();
            std::size_t l = 0;
            while (l < common && z1[l] && z1[l] == z2[l] ) ++l;
            common = l;
            if (common == 0) break;
        }
        char *comstr = static_cast<char *>(malloc(common+1));
        strncpy(comstr, _compl_tmp[0].get(), common);
        comstr[common] = 0;
        list[0] = comstr;

        //copy items, starting at index 1
        char **it = list+1;
        //it is iterator
        for (auto &x: _compl_tmp) {
            //assign to c-array
            *it++ = x.release();
        }
        //set null as final item
        *it++ = nullptr;

    }
    //return list (rl will handle deallocation)
    return list;
}

char *ReadLine::completion_word_break_hook() {
    if (curInst) {
        return const_cast<char *>(curInst->completionWordBreakHook(rl_line_buffer, rl_end, rl_point));
    } else {
        return rl_completer_word_break_characters;
    }
}

void ReadLine::CLikeDeleter::operator()(char *str) const {
    if (str) rl_free(str);
}

bool ReadLine::filterHistory(const std::string &line) noexcept {
    bool ok = !line.empty() && line != _prev_line;
    if (ok) _prev_line = line;
    return ok;
}

void ReadLine::postprocess(std::string &line) noexcept {
    //empty - no postprocessing
}

const char* ReadLine::completionWordBreakHook(const char *,std::size_t , std::size_t ) noexcept {
    return _config.word_break_chars.c_str();
}

ReadLine::HintItem ReadLine::allocHintItem(const std::string &str) {
    return HintItem(strdup(str.c_str()));
}

void ReadLine::editHints(const char *wholeLine, std::size_t start, std::size_t end, HintList &list) noexcept  {
    //empty;
}

ReadLine::HintItem ReadLine::allocHintItem(const std::string &str, std::size_t offset, std::size_t len) {
    offset = std::min(offset, str.length());
    len = std::min(len,str.length()-offset);
    char *c = reinterpret_cast<char *>(malloc(len+1));
    std::copy(str.begin()+offset, str.begin()+offset+len, c);
    c[len] = 0;
    return HintItem(c);

}


void ReadLine::initLibsInternal() {
    rl_initialize ();
    using_history();
    rl_attempted_completion_function = &global_completion;
    rl_completion_word_break_hook = &completion_word_break_hook;

}

static std::once_flag initLibsFlag;


void ReadLine::initLibs() {
    std::call_once(initLibsFlag, initLibsInternal);
}




void ReadLine::restoreRLState() const {
    if (_config.history_limit) {
        stifle_history(_config.history_limit);
    } else{
        unstifle_history();
    }
}


ReadLine::~ReadLine() {
    detach();
}

bool ReadLine::read(std::string &line) {
    if (!isatty(0)) { //use readline only if the standard input is terminal
        return !!std::getline(std::cin, line); //otherwise use standard std::getline
    }

    bool ok;
    run_locked([&]{
       static std::vector<HIST_ENTRY *> ptrentries;
       _history.enumHistoryInOrder([&](const std::string &s){
           ptrentries.push_back(alloc_history_entry(const_cast<char *>(s.c_str()),strdup("")));
       });
       //retrieve current history
       HISTORY_STATE *prevst = history_get_history_state();
       //Set our history as state;
       HISTORY_STATE st = {};
       st.size = st.length = ptrentries.size();
       ptrentries.push_back(nullptr);
       st.entries = ptrentries.data();
       history_set_history_state(&st);
       //perform readline
       auto ln = readline(_config.prompt.c_str());
       //restore previous history
       history_set_history_state(prevst);
       //free history state
       rl_free(prevst);
       //clear static entries
       for (auto &x : ptrentries) free_history_entry(x);
       ptrentries.clear();
       //if line is null
       if (!ln) {
           //exiting now
           ok = false;
       } else {
           //copy our line
           line = ln;
           //filter history
           if (filterHistory(line)) {
               _history.add(line);
           }
           //free line
           rl_free(ln);
           //its ok
           ok = true;
       }
    });
    if (ok) postprocess(line);
    return ok;
}


ReadLine::ReadLine():_dirty(false) {
    initLibs();
    _history.setLimit(_config.history_limit);
}

ReadLine::ReadLine(const ReadLineConfig &cfg):_config(cfg),_dirty(false) {
    initLibs();
    _history.setLimit(_config.history_limit);
}

ReadLine::ReadLine(ReadLine &&other)
:_config(std::move(other._config))
,_completionList(std::move(other._completionList))
,_history(std::move(other._history))
{
    other.detach();
}

ReadLine& ReadLine::operator =(ReadLine &&other) {
    if (this != &other) {
        other.detach();
        _config = std::move(other._config);
        _history = std::move(other._history);
        _completionList = std::move(other._completionList);
    }
    return *this;
}

void ReadLine::setPrompt(const std::string &prompt) {
    _config.prompt =prompt;
}

void ReadLine::setPrompt(std::string &&prompt) {
    _config.prompt = std::move(prompt);
}

void ReadLine::saveRLState() const {

    _dirty = false;
}

bool ReadLine::onComplete(const char *wholeLine, std::size_t start, std::size_t end, const HintCallback &cb) noexcept {
    if (_completionList.empty()) return false;

    const char *word = wholeLine + start;
    auto sz = end - start;
    std::cmatch m;
    for (const auto &x: _completionList) {
        if (std::regex_match<const char *>(wholeLine, wholeLine+start, m, x.pattern)) {
            x.generator(word, sz, m, cb);
        }
    }

    return true;
}

void ReadLine::setCompletionList(CompletionList &&list) {
    _completionList = std::move(list);
}

void ReadLine::setConfig(const ReadLineConfig &config) {
    detach();
    _config = config;
    _history.setLimit(_config.history_limit);
}

const ReadLineConfig &ReadLine::getConfig() const {
    return _config;
}


ReadLine::HintGenerator::HintGenerator(const std::initializer_list<std::string> &options)
:HintGenerator(std::vector<std::string>(options))
{

}

ReadLine::HintGenerator::HintGenerator(std::vector<std::string> &&options)
:GenFn([lst = std::move(options)](const char *word, std::size_t sz,const std::cmatch &m, const HintCallback &cb) {
    for (const auto &x: lst) {
        if (x.compare(0, sz, word) == 0) cb(x);
    }
})
{
}

void ReadLine::setAppName(const std::string &appName) {
    const char *homedir = getenv("HOME");
    if (homedir) {
        struct passwd *pw = getpwuid(getuid());
        homedir = pw->pw_dir;
    }
    std::string path = homedir;
    path.append("/.").append(appName).append("_history");
    setHistoryFile(path);
}

void ReadLine::setHistoryFile(const std::string &file) {
    _history.bindFile(file);
}


void ReadLine::detach() const {
    if (_dirty == true) {
        std::lock_guard<std::recursive_mutex> _(gmx);
        if (curInst == this) {
            saveRLState();
            curInst = nullptr;
        }
    }
}


class FileLookup {
public:
    FileLookup(const std::string &rootPath, const std::string &pattern, bool pathname)
        :_root(rootPath)
        ,_pattern(pattern)
        ,_match_all(pattern.empty())
        ,_pathname(pathname) {}

    void operator()(const char *word, std::size_t word_size ,const std::cmatch &m, const ReadLine::HintCallback &cb) const;

protected:
    std::string _root;
    std::regex _pattern;
    bool _match_all;
    bool _pathname;
};

ReadLine::HintGenerator ReadLine::fileLookup(const std::string &rootPath,
        const std::string &pattern, bool pathname) {
    return HintGenerator(GenFn(FileLookup(rootPath, pattern, pathname)));
}

inline void FileLookup::operator ()(const char *word, std::size_t word_size,
        const std::cmatch &m, const ReadLine::HintCallback &cb) const {
    std::string r = _root;
    std::string w (word, word_size);
    std::string entry;
    std::size_t count = 0;
    if (_pathname && w.find('/') != w.npos) {
        //word starting by / - means that we search from root
        if (w[0] == '/') {
            r = "/";    //set root
            entry = r;
            w.erase(0,1);  //erase separator
        //if there is a path and it doesn't end by '/' append it now
        } else if (!r.empty() || r.back() != '/')  {
            r.push_back('/');
        }
        auto sep = w.rfind('/'); //find last /
        if (sep != w.npos) {
            r.append(w.begin(), w.begin()+sep+1); //append path to r
            entry.append(w.begin(), w.begin()+sep+1); //append path to entry
            w.erase(w.begin(), w.begin()+sep+1); //remove from w
        }
    }
    auto rs = r.length();
    auto es = entry.length();

    std::string only_entry;
    //why we use opendir instead filesystem
    //we have already dependency on Posix, where opendir is part of it
    //and we don't include additional dependency on filesystem/boost filesystem
    //which is also problematic on edge between C++14 and C++17
    //so lets stick with old fashion "Posix way"
    DIR *dir = opendir(r.c_str());
    if (dir) {
        const struct dirent *e = readdir(dir);
        while (e) {
            const char *dname_beg = e->d_name;
            const char *dname_end = e->d_name+std::strlen(e->d_name);
            entry.resize(es);
            entry.append(dname_beg, dname_end);
            bool isdir = false;
            switch (e->d_type) {
                default: isdir = false;break;
                case DT_DIR: isdir = true;break;
                case DT_LNK:
                case DT_UNKNOWN: if (_pathname) {
                        struct stat st;
                        r.resize(rs);
                        r.push_back('/');
                        r.append(dname_beg, dname_end);
                        stat(r.c_str(), &st);
                        isdir = S_ISDIR(st.st_mode);
                } else {
                    isdir = false;
                }
            }
            if (_pathname && isdir) {
                entry.push_back('/');
            }
            if (_match_all || std::regex_match(entry,_pattern)) {
                if (entry.compare(0,word_size,word) ==0) {
                    count++;
                    if (count == 1 && isdir) only_entry = entry;
                    cb(entry);
                }
            }
            e = readdir(dir);
        }
        closedir(dir);
    }
    if (count == 1 && !only_entry.empty()) {
        this->operator ()(only_entry.c_str(), only_entry.length(), m, cb);
    }
}

void ReadLine::History::add(std::string &&item) {
    afterAdd(_history.emplace(std::move(item), _nextOrder));
    addToFile(item);
}
void ReadLine::History::afterAdd(std::pair<HistoryMapOrder::iterator, bool> &&iter) {
    if (!iter.second) {
       _order.erase(&(*iter.first));
       iter.first->second = _nextOrder;
    }
    _order.insert(&(*iter.first));
    ++_nextOrder;
    while (_limit && _order.size() > _limit) {
        auto f = _order.begin();
        const std::string &s = (*f) -> first;
        _order.erase(f);
        _history.erase(s);
    }
}

void ReadLine::History::add(const std::string &item) {
    afterAdd(_history.emplace(item, _nextOrder));
    addToFile(item);
}

unsigned int ReadLine::History::loadFile(const std::string &fname) {
    unsigned int cnt = 0;
    _order.clear();
    _history.clear();
    _nextOrder = 0;
    std::ifstream f(fname);
    if (!!f) {
        std::string line;
        while (!!std::getline(f,line)) {
            add(line);
            cnt++;
        }
    }
    return cnt;
}

void ReadLine::History::bindFile(const std::string &fname) {
    _histfile.clear();
    unsigned int cnt = loadFile(fname);
    if (cnt > _order.size()*2) {//defragmentate when 50% fragmentation
        //reorder and defragmentate history file
        std::ofstream f(fname, std::ios::out| std::ios::trunc);
        for (const auto &o: _order) {
            f << (*o).first << std::endl;
        }
    }
    _histfile = fname;
}

void ReadLine::History::setLimit(unsigned int limit) {
    _limit = limit;
}

bool ReadLine::History::OrderHistory::operator () (
        const HistoryMapOrder::value_type *a,
        const HistoryMapOrder::value_type *b)  const {
    return a->second < b->second;
}

void ReadLine::History::addToFile(const std::string &item) {
    if (!_histfile.empty()) {
        //why open and close file?
        //file can be shared by multiple instances
        //and file can be also subject of defragmentation when new file is geneted
        //so each add we can actually append to complete different file
        std::ofstream f(_histfile, std::ios::out| std::ios::app);
        f << item << std::endl;
    }
}
