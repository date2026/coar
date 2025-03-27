#ifndef _STRIPESTORE_HH_
#define _STRIPESTORE_HH_

#include "BlockingQueue.hh"
#include "Config.hh"
#include "SSEntry.hh"
#include "FileMeta.hh"
#include "../inc/include.hh"
#include "../protocol/CoorCommand.hh"

#define DELAY_THRESHOLD 1

using namespace std;


struct FileRecipe {
	std::string filename;
	int objNum;
	std::vector<int> objLocs;
	std::mutex fileRecipeMutex;
};
class StripeStore {
private:
    Config* _conf;

    unordered_map<string, SSEntry*> _ssEntryMap;  
    mutex _lockSSEntryMap;
    

    // backup
    string _entryStorePath = "entryStore";
    ofstream _entryStore;
    mutex _lockEntryStore;

    string _poolStorePath = "poolStore";
    ofstream _poolStore;
    mutex _lockPoolStore;
    
	std::unordered_map<std::string, FileMeta*> _fileMetas;
	int _curNodeId;
	std::mutex _fileMetasMutex;

public:
    StripeStore(Config* conf);
	~StripeStore();
    bool existEntry(string filename);
    void insertEntry(SSEntry* entry);

	bool existFile(const std::string& filename);
	std::vector<int> insertFile(const std::string& filename, int fileSize, int objNum);
	FileMeta* getFileMeta(const std::string& filename);
};

#endif
