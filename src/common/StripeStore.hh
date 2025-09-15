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
    const std::string _fileMetaPath = "../build/fileMeta";
	std::unordered_map<std::string, FileMeta*> _fileMetas;
	int _curNodeId;
	std::mutex _fileMetasMutex;
    void initFromFile();

public:
    StripeStore(Config* conf);
	~StripeStore();

	bool existFile(const std::string& filename);
	std::vector<int> insertFile(const std::string& filename, int fileSize, int objNum);
	FileMeta* getFileMeta(const std::string& filename);
    void dump2File();
};

#endif
