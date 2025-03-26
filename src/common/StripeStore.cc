#include "StripeStore.hh"

using namespace std;
StripeStore::StripeStore(Config* conf) {
	_conf = conf;
	_curNodeId = 0;
}

bool StripeStore::existEntry(string filename) {
  unordered_map<string, SSEntry*>::iterator it = _ssEntryMap.find(filename);
  return it == _ssEntryMap.end() ? false:true;
}

void StripeStore::insertEntry(SSEntry* entry) {

}

StripeStore::~StripeStore() {
	_fileRecipesMutex.lock();
	for (auto it = _fileRecipes.begin(); it != _fileRecipes.end(); it++) {
		delete it->second;
	}
	_fileRecipesMutex.unlock();
}

bool StripeStore::existFile(const std::string& filename) {
	// LOG_INFO("StripeStore::existFile %s start", filename.c_str());
	_fileRecipesMutex.lock();
	// LOG_INFO("StripeStore::existFile get lock");
	bool ret = _fileRecipes.find(filename) != _fileRecipes.end();
	// LOG_INFO("StripeStore::existFile get ret: %d", ret);
	_fileRecipesMutex.unlock();
	// LOG_INFO("StripeStore::existFile %s done", filename.c_str());
	return ret;
}

std::vector<int> StripeStore::insertFile(const std::string& filename, int objNum) {
	LOG_INFO("StripeStore::inserFile %s start", filename.c_str());
	_fileRecipesMutex.lock();
	FileRecipe* fileRecipe = new FileRecipe();
	fileRecipe->filename = filename;
	fileRecipe->objNum = objNum;
	for (int i = 0; i < objNum; i++) {
		fileRecipe->objLocs.push_back(_curNodeId);
		_curNodeId = (_curNodeId + 1) % _conf->_agent_num;
	}
	_fileRecipes[filename] = fileRecipe;
	_fileRecipesMutex.unlock();
	return fileRecipe->objLocs;
}