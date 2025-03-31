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
	_fileMetasMutex.lock();
	for (auto it = _fileMetas.begin(); it != _fileMetas.end(); it++) {
		delete it->second;
	}
	_fileMetasMutex.unlock();
}

bool StripeStore::existFile(const std::string& filename) {
	_fileMetasMutex.lock();
	bool ret = _fileMetas.find(filename) != _fileMetas.end();
	_fileMetasMutex.unlock();
	return ret;
}

/**
 * fileSize in Byte
 */
std::vector<int> StripeStore::insertFile(const std::string& filename, int fileSize, int objNum) {
	LOG_INFO("StripeStore::inserFile %s start", filename.c_str());
	_fileMetasMutex.lock();
	std::vector<int> objLocs;
    for (int i = 0; i < objNum; i++) {
        objLocs.push_back(_curNodeId);
		_curNodeId = (_curNodeId + 1) % _conf->_agent_num;
	}
    FileMeta* fileMeta = new FileMeta(filename, fileSize, objNum, objLocs);
	
	_fileMetas[filename] = fileMeta;
	_fileMetasMutex.unlock();
	return fileMeta->getObjLocs();
}

/**
 * get file meta
 * lock file meta
 * free by coordinator after done
 */
FileMeta* StripeStore::getFileMeta(const std::string& filename) {
    _fileMetasMutex.lock();
    assert(_fileMetas.find(filename) != _fileMetas.end());
    FileMeta* fileMeta = _fileMetas[filename];
    fileMeta->lock();
    _fileMetasMutex.unlock();
    return fileMeta;
}