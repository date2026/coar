#include "StripeStore.hh"

using namespace std;
StripeStore::StripeStore(Config* conf) {
	_conf = conf;
	_curNodeId = 0;
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


void StripeStore::dump2File() {
    _fileMetasMutex.lock();
    std::ofstream ofs(_fileMetaPath, std::ios::out | std::ios::trunc);
    if (!ofs.is_open()) {
        assert(false && "Failed to open fileMeta file");
    }

    for (const auto& it : _fileMetas) {
        const std::string& filename = it.first;
        const FileMeta* fileMeta = it.second;
        ofs << filename << std::endl;
        ofs << fileMeta->getFileSize() << std::endl;
        ofs << fileMeta->getObjNum() << std::endl;
        const std::vector<int>& objLocs = fileMeta->getObjLocs();
        for (int i = 0; i < objLocs.size(); i++) {
            ofs << objLocs[i] << " " << fileMeta->getRowId(objLocs[i]) << std::endl;
        }
    }

    _fileMetasMutex.unlock();
}