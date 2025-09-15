#include "StripeStore.hh"

using namespace std;
StripeStore::StripeStore(Config* conf) {
	_conf = conf;
	_curNodeId = 0;
    initFromFile();
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
    // TODO: curNodeId need to set correctly?
    _curNodeId = 0;
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
    ofs << _fileMetas.size() << std::endl;
    for (const auto& it : _fileMetas) {
        const std::string& filename = it.first;
        const FileMeta* fileMeta = it.second;
        ofs << filename << std::endl;
        ofs << fileMeta->getFileSize() << std::endl;                                    // _fileSize
        ofs << fileMeta->getObjNum() << std::endl;                                      // _objNum

        const std::vector<int>& objLocs = fileMeta->getObjLocs();                       // _objLocs
        ofs << objLocs.size() << std::endl;
        for (int i = 0; i < objLocs.size(); i++) {
            ofs << objLocs[i] << " ";
        }
        ofs << std::endl;

        const std::unordered_map<int, int>& objId2RowId = fileMeta->getObjId2RowId();   // _objId2RowId
        ofs << objId2RowId.size() << std::endl;
        for (const auto& it : objId2RowId) {
            ofs << it.first << " " << it.second << std::endl;
        }
        ofs << std::endl;
    }

    _fileMetasMutex.unlock();
}

void StripeStore::initFromFile() {
    _fileMetasMutex.lock();
    std::ifstream ifs(_fileMetaPath, std::ios::in);
    if (!ifs.is_open()) {
        _fileMetasMutex.unlock();
        return ;
    }
    int fileNum;
    ifs >> fileNum;
    for (int i = 0; i < fileNum; i++) {
        std::string filename;
        int fileSize, objNum;
        // _filename, _fileSize, _objNum
        ifs >> filename >> fileSize >> objNum;
        // _objLocs
        int objLocsSize;
        ifs >> objLocsSize;
        std::vector<int> objLocs;
        for (int j = 0; j < objLocsSize; j++) {
            int tmp, rowId;
            ifs >> tmp;
            objLocs.push_back(tmp);
        }
        // _objId2RowId
        int objId2RowIdSize;
        ifs >> objId2RowIdSize;
        std::unordered_map<int, int> objId2RowId;
        for (int j = 0; j < objId2RowIdSize; j++) {
            int objId, rowId;
            ifs >> objId >> rowId;
            objId2RowId[objId] = rowId;
        }
        FileMeta* fileMeta = new FileMeta(filename, fileSize, objNum, objLocs, objId2RowId);
        _fileMetas[filename] = fileMeta;
    }

    _fileMetasMutex.unlock();
}