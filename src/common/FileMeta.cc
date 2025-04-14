#include "FileMeta.hh"


/**
 * called by read agent after receive file meta from coordinator to paser file meta
 */
FileMeta::FileMeta(char* meta) {
    int offset = 0;
    // file size
    memcpy(&_fileSize, meta + offset, sizeof(int));
    _fileSize = ntohl(_fileSize);
    offset += sizeof(int);
    // obj num
    memcpy(&_objNum, meta + offset, sizeof(int));
    _objNum = ntohl(_objNum);
    offset += sizeof(int);
    // obj locs
    for (int i = 0; i < _objNum; i++) {
        int loc;
        memcpy(&loc, meta + offset, sizeof(int));
        _objLocs.push_back(ntohl(loc));
        offset += sizeof(int);
    }
}

FileMeta::FileMeta(const std::string& filename, int fileSize, int objNum, const std::vector<int> objLocs) {
    _fileSize = fileSize;
    _objNum = objNum;
    _objLocs = objLocs;
    _filename = filename;
    for (int i = 0; i < _objNum; i++) {
        _objId2RowId[i] = i;
    }
}


FileMeta::FileMeta(const std::string& filename, int fileSize, int objNum, const std::vector<int>& objLocs, 
                   const std::unordered_map<int, int>& objId2RowId) {
    _fileSize = fileSize;
    _objNum = objNum;
    _objLocs = objLocs;
    _filename = filename;
    _objLocs = objLocs;
    _objId2RowId = objId2RowId;    
}

FileMeta::~FileMeta() {}

/**
 * called by coordinator to send file meta to read agent
 */
void FileMeta::dumpFileMeto2Buf(char* buf) const {
    int offset = 0;
    // file size
    int tmpFileSize = htonl(_fileSize);
    memcpy(buf + offset, (char*)&tmpFileSize, sizeof(int));
    offset += sizeof(int);
    // obj num
    int tmpObjNum = htonl(_objNum);
    memcpy(buf + offset, (char*)&tmpObjNum, sizeof(int));
    offset += sizeof(int);
    // obj locs
    for (int i = 0; i < _objNum; i++) {
        int tmpLoc = htonl(_objLocs[i]);
        memcpy(buf + offset, (char*)&tmpLoc, sizeof(int));
        offset += sizeof(int);
    }
}

void FileMeta::dumpFileMeta() const {
    LOG_INFO("dump file meta");
    LOG_INFO("obj locs: %s", vec2String(_objLocs).c_str());
    LOG_INFO("obj id to row id: ");
    for (auto it = _objId2RowId.begin(); it != _objId2RowId.end(); it++) {
        LOG_INFO("obj id: %d, row id: %d", it->first, it->second);
    }
}