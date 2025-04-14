#pragma once
#include "../inc/include.hh"

class FileMeta {
public:
    FileMeta(char* meta);

    FileMeta(const std::string& filename, int fileSize, int objNum, const std::vector<int> objLocs);
    FileMeta(const std::string& filename, int fileSize, int objNum, const std::vector<int>& objLocs, 
             const std::unordered_map<int, int>& objId2RowId);

    ~FileMeta();

    int getFileSize() const { return _fileSize; }
    int getObjNum() const { return _objNum; }
    std::vector<int> getObjLocs() const { return _objLocs; }
    std::string getFileName() const { return _filename; }
    std::unordered_map<int, int> getObjId2RowId() const { return _objId2RowId; }
    int getRowId(int objId) const { assert(_objId2RowId.find(objId) != _objId2RowId.end()); return _objId2RowId.at(objId); }
    void setRowId(int objId, int rowId) { _objId2RowId[objId] = rowId; }
    void lock() { _mutex.lock(); }
    void unlock() { _mutex.unlock(); }

    void dumpFileMeto2Buf(char* buf) const;
    void dumpFileMeta() const;

private:
    int _fileSize;       // in Byte
    int _objNum;
    std::vector<int> _objLocs;
    std::unordered_map<int, int> _objId2RowId;
    std::mutex _mutex;
    std::string _filename;
};