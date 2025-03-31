#pragma once
#include "../inc/include.hh"

class FileMeta {
public:
    FileMeta(char* meta);

    FileMeta(const std::string& filename, int fileSize, int objNum, const std::vector<int> objLocs);


    ~FileMeta();

    int getFileSize() const { return _fileSize; }
    int getObjNum() const { return _objNum; }
    std::vector<int> getObjLocs() const { return _objLocs; }
    std::string getFileName() const { return _filename; }
    void lock() { _mutex.lock(); }
    void unlock() { _mutex.unlock(); }

    void dumpFileMeto2Buf(char* buf) const;


private:
    int _fileSize;       // in Byte
    int _objNum;
    std::vector<int> _objLocs;
    std::mutex _mutex;
    std::string _filename;
};