#pragma once
#include "../inc/include.hh"
#include "../util/hdfs.h"

enum class HDFSMode {
    READ = 0,
    WRITE = 1
};


class HDFSHandler {
public:
    HDFSHandler(const std::string& ip, int port);
    ~HDFSHandler();
    void write2HDFS(hdfsFile file, char* buf, int bufSize);
    void readFromHDFS(hdfsFile file, char* buf, int bufSize);
    void pReadFromHDFS(hdfsFile file, char* buf, int pos, int bufSize);
    hdfsFile openFile(const std::string& objname, HDFSMode mode);
    void closeFile(hdfsFile file);
private:
    hdfsFS _fs;


};