#include "HDFSHandler.hh"


HDFSHandler::HDFSHandler(const std::string& ip, int port) {
    _fs = hdfsConnect(ip.c_str(), port);
    assert(_fs != nullptr && "Failed to connect to hdfs");
    LOG_INFO("Connected to hdfs, ip: %s, port: %d", ip.c_str(), port);
}

HDFSHandler::~HDFSHandler() {
    int ret = hdfsDisconnect(_fs);
    assert(ret == 0 && "Failed to disconnect hdfs");
}



hdfsFile HDFSHandler::openFile(const std::string& objname, HDFSMode mode) {
    assert(_fs != nullptr);
    hdfsFile file;
    switch (mode) {
        case HDFSMode::READ:
            assert(false && "Not implemented");
        case HDFSMode::WRITE:
            file = hdfsOpenFile(_fs, objname.c_str(), O_WRONLY | O_CREAT, 0, 0, 0);
            assert(file != nullptr && "Failed to open file");
            break;
        default:
            assert(false && "Invalid mode");
    }    
    return file;
}

void HDFSHandler::write2HDFS(hdfsFile file, char* buf, int bufSize) {
    assert(_fs != nullptr && file != nullptr);
    int writeSize = hdfsWrite(_fs, file, buf, bufSize);
    assert(writeSize == bufSize && "Failed to write to file");
}



void HDFSHandler::closeFile(hdfsFile file) {
    assert(_fs != nullptr);
    int ret = hdfsCloseFile(_fs, file);
    assert(ret == 0 && "Failed to close file");
}