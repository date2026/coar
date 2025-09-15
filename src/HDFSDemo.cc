#include <string>
#include <cassert>
#include <iostream>
#include <cstring>
#include "util/hdfs.h"
int main() {

    // hdfsFS fs = hdfsConnect("192.168.220.160", 9000);
    // assert(fs != nullptr && "Failed to connect to hdfs");

    // hdfsFile file = hdfsOpenFile(fs, "/test1", O_WRONLY | O_CREAT, 0, 0, 0);
    // assert(file != nullptr && "Failed to open file");


    // std::string buf = "Hello, World!";
    // int writeSize = hdfsWrite(fs, file, buf.c_str(), buf.size());
    // assert(writeSize == buf.size() && "Failed to write to file");


    // assert(hdfsCloseFile(fs, file) == 0);
    // assert(hdfsDisconnect(fs) == 0);   


    hdfsFS fs = hdfsConnect("192.168.220.160", 9000);
    assert(fs != nullptr && "Failed to connect to hdfs");

    hdfsFile file = hdfsOpenFile(fs, "/input_16MB_lmqobj_0", O_RDONLY, 0, 0, 0);
    assert(file != nullptr && "Failed to open file");


    char buffer[4194304];
    memset(buffer, 0, 4194304);
    int readSize = hdfsRead(fs, file, buffer, 4194304);
    std::cout << "Read " << readSize << std::endl;
    assert(hdfsCloseFile(fs, file) == 0);
    assert(hdfsDisconnect(fs) == 0);   
}