#pragma once
#include "Config.hh"
#include "ECDataPacket.hh"
#include "BlockingQueue.hh"
#include "../inc/include.hh"
#include "../util/RedisUtil.hh"
#include "../protocol/AGCommand.hh"

class ECInputStream {
public:
    ECInputStream(Config* conf, const std::string& filename);
    ~ECInputStream();
    void output2File(const std::string& saveas);

private:
    void init();
    void readWorker(int objIdx);

    std::string _filename;
    Config* _conf;
    redisContext* _localCtx;
    int _fileSize;
    std::unordered_map<int, ECDataPacket*> _objMap;
    std::vector<std::thread> _readThreads;
};



