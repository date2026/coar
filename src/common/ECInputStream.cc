#include "ECInputStream.hh"


ECInputStream::ECInputStream(Config* conf, const std::string& filename) {
    _conf = conf;
    _filename = filename;
    _localCtx = RedisUtil::createContext(_conf->_localIp);
    assert(_localCtx != NULL && "create redis context error");
    init();
}


ECInputStream::~ECInputStream() {
    redisFree(_localCtx);
}

void ECInputStream::init() {

    // 1. get file size from agent
    AGCommand* agCmd = new AGCommand();
    agCmd->buildType1(1, _filename);
    agCmd->sendTo(_conf->_localIp);
    delete agCmd;
    LOG_INFO("ECInputStream send file size request to local agent, filename: %s", _filename.c_str());

    const std::string wait4FileSizeKey = _filename + "_filesize";
    redisReply* rReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", wait4FileSizeKey.c_str());
    assert(rReply != NULL && rReply->type == REDIS_REPLY_ARRAY && rReply->elements == 2);
    char* fileSizeStr = rReply->element[1]->str;
    int tmpFileSize;
    memcpy(&tmpFileSize, fileSizeStr, sizeof(int));
    freeReplyObject(rReply);
    _fileSize  = ntohl(tmpFileSize);
    LOG_INFO("ECInputStream get file size, filename: %s, fileSize: %d", _filename.c_str(), _fileSize);


    // 2. create read threads
    assert(_fileSize % (_conf->_objSize * 1024 * 1024) == 0);
    int objNum = _fileSize / (_conf->_objSize * 1024 * 1024);
    _readThreads = std::vector<std::thread>(objNum);
    for (int i = 0; i < objNum; i++) {
        // readWorker(i);
        _readThreads[i] = std::thread([=]{ readWorker(i); });
    }
    
}

void ECInputStream::readWorker(int objIdx) {
    const std::string readObjKey = _filename + "_lmqobj_" + std::to_string(objIdx) + "_read";
    LOG_INFO("ECInputStream::readWorker start, objname: %s, objIdx: %d", readObjKey.c_str(), objIdx);
    redisContext* readObjCtx = RedisUtil::createContext(_conf->_localIp);
    redisReply* rReply = (redisReply*)redisCommand(readObjCtx, "blpop %s 0", readObjKey.c_str());
    assert(rReply != NULL && rReply->type == REDIS_REPLY_ARRAY && rReply->elements == 2);
    char* content = rReply->element[1]->str;
    LOG_INFO("ECInputStream::readWorker get obj content, objname: %s, objIdx: %d", readObjKey.c_str(), objIdx);
    ECDataPacket* pkt = new ECDataPacket(_conf->_objSize * 1024 * 1024);
    pkt->setData(content);
    freeReplyObject(rReply);
    redisFree(readObjCtx);
    LOG_INFO("ECInputStream::readWorker done, objname: %s, objIdx: %d", readObjKey.c_str(), objIdx);
    assert(_objMap.find(objIdx) == _objMap.end());
    _objMap[objIdx] = pkt;
}


void ECInputStream::output2File(const std::string& saveas) {
    FILE* outputfile = fopen(saveas.c_str(), "wb");
    assert(outputfile != NULL && "Failed to open file");

    for (int i = 0; i < _readThreads.size(); i++) {
        _readThreads[i].join();
    }

    for (int i = 0; i < _readThreads.size(); i++) {
        // _readThreads[i].join();
        assert(_objMap.find(i) != _objMap.end());
        ECDataPacket* pkt = _objMap[i];
        char* data = pkt->getData();
        assert(pkt != NULL && pkt->getDatalen() == _conf->_objSize * 1024 * 1024);
        fwrite(data, 1, _conf->_objSize * 1024 * 1024, outputfile);

        delete pkt;
    }

    fclose(outputfile);
    LOG_INFO("ECInputStream::output2File done, saveas: %s", saveas.c_str());
}