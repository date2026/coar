#include "common/Config.hh"
#include "common/ECOutputStream.hh"
#include "common/ECInputStream.hh"
void usage() {
    std::cout << "usage: ./OECClient write filepath saveas ecid size(MB)" << std::endl;
    std::cout << "       ./OECClient read filename saveas" << std::endl;
    std::cout << "       ./OECClient encode filename" << std::endl;
    std::cout << "       ./OECClient repair" << std::endl;
}

const std::string confPath = "/root/coar/conf/1.json";


void write(const std::string& file_path, const std::string& saveas, const std::string& ecidpool, int size);
void read(const std::string& file_name, const std::string& saveas);
void encode(const std::string& filename, const std::string& ecdagPath);
void decode(const std::string& filename, const std::string& ecdagPath, std::vector<int>& survivedObjIds, int failedObjId);

int main(int argc, char** argv) {
    assert(argc >= 2);

    std::string req_type(argv[1]);
    if (req_type == "write") {
        assert(argc == 6);
        std::string file_path(argv[2]);
        std::string saveas(argv[3]);
        std::string ecidpool(argv[4]);
        int size = std::stoi(argv[5]);
        write(file_path, saveas, ecidpool, size);
    } else if (req_type == "read") {
        assert(argc == 4);
        std::string file_name(argv[2]);
        std::string saveas(argv[3]);
        LOG_INFO("read, file_name: %s, save_as: %s", file_name.c_str(), saveas.c_str());
        read(file_name, saveas);
    } else if (req_type == "encode") {
        assert(argc == 4);
        const std::string filename(argv[2]);
        const std::string ecdagPath(argv[3]);
        encode(filename, ecdagPath);
    } else if (req_type == "decode") {
        assert(argc > 4);
        const std::string filename(argv[2]);
        const std::string ecdagPath(argv[3]);
        std::vector<int> survivedObjIds;
        for (int i = 4; i < argc - 1; i++) {
            int objId = std::stoi(argv[i]);
            survivedObjIds.push_back(objId);
        } 
        int failedObjId = std::stoi(argv[argc - 1]);
        decode(filename, ecdagPath, survivedObjIds, failedObjId);
    } else if (req_type == "repair") {
        assert(argc == 2);
    } else {
        assert("Invalid request type");
    }

    return 0;
}


void write(const std::string& filePath, const std::string& saveAs, const std::string& ecidpool, int sizeinMB) {
    LOG_INFO("write, filePath: %s, saveAs: %s, ecidpool: %s, sizeinMB: %d", filePath.c_str(), saveAs.c_str(), ecidpool.c_str(), sizeinMB);
    
    Config* conf = new Config(confPath);
    
    struct timeval writeStart, writeEnd;
    gettimeofday(&writeStart, NULL);


    FILE* inputfile = fopen(filePath.c_str(), "rb");
    assert(inputfile != NULL && "Failed to open file");

    ECOutputStream* outstream = new ECOutputStream(conf, saveAs, ecidpool, "offline", sizeinMB);


    int sizeinByte = sizeinMB * 1024 * 1024;
    assert(sizeinByte % conf->_pktSize == 0);
    int pktNum = sizeinByte / conf->_pktSize;

    char* buf = new char [conf->_pktSize + sizeof(int)];    // datalen | data
    for (int i = 0; i < pktNum; i++) {
        int tmplen = htonl(conf->_pktSize);
        memcpy(buf, (char*)&tmplen, sizeof(int));
        fread(buf + sizeof(int), conf->_pktSize, 1, inputfile);
        outstream->write(buf, conf->_pktSize + sizeof(int));
    }
    delete [] buf;

    outstream->close();
    delete outstream;
    fclose(inputfile);
    delete conf;


    gettimeofday(&writeEnd, NULL);
    LOG_INFO("write time: %f ms, write throughput: %f MB/s", RedisUtil::duration(writeStart, writeEnd),
             (double)sizeinMB / RedisUtil::duration(writeStart, writeEnd) * 1000);

}

void read(const std::string& filePath, const std::string& saveas) {
    Config* conf = new Config(confPath);
    struct timeval readStart, readEnd;
    gettimeofday(&readStart, NULL);

    ECInputStream* instream = new ECInputStream(conf, filePath);
    instream->output2File(saveas);

    gettimeofday(&readEnd, NULL);
    cout << "read.overall.duration: " << RedisUtil::duration(readStart, readEnd)<< endl;
  
    delete instream;
    delete conf;
}


void encode(const std::string& filename, const std::string& ecdagPath) {
    LOG_INFO("encode, filename: %s, ecdagPath: %s", filename.c_str(), ecdagPath.c_str());
    Config* conf = new Config(confPath);

    struct timeval encodeStart, encodeEnd;
    gettimeofday(&encodeStart, NULL);
    
    // 1. send encode request to agent
    AGCommand* agCmd = new AGCommand();
    agCmd->buildType14(14, filename, ecdagPath);
    agCmd->sendTo(conf->_localIp);
    delete agCmd;

    // 2. wait for encode done
    const std::string waitEncodeDoneKey = filename + "_agent_encode_done";
    redisContext* waitEncodeDoneCtx = RedisUtil::createContext(conf->_localIp);
    assert(waitEncodeDoneCtx != NULL && "Failed to create redis context");
    LOG_INFO("ECClient wait for encode done, filename: %s, ecdagPath: %s", filename.c_str(), ecdagPath.c_str());

    redisReply* waitEncodeDoneReply = (redisReply*)redisCommand(waitEncodeDoneCtx, "blpop %s 0", waitEncodeDoneKey.c_str());
    assert(waitEncodeDoneReply != NULL && waitEncodeDoneReply->type == REDIS_REPLY_ARRAY 
            && waitEncodeDoneReply->elements == 2);
    freeReplyObject(waitEncodeDoneReply);
    redisFree(waitEncodeDoneCtx);
    LOG_INFO("ECClient receive agent encode done, filename: %s, ecdagPath: %s", filename.c_str(), ecdagPath.c_str());

    // 3. clean
    gettimeofday(&encodeEnd, NULL);
    LOG_INFO("encode time: %f ms, encode throughput: %f MB/s", RedisUtil::duration(encodeStart, encodeEnd), 
             (double)conf->_objSize / RedisUtil::duration(encodeStart, encodeEnd) * 1000);
    delete conf;
}


void decode(const std::string& filename, const std::string& ecdagPath, std::vector<int>& survivedObjIds, int failedObjId) {
    LOG_INFO("decode, filename: %s, ecdagPath: %s, survivedObjIds: %s, failedObjId: %d", 
             filename.c_str(), ecdagPath.c_str(), vec2String(survivedObjIds).c_str(), failedObjId);
    Config* conf = new Config(confPath);

    struct timeval decodeStart, decodeEnd;
    gettimeofday(&decodeStart, NULL);
    
    // 1. send decode request to agent
    AGCommand* agCmd = new AGCommand();
    agCmd->buildType15(15, filename, ecdagPath, survivedObjIds, failedObjId);
    agCmd->sendTo(conf->_localIp);
    delete agCmd;

    // 2. wait for decode done
    const std::string waitDecodeDoneKey = filename + "_agent_decode_done";
    redisContext* waitDecodeDoneCtx = RedisUtil::createContext(conf->_localIp);
    assert(waitDecodeDoneCtx != NULL && "Failed to create redis context");
    LOG_INFO("ECClient wait for decode done, filename: %s, ecdagPath: %s", filename.c_str(), ecdagPath.c_str());

    redisReply* waitDecodeDoneReply = (redisReply*)redisCommand(waitDecodeDoneCtx, "blpop %s 0", waitDecodeDoneKey.c_str());
    assert(waitDecodeDoneReply != NULL && waitDecodeDoneReply->type == REDIS_REPLY_ARRAY 
            && waitDecodeDoneReply->elements == 2);
    freeReplyObject(waitDecodeDoneReply);
    redisFree(waitDecodeDoneCtx);
    LOG_INFO("ECClient receive agent decode done, filename: %s, ecdagPath: %s", filename.c_str(), ecdagPath.c_str());

    // 3. clean
    gettimeofday(&decodeEnd, NULL);
    LOG_INFO("decode time: %f ms, decode throughput: %f MB/s", RedisUtil::duration(decodeStart, decodeEnd), 
             (double)conf->_objSize / RedisUtil::duration(decodeStart, decodeEnd) * 1000);
    std::ofstream logFile("/root/coar/build/ECClient.log", std::ios::app);
    assert(logFile.is_open());
    logFile << RedisUtil::duration(decodeStart, decodeEnd) << " " << 
               (double)conf->_objSize / RedisUtil::duration(decodeStart, decodeEnd) * 1000 << std::endl;
    logFile.close();
    delete conf;
}