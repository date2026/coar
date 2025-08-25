#pragma once
#include <sstream>
#include "../common/Config.hh"
#include "../inc/include.hh"
#include "../util/galois.h"
#include "../common/FileMeta.hh"
#include "../protocol/AGCommand.hh"




enum class ECTaskType {
    SEND,
    RECEIVE,
    ENCODE,
    PERSIST,
    FETCH,
    ENCODE_PARTIAL
};

static ECTaskType str2ECTaskType(const std::string& str) {
    if (str == "SEND") {
        return ECTaskType::SEND;
    } else if (str == "RECEIVE") {
        return ECTaskType::RECEIVE;
    } else if (str == "ENCODE") {
        return ECTaskType::ENCODE;
    } else if (str == "PERSIST") {
        return ECTaskType::PERSIST;
    } else if (str == "FETCH") {
        return ECTaskType::FETCH;
    } else if (str == "ENCODE_PARTIAL") {
        return ECTaskType::ENCODE_PARTIAL;
    } else {
        assert(false && "undefined ECTaskType");
    }
}

static ECTaskType int2ECTaskType(int type) {
    switch (type) {
        case 0:
            return ECTaskType::SEND;
        case 1:
            return ECTaskType::RECEIVE;
        case 2:
            return ECTaskType::ENCODE;
        case 3:
            return ECTaskType::PERSIST;
        case 4:
            return ECTaskType::FETCH;
        case 5: 
            return ECTaskType::ENCODE_PARTIAL;
        default:
            assert(false && "undefined ECTaskType");
    }
}

static int ECTaskType2int(ECTaskType type) {
    switch (type) {
        case ECTaskType::SEND:
            return 0;
        case ECTaskType::RECEIVE:
            return 1;
        case ECTaskType::ENCODE:
            return 2;
        case ECTaskType::PERSIST:
            return 3;
        case ECTaskType::FETCH:
            return 4;
        case ECTaskType::ENCODE_PARTIAL:
            return 5;
        default:
            assert(false && "undefined ECTaskType");
    }
}


class ECTask {
public:
    std::pair<char*, int> dump() const;
    void parse(const char* buf);

    std::pair<char*, int> dumpFG() const;
    void parseFG(const char* buf);


    ECTaskType _type;
    std::string _filename;
    

    int _nodeId;

    // for SEND
    int _srcNodeId;
    int _dstNodeId;
    int _objId;

    // for RECEIVE
    int _tmpObjId;

    // for COMPUTE
    std::vector<int> _objIds;
    int _encodePatternId;
    std::vector<int> _coefs;
    
    // for PERSIST

    // for ENCODE_PARTIAL
    int _objNum;

    // for fg
    int _leftBound;
    int _rightBound;
};


class ECPlan {
public:
    ECPlan(Config* conf, FileMeta* fileMeta, const std::string& ecdagPath);

    ~ECPlan();

    // send tasks to corresponding agents
    void send();
    // wait for tasks done
    void receive();

protected:
    
    std::string _ecdagPath;
    Config* _conf;
    FileMeta* _fileMeta;
    
    std::unordered_map<int, std::vector<ECTask*>> _tasks;   // node id->tasks
    




};