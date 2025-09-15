#include "ECPlan.hh"

/**
 * SEND [nodeId/srcNodeId] [dstNodeId] [objId]
 * RECEIVE [nodeId/dstNodeId][srcNodeId] [objId] [tmpObjId]
 * ENCODE [nodeId] [objNum] [objId...] [tmpObjId] [encodePatternId] [coef...]
 * PERSIST [nodeId] [tmpObjId] [objId]
 */
std::pair<char*, int> ECTask::dump() const {
    int len;
    char* buf;
    int offset = 0;
    int tmpType = ECTaskType2int(_type);
    int objNum;
    switch (_type) {
        case ECTaskType::SEND:
            len = sizeof(int) * 4;
            buf = new char[len];
            memcpy(buf + offset, (char*)&tmpType, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_srcNodeId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_dstNodeId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_objId, sizeof(int));
            offset += sizeof(int);
            break;
        case ECTaskType::RECEIVE:
            len = sizeof(int) * 5;
            buf = new char[len];
            memcpy(buf + offset, (char*)&tmpType, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_dstNodeId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_srcNodeId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_objId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_tmpObjId, sizeof(int));
            offset += sizeof(int);
            break;
        case ECTaskType::ENCODE:
            len = sizeof(int) * (5 + _objIds.size() + _coefs.size());
            buf = new char[len];
            memcpy(buf + offset, (char*)&tmpType, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_nodeId, sizeof(int));
            offset += sizeof(int);
            objNum = _objIds.size();
            memcpy(buf + offset, (char*)&objNum, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                memcpy(buf + offset, (char*)&_objIds[i], sizeof(int));
                offset += sizeof(int);
            }
            memcpy(buf + offset, (char*)&_tmpObjId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_encodePatternId, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                memcpy(buf + offset, (char*)&_coefs[i], sizeof(int));
                offset += sizeof(int);
            }
            break;
        case ECTaskType::ENCODE_PARTIAL:
            len = sizeof(int) * (4 + _objIds.size() + _coefs.size());
            buf = new char[len];
            memcpy(buf + offset, (char*)&tmpType, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_nodeId, sizeof(int));
            offset += sizeof(int);
            objNum = _objIds.size();
            memcpy(buf + offset, (char*)&objNum, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                memcpy(buf + offset, (char*)&_objIds[i], sizeof(int));
                offset += sizeof(int);
            }
            memcpy(buf + offset, (char*)&_tmpObjId, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                memcpy(buf + offset, (char*)&_coefs[i], sizeof(int));
                offset += sizeof(int);
            }
            break;
        case ECTaskType::PERSIST:
            len = sizeof(int) * 4;
            buf = new char [len];
            memcpy(buf + offset, (char*)&tmpType, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_nodeId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_tmpObjId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_objId, sizeof(int));
            offset += sizeof(int);
            break;
        case ECTaskType::FETCH:
            len = sizeof(int) * 4;
            buf = new char [len];
            memcpy(buf + offset, (char*)&tmpType, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_nodeId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_tmpObjId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_objId, sizeof(int));
            offset += sizeof(int);
            break;
        default:
            assert(false && "undefined ECTaskType");
    }
    return {buf, len};
}

void ECTask::parse(const char* buf) {
    int offset = 0;
    // read type
    int tmpType;
    memcpy((char*)&tmpType, buf + offset, sizeof(int));
    offset += sizeof(int);
    _type = int2ECTaskType(tmpType);
    switch (_type) {
        case ECTaskType::SEND:
            memcpy((char*)&_srcNodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_dstNodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_objId, buf + offset, sizeof(int));
            offset += sizeof(int);
            _nodeId = _srcNodeId;
            break;
        case ECTaskType::RECEIVE:
            memcpy((char*)&_dstNodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_srcNodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_objId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_tmpObjId, buf + offset, sizeof(int));
            offset += sizeof(int);
            _nodeId = _dstNodeId;
            break;
        case ECTaskType::ENCODE:
            memcpy((char*)&_nodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            int objNum;
            memcpy((char*)&objNum, buf + offset, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                int objId;
                memcpy((char*)&objId, buf + offset, sizeof(int));
                _objIds.push_back(objId);
                offset += sizeof(int);
            }
            memcpy((char*)&_tmpObjId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_encodePatternId, buf + offset, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                int coef;
                memcpy((char*)&coef, buf + offset, sizeof(int));
                _coefs.push_back(coef);
                offset += sizeof(int);
            }           
            break;
        case ECTaskType::ENCODE_PARTIAL:
            memcpy((char*)&_nodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&objNum, buf + offset, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                int objId;
                memcpy((char*)&objId, buf + offset, sizeof(int));
                _objIds.push_back(objId);
                offset += sizeof(int);
            }
            memcpy((char*)&_tmpObjId, buf + offset, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                int coef;
                memcpy((char*)&coef, buf + offset, sizeof(int));
                _coefs.push_back(coef);
                offset += sizeof(int);
            }           
            break;
        case ECTaskType::PERSIST:
            memcpy((char*)&_nodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_tmpObjId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_objId, buf + offset, sizeof(int));
            offset += sizeof(int);
            break;
        case ECTaskType::FETCH:
            memcpy((char*)&_nodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_tmpObjId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_objId, buf + offset, sizeof(int));
            offset += sizeof(int);
            break;
        default:
            assert(false && "undefined ECTaskType");
    }
}



/**
 * SEND [nodeId/srcNodeId] [dstNodeId] [objId] [leftBound] [rightBound]
 * RECEIVE [nodeId/dstNodeId] [srcNodeId] [objId] [tmpObjId] [leftBound] [rightBound]
 * ENCODE [nodeId] [objNum] [objId...] [tmpObjId] [encodePatternId] [coef...] [leftBound] [rightBound]
 * ENCODE_PARTIAL [nodeId] [objNum] [objIds...] [tmpObjId] [coefs...] [leftBound] [rightBound]
 * PERSIST [nodeId] [tmpObjId] [objId] [leftBound] [rightBound]
 * FETCH [nodeId] [objId] [tmpObjId] [leftBound] [rightBound]
 */
std::pair<char*, int> ECTask::dumpFG() const {
    int len;
    char* buf;
    int offset = 0;
    int tmpType = ECTaskType2int(_type);
    int objNum;
    switch (_type) {
        case ECTaskType::SEND:
            len = sizeof(int) * 6;
            buf = new char[len];
            memcpy(buf + offset, (char*)&tmpType, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_srcNodeId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_dstNodeId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_objId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_leftBound, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_rightBound, sizeof(int));
            offset += sizeof(int);
            break;
        case ECTaskType::RECEIVE:
            len = sizeof(int) * 7;
            buf = new char[len];
            memcpy(buf + offset, (char*)&tmpType, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_dstNodeId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_srcNodeId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_objId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_tmpObjId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_leftBound, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_rightBound, sizeof(int));
            offset += sizeof(int);
            break;
        case ECTaskType::ENCODE:
            len = sizeof(int) * (7 + _objIds.size() + _coefs.size());
            buf = new char[len];
            memcpy(buf + offset, (char*)&tmpType, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_nodeId, sizeof(int));
            offset += sizeof(int);
            objNum = _objIds.size();
            memcpy(buf + offset, (char*)&objNum, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                memcpy(buf + offset, (char*)&_objIds[i], sizeof(int));
                offset += sizeof(int);
            }
            memcpy(buf + offset, (char*)&_tmpObjId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_encodePatternId, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                memcpy(buf + offset, (char*)&_coefs[i], sizeof(int));
                offset += sizeof(int);
            }
            memcpy(buf + offset, (char*)&_leftBound, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_rightBound, sizeof(int));
            offset += sizeof(int);
            break;
        case ECTaskType::ENCODE_PARTIAL:
            len = sizeof(int) * (6 + _objIds.size() + _coefs.size());
            buf = new char[len];
            memcpy(buf + offset, (char*)&tmpType, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_nodeId, sizeof(int));
            offset += sizeof(int);
            objNum = _objIds.size();
            memcpy(buf + offset, (char*)&objNum, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                memcpy(buf + offset, (char*)&_objIds[i], sizeof(int));
                offset += sizeof(int);
            }
            memcpy(buf + offset, (char*)&_tmpObjId, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                memcpy(buf + offset, (char*)&_coefs[i], sizeof(int));
                offset += sizeof(int);
            }
            memcpy(buf + offset, (char*)&_leftBound, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_rightBound, sizeof(int));
            offset += sizeof(int);
            break;
        case ECTaskType::PERSIST:
            len = sizeof(int) * 6;
            buf = new char [len];
            memcpy(buf + offset, (char*)&tmpType, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_nodeId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_tmpObjId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_objId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_leftBound, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_rightBound, sizeof(int));
            offset += sizeof(int);
            break;
        case ECTaskType::FETCH:
            len = sizeof(int) * 6;
            buf = new char [len];
            memcpy(buf + offset, (char*)&tmpType, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_nodeId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_tmpObjId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_objId, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_leftBound, sizeof(int));
            offset += sizeof(int);
            memcpy(buf + offset, (char*)&_rightBound, sizeof(int));
            offset += sizeof(int);
            break;
        default:
            assert(false && "undefined ECTaskType");
    }
    return {buf, len};
}



void ECTask::parseFG(const char* buf) {
    int offset = 0;
    // read type
    int tmpType;
    memcpy((char*)&tmpType, buf + offset, sizeof(int));
    offset += sizeof(int);
    _type = int2ECTaskType(tmpType);
    switch (_type) {
        case ECTaskType::SEND:
            memcpy((char*)&_srcNodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_dstNodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_objId, buf + offset, sizeof(int));
            offset += sizeof(int);
            _nodeId = _srcNodeId;
            memcpy((char*)&_leftBound, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_rightBound, buf + offset, sizeof(int));
            offset += sizeof(int);
            break;
        case ECTaskType::RECEIVE:
            memcpy((char*)&_dstNodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_srcNodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_objId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_tmpObjId, buf + offset, sizeof(int));
            offset += sizeof(int);
            _nodeId = _dstNodeId;
            memcpy((char*)&_leftBound, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_rightBound, buf + offset, sizeof(int));
            offset += sizeof(int);
            break;
        case ECTaskType::ENCODE:
            memcpy((char*)&_nodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            int objNum;
            memcpy((char*)&objNum, buf + offset, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                int objId;
                memcpy((char*)&objId, buf + offset, sizeof(int));
                _objIds.push_back(objId);
                offset += sizeof(int);
            }
            memcpy((char*)&_tmpObjId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_encodePatternId, buf + offset, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                int coef;
                memcpy((char*)&coef, buf + offset, sizeof(int));
                _coefs.push_back(coef);
                offset += sizeof(int);
            }           
            memcpy((char*)&_leftBound, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_rightBound, buf + offset, sizeof(int));
            offset += sizeof(int);
            break;
        case ECTaskType::ENCODE_PARTIAL:
            memcpy((char*)&_nodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&objNum, buf + offset, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                int objId;
                memcpy((char*)&objId, buf + offset, sizeof(int));
                _objIds.push_back(objId);
                offset += sizeof(int);
            }
            memcpy((char*)&_tmpObjId, buf + offset, sizeof(int));
            offset += sizeof(int);
            for (int i = 0; i < objNum; i++) {
                int coef;
                memcpy((char*)&coef, buf + offset, sizeof(int));
                _coefs.push_back(coef);
                offset += sizeof(int);
            }          
            memcpy((char*)&_leftBound, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_rightBound, buf + offset, sizeof(int));
            offset += sizeof(int); 
            break;
        case ECTaskType::PERSIST:
            memcpy((char*)&_nodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_tmpObjId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_objId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_leftBound, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_rightBound, buf + offset, sizeof(int));
            offset += sizeof(int);
            break;
        case ECTaskType::FETCH:
            memcpy((char*)&_nodeId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_tmpObjId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_objId, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_leftBound, buf + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)&_rightBound, buf + offset, sizeof(int));
            offset += sizeof(int);
            break;
        default:
            assert(false && "undefined ECTaskType");
    }
}



ECPlan::ECPlan(Config* conf, FileMeta* fileMeta, const std::string& ecdagPath) {    
    _conf = conf;
    _fileMeta = fileMeta;
    _ecdagPath = ecdagPath;

}

ECPlan::~ECPlan() {
    for (auto it = _tasks.begin(); it != _tasks.end(); it++) {
        auto tasks = it->second;
        for (auto task : tasks) {
            delete task;
        }
    }
}

/**
 * send tasks to corresponding agents
 * data nodes has 3 types of tasks sequence
 * 1. send
 * 2. receive, encode, route
 * 3. receive, encode, persist
 */
void ECPlan::send() {
    // for each node in this job
    for (auto it = _tasks.begin(); it != _tasks.end(); it++) {
        int nodeId = it->first;
        unsigned int sendIp = _conf->_agent_ips[nodeId];
        int taskNum = it->second.size();
        std::vector<ECTask*> tasks = it->second;
        std::vector<std::pair<char*, int>> taskBufs;
        for (auto task : tasks) {
            if (_conf->_ecPolicy == ECPolicy::PipeFG) {
                taskBufs.push_back(task->dumpFG());
            } else {
                taskBufs.push_back(task->dump());
            }
        }

        // send task num to agent
        AGCommand* agCmd = new AGCommand();
        agCmd->buildType16(16, _fileMeta->getFileName(), taskNum);
        agCmd->sendTo(sendIp);
        delete agCmd;
        LOG_INFO("send task to agent, node id: %d, task num: %d", nodeId, taskNum);
        // send tasks to agent
        const std::string sendEcTasksKey = _fileMeta->getFileName() + "_ecTasks";
        redisContext* sendEcTasksCtx = RedisUtil::createContext(sendIp);
        assert(sendEcTasksCtx != NULL && "create redis context failed");
        redisReply* sendEcTasksReply;
        for (int i = 0; i < taskNum; i++) {
            sendEcTasksReply = (redisReply*)redisCommand(sendEcTasksCtx, "rpush %s %b", sendEcTasksKey.c_str(), taskBufs[i].first, taskBufs[i].second);
            assert(sendEcTasksReply != NULL && sendEcTasksReply->type == REDIS_REPLY_INTEGER);
            freeReplyObject(sendEcTasksReply);
        }
        redisFree(sendEcTasksCtx);

        // free task buf
        for (int i = 0; i < taskNum; i++) {
            delete [] taskBufs[i].first;
        }

    }
}



void ECPlan::receive() {
    for (auto it = _tasks.begin(); it != _tasks.end(); it++) {
        int nodeId = it->first;
        unsigned int sendIp = _conf->_agent_ips[nodeId];
        int taskNum = it->second.size();
        const std::string waitEcTasksDoneKey = _fileMeta->getFileName() + "_ecTasks_done";
        redisContext* waitEcTasksDoneCtx = RedisUtil::createContext(_conf->_localIp);
        assert(waitEcTasksDoneCtx != NULL && "Failed to create redis context");
        redisReply* waitEcTasksDoneReply = (redisReply*)redisCommand(waitEcTasksDoneCtx, "blpop %s 0", waitEcTasksDoneKey.c_str());
        assert(waitEcTasksDoneReply != NULL && waitEcTasksDoneReply->type == REDIS_REPLY_ARRAY 
                && waitEcTasksDoneReply->elements == 2);
        freeReplyObject(waitEcTasksDoneReply);
        redisFree(waitEcTasksDoneCtx);
        LOG_INFO("receive tasks done, node id: %d", nodeId);
    }
}
