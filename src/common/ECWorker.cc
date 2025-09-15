#include "ECWorker.hh"
#include "../util/httplib.h"
using namespace httplib;
ECWorker::ECWorker(Config* conf) : _conf(conf) {
	// create local context
	try {
		_processCtx = RedisUtil::createContext(_conf -> _localIp);
		_localCtx = RedisUtil::createContext(_conf -> _localIp);
		_coorCtx = RedisUtil::createContext(_conf -> _coorIp);
	} catch (int e) {
		cerr << "initializing redis context error" << endl;
	}

	// _underfs = FSUtil::createFS(_conf->_fsType, _conf->_fsFactory[_conf->_fsType], _conf);
    _hdfsHandler = new HDFSHandler(_conf->_fsParam[0], std::stoi(_conf->_fsParam[1]));

}

ECWorker::~ECWorker() {
	redisFree(_localCtx);
	redisFree(_processCtx);
	redisFree(_coorCtx);
	// delete _underfs;
    delete _hdfsHandler;
}

void ECWorker::doProcess() {
	redisReply* rReply;
	while (true) {
		LOG_INFO("ECWorker::doProcess waiting for request");
		rReply = (redisReply*)redisCommand(_processCtx, "blpop ag_request 0");
		assert(rReply != NULL && rReply -> type == REDIS_REPLY_ARRAY && rReply -> elements == 2);
		char* reqStr = rReply -> element[1] -> str;
		AGCommand* agCmd = new AGCommand(reqStr);
		int type = agCmd->getType();
		switch (type) {
			case 0: clientWrite(agCmd); break;
            case 1: clientRead(agCmd); break;
			case 12: receiveObjAndPersist(agCmd); break;
            case 13: readObj(agCmd); break;
            case 14: clientEncode(agCmd); break;
            case 15: clientDecode(agCmd); break;
            case 16: 
                if (_conf->_ecPolicy == ECPolicy::CONV || _conf->_ecPolicy == ECPolicy::PPR) {
                    execECTasksParallel(agCmd);
                } else if (_conf->_ecPolicy == ECPolicy::Pipe) {
                    execECPipeTasksParallel(agCmd);
                } else if (_conf->_ecPolicy == ECPolicy::PipeFG) {
                    execECPipeFGTasksParallel(agCmd);
                } else {
                    assert(false && "undefined ec policy");
                }
                break;
			default:break;
		}

		delete agCmd;
	}
	freeReplyObject(rReply); 
}


void ECWorker::clientWrite(AGCommand* agcmd) {
	string filename = agcmd->getFilename();
	string ecpoolid = agcmd->getEcid();
	string mode = agcmd->getMode();
	int filesizeMB = agcmd->getFilesizeMB();
	LOG_INFO("clientWrite start, filename: %s, ecpoolid: %s, mode: %s, filesizeMB: %d", filename.c_str(), ecpoolid.c_str(), mode.c_str(), filesizeMB);
	struct timeval time1, time2, time3, time4;
	
	// 0. send request to coordinator that I want to write a file with offline erasure coding
	//    wait for responses from coordinator with a set of tasks
	gettimeofday(&time1, NULL);
	CoorCommand* coorCmd = new CoorCommand();
	coorCmd->buildType0(0, _conf->_localIp, filename, ecpoolid, 1, filesizeMB);
	coorCmd->sendTo(_coorCtx);
	delete coorCmd;

	// 1. wait for coordinator's instructions
	redisReply* rReply;
	redisContext* waitCtx = RedisUtil::createContext(_conf->_localIp);
	string wkey = "registerFile:" + filename;
	rReply = (redisReply*)redisCommand(waitCtx, "blpop %s 0", wkey.c_str());
	assert(rReply != NULL && rReply -> type == REDIS_REPLY_ARRAY && rReply -> elements == 2);
	char* reqStr = rReply -> element[1] -> str;
	AGCommand* agCmd = new AGCommand(reqStr);
	freeReplyObject(rReply);
	redisFree(waitCtx);
	
	int objnum = agCmd->getObjnum();
	int basesizeMB = agCmd->getBasesizeMB();
	std::vector<int> objLocs = agCmd->getObjLocs();
	delete agCmd;
	gettimeofday(&time2, NULL);
	LOG_INFO("offlineWrite::get response from coordinator, objnum: %d, basesizeMB: %d", objnum, basesizeMB);



	int pktNumPerObj = _conf->_objSize * 1024 * 1024 / _conf->_pktSize;
	// 2. create outputstream for each obj
	// FSObjOutputStream** objstreams = (FSObjOutputStream**)calloc(objnum, sizeof(FSObjOutputStream*));
	// for (int i = 0; i < objnum; i++) {
	// 	string objname = filename+"_lmqobj_"+to_string(i);
	// 	objstreams[i] = new FSObjOutputStream(_conf, objname, _underfs, pktNumPerObj);
	// }


	BlockingQueue<ECDataPacket*>** loadQueue = (BlockingQueue<ECDataPacket*>**)calloc(objnum, sizeof(BlockingQueue<ECDataPacket*>*));
	for (int i=0; i<objnum; i++) {
		// loadQueue[i] = objstreams[i]->getQueue();
		loadQueue[i] = new BlockingQueue<ECDataPacket*>();
	}

	// 3. create loadThreads
	vector<thread> loadThreads = vector<thread>(objnum);
	for (int i = 0; i < objnum; i++) {
		int startPktIdx = i * pktNumPerObj;
		loadThreads[i] = thread([=]{ loadWorker(loadQueue[i], filename, startPktIdx, 1, pktNumPerObj, false); });
	}

	// 4. create persistThreads
	vector<thread> persistThreads = vector<thread>(objnum);
	for (int i = 0; i < objnum; i++) {  
		string objname = filename+"_lmqobj_"+to_string(i);
		persistThreads[i] = thread([=]{ send4PersistObjWorker(loadQueue[i], objname, pktNumPerObj, objLocs[i]); });
	}

	// join
	for (int i = 0; i < objnum; i++) loadThreads[i].join();
	for (int i = 0; i < objnum; i++) persistThreads[i].join();



	// writefinish:filename
	redisReply* waitFinishReply;
	redisContext* waitFinishCtx = RedisUtil::createContext(_conf->_localIp);
	string waitFinishkey = "writefinish:" + filename;
	cout << "write " << wkey << " into redis" << endl;
	int tmpval = htonl(1);
	waitFinishReply = (redisReply*)redisCommand(waitFinishCtx, "rpush %s %b", waitFinishkey.c_str(), (char*)&tmpval, sizeof(tmpval));
	assert(waitFinishReply != NULL && waitFinishReply -> type == REDIS_REPLY_INTEGER);
	freeReplyObject(waitFinishReply);
	redisFree(waitFinishCtx);
	
	
	// free
	// for (int i = 0; i < objnum; i++) delete objstreams[i];
	// free(objstreams);
	for (int i = 0; i < objnum; i++) { 
		delete loadQueue[i];
	}
	// delete [] loadQueue;
    free(loadQueue);

}

void ECWorker::loadWorker(BlockingQueue<ECDataPacket*>* readQueue,
                    string keybase,
                    int startid,
                    int step,
                    int round,
                    bool zeropadding) {
	LOG_INFO("loadWorker, objname: %s, startPktIdx: %d, pktNum: %d", keybase.c_str(), startid, round);
	
	struct timeval time1, time2, time3;
	gettimeofday(&time1, NULL);
	redisContext* readCtx = RedisUtil::createContext(_conf->_localIp);
	int startidx = startid;
	for (int i = 0; i < round; i++) {
		int curidx = startidx + i * step;
		string key = keybase + ":" + to_string(curidx);
		redisAppendCommand(readCtx, "blpop %s 0", key.c_str());
	}
	redisReply* rReply;
	for (int i=0; i<round; i++) {
		redisGetReply(readCtx, (void**)&rReply);
        assert(rReply != NULL && rReply->type == REDIS_REPLY_ARRAY && rReply->elements == 2);
		char* content = rReply->element[1]->str;
		ECDataPacket* pkt = new ECDataPacket(content);
		int curDataLen = pkt->getDatalen();
		readQueue->push(pkt);
		freeReplyObject(rReply);
	}
	
	redisFree(readCtx);
	gettimeofday(&time2, NULL);
	LOG_INFO("loadWorker done, objname: %s, startPktIdx: %d, pktNum: %d", keybase.c_str(), startid, round);

}


void ECWorker::send4PersistObjWorker(BlockingQueue<ECDataPacket*>* readQueue,
									const std::string& objname, int pktNum, int objLoc) {
	LOG_INFO("FSObjOutputStream::send4PersistObj start, objname: %s, loc: %d", objname.c_str(), objLoc);
	int pktid = 0;
	int bufSize = _conf->_objSize * 1024 * 1024;
	assert(bufSize == _conf->_pktSize * pktNum);
	char* buf = new char [bufSize];
	for (int pktid = 0; pktid < pktNum; pktid++) {
		ECDataPacket* curPkt = readQueue->pop();
		assert(curPkt->getDatalen() == _conf->_pktSize);
		memcpy(buf + pktid * _conf->_pktSize, curPkt->getData(), curPkt->getDatalen());
		delete curPkt;
	}
	AGCommand* agCmd = new AGCommand();
	agCmd->buildType12(12, objname);
	agCmd->sendTo(_conf->_agent_ips[objLoc]);
	delete agCmd;
	const std::string key = objname + "_persist";
	redisContext* persistCtx = RedisUtil::createContext(_conf->_agent_ips[objLoc]);
	redisReply* persistReply = (redisReply*)redisCommand(persistCtx, "rpush %s %b", key.c_str(), buf, bufSize);
	assert(persistReply != NULL && persistReply -> type == REDIS_REPLY_INTEGER);
	freeReplyObject(persistReply);
	redisFree(persistCtx);

	delete [] buf;
	LOG_INFO("FSObjOutputStream::send4PersistObj done, objname: %s, loc: %d", objname.c_str(), objLoc);
}

void ECWorker::receiveObjAndPersist(AGCommand* agCmd) {
    struct timeval receiveStart, receiveEnd, persistStart, persistEnd;
    gettimeofday(&receiveStart, NULL);
	const std::string objname = agCmd->getFilename();
	const std::string key = objname + "_persist";
	LOG_INFO("receiveObjAndPersist start, objname: %s", objname.c_str());
	char* buf = new char [_conf->_objSize * 1024 * 1024];
	int bufSize = _conf->_objSize * 1024 * 1024;
	redisReply* rReply;
	rReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", key.c_str());
	assert(rReply != NULL && rReply->type == REDIS_REPLY_ARRAY && rReply->elements == 2);
	memcpy(buf, rReply->element[1]->str, bufSize);
	freeReplyObject(rReply);
    gettimeofday(&receiveEnd, NULL);
	LOG_INFO("receiveObjAndPersist, receive objname: %s, receive time: %f ms", objname.c_str(), RedisUtil::duration(receiveStart, receiveEnd));
    gettimeofday(&persistStart, NULL);
    hdfsFile file = _hdfsHandler->openFile(objname, HDFSMode::WRITE);
    _hdfsHandler->write2HDFS(file, buf, bufSize);
    _hdfsHandler->closeFile(file);
    gettimeofday(&persistEnd, NULL);
	// write2HDFS((hdfsFS)_underfs, objname, buf, bufSize);
	delete[] buf;
	LOG_INFO("receiveObjAndPersist done, objname: %s, persist time: %f ms", objname.c_str(), RedisUtil::duration(persistStart, persistEnd));
}



/**
 * local client readfile
 * get file info from coordinator
 * return file size to client
 * send read request to agents
 */
void ECWorker::clientRead(AGCommand* agCmd) {
    const std::string filename = agCmd->getFilename();
    LOG_INFO("clientRead start, filename: %s", filename.c_str());

    // 1. get file meta from coordinator
    CoorCommand* coorCmd = new CoorCommand();
    coorCmd->buildType3(3, _conf->_localIp, filename);
    coorCmd->sendTo(_coorCtx);
    delete coorCmd;
    LOG_INFO("clientRead send file meta request to coordinator, filename: %s", filename.c_str());

    const std::string fileMetaKey = filename + "_meta";
    redisReply* rReply;
    rReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", fileMetaKey.c_str());
    assert(rReply != NULL && rReply->type == REDIS_REPLY_ARRAY && rReply->elements == 2);
    char* metaStr = rReply->element[1]->str;
    const FileMeta* fileMeta = new FileMeta(metaStr);
    freeReplyObject(rReply);

    LOG_INFO("clientRead get file meta, filename: %s, fileSize: %d, objNum: %d, objLocs: %s", 
            filename.c_str(), fileMeta->getFileSize(), fileMeta->getObjNum(), vec2String(fileMeta->getObjLocs()).c_str());


    // 2. return file size to client
    const std::string retFileSizeKey = filename + "_filesize";
    int fileSize = fileMeta->getFileSize();
    int tmpFileSize = htonl(fileSize);
    redisReply* sizeReply = (redisReply*)redisCommand(_localCtx, "rpush %s %b", retFileSizeKey.c_str(), (char*)&tmpFileSize, sizeof(int));
    assert(sizeReply != NULL && sizeReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(sizeReply);
    
    LOG_INFO("clientRead return file size to client, filename: %s, fileSize: %d", filename.c_str(), fileSize);
    
    // 3. send read obj request to agents
    int objNum = fileMeta->getObjNum();
    std::vector<int> objLocs = fileMeta->getObjLocs();
    for (int i = 0; i < objNum; i++) {
        AGCommand* agCmd = new AGCommand();
        agCmd->buildType13(13, _conf->_localIp, filename, i);
        agCmd->sendTo(_conf->_agent_ips[objLocs[i]]);
        delete agCmd;
        LOG_INFO("clientRead send read obj request to agent, filename: %s, objIdx: %d, objLoc: %d", filename.c_str(), i, objLocs[i]);
    }


    delete fileMeta;
}

/**
 * source agent receive clientRead request, call this agent to readObj
 * read obj from hdfs
 * return to source agent
 */
void ECWorker::readObj(AGCommand* agCmd) {
    const std::string filename = agCmd->getFilename();
    const int objIdx = agCmd->getObjIdx();
    unsigned int sendIp = agCmd->getSendIp();
    LOG_INFO("readObj start, send ip: %s, filename: %s, objIdx: %d", 
            RedisUtil::ip2Str(sendIp).c_str(), filename.c_str(), objIdx);


    // 1. read obj from hdfs
    const std::string objname = filename + "_lmqobj_" + std::to_string(objIdx);
    hdfsFile file = _hdfsHandler->openFile(objname, HDFSMode::READ);
    int bufSize = _conf->_objSize * 1024 * 1024;
    char* buf = new char [bufSize];
    _hdfsHandler->readFromHDFS(file, buf, bufSize);
    _hdfsHandler->closeFile(file);

    // 2. send obj to agent
    const std::string readObjKey = objname + "_read";
    redisContext* readObjCtx = RedisUtil::createContext(sendIp);
    redisReply* rReply = (redisReply*)redisCommand(readObjCtx, "rpush %s %b", 
                        readObjKey.c_str(), buf, bufSize);
    assert(rReply != NULL && rReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(rReply);
    redisFree(readObjCtx);
    LOG_INFO("readObj done, send ip: %s, filename: %s, objIdx: %d, size: %d", 
            RedisUtil::ip2Str(sendIp).c_str(), filename.c_str(), objIdx, bufSize);
    delete [] buf;
}

/**
 * receive encode request from client
 * send encode request to coordinator
 * wait for encode done
 */
void ECWorker::clientEncode(AGCommand* agCmd) {
    const std::string filename = agCmd->getFilename();
    const std::string ecdagPath = agCmd->getEcdagPath();
    LOG_INFO("clientEncode start, filename: %s, ecdagPath: %s", filename.c_str(), ecdagPath.c_str());

    // 1. send encode request to coordinator
    CoorCommand* coorCmd = new CoorCommand();
    coorCmd->buildType13(13, filename, _conf->_localIp, ecdagPath);
    coorCmd->sendTo(_coorCtx);
    delete coorCmd;
    LOG_INFO("clientEncode send encode request to coordinator, wait for encode done, filename: %s", filename.c_str());

    // 2. wait for encode done
    const std::string waitEncodeDoneKey = filename + "_coordinator_encode_done";
    LOG_INFO("ECWorker::clientEncode wait for encode done, filename: %s, ecdagPath: %s", filename.c_str(), ecdagPath.c_str());
    redisReply* waitEncodeDoneReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", waitEncodeDoneKey.c_str());
    assert(waitEncodeDoneReply != NULL && waitEncodeDoneReply->type == REDIS_REPLY_ARRAY 
            && waitEncodeDoneReply->elements == 2);
    freeReplyObject(waitEncodeDoneReply);
    LOG_INFO("ECWorker::clientEncode receive coordinator encode done, filename: %s, ecdagPath: %s", filename.c_str(), ecdagPath.c_str());

    // 3. send encode done to client
    const std::string encodeDoneKey = filename + "_agent_encode_done";
    redisReply* agentEncodeDoneReply = (redisReply*)redisCommand(_localCtx, "rpush %s 1", encodeDoneKey.c_str());
    assert(agentEncodeDoneReply != NULL && agentEncodeDoneReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(agentEncodeDoneReply);
    LOG_INFO("ECWorker::clientEncode done, filename: %s, ecdagPath: %s", filename.c_str(), ecdagPath.c_str());
}



/**
 * receive decode request from client
 * send decode request to coordinator
 * wait for decode done
 */
void ECWorker::clientDecode(AGCommand* agCmd) {
    const std::string filename = agCmd->getFilename();
    const std::string ecdagPath = agCmd->getEcdagPath();
    const std::vector<int> survivedObjIds = agCmd->getSurvivedObjIds();
    const int failedObjId = agCmd->getFailedObjId();
    LOG_INFO("clientDecode start, filename: %s, ecdagPath: %s, survivedObjIds: %s, failedObjId: %d", 
             filename.c_str(), ecdagPath.c_str(), vec2String(survivedObjIds).c_str(), failedObjId);

    // 1. send decode request to coordinator
    CoorCommand* coorCmd = new CoorCommand();
    coorCmd->buildType14(14, filename, _conf->_localIp, ecdagPath, survivedObjIds, failedObjId);
    coorCmd->sendTo(_coorCtx);
    delete coorCmd;
    LOG_INFO("clientDecode send decode request to coordinator, wait for decode done, filename: %s", filename.c_str());

    // 2. wait for decode done
    const std::string waitDecodeDoneKey = filename + "_coordinator_decode_done";
    LOG_INFO("ECWorker::clientDecode wait for decode done, filename: %s, ecdagPath: %s", filename.c_str(), ecdagPath.c_str());
    redisReply* waitDecodeDoneReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", waitDecodeDoneKey.c_str());
    assert(waitDecodeDoneReply != NULL && waitDecodeDoneReply->type == REDIS_REPLY_ARRAY 
            && waitDecodeDoneReply->elements == 2);
    freeReplyObject(waitDecodeDoneReply);
    LOG_INFO("ECWorker::clientDecode receive coordinator decode done, filename: %s, ecdagPath: %s", filename.c_str(), ecdagPath.c_str());

    // 3. send decode done to client
    const std::string decodeDoneKey = filename + "_agent_decode_done";
    redisReply* agentDecodeDoneReply = (redisReply*)redisCommand(_localCtx, "rpush %s 1", decodeDoneKey.c_str());
    assert(agentDecodeDoneReply != NULL && agentDecodeDoneReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(agentDecodeDoneReply);
    LOG_INFO("ECWorker::clientDecode done, filename: %s, ecdagPath: %s", filename.c_str(), ecdagPath.c_str());
}

/**
 * receive ec tasks from coordinator
 */
void ECWorker::execECTasks(AGCommand* agCmd) {
    const std::string filename = agCmd->getFilename();
    int taskNum = agCmd->getEcTaskNum();
    LOG_INFO("execECTasks start, filename: %s, taskNum: %d", filename.c_str(), taskNum);

    // store obj and tmpobj, objname->obj
    ObjBuffer* objBuffer = new ObjBuffer();

    // 1. get tasks from coordinator
    const std::string receiveEcTasksKey = filename + "_ecTasks";
    std::vector<ECTask*> tasks;

    redisReply* receiveEcTasksReply;
    

    for (int i = 0; i < taskNum; i++) {
        receiveEcTasksReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", receiveEcTasksKey.c_str());
        assert(receiveEcTasksReply != NULL && receiveEcTasksReply->type == REDIS_REPLY_ARRAY 
                && receiveEcTasksReply->elements == 2);
        char* taskStr = receiveEcTasksReply->element[1]->str;
        ECTask* task = new ECTask();
        task->parse(taskStr);
        LOG_INFO("ECWorker receive task, type: %d, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, tmpObjId: %d, objIds: %s, encodePatternId: %d, coefs: %s", 
                ECTaskType2int(task->_type), task->_nodeId, task->_srcNodeId, task->_dstNodeId, task->_objId, task->_tmpObjId, 
                vec2String(task->_objIds).c_str(), task->_encodePatternId, vec2String(task->_coefs).c_str());
        tasks.push_back(task);
        freeReplyObject(receiveEcTasksReply);
    }

    // 2. exec tasks
    // TODO: parallelize tasks
    LOG_INFO("ECWorker exec tasks, filename: %s, taskNum: %d", filename.c_str(), taskNum);
    double sendTime = 0.0, receiveTime = 0.0, encodeTime = 0.0, persistTime = 0.0;

    // TODO:
    if (taskNum == 6) {
        LOG_INFO("exec 6 tasks");
        // exec receive task
        std::thread receiveThds[4];
        double receiveTimes[4];
        for (int i = 0; i < 4; i++) {
            receiveThds[i] = std::thread([=, &receiveTimes](){
                const auto task = tasks[i];
                assert(tasks[i]->_type == ECTaskType::RECEIVE);
                // receiveTimes[i] = execReceiveECTask(filename, tasks[i], objBuffer);
                receiveTimes[i] = execReceiveECTaskByHttp(filename, tasks[i], objBuffer);

            });
        }

        for (int i = 0; i < 4; i++) {
            receiveThds[i].join();
        }    
        double receiveStart = receiveTimes[0];
        for (int i = 0; i < 4; i++) {
            receiveStart = std::min(receiveStart, receiveTimes[i]);
        }
        struct timeval receiveEnd;
        gettimeofday(&receiveEnd, NULL);
        receiveTime = receiveEnd.tv_sec * 1000.0 + receiveEnd.tv_usec / 1000.0 - receiveStart;

        // encode/decode
        assert(tasks[4]->_type == ECTaskType::ENCODE);
        encodeTime += execEncodeECTask(filename, tasks[4], objBuffer);

        // persist
        assert(tasks[5]->_type == ECTaskType::PERSIST);
        persistTime += execPersistECTask(filename, tasks[5], objBuffer);
        std::ofstream logFile("/root/coar/build/repair.log", std::ios::app);
        assert(logFile.is_open());
        logFile << receiveTime << " " << encodeTime << " " << persistTime << std::endl;
        logFile.close();
        goto SKIP;
    }
    // TODO: modify time collection
    for (const auto task : tasks) {
        switch (task->_type) {
            case ECTaskType::SEND:
                sendTime += execSendECTask(filename, task, objBuffer);
                // sendTime += execSendECTaskByHttp(filename, task, objBuffer);
                break;
            case ECTaskType::RECEIVE:
                receiveTime += execReceiveECTask(filename, task, objBuffer);
                // receiveTime += execReceiveECTaskByHttp(filename, task, objBuffer);
                break;
            case ECTaskType::ENCODE:
                encodeTime += execEncodeECTask(filename, task, objBuffer);
                break;
            case ECTaskType::PERSIST:
                persistTime += execPersistECTask(filename, task, objBuffer);
                break;
            default:
                assert(false && "undefined ECTaskType");
        }
    }    

SKIP:
    LOG_INFO("ECWorker exec tasks done, filename: %s, taskNum: %d, sendTime: %f, receiveTime: %f, encodeTime: %f, persistTime: %f", 
             filename.c_str(), taskNum, sendTime, receiveTime, encodeTime, persistTime);

    // 3. send encode done to coordinator
    const std::string execEcTasksDoneKey = filename + "_ecTasks_done";
    redisReply* execEcTasksDoneReply = (redisReply*)redisCommand(_coorCtx, "rpush %s %b", 
            execEcTasksDoneKey.c_str(), (char*)&taskNum, sizeof(int));
    assert(execEcTasksDoneReply != NULL && execEcTasksDoneReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(execEcTasksDoneReply);
    LOG_INFO("ECWorker send encode done to coordinator, filename: %s, taskNum: %d", filename.c_str(), taskNum);


    // 4. free tasks
    for (auto task : tasks) {
        delete task;
    }

    delete objBuffer;
}


double ECWorker::execSendECTask(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer) {
    if (_conf->_ioPolicy == "redis") {
        return execSendECTaskByRedis(filename, task, objBuffer);
    } else if (_conf->_ioPolicy == "http") {
        return execSendECTaskByHttp(filename, task, objBuffer);
    } else {
        assert(false && "undefined io policy");
    }
}

double ECWorker::execSendECTaskByRedis(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const int srcNodeId = task->_srcNodeId;
    const int dstNodeId = task->_dstNodeId;
    const int objId = task->_objId;
    struct timeval sendStart, sendEnd;
    gettimeofday(&sendStart, NULL);
    LOG_INFO("execSendECTask start, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d", 
            filename.c_str(), nodeId, srcNodeId, dstNodeId, objId);
    
    // 1. read obj from hdfs
    int bufSizeByte = _conf->_objSize * 1024 * 1024;
    char* buf;                              // get from objBuffer or new, free by objBuffer
    if (objBuffer->existObj(objId)) {
        buf = objBuffer->getObj(objId);
    } else {
        buf = new char [bufSizeByte];
        const std::string objname = filename + "_lmqobj_" + std::to_string(objId);
        hdfsFile file = _hdfsHandler->openFile(objname, HDFSMode::READ);
        _hdfsHandler->readFromHDFS(file, buf, bufSizeByte);
        _hdfsHandler->closeFile(file);
        objBuffer->insertObj(objId, buf);
    }


    const std::string sendObjKey = filename + "_send_" + std::to_string(srcNodeId) + "_" + 
                                      std::to_string(dstNodeId) + "_" + std::to_string(objId);
    redisContext* sendObjCtx = RedisUtil::createContext(_conf->_agent_ips[dstNodeId]);
    assert(sendObjCtx != NULL && "Failed to create redis context");
    redisReply* sendObjReply = (redisReply*)redisCommand(sendObjCtx, "rpush %s %b", 
                                   sendObjKey.c_str(), buf, bufSizeByte);
    assert(sendObjReply != NULL && sendObjReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(sendObjReply);
    redisFree(sendObjCtx);
    gettimeofday(&sendEnd, NULL);
    LOG_INFO("execSendECTask done, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, time: %f ms", 
            filename.c_str(), nodeId, srcNodeId, dstNodeId, objId, RedisUtil::duration(sendStart, sendEnd));
    return RedisUtil::duration(sendStart, sendEnd);
}

double ECWorker::execReceiveECTask(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer) {
    if (_conf->_ioPolicy == "redis") {
        return execReceiveECTaskByRedis(filename, task, objBuffer);
    } else if (_conf->_ioPolicy == "http") {
        return execReceiveECTaskByHttp(filename, task, objBuffer);
    } else {
        assert(false && "undefined io policy");
    }
}

double ECWorker::execReceiveECTaskByRedis(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const int dstNodeId = task->_dstNodeId;
    const int srcNodeId = task->_srcNodeId;
    const int objId = task->_objId;
    const int tmpObjId = task->_tmpObjId;
    struct timeval receiveStart, receiveEnd;
    gettimeofday(&receiveStart, NULL);
    LOG_INFO("execReceiveECTask start, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, tmpObjId: %d", 
            filename.c_str(), nodeId, srcNodeId, dstNodeId, objId, tmpObjId);
    const std::string receiveObjKey = filename + "_send_" + std::to_string(srcNodeId) + "_" + 
                                    std::to_string(dstNodeId) + "_" + std::to_string(objId);
    redisContext* receiveObjCtx = RedisUtil::createContext(_conf->_localIp);
    redisReply* receiveObjRely = (redisReply*)redisCommand(receiveObjCtx, "blpop %s 0", receiveObjKey.c_str());
    assert(receiveObjRely != NULL && receiveObjRely->type == REDIS_REPLY_ARRAY 
            && receiveObjRely->elements == 2);
    char* taskStr = receiveObjRely->element[1]->str;
    int objSizeByte = _conf->_objSize * 1024 * 1024;
    char* obj = new char[objSizeByte];      // insert into objBuffer, free by objBuffer
    memcpy(obj, taskStr, objSizeByte);
    objBuffer->insertObj(tmpObjId, obj);
    freeReplyObject(receiveObjRely);
    redisFree(receiveObjCtx);
    gettimeofday(&receiveEnd, NULL);
    LOG_INFO("execReceiveECTask done, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, tmpObjId: %d, receive time: %f ms", 
             filename.c_str(), nodeId, srcNodeId, dstNodeId, objId, tmpObjId, RedisUtil::duration(receiveStart, receiveEnd));
    return RedisUtil::duration(receiveStart, receiveEnd);
}

double ECWorker::execSendECTaskByHttp(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const int srcNodeId = task->_srcNodeId;
    const int dstNodeId = task->_dstNodeId;
    const int objId = task->_objId;
    struct timeval sendStart, sendEnd;

    LOG_INFO("execSendECTask start, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d", 
            filename.c_str(), nodeId, srcNodeId, dstNodeId, objId);
    
    // 1. read obj from hdfs
    int bufSizeByte = _conf->_objSize * 1024 * 1024;
    char* buf;                              // get from objBuffer or new, free by objBuffer
    if (objBuffer->existObj(objId)) {
        buf = objBuffer->getObj(objId);
    } else {
        buf = new char [bufSizeByte];
        const std::string objname = filename + "_lmqobj_" + std::to_string(objId);
        hdfsFile file = _hdfsHandler->openFile(objname, HDFSMode::READ);
        _hdfsHandler->readFromHDFS(file, buf, bufSizeByte);
        _hdfsHandler->closeFile(file);
        objBuffer->insertObj(objId, buf);
    }

    // 1. send time to receive node
    const std::string sendObjKey = filename + "_send_" + std::to_string(srcNodeId) + "_" + 
                                      std::to_string(dstNodeId) + "_" + std::to_string(objId);
    redisContext* sendObjCtx = RedisUtil::createContext(_conf->_agent_ips[dstNodeId]);
    assert(sendObjCtx != NULL && "Failed to create redis context");
    
    // 2. send data and start time
    httplib::Server svr;

    svr.Get("/data", [=, &sendStart](const httplib::Request& req, httplib::Response& res) {
		LOG_INFO("http send, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d", 
                filename.c_str(), nodeId, srcNodeId, dstNodeId, objId);
        char* sendBuf = new char [bufSizeByte + sizeof(timeval)];
        // copy obj data
        memcpy(sendBuf + sizeof(timeval), buf, bufSizeByte);
        // copy send start time
        gettimeofday(&sendStart, NULL);
        memcpy(sendBuf, &sendStart, sizeof(timeval));
		res.set_content(sendBuf, bufSizeByte + sizeof(timeval), "application/octet-stream");
        delete [] sendBuf;
        LOG_INFO("http send done, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d", 
                filename.c_str(), nodeId, srcNodeId, dstNodeId, objId);
	});


    std::thread sendThd = std::thread([&](){
        LOG_INFO("http svr start");
		svr.listen("0.0.0.0", 8080);
    });
    redisReply* sendObjReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", sendObjKey.c_str());
    assert(sendObjReply != NULL && sendObjReply->type == REDIS_REPLY_ARRAY && sendObjReply->elements == 2);
    freeReplyObject(sendObjReply);
    LOG_INFO("http send done, close svr");
    svr.stop();
    sendThd.join();

    redisFree(sendObjCtx);
    gettimeofday(&sendEnd, NULL);
    LOG_INFO("execSendECTask done, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, time: %f ms", 
            filename.c_str(), nodeId, srcNodeId, dstNodeId, objId, RedisUtil::duration(sendStart, sendEnd));
    return RedisUtil::duration(sendStart, sendEnd);
}

double ECWorker::execReceiveECTaskByHttp(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const int dstNodeId = task->_dstNodeId;
    const int srcNodeId = task->_srcNodeId;
    const int objId = task->_objId;
    const int tmpObjId = task->_tmpObjId;
    struct timeval receiveStart, receiveEnd;
    LOG_INFO("execReceiveECTask start, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, tmpObjId: %d", 
            filename.c_str(), nodeId, srcNodeId, dstNodeId, objId, tmpObjId);
    const std::string receiveObjKey = filename + "_send_" + std::to_string(srcNodeId) + "_" + 
                                    std::to_string(dstNodeId) + "_" + std::to_string(objId);
    
    // 1. receive data and start time from sender 
    Client cli(RedisUtil::ip2Str(_conf->_agent_ips[srcNodeId]).c_str(), 8080);
    LOG_INFO("http receive from ip: %s, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d", 
             RedisUtil::ip2Str(_conf->_agent_ips[srcNodeId]).c_str(), filename.c_str(), nodeId, srcNodeId, dstNodeId, objId);
    do {
        auto res = cli.Get("/data");
        if (res && res->status == 200) {
            assert(res->body.size() == _conf->_objSize * 1024 * 1024 + sizeof(struct timeval)); // start time | data
            LOG_INFO("http receive done, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d", 
            filename.c_str(), nodeId, srcNodeId, dstNodeId, objId);
            int objSizeByte = _conf->_objSize * 1024 * 1024;
            char* obj = new char[objSizeByte];      // insert into objBuffer, free by objBuffer
            memcpy(&receiveStart, res->body.data(), sizeof(struct timeval));
            LOG_INFO("http receive start time: %ld, %ld", receiveStart.tv_sec, receiveStart.tv_usec);
            memcpy(obj, res->body.data() + sizeof(timeval), objSizeByte);
            objBuffer->insertObj(tmpObjId, obj);
            break;
        }
    } while (true);

    // 3. send reply to sender
    redisContext* receiveObjCtx = RedisUtil::createContext(_conf->_agent_ips[srcNodeId]);
    redisReply* receiveObjReply = (redisReply*)redisCommand(receiveObjCtx, "rpush %s 1", receiveObjKey.c_str());
    assert(receiveObjReply != NULL && receiveObjReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(receiveObjReply);
    redisFree(receiveObjCtx);
    
    gettimeofday(&receiveEnd, NULL);
    LOG_INFO("execReceiveECTask done, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, tmpObjId: %d, receive time: %f ms", 
             filename.c_str(), nodeId, srcNodeId, dstNodeId, objId, tmpObjId, RedisUtil::duration(receiveStart, receiveEnd));
    return receiveStart.tv_sec * 1000.0 + receiveStart.tv_usec / 1000.0;
}

double ECWorker::execEncodeECTask(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const std::vector<int> objIds = task->_objIds;
    const int tmpObjId = task->_tmpObjId;
    const std::vector<int> coefs = task->_coefs;
    int objSizeByte = _conf->_objSize * 1024 * 1024;
    struct timeval encodeStart, encodeEnd;
    gettimeofday(&encodeStart, NULL);
    LOG_INFO("execEncodeECTask start, filename: %s, nodeId: %d, objNum: %ld, objIds: %s, tmpObjId: %d, encodePatternId: %d, coefs: %s",
             filename.c_str(), nodeId, objIds.size(), vec2String(objIds).c_str(), tmpObjId, 
             task->_encodePatternId, vec2String(coefs).c_str()); 
    
    std::vector<const char*> objBufs;                   // get obj from objBuffer, free by objBuffer
    for (int objId : objIds) {
        if (!objBuffer->existObj(objId)) {
            LOG_ERROR("execEncodeECTask, objId: %d not exist", objId);
            assert(false && "obj not exist");
        } 
        const char* objBuf = objBuffer->getObj(objId);  // get obj from objBuffer, free by objBuffer
        objBufs.push_back(objBuf);
    }
    char* encodeBuf = new char[objSizeByte];            // will insert into objBuffer, free by objBuffer
    memset(encodeBuf, 0, objSizeByte);
    RSPlan::encode(objBufs, encodeBuf, coefs, _conf->_rsParam.w, objSizeByte);
    objBuffer->insertObj(tmpObjId, encodeBuf);
    gettimeofday(&encodeEnd, NULL);
    LOG_INFO("execEncodeECTask done, filename: %s, nodeId: %d, objNum: %ld, objIds: %s, tmpObjId: %d, encodePatternId: %d, coefs: %s, encode time: %f ms",
             filename.c_str(), nodeId, objIds.size(), vec2String(objIds).c_str(), tmpObjId, 
             task->_encodePatternId, vec2String(coefs).c_str(), RedisUtil::duration(encodeStart, encodeEnd)); 
    return RedisUtil::duration(encodeStart, encodeEnd);
}

double ECWorker::execPersistECTask(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const int objId = task->_objId;
    const int tmpObjId = task->_tmpObjId;
    struct timeval persistStart, persistEnd;
    gettimeofday(&persistStart, NULL);
    LOG_INFO("execPersistECTask start, filename: %s, nodeId: %d, objId: %d, tmpObjId: %d", filename.c_str(), nodeId, objId, tmpObjId);
    if (!objBuffer->existObj(tmpObjId)) {
        LOG_ERROR("execPersistECTask, tmpObjId: %d not exist", tmpObjId);
        assert(false && "tmpObj not exist");
    }
    char* objBuf = objBuffer->getObj(tmpObjId);         // free by objBuffer
    const std::string objname = filename + "_lmqobj_" + std::to_string(objId);
    int objSizeByte = _conf->_objSize * 1024 * 1024;

    hdfsFile file = _hdfsHandler->openFile(objname, HDFSMode::WRITE);
    _hdfsHandler->write2HDFS(file, objBuf, objSizeByte);
    _hdfsHandler->closeFile(file);
    gettimeofday(&persistEnd, NULL);
    LOG_INFO("execPersistECTask done, filename: %s, nodeId: %d, objId: %d, tmpObjId: %d, persist time: %f ms", 
            filename.c_str(), nodeId, objId, tmpObjId, RedisUtil::duration(persistStart, persistEnd));
    return RedisUtil::duration(persistStart, persistEnd);
}

/**
 * exec ec tasks in parallel
 */
void ECWorker::execECTasksParallel(AGCommand* agCmd) {
    const std::string filename = agCmd->getFilename();
    int taskNum = agCmd->getEcTaskNum();
    LOG_INFO("execECTasksParallel start, filename: %s, taskNum: %d", filename.c_str(), taskNum);

    // store obj and tmpobj, objname->obj
    ObjParallelBuffer* objBuffer = new ObjParallelBuffer();

    // 1. get tasks from coordinator
    const std::string receiveEcTasksKey = filename + "_ecTasks";
    std::vector<ECTask*> tasks;

    redisReply* receiveEcTasksReply;
    

    for (int i = 0; i < taskNum; i++) {
        receiveEcTasksReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", receiveEcTasksKey.c_str());
        assert(receiveEcTasksReply != NULL && receiveEcTasksReply->type == REDIS_REPLY_ARRAY 
                && receiveEcTasksReply->elements == 2);
        char* taskStr = receiveEcTasksReply->element[1]->str;
        ECTask* task = new ECTask();
        task->parse(taskStr);
        LOG_INFO("ECWorker receive task, type: %d, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, tmpObjId: %d, objIds: %s, encodePatternId: %d, coefs: %s", 
                ECTaskType2int(task->_type), task->_nodeId, task->_srcNodeId, task->_dstNodeId, task->_objId, task->_tmpObjId, 
                vec2String(task->_objIds).c_str(), task->_encodePatternId, vec2String(task->_coefs).c_str());
        tasks.push_back(task);
        freeReplyObject(receiveEcTasksReply);
    }

    // 2. exec tasks
    LOG_INFO("ECWorker exec tasks, filename: %s, taskNum: %d", filename.c_str(), taskNum);
    ConcurrentMap timeMap;
    double sendTime = 0.0, receiveTime = 0.0, encodeTime = 0.0, persistTime = 0.0;
    std::thread execThds[taskNum];
    httplib::Server svr;
    std::thread svrThd;             // thd for svr to listen
    std::thread httpThd = std::thread([this, &svr, &tasks, &svrThd](){ startHttpService(svr, tasks, svrThd); });
    for (int i = 0; i < taskNum; i++) {
        switch (tasks[i]->_type) {
            case ECTaskType::SEND:
                execThds[i] = std::thread([=, &svr, &timeMap](){
                    auto ret = execSendECTaskParallel(filename, tasks[i], objBuffer, svr);
                    timeMap.setTime(i, ret);
                });
                break;
            case ECTaskType::RECEIVE:
                execThds[i] = std::thread([=, &timeMap](){
                    auto ret = execReceiveECTaskParallel(filename, tasks[i], objBuffer);
                    timeMap.setTime(i, ret);
                });
                break;
            case ECTaskType::ENCODE:
            case ECTaskType::ENCODE_PARTIAL:
                execThds[i] = std::thread([=, &timeMap](){
                    auto ret = execEncodeECTaskParallel(filename, tasks[i], objBuffer);
                    timeMap.setTime(i, ret);
                });
                break;
            case ECTaskType::PERSIST:
                execThds[i] = std::thread([=, &timeMap](){
                    auto ret = execPersistECTaskParallel(filename, tasks[i], objBuffer);
                    timeMap.setTime(i, ret);
                });
                break;
            case ECTaskType::FETCH:
                execThds[i] = std::thread([=, &timeMap](){
                    auto ret = execFetchECTaskParallel(filename, tasks[i], objBuffer);
                    timeMap.setTime(i, ret);
                });
                break;
            default:
                assert(false && "undefined ECTaskType");
        }
    }
    // 3. wait for tasks done, clean
    for (int i = 0; i < taskNum; i++) {     // clean thds to do fetch, send, receive, encode, persist 
        execThds[i].join();
    }
    if (svr.is_running()) {                 // if this node has send task, stop http svr, clean svrThd
        svr.stop();
        svrThd.join();
    }
    httpThd.join();


    printTime(timeMap, taskNum, tasks);

    // 3. send encode done to coordinator
    const std::string execEcTasksDoneKey = filename + "_ecTasks_done";
    redisReply* execEcTasksDoneReply = (redisReply*)redisCommand(_coorCtx, "rpush %s %b", 
            execEcTasksDoneKey.c_str(), (char*)&taskNum, sizeof(int));
    assert(execEcTasksDoneReply != NULL && execEcTasksDoneReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(execEcTasksDoneReply);
    LOG_INFO("ECWorker send exec tasks done to coordinator, filename: %s, taskNum: %d", filename.c_str(), taskNum);

    // 4. free tasks
    for (auto task : tasks) {
        delete task;
    }

    delete objBuffer;
}

std::pair<timeval, timeval> ECWorker::execFetchECTaskParallel(const std::string& filename, const ECTask* task, ObjParallelBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const int srcNodeId = task->_srcNodeId;
    const int dstNodeId = task->_dstNodeId;
    const int objId = task->_objId;
    const int tmpObjId = task->_tmpObjId;
    struct timeval fetchStart, fetchEnd;
    gettimeofday(&fetchStart, NULL);
    LOG_INFO("execFetchECTaskParallel start, filename: %s, nodeId: %d, odjId: %d, tmpObjId: %d", filename.c_str(), nodeId, objId, tmpObjId);
    int bufSizeByte = _conf->_objSize * 1024 * 1024;
    char* buf = new char [bufSizeByte];                            // free by objBuffer
    const std::string objname = filename + "_lmqobj_" + std::to_string(objId);
    hdfsFile file = _hdfsHandler->openFile(objname, HDFSMode::READ);
    _hdfsHandler->readFromHDFS(file, buf, bufSizeByte);
    _hdfsHandler->closeFile(file);
    objBuffer->insertObj(tmpObjId, buf);
    LOG_INFO("execFetchECTaskParallel done, filename: %s, nodeId: %d, odjId: %d, tmpObjId: %d", filename.c_str(), nodeId, objId, tmpObjId);
    gettimeofday(&fetchEnd, NULL);
    return {fetchStart, fetchEnd};
}


/**
 * read obj from objbuffer, send to requestor by http
 */
std::pair<timeval, timeval> ECWorker::execSendECTaskParallel(const std::string& filename, const ECTask* task, 
                                                             ObjParallelBuffer* objBuffer, httplib::Server& svr) {
    const int nodeId = task->_nodeId;
    const int srcNodeId = task->_srcNodeId;
    const int dstNodeId = task->_dstNodeId;
    const int objId = task->_objId;
    struct timeval sendStart, sendEnd;

    LOG_INFO("execSendECTaskParallel start, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d", 
            filename.c_str(), nodeId, srcNodeId, dstNodeId, objId);
    
    // 1. read from objBuffer
    int bufSizeByte = _conf->_objSize * 1024 * 1024;
    char* buf = objBuffer->getObj(objId);                   // get from objBuffer, free by objBuffer
    LOG_INFO("execSendECTaskParallel read obj from objBuffer done, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d", 
             filename.c_str(), nodeId, srcNodeId, dstNodeId, objId);
    // 2. set svr for send data and start time 
    // | sendStart | data |
    const std::string key = filename + "_send_" + std::to_string(srcNodeId) + "_" + 
                                         std::to_string(dstNodeId) + "_" + std::to_string(objId);

    svr.Get("/" + key, [=, &sendStart, &sendEnd](const httplib::Request& req, httplib::Response& res) {
		LOG_INFO("http send, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d", 
                filename.c_str(), nodeId, srcNodeId, dstNodeId, objId);
        char* sendBuf = new char [bufSizeByte + sizeof(timeval)];
        memcpy(sendBuf + sizeof(timeval), buf, bufSizeByte);    // TODO: remove memcpy, send directly
        gettimeofday(&sendStart, NULL);
        memcpy(sendBuf, &sendStart, sizeof(timeval));
		res.set_content(sendBuf, bufSizeByte + sizeof(timeval), "application/octet-stream");
        delete [] sendBuf;
        gettimeofday(&sendEnd, NULL);
        LOG_INFO("http send done, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d", 
                filename.c_str(), nodeId, srcNodeId, dstNodeId, objId);
	});

    // 3. send ready flag to httpServiceThd
    redisContext* startHttpServiceCtx = RedisUtil::createContext(_conf->_localIp);
    assert(startHttpServiceCtx != NULL);
    redisReply* startHttpServiceReply = (redisReply*)redisCommand(startHttpServiceCtx, "rpush execSendECTaskParallel_start 0");
    assert(startHttpServiceReply != NULL && startHttpServiceReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(startHttpServiceReply);


    // 4. wait for receiver flag
    redisContext* wait4ReceiverCtx = RedisUtil::createContext(_conf->_localIp);
    assert(wait4ReceiverCtx != NULL);
    redisReply* wait4ReceiverReply = (redisReply*)redisCommand(wait4ReceiverCtx, "blpop %s 0", key.c_str());
    assert(wait4ReceiverReply != NULL && wait4ReceiverReply->type == REDIS_REPLY_ARRAY && wait4ReceiverReply->elements == 2);
    freeReplyObject(wait4ReceiverReply);
    redisFree(wait4ReceiverCtx);
    LOG_INFO("execSendECTaskParallel receive flag from dstNodeId: %d, objId: %d", dstNodeId, objId);

    LOG_INFO("execSendECTask done, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, time: %f ms", 
            filename.c_str(), nodeId, srcNodeId, dstNodeId, objId, RedisUtil::duration(sendStart, sendEnd));
    return {sendStart, sendEnd};
}

/**
 * receive obj by http, insert to objBuffer
 */
std::pair<timeval, timeval> ECWorker::execReceiveECTaskParallel(const std::string& filename, const ECTask* task, ObjParallelBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const int dstNodeId = task->_dstNodeId;
    const int srcNodeId = task->_srcNodeId;
    const int objId = task->_objId;
    const int tmpObjId = task->_tmpObjId;
    struct timeval receiveStart, receiveEnd;
    LOG_INFO("execReceiveECTaskParallel start, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, tmpObjId: %d", 
            filename.c_str(), nodeId, srcNodeId, dstNodeId, objId, tmpObjId);
    const std::string receiveObjKey = filename + "_send_" + std::to_string(srcNodeId) + "_" + 
                                    std::to_string(dstNodeId) + "_" + std::to_string(objId);
    // TODO: get ready flag from sender
    // 1. receive data and start time from sender 
    Client cli(RedisUtil::ip2Str(_conf->_agent_ips[srcNodeId]).c_str(), 8080);
    
    do {
        auto res = cli.Get("/" + receiveObjKey);
        if (res && res->status == 200) {
            assert(res->body.size() == _conf->_objSize * 1024 * 1024 + sizeof(struct timeval)); // start time | data

            int objSizeByte = _conf->_objSize * 1024 * 1024;
            char* obj = new char[objSizeByte];      // insert into objBuffer, free by objBuffer
            memcpy(&receiveStart, res->body.data(), sizeof(struct timeval));
            gettimeofday(&receiveEnd, NULL);
            memcpy(obj, res->body.data() + sizeof(timeval), objSizeByte);
            objBuffer->insertObj(tmpObjId, obj);
            break;
        }
    } while (true);

    // 3. send reply to sender
    redisContext* receiveObjCtx = RedisUtil::createContext(_conf->_agent_ips[srcNodeId]);
    redisReply* receiveObjReply = (redisReply*)redisCommand(receiveObjCtx, "rpush %s 1", receiveObjKey.c_str());
    assert(receiveObjReply != NULL && receiveObjReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(receiveObjReply);
    redisFree(receiveObjCtx);
    
    // gettimeofday(&receiveEnd, NULL);
    LOG_INFO("execReceiveECTaskParallel done, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, tmpObjId: %d, receive time: %f ms", 
             filename.c_str(), nodeId, srcNodeId, dstNodeId, objId, tmpObjId, RedisUtil::duration(receiveStart, receiveEnd));
    return {receiveStart, receiveEnd};
}

/**
 * read buf from objBuffer, encode insert to objBuffer
 */
std::pair<timeval, timeval> ECWorker::execEncodeECTaskParallel(const std::string& filename, const ECTask* task, ObjParallelBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const std::vector<int> objIds = task->_objIds;
    const int tmpObjId = task->_tmpObjId;
    const std::vector<int> coefs = task->_coefs;
    int objSizeByte = _conf->_objSize * 1024 * 1024;
    timeval encodeStart, encodeEnd;
    LOG_INFO("execEncodeECTaskParallel start, filename: %s, nodeId: %d, objNum: %ld, objIds: %s, tmpObjId: %d, encodePatternId: %d, coefs: %s",
             filename.c_str(), nodeId, objIds.size(), vec2String(objIds).c_str(), tmpObjId, task->_encodePatternId, vec2String(coefs).c_str()); 
    
    std::vector<const char*> objBufs;                   // get obj from objBuffer, free by objBuffer
    for (int objId : objIds) {
        const char* objBuf = objBuffer->getObj(objId);
        objBufs.push_back(objBuf);
    }
    gettimeofday(&encodeStart, NULL);
    char* encodeBuf = new char[objSizeByte];            // will insert into objBuffer, free by objBuffer
    memset(encodeBuf, 0, objSizeByte); 
    // #define MULTI_CHUNK_NUM 2
    // for (int i = 0; i < MULTI_CHUNK_NUM; i++)
    RSPlan::encode(objBufs, encodeBuf, coefs, _conf->_rsParam.w, objSizeByte);
    objBuffer->insertObj(tmpObjId, encodeBuf);
    gettimeofday(&encodeEnd, NULL);
    LOG_INFO("execEncodeECTaskParallel done, filename: %s, nodeId: %d, objNum: %ld, objIds: %s, tmpObjId: %d, encodePatternId: %d, coefs: %s, encode time: %f ms",
             filename.c_str(), nodeId, objIds.size(), vec2String(objIds).c_str(), tmpObjId, 
             task->_encodePatternId, vec2String(coefs).c_str(), RedisUtil::duration(encodeStart, encodeEnd)); 
    return {encodeStart, encodeEnd};
}


std::pair<timeval, timeval> ECWorker::execPersistECTaskParallel(const std::string& filename, const ECTask* task, ObjParallelBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const int objId = task->_objId;
    const int tmpObjId = task->_tmpObjId;
    struct timeval persistStart, persistEnd;
    LOG_INFO("execPersistECTaskParallel start, filename: %s, nodeId: %d, objId: %d, tmpObjId: %d", filename.c_str(), nodeId, objId, tmpObjId);
    
    char* objBuf = objBuffer->getObj(tmpObjId);         // free by objBuffer
    gettimeofday(&persistStart, NULL);

    const std::string objname = filename + "_lmqobj_" + std::to_string(objId);
    int objSizeByte = _conf->_objSize * 1024 * 1024;

    hdfsFile file = _hdfsHandler->openFile(objname, HDFSMode::WRITE);
    _hdfsHandler->write2HDFS(file, objBuf, objSizeByte);
    _hdfsHandler->closeFile(file);
    gettimeofday(&persistEnd, NULL);
    LOG_INFO("execPersistECTaskParallel done, filename: %s, nodeId: %d, objId: %d, tmpObjId: %d, persist time: %f ms", 
            filename.c_str(), nodeId, objId, tmpObjId, RedisUtil::duration(persistStart, persistEnd));
    return {persistStart, persistEnd};
}


/**
 * 1. wait for sendECTask threads get ready done
 * 2. start http listen
 */
void ECWorker::startHttpService(httplib::Server& svr, const std::vector<ECTask*>& tasks, std::thread& svrThd) {
    int sendECTaskNum = 0;
    for (const auto& task : tasks) {
        if (task->_type == ECTaskType::SEND) {
            sendECTaskNum++;
        }
    }
    if (sendECTaskNum == 0) {
        return;
    }
    redisContext* startHttpServiceCtx = RedisUtil::createContext(_conf->_localIp);
    assert(startHttpServiceCtx != NULL);
    for (int i = 0; i < sendECTaskNum; i++) {
        redisReply* startHttpServiceReply = (redisReply*)redisCommand(startHttpServiceCtx, "blpop %s 0", "execSendECTaskParallel_start");
		assert(startHttpServiceReply != NULL && startHttpServiceReply -> type == REDIS_REPLY_ARRAY 
               && startHttpServiceReply -> elements == 2);
        freeReplyObject(startHttpServiceReply);
    }
    LOG_INFO("start listen, send task num: %d", sendECTaskNum);
    svrThd = std::thread([&](){
		svr.listen("0.0.0.0", 8080);
    });

    redisFree(startHttpServiceCtx);
}


void ECWorker::printTime(const ConcurrentMap& timeMap, int taskNum, const std::vector<ECTask*>& tasks) {
    double fetchStartTime, sendStartTime, receiveStartTime, encodeStartTime, persistStartTime;
    double fetchEndTime, sendEndTime, receiveEndTime, encodeEndTime, persistEndTime;
    fetchStartTime = sendStartTime = receiveStartTime = encodeStartTime = persistStartTime = std::numeric_limits<double>::max();
    fetchEndTime = sendEndTime = receiveEndTime = encodeEndTime = persistEndTime = std::numeric_limits<double>::min();
    double execStartTime = std::numeric_limits<double>::max();
    double execEndTime = std::numeric_limits<double>::min();
    for (int i = 0; i < taskNum; i++) {
        switch (tasks[i]->_type) {
            case ECTaskType::FETCH:
                fetchStartTime = std::min(fetchStartTime, timeMap._timeMap.at(i).first.tv_sec * 1000.0 
                                          + timeMap._timeMap.at(i).first.tv_usec / 1000.0);
                fetchEndTime = std::max(fetchEndTime, timeMap._timeMap.at(i).second.tv_sec * 1000.0 
                                        + timeMap._timeMap.at(i).second.tv_usec / 1000.0);
                execStartTime = std::min(execStartTime, fetchStartTime);
                execEndTime = std::max(execEndTime, fetchEndTime);
                break;
            case ECTaskType::SEND:
                sendStartTime = std::min(sendStartTime, timeMap._timeMap.at(i).first.tv_sec * 1000.0 
                                         + timeMap._timeMap.at(i).first.tv_usec / 1000.0);
                sendEndTime = std::max(sendEndTime, timeMap._timeMap.at(i).second.tv_sec * 1000.0 
                                        + timeMap._timeMap.at(i).second.tv_usec / 1000.0);
                execStartTime = std::min(execStartTime, sendStartTime);
                execEndTime = std::max(execEndTime, sendEndTime);
                break;
            case ECTaskType::RECEIVE:
                receiveStartTime = std::min(receiveStartTime, timeMap._timeMap.at(i).first.tv_sec * 1000.0 
                                            + timeMap._timeMap.at(i).first.tv_usec / 1000.0);
                receiveEndTime = std::max(receiveEndTime, timeMap._timeMap.at(i).second.tv_sec * 1000.0 
                                          + timeMap._timeMap.at(i).second.tv_usec / 1000.0);
                execStartTime = std::min(execStartTime, receiveStartTime);
                execEndTime = std::max(execEndTime, receiveEndTime);
                break;
            case ECTaskType::ENCODE:
            case ECTaskType::ENCODE_PARTIAL:
                encodeStartTime = std::min(encodeStartTime, timeMap._timeMap.at(i).first.tv_sec * 1000.0 
                                           + timeMap._timeMap.at(i).first.tv_usec / 1000.0);
                encodeEndTime = std::max(encodeEndTime, timeMap._timeMap.at(i).second.tv_sec * 1000.0 
                                         + timeMap._timeMap.at(i).second.tv_usec / 1000.0);
                execStartTime = std::min(execStartTime, encodeStartTime);
                execEndTime = std::max(execEndTime, encodeEndTime);
                break;
            case ECTaskType::PERSIST:
                persistStartTime = std::min(persistStartTime, timeMap._timeMap.at(i).first.tv_sec * 1000.0 
                                            + timeMap._timeMap.at(i).first.tv_usec / 1000.0);
                persistEndTime = std::max(persistEndTime, timeMap._timeMap.at(i).second.tv_sec * 1000.0 
                                          + timeMap._timeMap.at(i).second.tv_usec / 1000.0);
                execStartTime = std::min(execStartTime, persistStartTime);
                execEndTime = std::max(execEndTime, persistEndTime);
                break;
        }
    }

    double fetchTime = fetchStartTime != std::numeric_limits<double>::max() ? fetchEndTime - fetchStartTime : -1.0;
    double sendTime = sendStartTime != std::numeric_limits<double>::max() ? sendEndTime - sendStartTime : -1.0;
    double receiveTime = receiveStartTime != std::numeric_limits<double>::max() ? receiveEndTime - receiveStartTime : -1.0;
    double encodeTime = encodeStartTime != std::numeric_limits<double>::max() ? encodeEndTime - encodeStartTime : -1.0;
    double persistTime = persistStartTime != std::numeric_limits<double>::max() ? persistEndTime - persistStartTime : -1.0;
    double execTime = execStartTime != std::numeric_limits<double>::max() ? execEndTime - execStartTime : -1.0;
    std::ofstream logFile("/root/coar/build/repair.log", std::ios::app);
    assert(logFile.is_open());
    logFile << fetchTime << " " << sendTime << " " <<  receiveTime << " " << encodeTime << " " << persistTime << " " << execTime << std::endl;
    logFile.close();


    LOG_INFO("fetch time: %f, send time: %f, receive time: %f, encode time: %f, persist time: %f, execTime: %f", 
             fetchTime, sendTime, receiveTime, encodeTime, persistTime, execTime);
}

/**
 * Record GF computation history data to Redis for overhead prediction
 */
void ECWorker::recordGFComputationHistory(double cpu_util, int num_blocks, int block_size, double overhead_ms) {
    const std::string queue_name = "gf_overhead_queue";
    // node_id:cpu_usage:w:blocks:block_size:time

    const std::string record = std::to_string(_conf->_node_id) + ":" + 
                                std::to_string(cpu_util) + ":" + 
                                std::to_string(_conf->_rsParam.w) + ":" + 
                                std::to_string(num_blocks) + ":" + 
                                std::to_string(block_size) + ":" +
                                std::to_string(overhead_ms); 
    redisReply* reply = (redisReply*)redisCommand(_coorCtx, "rpush %s %b", queue_name.c_str(), record.c_str(), record.size());
    assert(reply != NULL && reply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(reply);

    LOG_INFO("Recorded GF computation history: cpu_util: %f, num_blocks: %d, block_size: %d, overhead: %f", 
            cpu_util, num_blocks, block_size, overhead_ms);    
}

/**
 * Get current CPU utilization percentage before exec task
 */
double ECWorker::getCurrentCPUUtilization() {
    const std::string key = "cpu_" + _conf->_localIpStr;
    redisReply* reply = (redisReply*)redisCommand(_localCtx, "GET %s", key.c_str());
    assert(reply != NULL && reply->type == REDIS_REPLY_STRING);
    double cpu_util = std::stod(reply->str);
    freeReplyObject(reply);
    return cpu_util;
}
