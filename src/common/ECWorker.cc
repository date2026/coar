#include "ECWorker.hh"

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
            case 16: execECTasks(agCmd); break;
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
	delete [] loadQueue;

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
		redisAppendCommand(readCtx, "blpop %s 1", key.c_str());
	}
	redisReply* rReply;
	for (int i=0; i<round; i++) {
		redisGetReply(readCtx, (void**)&rReply);
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
	LOG_INFO("receiveObjAndPersist, receive objname: %s", objname.c_str());

    hdfsFile file = _hdfsHandler->openFile(objname, HDFSMode::WRITE);
    _hdfsHandler->write2HDFS(file, buf, bufSize);
    _hdfsHandler->closeFile(file);

	// write2HDFS((hdfsFS)_underfs, objname, buf, bufSize);
	delete[] buf;
	LOG_INFO("receiveObjAndPersist done, objname: %s", objname.c_str());
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
    LOG_INFO("clientDecode start, filename: %s, ecdagPath: %s", filename.c_str(), ecdagPath.c_str());

    // 1. send decode request to coordinator
    CoorCommand* coorCmd = new CoorCommand();
    coorCmd->buildType14(14, filename, _conf->_localIp, ecdagPath);
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
    redisReply* agentDecodeDoneReply = (redisReply*)redisCommand(_localCtx, "rpush 1", decodeDoneKey.c_str());
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
    for (const auto task : tasks) {
        switch (task->_type) {
            case ECTaskType::SEND:
                execSendECTask(filename, task, objBuffer);
                break;
            case ECTaskType::RECEIVE:
                execReceiveECTask(filename, task, objBuffer);
                break;
            case ECTaskType::ENCODE:
                execEncodeECTask(filename, task, objBuffer);
                break;
            case ECTaskType::PERSIST:
                execPersistECTask(filename, task, objBuffer);
                break;
            default:
                assert(false && "undefined ECTaskType");
        }
    }    
    LOG_INFO("ECWorker exec tasks done, filename: %s, taskNum: %d", filename.c_str(), taskNum);

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


void ECWorker::execSendECTask(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const int srcNodeId = task->_srcNodeId;
    const int dstNodeId = task->_dstNodeId;
    const int objId = task->_objId;

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
    LOG_INFO("execSendECTask done, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d", 
            filename.c_str(), nodeId, srcNodeId, dstNodeId, objId);
}

void ECWorker::execReceiveECTask(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const int dstNodeId = task->_dstNodeId;
    const int srcNodeId = task->_srcNodeId;
    const int objId = task->_objId;
    const int tmpObjId = task->_tmpObjId;

    LOG_INFO("execReceiveECTask start, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, tmpObjId: %d", 
            filename.c_str(), nodeId, srcNodeId, dstNodeId, objId, tmpObjId);
    const std::string receiveObjKey = filename + "_send_" + std::to_string(srcNodeId) + "_" + 
                                    std::to_string(dstNodeId) + "_" + std::to_string(objId);
    redisReply* receiveObjRely = (redisReply*)redisCommand(_localCtx, "blpop %s 0", receiveObjKey.c_str());
    assert(receiveObjRely != NULL && receiveObjRely->type == REDIS_REPLY_ARRAY 
            && receiveObjRely->elements == 2);
    char* taskStr = receiveObjRely->element[1]->str;
    int objSizeByte = _conf->_objSize * 1024 * 1024;
    char* obj = new char[objSizeByte];      // insert into objBuffer, free by objBuffer
    memcpy(obj, taskStr, objSizeByte);
    objBuffer->insertObj(tmpObjId, obj);
    freeReplyObject(receiveObjRely);
    LOG_INFO("execReceiveECTask done, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, tmpObjId: %d", 
             filename.c_str(), nodeId, srcNodeId, dstNodeId, objId, tmpObjId);
}

void ECWorker::execEncodeECTask(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const std::vector<int> objIds = task->_objIds;
    const int tmpObjId = task->_tmpObjId;
    const std::vector<int> coefs = task->_coefs;
    int objSizeByte = _conf->_objSize * 1024 * 1024;

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
    // TODO: check parameters for encode, especially coefs(matrix)
    RSPlan::encode(objBufs, encodeBuf, coefs, _conf->_rsParam.w, objSizeByte);
    objBuffer->insertObj(tmpObjId, encodeBuf);
    LOG_INFO("execEncodeECTask done, filename: %s, nodeId: %d, objNum: %ld, objIds: %s, tmpObjId: %d, encodePatternId: %d, coefs: %s",
             filename.c_str(), nodeId, objIds.size(), vec2String(objIds).c_str(), tmpObjId, 
             task->_encodePatternId, vec2String(coefs).c_str()); 

}

void ECWorker::execPersistECTask(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const int objId = task->_objId;
    const int tmpObjId = task->_tmpObjId;

    LOG_INFO("execSendECTask start, filename: %s, nodeId: %d, objId: %d, tmpObjId: %d", filename.c_str(), nodeId, objId, tmpObjId);
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
    
}