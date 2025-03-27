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