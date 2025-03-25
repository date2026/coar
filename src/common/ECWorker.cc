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

	_underfs = FSUtil::createFS(_conf->_fsType, _conf->_fsFactory[_conf->_fsType], _conf);

}

ECWorker::~ECWorker() {
	redisFree(_localCtx);
	redisFree(_processCtx);
	redisFree(_coorCtx);
	delete _underfs;
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
			default:break;
		}

		delete agCmd;
	}
	freeReplyObject(rReply); 
}


void ECWorker::clientWrite(AGCommand* agcmd) {
	string filename = agcmd->getFilename();
	string ecid = agcmd->getEcid();
	string mode = agcmd->getMode();
	int filesizeMB = agcmd->getFilesizeMB();
	offlineWrite(filename, ecid, filesizeMB);
}

void ECWorker::offlineWrite(string filename, string ecpoolid, int filesizeMB) {
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
	delete agCmd;
	gettimeofday(&time2, NULL);
	LOG_INFO("offlineWrite::get response from coordinator, objnum: %d, basesizeMB: %d", objnum, basesizeMB);



	int pktNumPerObj = _conf->_objSize * 1024 * 1024 / _conf->_pktSize;
	// 2. create outputstream for each obj
	FSObjOutputStream** objstreams = (FSObjOutputStream**)calloc(objnum, sizeof(FSObjOutputStream*));
	for (int i = 0; i < objnum; i++) {
		string objname = filename+"_lmqobj_"+to_string(i);
		objstreams[i] = new FSObjOutputStream(_conf, objname, _underfs, pktNumPerObj);
	}


	BlockingQueue<ECDataPacket*>** loadQueue = (BlockingQueue<ECDataPacket*>**)calloc(objnum, sizeof(BlockingQueue<ECDataPacket*>*));
	for (int i=0; i<objnum; i++) {
		loadQueue[i] = objstreams[i]->getQueue();
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
		persistThreads[i] = thread([=]{ objstreams[i]->writeObj(); });
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
	for (int i = 0; i < objnum; i++) delete objstreams[i];
	free(objstreams);
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
