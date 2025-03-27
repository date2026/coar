#include "Coordinator.hh"

Coordinator::Coordinator(Config* conf, StripeStore* ss) : _conf(conf) {
	try {
		_localCtx = RedisUtil::createContext(_conf -> _localIp);
	} catch (int e) {
		cerr << "initializing redis context to " << " error" << endl;
	}
	_stripeStore = ss;
	_underfs = FSUtil::createFS(_conf->_fsType, _conf->_fsFactory[_conf->_fsType], _conf);
	srand((unsigned)time(0));
}

Coordinator::~Coordinator() {
  redisFree(_localCtx);
}

void Coordinator::doProcess() {
	redisReply* rReply;
	while (true) {
		LOG_INFO("Coordinator::doProcess waiting for request");
		rReply = (redisReply*)redisCommand(_localCtx, "blpop coor_request 0");
		assert(rReply != NULL && rReply -> type == REDIS_REPLY_ARRAY && rReply -> elements == 2);
		char* reqStr = rReply -> element[1] -> str;
		CoorCommand* coorCmd = new CoorCommand(reqStr);
		int type = coorCmd->getType();
		switch (type) {
			case 0: registerFile(coorCmd); break;
            case 3: readFileMeta(coorCmd); break;
			default: break;
		}
		delete coorCmd;
	}
		
	freeReplyObject(rReply);
}

void Coordinator::registerFile(CoorCommand* coorCmd) {
	unsigned int clientIp = coorCmd->getClientip();
	string filename = coorCmd->getFilename();
	string ecpoolid = coorCmd->getEcid();
	int mode = coorCmd->getMode();
	int filesizeMB = coorCmd->getFilesizeMB();

	LOG_INFO("Coordinator::registerOfflineEC, filename: %s, ecpoolid: %s, filesizeMB: %d", filename.c_str(), ecpoolid.c_str(), filesizeMB);
	struct timeval time1, time2, time3, time4;
		
	int objSize = _conf->_objSize;
	
	assert(filesizeMB % objSize == 0);
	int objnum = filesizeMB / objSize;


	// 1. ensure file not exist
	assert(!_stripeStore->existFile(filename));
	LOG_INFO("check file: %s exist done", filename.c_str());

	// 2. create file recipe, get ojblocs
	std::vector<int> objLocs = _stripeStore->insertFile(filename, filesizeMB * 1024 * 1024, objnum);
	LOG_INFO("register file: %s, objnum: %d, objSize: %d, objLocs: %s", filename.c_str(), objnum, objSize, vec2String(objLocs).c_str());
	
	vector<string> fileobjnames;


	// 3. send to agent instructions
	AGCommand* agCmd = new AGCommand();
	agCmd->buildType11(11, objnum, objSize, objLocs);
	agCmd->setRkey("registerFile:"+filename);
	agCmd->sendTo(clientIp);
	delete agCmd;

}

/**
 * read file meta
 * called by agent
 * return file meta
 */
void Coordinator::readFileMeta(CoorCommand* coorCmd) {
    string filename = coorCmd->getFilename();
    LOG_INFO("Coordinator::readFileMeta, filename: %s", filename.c_str());
    
    // 1. get file recipe
    const FileMeta* fileMeta = _stripeStore->getFileMeta(filename);
    std::vector<int> objLocs = fileMeta->getObjLocs();
    int objnum = objLocs.size();
    int filesizeMB = objnum * _conf->_objSize;
    LOG_INFO("Coordinator::readFileMeta, send meta to agent, filename: %s, filesizeMB: %d, objnum: %d, objLocs: %s", 
            filename.c_str(), filesizeMB, objnum, vec2String(objLocs).c_str());
    // 2. return filemeta to agent
    int fileMetaSize = sizeof(int) + sizeof(int) + objnum * sizeof(int);
    char* buf = new char [fileMetaSize];
    fileMeta->dumpFileMeto2Buf(buf);
    const std::string retFileMetaKey = filename + "_meta";
    redisContext* retFileMetaCtx = RedisUtil::createContext(coorCmd->getClientip());
    redisReply* retFileMetaReply = (redisReply*)redisCommand(retFileMetaCtx, 
                                "rpush %s %b", retFileMetaKey.c_str(), buf, fileMetaSize);
    assert(retFileMetaReply != NULL && retFileMetaReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(retFileMetaReply);
    redisFree(retFileMetaCtx);
}
