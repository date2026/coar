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
            case 13: encode(coorCmd); break;
            case 14: decode(coorCmd); break;
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

    _stripeStore->dump2File();
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
    FileMeta* fileMeta = _stripeStore->getFileMeta(filename);
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
    delete [] buf;
    fileMeta->unlock();
}

/**
 * receive encode request from agent
 * coordinator encode plan with agents
 */
void Coordinator::encode(CoorCommand* coorCmd) {
    const std::string filename = coorCmd->getFilename();
    unsigned sendIp = coorCmd->getClientip();
    const std::string ecdagPath = coorCmd->getEcdagPath();
    LOG_INFO("Coordinator::encode, filename: %s, ecdagPath: %s, sendIp: %s", filename.c_str(), ecdagPath.c_str(), RedisUtil::ip2Str(sendIp).c_str());
    
    // 1. ensure file exist
    assert(_stripeStore->existFile(filename));
    FileMeta* fileMeta = _stripeStore->getFileMeta(filename);
    LOG_INFO("Coordinator:encode get file meta done, filename: %s, file size: %d, obj num: %d, obj locs: %s", 
             filename.c_str(), fileMeta->getFileSize(), fileMeta->getObjNum(), vec2String(fileMeta->getObjLocs()).c_str());

    // 2. get failed chunks, source chunks, dst node, coef, repair plan

    switch (_conf->_ecType) {
        case ECType::RS:
            encodeRS(filename, fileMeta, ecdagPath);
            break;
        case ECType::LRC:
            assert(false && "not implemented");
            break;
        case ECType::MSR:
            assert(false && "not implemented");
            break;
        default:
            assert(false && "undefined ec type");
            break;
    }

    // 3. return to coresponding agent
    const std::string encodeDoneKey = filename + "_coordinator_encode_done";
    redisContext* encodeDoneCtx = RedisUtil::createContext(sendIp);
    assert(encodeDoneCtx != NULL && "Failed to create redis context");
    redisReply* encodeDoneReply = (redisReply*)redisCommand(encodeDoneCtx, "rpush %s 1", encodeDoneKey.c_str());
    assert(encodeDoneReply != NULL && encodeDoneReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(encodeDoneReply);
    redisFree(encodeDoneCtx);
    LOG_INFO("Coordinator:encode done, filename: %s, sendIp: %s", filename.c_str(), RedisUtil::ip2Str(sendIp).c_str());
    _stripeStore->dump2File();
}


/**
 * receive decode request from agent
 * coordinator decode plan with agents
 */
void Coordinator::decode(CoorCommand* coorCmd) {
    const std::string filename = coorCmd->getFilename();
    unsigned sendIp = coorCmd->getClientip();
    const std::string ecdagPath = coorCmd->getEcdagPath();
    const std::vector<int> survivedObjIds = coorCmd->getSurvivedObjIds();
    const int failedObjId = coorCmd->getFailedObjId();
    LOG_INFO("Coordinator::decode, filename: %s, ecdagPath: %s, sendIp: %s, survivedObjIds: %s, failedObjId: %d", 
             filename.c_str(), ecdagPath.c_str(), RedisUtil::ip2Str(sendIp).c_str(), vec2String(survivedObjIds).c_str(), failedObjId);
    
    // 1. ensure file exist
    assert(_stripeStore->existFile(filename));
    FileMeta* fileMeta = _stripeStore->getFileMeta(filename);
    LOG_INFO("Coordinator:decode get file meta done, filename: %s, file size: %d, obj num: %d, obj locs: %s", 
             filename.c_str(), fileMeta->getFileSize(), fileMeta->getObjNum(), vec2String(fileMeta->getObjLocs()).c_str());

    // 2. get failed chunks, source chunks, dst node, coef, repair plan

    switch (_conf->_ecType) {
        case ECType::RS:
            decodeRS(filename, fileMeta, ecdagPath, survivedObjIds, failedObjId);
            break;
        case ECType::LRC:
            assert(false && "not implemented");
            break;
        case ECType::MSR:
            assert(false && "not implemented");
            break;
        default:
            assert(false && "undefined ec type");
            break;
    }

    // 3. return to coresponding agent
    const std::string decodeDoneKey = filename + "_coordinator_decode_done";
    redisContext* decodeDoneCtx = RedisUtil::createContext(sendIp);
    assert(decodeDoneCtx != NULL && "Failed to create redis context");
    redisReply* decodeDoneReply = (redisReply*)redisCommand(decodeDoneCtx, "rpush %s 1", decodeDoneKey.c_str());
    assert(decodeDoneReply != NULL && decodeDoneReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(decodeDoneReply);
    redisFree(decodeDoneCtx);
    LOG_INFO("Coordinator:decode done, filename: %s, sendIp: %s", filename.c_str(), RedisUtil::ip2Str(sendIp).c_str());
    _stripeStore->dump2File();
}






void Coordinator::encodeRS(const std::string& filename, FileMeta* fileMeta, const std::string& ecdagPath) {
    int k = _conf->_rsParam.k;
    int n = _conf->_rsParam.n;
    int w = _conf->_rsParam.w;

    // 1. init ECPlan
    RSPlan* rsPlan = new RSPlan(_conf, fileMeta, ecdagPath, k, n, w);

    // 2. send ec tasks
    rsPlan->send();
    
    // 3. wait for tasks done
    rsPlan->receive();
    delete rsPlan;
    fileMeta->unlock();
}


void Coordinator::decodeRS(const std::string& filename, FileMeta* fileMeta, const std::string& ecdagPath,
                           const std::vector<int>& survivedObjIds, int failedObjId) {      
    int k = _conf->_rsParam.k;
    int n = _conf->_rsParam.n;
    int w = _conf->_rsParam.w;

    // 1. init ECPlan
    RSPlan* rsPlan = new RSPlan(_conf, fileMeta, ecdagPath, k, n, w, survivedObjIds, failedObjId);

    // 2. send ec tasks
    rsPlan->send();
    
    // 3. wait for tasks done
    rsPlan->receive();
    delete rsPlan;
    fileMeta->unlock();
}
