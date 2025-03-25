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
			default: break;
		}
		delete coorCmd;
	}
		
	freeReplyObject(rReply);
}

void Coordinator::registerFile(CoorCommand* coorCmd) {
	unsigned int clientIp = coorCmd->getClientip();
	string filename = coorCmd->getFilename();
	string ecid = coorCmd->getEcid();
	int mode = coorCmd->getMode();
	int filesizeMB = coorCmd->getFilesizeMB();

	registerOfflineEC(clientIp, filename, ecid, filesizeMB);
}

void Coordinator::registerOfflineEC(unsigned int clientIp, string filename, string ecpoolid, int filesizeMB) {
	LOG_INFO("Coordinator::registerOfflineEC, filename: %s, ecpoolid: %s, filesizeMB: %d", filename.c_str(), ecpoolid.c_str(), filesizeMB);
	struct timeval time1, time2, time3, time4;
		
	int basesizeMB = _conf->_objSize;
	LOG_INFO("basesizeMB: %d", basesizeMB);
	
	assert(filesizeMB % basesizeMB == 0);
	int objnum = filesizeMB / basesizeMB;


	vector<string> fileobjnames;
	vector<unsigned int> fileobjlocs;


	// 3. send to agent instructions
	AGCommand* agCmd = new AGCommand();
	agCmd->buildType11(11, objnum, basesizeMB);
	agCmd->setRkey("registerFile:"+filename);
	agCmd->sendTo(clientIp);
	delete agCmd;

}
