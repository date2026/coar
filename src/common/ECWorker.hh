#ifndef _OECWORKER_HH_
#define _OECWORKER_HH_

#include "BlockingQueue.hh"
#include "Config.hh"
#include "FSObjOutputStream.hh"
#include "ECDataPacket.hh"
#include "FileMeta.hh"


#include "../ec/ECTask.hh"
#include "../fs/UnderFS.hh"
#include "../fs/FSUtil.hh"
#include "../fs/HDFSHandler.hh"
#include "../inc/include.hh"
#include "../protocol/AGCommand.hh"
#include "../protocol/CoorCommand.hh"
#include "../util/RedisUtil.hh"
using namespace std;
class ECWorker {
private: 
    Config* _conf;

    redisContext* _processCtx;
    redisContext* _localCtx;
    redisContext* _coorCtx;

    UnderFS* _underfs;
    HDFSHandler* _hdfsHandler;
public:
    ECWorker(Config* conf);
    ~ECWorker();
    void doProcess();
    // deal with client request
    void clientWrite(AGCommand* agCmd);
    void clientRead(AGCommand* agCmd);
	void receiveObjAndPersist(AGCommand* agCmd);

    // load data from redis
    void loadWorker(BlockingQueue<ECDataPacket*>* readQueue,
                    string keybase,
                    int startid,
                    int step,
                    int round,
                    bool zeropadding);

    void send4PersistObjWorker(BlockingQueue<ECDataPacket*>* readQueue, 
                                const std::string& objname, int pktNum, int objLoc);
    void readObj(AGCommand* agCmd);
};

#endif
