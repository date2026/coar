#ifndef _OECWORKER_HH_
#define _OECWORKER_HH_

#include "BlockingQueue.hh"
#include "Config.hh"
#include "FSObjOutputStream.hh"
#include "ECDataPacket.hh"
#include "FileMeta.hh"


#include "../fs/UnderFS.hh"
#include "../fs/FSUtil.hh"
#include "../fs/HDFSHandler.hh"
#include "../inc/include.hh"
#include "../protocol/AGCommand.hh"
#include "../protocol/CoorCommand.hh"
#include "../util/RedisUtil.hh"
#include "../ec/ECPlan.hh"
#include "../ec/RSPlan.hh"
#include "ObjBuffer.hh"
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
    void clientEncode(AGCommand* agCmd);
    void clientDecode(AGCommand* agCmd);
	void receiveObjAndPersist(AGCommand* agCmd);
    void execECTasks(AGCommand* agCmd);

    // load data from redis, called by clientWrite
    void loadWorker(BlockingQueue<ECDataPacket*>* readQueue,
                    string keybase,
                    int startid,
                    int step,
                    int round,
                    bool zeropadding);
    // send obj to agents to persist, called by clientWrite to persist objs to agents
    void send4PersistObjWorker(BlockingQueue<ECDataPacket*>* readQueue, 
                                const std::string& objname, int pktNum, int objLoc);
    void readObj(AGCommand* agCmd);

    // exec ec task, called by execECTasks
    double execSendECTask(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer);
    double execReceiveECTask(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer);
    double execSendECTaskByRedis(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer);
    double execReceiveECTaskByRedis(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer);
    double execEncodeECTask(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer);
    double execPersistECTask(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer);
    double execSendECTaskByHttp(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer);
    double execReceiveECTaskByHttp(const std::string& filename, const ECTask* task, ObjBuffer* objBuffer);


};

#endif
