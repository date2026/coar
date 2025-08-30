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
#include "../util/httplib.h"
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

    std::mutex _svrMutex;                // provide concurrency when init api for httpserver
    // Record GF computation history data
    void recordGFComputationHistory(double cpu_util, int num_blocks, int block_size, double overhead_ms);
    double getCurrentCPUUtilization();

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
    void execECTasksParallel(AGCommand* agCmd);
    void execECPipeTasksParallel(AGCommand* agCmd);
    void execECPipeFGTasksParallel(AGCommand* agCmd);
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
    std::pair<timeval, timeval> execFetchECTaskParallel(const std::string& filename, const ECTask* task, ObjParallelBuffer* objBuffer);
    std::pair<timeval, timeval> execSendECTaskParallel(const std::string& filename, const ECTask* task, ObjParallelBuffer* objBuffer, httplib::Server& svr);
    std::pair<timeval, timeval> execReceiveECTaskParallel(const std::string& filename, const ECTask* task, ObjParallelBuffer* objBuffer);
    std::pair<timeval, timeval> execEncodeECTaskParallel(const std::string& filename, const ECTask* task, ObjParallelBuffer* objBuffer);
    std::pair<timeval, timeval> execPersistECTaskParallel(const std::string& filename, const ECTask* task, ObjParallelBuffer* objBuffer);
    void startHttpService(httplib::Server& svr, const std::vector<ECTask*>& tasks, std::thread& svrThd);
    void printTime(const ConcurrentMap& timeMap, int taskNum, const std::vector<ECTask*>& tasks);
    std::pair<timeval, timeval> execFetchECPipeTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer);
    std::pair<timeval, timeval> execSendECPipeTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer, httplib::Server& svr);
    std::pair<timeval, timeval> execReceiveECPipeTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer);
    std::pair<timeval, timeval> execEncodeECPipeTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer);
    std::pair<timeval, timeval> execPersistECPipeTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer);

    /**
     * exec ecpipe fg task, called by execECPipeFGTasksParallel
     */
    std::pair<timeval, timeval> execFetchECPipeFGTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer);
    std::pair<timeval, timeval> execSendECPipeFGTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer, httplib::Server& svr);
    std::pair<timeval, timeval> execReceiveECPipeFGTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer);
    std::pair<timeval, timeval> execEncodeECPipeFGTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer);
    std::pair<timeval, timeval> execPersistECPipeFGTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer);
};

#endif
