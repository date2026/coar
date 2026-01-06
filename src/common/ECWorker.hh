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

// #define USE_GRPC

#include <grpcpp/grpcpp.h>
#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "helloworld.grpc.pb.h"
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;
using helloworld::Greeter;
using helloworld::HelloReply;
using helloworld::HelloRequest;


#ifdef USE_GRPC
class GreeterClient {
 public:
  GreeterClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(Greeter::NewStub(channel)) {}

  std::string SayHello(const std::string& user) {
    HelloRequest request;
    request.set_name(user);

    HelloReply reply;

    grpc::ClientContext context;

    grpc::Status status = stub_->SayHello(&context, request, &reply);

    if (status.ok()) {
      return reply.message();
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  std::string GetObj(int objId, int left, int right) {
    HelloRequest request;
    request.set_objid(objId);
    request.set_left(left);
    request.set_right(right);
    HelloReply reply;
    grpc::ClientContext context;

    while (true) {
        grpc::Status status = stub_->GetObj(&context, request, &reply);
        if (status.ok()) {
            return reply.obj();
        } 

    }
  }

 private:
  std::unique_ptr<Greeter::Stub> stub_;
};

class GreeterServiceImpl final : public Greeter::Service {
 public:
  GreeterServiceImpl(ObjParallelBuffer* objBuffer) : objBuffer_(objBuffer) {}

  Status SayHello(ServerContext* context, const HelloRequest* request,
                  HelloReply* reply) override {
    std::string prefix("Hello ");
    reply->set_message(prefix + request->name());
    return Status::OK;
  }

  Status GetObj(ServerContext* context, const HelloRequest* request,
    HelloReply* reply) override {
    // Now you can access objBuffer through objBuffer_
    int objId = request->objid();
    char* obj = objBuffer_->getObj(objId);
    int left = request->left();
    int right = request->right();
    int objSize = right - left + 1;
    reply->set_obj(obj, objSize);
    return Status::OK;
  }

 private:
  ObjParallelBuffer* objBuffer_;  // Pointer to objBuffer
};

#endif
class ECWorker {
private: 
    Config* _conf;

    redisContext* _processCtx;
    redisContext* _localCtx;
    redisContext* _coorCtx;

    UnderFS* _underfs;
    HDFSHandler* _hdfsHandler;

    std::mutex _svrMutex;                // provide concurrency when init api for httpserver
#ifdef USE_GRPC
    std::shared_ptr<grpc::Server> _grpcServer;  // gRPC server instance
    std::mutex _grpcServerMutex;                 // mutex for gRPC server access
#endif
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
    std::pair<timeval, timeval> execSendECTaskParallelWithGRPC(const std::string& filename, const ECTask* task, ObjParallelBuffer* objBuffer, std::shared_ptr<grpc::Server> svr);
    std::pair<timeval, timeval> execReceiveECTaskParallelWithGRPC(const std::string& filename, const ECTask* task, ObjParallelBuffer* objBuffer);
    std::pair<timeval, timeval> execEncodeECTaskParallel(const std::string& filename, const ECTask* task, ObjParallelBuffer* objBuffer);
    std::pair<timeval, timeval> execPersistECTaskParallel(const std::string& filename, const ECTask* task, ObjParallelBuffer* objBuffer);
    void startHttpService(httplib::Server& svr, const std::vector<ECTask*>& tasks, std::thread& svrThd);
    void startGRPCService(const std::vector<ECTask*>& tasks, ObjParallelBuffer* objBuffer, std::thread& svrThd);
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
