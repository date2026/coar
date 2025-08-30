#include "ECWorker.hh"
using namespace httplib;
/**
 * exec ecpipe tasks in parallel
 */
void ECWorker::execECPipeTasksParallel(AGCommand* agCmd) {
    const std::string filename = agCmd->getFilename();
    int taskNum = agCmd->getEcTaskNum();
    LOG_INFO("execECPipeTasksParallel start, filename: %s, taskNum: %d", filename.c_str(), taskNum);

    // store obj and tmpobj, objname->obj
    BlockingQueueParallelBuffer* objBuffer = new BlockingQueueParallelBuffer();
    // 1. get tasks from coordinator
    const std::string receiveEcTasksKey = filename + "_ecTasks";
    std::vector<ECTask*> tasks;

    redisReply* receiveEcTasksReply;
    timeval execStart, execEnd;
    gettimeofday(&execStart, NULL);

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
    LOG_INFO("ECWorker exec tasks, filename: %s, taskNum: %d", filename.c_str(), taskNum);
    ConcurrentMap timeMap;
    double sendTime = 0.0, receiveTime = 0.0, encodeTime = 0.0, persistTime = 0.0;
    std::thread execThds[taskNum];
    httplib::Server svr;
    std::thread svrThd;             // thd for svr to listen
    std::thread httpThd = std::thread([this, &svr, &tasks, &svrThd](){ startHttpService(svr, tasks, svrThd); });
    /**
     * 叶子节点：fetch->send
     * 中间节点节点：receive, encode, send
     * 根节点：receive, persist
     */
    int receiveTaskId = 0;
    for (int i = 0; i < taskNum; i++) {
        switch (tasks[i]->_type) {
            case ECTaskType::SEND:
                execThds[i] = std::thread([=, &svr, &timeMap](){
                    auto ret = execSendECPipeTaskParallel(filename, tasks[i], objBuffer, svr);
                    timeMap.setTime(i, ret);
                });
                break;
            case ECTaskType::RECEIVE:
                execThds[i] = std::thread([=, &timeMap](){
                    auto ret = execReceiveECPipeTaskParallel(filename, tasks[i], objBuffer);
                    timeMap.setTime(i, ret);
                });
                receiveTaskId++;
                break;
            case ECTaskType::ENCODE:
            case ECTaskType::ENCODE_PARTIAL:
                execThds[i] = std::thread([=, &timeMap](){
                    auto ret = execEncodeECPipeTaskParallel(filename, tasks[i], objBuffer);
                    timeMap.setTime(i, ret);
                });
                break;
            case ECTaskType::PERSIST:
                execThds[i] = std::thread([=, &timeMap](){
                    auto ret = execPersistECPipeTaskParallel(filename, tasks[i], objBuffer);
                    timeMap.setTime(i, ret);
                });
                break;
            case ECTaskType::FETCH:
                execThds[i] = std::thread([=, &timeMap](){
                    auto ret = execFetchECPipeTaskParallel(filename, tasks[i], objBuffer);
                    timeMap.setTime(i, ret);
                });
                break;
            default:
                assert(false && "undefined ECTaskType");
        }
    }
    // 3. wait for tasks done, clean
    for (int i = 0; i < taskNum; i++) {     // clean thds to do fetch, send, receive, encode, persist 
        execThds[i].join();
    }
    if (svr.is_running()) {                 // if this node has send task, stop http svr, clean svrThd
        svr.stop();
        svrThd.join();
    }
    httpThd.join();


    printTime(timeMap, taskNum, tasks);
    gettimeofday(&execEnd, NULL);
    LOG_INFO("ECWorker exec tasks done, filename: %s, taskNum: %d, time: %f ms", filename.c_str(), taskNum, RedisUtil::duration(execStart, execEnd));
    // 3. send encode done to coordinator
    const std::string execEcTasksDoneKey = filename + "_ecTasks_done";
    redisReply* execEcTasksDoneReply = (redisReply*)redisCommand(_coorCtx, "rpush %s %b", 
            execEcTasksDoneKey.c_str(), (char*)&taskNum, sizeof(int));
    assert(execEcTasksDoneReply != NULL && execEcTasksDoneReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(execEcTasksDoneReply);
    LOG_INFO("ECWorker send exec tasks done to coordinator, filename: %s, taskNum: %d", filename.c_str(), taskNum);

    // 4. free tasks
    for (auto task : tasks) {
        delete task;
    }

    delete objBuffer;
}



/**
 * read from queue, pushed by fetch task or encode task
 * free slice after send
 */
std::pair<timeval, timeval> ECWorker::execSendECPipeTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer, httplib::Server& svr) {
    const int nodeId = task->_nodeId;
    const int dstNodeId = task->_dstNodeId;
    const int srcNodeId = task->_srcNodeId;
    const int objId = task->_objId;

    const int objSizeByte = _conf->_objSize * 1024 * 1024;
    const int sliceSizeByte = _conf->_sliceSize * 1024 * 1024;
    assert(objSizeByte % sliceSizeByte == 0);
    int sliceNum = objSizeByte / sliceSizeByte;

    struct timeval sendStart, sendEnd;

    LOG_INFO("execSendECPipeTaskParallel start, filename: %s, nodeId: %d, dstNodeId: %d, objId: %d", 
             filename.c_str(), nodeId, dstNodeId, objId);
    gettimeofday(&sendStart, NULL);
    // 1. read from objBuffer
    BlockingQueue<char*>* sendQueue = objBuffer->getObj(objId);         // pushed by fetch task or encode task, content free by send task, queue free by objBuffer

    // 2. set svr for send data and start time 

    const std::string key = filename + "_send_" + std::to_string(srcNodeId) + "_" + 
    std::to_string(dstNodeId) + "_" + std::to_string(objId);
    int sliceId = 0;
    svr.Get("/" + key, [=, &sendStart, &sendEnd, &sliceId](const httplib::Request& req, httplib::Response& res) {
        LOG_INFO("http send, filename: %s, nodeId: %d, dstNodeId: %d, objId: %d, sliceId: %d", 
                 filename.c_str(), nodeId, dstNodeId, objId, sliceId);
        char* sendBuf = sendQueue->pop();
        res.set_content(sendBuf, sliceSizeByte, "application/octet-stream");
        delete [] sendBuf;
        LOG_INFO("http send done, filename: %s, nodeId: %d, dstNodeId: %d, objId: %d, sliceId: %d", 
                 filename.c_str(), nodeId, dstNodeId, objId, sliceId);
        sliceId++;
    });

    // 3. send ready flag to httpServiceThd
    redisContext* startHttpServiceCtx = RedisUtil::createContext(_conf->_localIp);
    assert(startHttpServiceCtx != NULL);
    redisReply* startHttpServiceReply = (redisReply*)redisCommand(startHttpServiceCtx, "rpush execSendECTaskParallel_start 0");
    assert(startHttpServiceReply != NULL && startHttpServiceReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(startHttpServiceReply);


    // 4. wait for receiver flag
    redisContext* wait4ReceiverCtx = RedisUtil::createContext(_conf->_localIp);
    assert(wait4ReceiverCtx != NULL);
    redisReply* wait4ReceiverReply = (redisReply*)redisCommand(wait4ReceiverCtx, "blpop %s 0", key.c_str());
    assert(wait4ReceiverReply != NULL && wait4ReceiverReply->type == REDIS_REPLY_ARRAY && wait4ReceiverReply->elements == 2);
    freeReplyObject(wait4ReceiverReply);
    redisFree(wait4ReceiverCtx);
    LOG_INFO("execSendECTaskParallel receive flag from dstNodeId: %d, objId: %d", dstNodeId, objId);
    gettimeofday(&sendEnd, NULL);
    LOG_INFO("execSendECTask done, filename: %s, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, time: %f ms", 
    filename.c_str(), nodeId, srcNodeId, dstNodeId, objId, RedisUtil::duration(sendStart, sendEnd));
    return {sendStart, sendEnd};
}



/**
 * read obj from hdfs, devide by slice, push to queue for send or encode
 * slice will free by send task or encode task
 */
std::pair<timeval, timeval> ECWorker::execFetchECPipeTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const int objId = task->_objId;
    const int tmpObjId = task->_tmpObjId;
    struct timeval fetchStart, fetchEnd;

    LOG_INFO("execFetchECTaskParallel start, filename: %s, nodeId: %d, odjId: %d, tmpObjId: %d", filename.c_str(), nodeId, objId, tmpObjId);
    
    gettimeofday(&fetchStart, NULL);
    BlockingQueue<char*>* fetchQueue = new BlockingQueue<char*>();
    objBuffer->insertObj(tmpObjId, fetchQueue); 
    int bufSizeByte = _conf->_objSize * 1024 * 1024;
    int sliceSizeByte = _conf->_sliceSize * 1024 * 1024;
    assert(bufSizeByte % sliceSizeByte == 0);
    int sliceNum = bufSizeByte / sliceSizeByte;          
    char* buf = new char [bufSizeByte];               
    const std::string objname = filename + "_lmqobj_" + std::to_string(objId);
    hdfsFile file = _hdfsHandler->openFile(objname, HDFSMode::READ);
    _hdfsHandler->readFromHDFS(file, buf, bufSizeByte);
    _hdfsHandler->closeFile(file);
    for (int i = 0; i < sliceNum; i++) {
        LOG_INFO("execFetchECTaskParallel, filename: %s, nodeId: %d, odjId: %d, tmpObjId: %d, sliceId: %d", filename.c_str(), nodeId, objId, tmpObjId, i);
        char* sliceBuf = new char [sliceSizeByte];                              // pulled by send task or encode task + send task, free by send task         
        memcpy(sliceBuf, buf + i * sliceSizeByte, sliceSizeByte);
        fetchQueue->push(sliceBuf);
    }
    delete [] buf;
    LOG_INFO("execFetchECTaskParallel done, filename: %s, nodeId: %d, odjId: %d, tmpObjId: %d", filename.c_str(), nodeId, objId, tmpObjId);
    gettimeofday(&fetchEnd, NULL);
    return {fetchStart, fetchEnd};
}


/**
 * read slice from sender, push to queue for encode or persist
 * slice will free by encode task or persist task
 */
std::pair<timeval, timeval> ECWorker::execReceiveECPipeTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const int dstNodeId = task->_dstNodeId;
    const int srcNodeId = task->_srcNodeId;
    const int objId = task->_objId;
    const int tmpObjId = task->_tmpObjId;
    struct timeval receiveStart, receiveEnd;
    LOG_INFO("execReceiveECTaskParallel start, filename: %s, nodeId: %d, srcNodeId: %d, objId: %d, tmpObjId: %d", 
            filename.c_str(), nodeId, srcNodeId, objId, tmpObjId);
    const std::string receiveObjKey = filename + "_send_" + std::to_string(srcNodeId) + "_" + 
                                    std::to_string(dstNodeId) + "_" + std::to_string(objId);
    int objSizeByte = _conf->_objSize * 1024 * 1024;
    int sliceSizeByte = _conf->_sliceSize * 1024 * 1024;
    assert(objSizeByte % sliceSizeByte == 0);
    int sliceNum = objSizeByte / sliceSizeByte;
    
    BlockingQueue<char*>* receiveQueue = new BlockingQueue<char*>();            // pushed by receive task, pulled by encode or persist task, content is free by them
    objBuffer->insertObj(tmpObjId, receiveQueue);
    
    // 1. receive data from sender 
    gettimeofday(&receiveStart, NULL);
    Client cli(RedisUtil::ip2Str(_conf->_agent_ips[srcNodeId]).c_str(), 8080);
    for (int i = 0; i < sliceNum; i++) {
        do {
            auto res = cli.Get("/" + receiveObjKey);
            if (res && res->status == 200) {
                LOG_INFO("execReceiveECTaskParallel, filename: %s, nodeId: %d, srcNodeId: %d, objId: %d, tmpObjId: %d, sliceId: %d", 
                         filename.c_str(), nodeId, srcNodeId, objId, tmpObjId, i);
                assert(res->body.size() == sliceSizeByte);
                char* obj = new char[sliceSizeByte];
                memcpy(obj, res->body.data(), sliceSizeByte);

                receiveQueue->push(obj);
                break;
            }
            printf("try for sliceId: %d\n", i);
        } while (true);
    }

    // 2. send reply to sender
    redisContext* receiveObjCtx = RedisUtil::createContext(_conf->_agent_ips[srcNodeId]);
    redisReply* receiveObjReply = (redisReply*)redisCommand(receiveObjCtx, "rpush %s 1", receiveObjKey.c_str());
    assert(receiveObjReply != NULL && receiveObjReply->type == REDIS_REPLY_INTEGER);
    freeReplyObject(receiveObjReply);
    redisFree(receiveObjCtx);
    
    // gettimeofday(&receiveEnd, NULL);
    LOG_INFO("execReceiveECTaskParallel done, filename: %s, nodeId: %d, srcNodeId: %d, objId: %d, tmpObjId: %d, receive time: %f ms", 
             filename.c_str(), nodeId, srcNodeId, objId, tmpObjId, RedisUtil::duration(receiveStart, receiveEnd));
    
    
    return {receiveStart, receiveEnd};
}

/**
 * read slice from queue from receive task or fetch task, free slices
 * encode and push to queue for send task
 * encode result slice will free by send task
 */
std::pair<timeval, timeval> ECWorker::execEncodeECPipeTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer) {
    const int nodeId = task->_nodeId;
    const std::vector<int> objIds = task->_objIds;
    const int tmpObjId = task->_tmpObjId;
    const std::vector<int> coefs = task->_coefs;
    int objSizeByte = _conf->_objSize * 1024 * 1024;
    int sliceSizeByte = _conf->_sliceSize * 1024 * 1024;
    assert(objSizeByte % sliceSizeByte == 0);
    int sliceNum = objSizeByte / sliceSizeByte;
    timeval encodeStart, encodeEnd;
    double cpu_util = getCurrentCPUUtilization();

    LOG_INFO("execEncodeECTaskParallel start, filename: %s, nodeId: %d, objNum: %ld, objIds: %s, tmpObjId: %d, coefs: %s",
             filename.c_str(), nodeId, objIds.size(), vec2String(objIds).c_str(), tmpObjId, vec2String(coefs).c_str()); 
    std::vector<BlockingQueue<char*>*> receiveQueues;
    for (int i = 0; i < objIds.size(); i++) {
        int objId = objIds[i];
        BlockingQueue<char*>* queue = objBuffer->getObj(objId);             // pushed by receive task, content free by encode task, queue free by objBuffer
        receiveQueues.push_back(queue);
    }
    BlockingQueue<char*>* encodeQueue = new BlockingQueue<char*>();         // pushed by encode task, pulled by send task, content free by send task, queue free by objBuffer
    objBuffer->insertObj(tmpObjId, encodeQueue);
    gettimeofday(&encodeStart, NULL);

    timeval sliceStart, sliceEnd;
    double sliceEncodeTime = 0.0;
    for (int i = 0; i < sliceNum; i++) {
        std::vector<const char*> sliceBufs;
        for (int j = 0; j < objIds.size(); j++) {
            const char* sliceBuf = receiveQueues[j]->pop();                 // free after encode
            sliceBufs.push_back(sliceBuf);         
        }
        char* encodeBuf = new char[sliceSizeByte];                          // free after send task or persist task
        memset(encodeBuf, 0, sliceSizeByte);
        gettimeofday(&sliceStart, NULL);
        RSPlan::encode(sliceBufs, encodeBuf, coefs, _conf->_rsParam.w, sliceSizeByte);
        gettimeofday(&sliceEnd, NULL);
        sliceEncodeTime += RedisUtil::duration(sliceStart, sliceEnd);
        encodeQueue->push(encodeBuf);
        // free sliceBufs
        for (int j = 0; j < objIds.size(); j++) {
            delete [] sliceBufs[j];
        }
        LOG_INFO("execEncodeECTaskParallel, filename: %s, nodeId: %d, objNum: %ld, objIds: %s, tmpObjId: %d, coefs: %s, sliceId: %d",
                 filename.c_str(), nodeId, objIds.size(), vec2String(objIds).c_str(), tmpObjId, vec2String(coefs).c_str(), i);
    } 

    gettimeofday(&encodeEnd, NULL);
    LOG_INFO("execEncodeECTaskParallel done, filename: %s, nodeId: %d, objNum: %ld, objIds: %s, tmpObjId: %d, coefs: %s, encode time: %f ms",
                filename.c_str(), nodeId, objIds.size(), vec2String(objIds).c_str(), tmpObjId, vec2String(coefs).c_str(), RedisUtil::duration(encodeStart, encodeEnd)); 
    double encodeTime = RedisUtil::duration(encodeStart, encodeEnd);
    LOG_INFO("encodeTime: %f, sliceEncodeTime: %f", encodeTime, sliceEncodeTime);
    // Record GF computation history data for overhead prediction
    recordGFComputationHistory(cpu_util, sliceNum, sliceNum * _conf->_sliceSize, sliceEncodeTime);
    return {encodeStart, encodeEnd};                                     
}


/**
 * read from receive queue
 * persist to hdfs, free slices
 */
std::pair<timeval, timeval> ECWorker::execPersistECPipeTaskParallel(const std::string& filename, const ECTask* task, BlockingQueueParallelBuffer* objBuffer) {

    const int nodeId = task->_nodeId;
    const int objId = task->_objId;
    const int tmpObjId = task->_tmpObjId;
    struct timeval persistStart, persistEnd;
    LOG_INFO("execPersistECPipeTaskParallel start, filename: %s, nodeId: %d, objId: %d, tmpObjId: %d", filename.c_str(), nodeId, objId, tmpObjId);
    

    gettimeofday(&persistStart, NULL);
    const std::string objname = filename + "_lmqobj_" + std::to_string(objId);
    int objSizeByte = _conf->_objSize * 1024 * 1024;
    int sliceSizeByte = _conf->_sliceSize * 1024 * 1024;
    assert(objSizeByte % sliceSizeByte == 0);
    int sliceNum = objSizeByte / sliceSizeByte;
    char* objBuf = new char [objSizeByte];        

    BlockingQueue<char*>* receiveQueue = objBuffer->getObj(tmpObjId);         // pushed by receive task, content free by persist task, queue free by objBuffer

    for (int i = 0; i < sliceNum; i++) {
        char* sliceBuf = receiveQueue->pop();
        memcpy(objBuf + i * sliceSizeByte, sliceBuf, sliceSizeByte);
        delete [] sliceBuf;
        LOG_INFO("execPersistECPipeTaskParallel, filename: %s, nodeId: %d, objId: %d, tmpObjId: %d, sliceId: %d", filename.c_str(), nodeId, objId, tmpObjId, i);    
    }


    hdfsFile file = _hdfsHandler->openFile(objname, HDFSMode::WRITE);
    _hdfsHandler->write2HDFS(file, objBuf, objSizeByte);
    _hdfsHandler->closeFile(file);
    delete [] objBuf;
    gettimeofday(&persistEnd, NULL);
    LOG_INFO("execPersistECPipeTaskParallel done, filename: %s, nodeId: %d, objId: %d, tmpObjId: %d, persist time: %f ms", 
            filename.c_str(), nodeId, objId, tmpObjId, RedisUtil::duration(persistStart, persistEnd));
    return {persistStart, persistEnd};
}