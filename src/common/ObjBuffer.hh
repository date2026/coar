#pragma once
#include "../inc/include.hh"

class ConcurrentMap {
public:
    void setTime(int taskId, std::pair<timeval, timeval> t) {
        std::lock_guard<std::mutex> lock(_mutex);
        _timeMap[taskId] = t;
    }
    std::unordered_map<int, std::pair<timeval, timeval>> _timeMap;
    std::mutex _mutex;
};

class ObjBuffer {
public:
    ObjBuffer() {}
    ~ObjBuffer() {
        for (auto& obj : _objBuffer) {
            delete[] obj.second;
        }
    }

    bool existObj(int objId) {
        std::lock_guard<std::mutex> lock(_mutex);
        return _objBuffer.find(objId) != _objBuffer.end();
    }

    void insertObj(int objId, char* obj) {
        assert(_objBuffer.find(objId) == _objBuffer.end());
        std::lock_guard<std::mutex> lock(_mutex);
        _objBuffer[objId] = obj;
    }

    char* getObj(int objId) {
        std::lock_guard<std::mutex> lock(_mutex);
        assert(_objBuffer.find(objId) != _objBuffer.end());
        return _objBuffer[objId];
    }
private:
    std::unordered_map<int, char*> _objBuffer;  // objId -> obj
    std::mutex _mutex;
};


class ObjParallelBuffer {
public:     
    ObjParallelBuffer() {}
    ~ObjParallelBuffer() {
        std::unique_lock<std::mutex> lck(_mutex);
        for (auto& it : _objBuffer) {
            delete[] it.second;
        }
    }

    void insertObj(int objId, char* obj) {
        std::unique_lock<std::mutex> lck(_mutex);
        assert(_objBuffer.find(objId) == _objBuffer.end());
        _objBuffer[objId] = obj;
        _cv.notify_all();
    }

    char* getObj(int objId) {
        std::unique_lock<std::mutex> lck(_mutex);
        if (_objBuffer.find(objId) == _objBuffer.end()) {
            _cv.wait(lck, [this, objId] { return _objBuffer.find(objId) != _objBuffer.end(); });
        }
        return _objBuffer[objId];
    }

private:  
    std::unordered_map<int, char*> _objBuffer;
    std::mutex _mutex;
    std::condition_variable _cv;

};