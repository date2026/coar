#pragma once
#include "../inc/include.hh"

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