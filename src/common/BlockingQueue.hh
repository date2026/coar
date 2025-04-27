#ifndef _BLOCKINGQUEUE_HH_
#define _BLOCKINGQUEUE_HH_

#include <condition_variable>
#include <deque>
#include <iostream>
#include <mutex>
#include <cassert>
#include <unordered_map>
using namespace std;

template <class T>
class BlockingQueue {
  private:
    mutex _mutex;
    condition_variable _cv;
    deque<T> _queue;
  public:
    void push(T value) {
      {
        unique_lock<mutex> lock(_mutex);
	_queue.push_back(value);
      }
      _cv.notify_one();
    };

    T pop(){
      unique_lock<mutex> lock(_mutex);
      _cv.wait(lock, [=]{ return !_queue.empty(); });
      T toret(_queue.front());
      _queue.pop_front();

      return toret;
    };

    int getSize() {
      return _queue.size();
    }
};

class BlockingQueueParallelBuffer {
    public:     
        BlockingQueueParallelBuffer() {}
        ~BlockingQueueParallelBuffer() {
            std::unique_lock<std::mutex> lck(_mutex);
            for (auto it = _objBuffer.begin(); it != _objBuffer.end(); ++it) {
                delete it->second;
            }
        }                           
    
        void insertObj(int objId, BlockingQueue<char*>* objQueue) {
            std::unique_lock<std::mutex> lck(_mutex);
            assert(_objBuffer.find(objId) == _objBuffer.end());
            _objBuffer[objId] = objQueue;
            _cv.notify_all();
        }
        
        BlockingQueue<char*>* getObj(int objId) {
            std::unique_lock<std::mutex> lck(_mutex);
            if (_objBuffer.find(objId) == _objBuffer.end()) {
                _cv.wait(lck, [this, objId] { return _objBuffer.find(objId) != _objBuffer.end(); });
            }
            return _objBuffer[objId];
        }
    
    private:  
        std::unordered_map<int, BlockingQueue<char*>*> _objBuffer;
        std::mutex _mutex;
        std::condition_variable _cv;
};
#endif
