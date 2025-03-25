#include "ECOutputStream.hh"

ECOutputStream::ECOutputStream(Config* conf,
                                 string filename,
                                 string ecidpool,
                                 string mode,
                                 int filesizeMB) {
  _conf = conf;
  _filename = filename;
  _ecidpool = ecidpool;
  _mode = mode;
  _filesizeMB = filesizeMB;
  _localCtx = RedisUtil::createContext(_conf->_localIp);
  _pktid = 0;
  _replyid = 0;
  init();
}

ECOutputStream::~ECOutputStream() {
  redisFree(_localCtx);
}

void ECOutputStream::init() {
  /*
   *  tell local OECAgent that I want to write a file of size
   */
  AGCommand* agCmd = new AGCommand();  
  agCmd->buildType0(0, _filename, _ecidpool, _mode, _filesizeMB);
  agCmd->sendTo(_conf->_localIp);

  // free
  delete agCmd;
}

void ECOutputStream::write(char* buf, int len) {
  /*
   * ECOutputStream write packet to local redis in this format
   * |key = filename|
   * |value = |datalen|data|
   */
  string key = _filename + ":" + to_string(_pktid++);
  // pipelining
  redisAppendCommand(_localCtx, "RPUSH %s %b", key.c_str(), buf, len);
}

void ECOutputStream::close() {
  struct timeval time1, time2, time3;
  gettimeofday(&time1, NULL);
 
  redisReply* rReply;
  for (int i=_replyid; i<_pktid; i++) { 
    redisGetReply(_localCtx, (void**)&rReply);
    freeReplyObject(rReply);
  }
 
  gettimeofday(&time2, NULL);
//  cout << "ECOutputStream.close.wait for all reply time = " << RedisUtil::duration(time1, time2) << endl; 
 
  // wait for finish
  string wkey = "writefinish:"+_filename;
  rReply = (redisReply*)redisCommand(_localCtx, "blpop %s 0", wkey.c_str());
  freeReplyObject(rReply);
  gettimeofday(&time3, NULL);
//  cout << "ECOutputStream.close.wait for finish flag time = " << RedisUtil::duration(time2, time3) << endl;
}
