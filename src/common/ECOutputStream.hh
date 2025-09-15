#pragma once

#include "Config.hh"

#include "../inc/include.hh"
#include "../protocol/AGCommand.hh"
#include "../util/RedisUtil.hh"

using namespace std;

class ECOutputStream {
  private:
    Config* _conf;
    string _filename;
    string _ecidpool;
    string _mode;
    int _filesizeMB;
    redisContext* _localCtx;
    int _pktid;
    int _replyid;
  public:
    ECOutputStream(Config* conf, string filename, string ecidpool, string mode, int filesizeMB);
    ~ECOutputStream();
    void init();
    void write(char* buf, int len);
    void close();
};

