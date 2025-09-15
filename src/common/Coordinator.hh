#ifndef _COORDINATOR_HH_
#define _COORDINATOR_HH_

//#include "AGCommand.hh"
#include "Config.hh"
//#include "RedisUtil.hh"
#include "StripeStore.hh"
//#include "SSEntry.hh"
//#include "UnderFile.hh"
//#include "Util/hdfs.h"

#include "../fs/FSUtil.hh"
#include "../fs/UnderFS.hh"
#include "../inc/include.hh"
#include "../protocol/AGCommand.hh"
#include "../protocol/CoorCommand.hh"
#include "logger.hh"
#include "../ec/ECPlan.hh"
#include "../ec/RSPlan.hh"
using namespace std;

class Coordinator {
private:
    Config* _conf;
    redisContext* _localCtx;
    StripeStore* _stripeStore;
    UnderFS* _underfs;
    mutex _lockSelect;

public:
    Coordinator(Config* conf, StripeStore* ss);
    ~Coordinator();

    void doProcess();

    void registerFile(CoorCommand* coorCmd);
    void readFileMeta(CoorCommand* coorCmd);
    void encode(CoorCommand* coorCmd);
    void decode(CoorCommand* coorCmd);
    void encodeRS(const std::string& filename, FileMeta* fileMeta, const std::string& ecdagPath);
    void decodeRS(const std::string& filename, FileMeta* fileMeta, const std::string& ecdagPath,
                  const std::vector<int>& survivedObjIds, int failedObjId);

};

#endif
