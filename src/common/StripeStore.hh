#ifndef _STRIPESTORE_HH_
#define _STRIPESTORE_HH_

#include "BlockingQueue.hh"
#include "Config.hh"
#include "SSEntry.hh"

#include "../inc/include.hh"
#include "../protocol/CoorCommand.hh"

#define DELAY_THRESHOLD 1

using namespace std;

class StripeStore {
  private:
    Config* _conf;

    // map original file name to SSEntry
    // for online-encoded file, we can get objname for each split
    // for offline encoded file, we can get splited blocks
    unordered_map<string, SSEntry*> _ssEntryMap;  
    mutex _lockSSEntryMap;
    

    // backup
    string _entryStorePath = "entryStore";
    ofstream _entryStore;
    mutex _lockEntryStore;

    string _poolStorePath = "poolStore";
    ofstream _poolStore;
    mutex _lockPoolStore;
    
  public:
    StripeStore(Config* conf);

    bool existEntry(string filename);
    void insertEntry(SSEntry* entry);

};

#endif
