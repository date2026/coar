#include "Coordinator.hh"

Coordinator::Coordinator(Config* conf, StripeStore* ss) : _conf(conf) {
  // create local context
  try {
    _localCtx = RedisUtil::createContext(_conf -> _localIp);
  } catch (int e) {
    // TODO: error handling
    cerr << "initializing redis context to " << " error" << endl;
  }
  _stripeStore = ss;
  _underfs = FSUtil::createFS(_conf->_fsType, _conf->_fsFactory[_conf->_fsType], _conf);
  srand((unsigned)time(0));
}

Coordinator::~Coordinator() {
  redisFree(_localCtx);
}

void Coordinator::doProcess() {
  redisReply* rReply;
  while (true) {
    cout << "Coordinator::doProcess" << endl;
    // will never stop looping
    rReply = (redisReply*)redisCommand(_localCtx, "blpop coor_request 0");
    if (rReply -> type == REDIS_REPLY_NIL) {
      cerr << "Coordinator::doProcess() get feed back empty queue " << endl;
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      cerr << "Coordinator::doProcess() get feed back ERROR happens " << endl;
    } else {
      cout << "Coordinator::doProcess() receive a request!" << endl;
      char* reqStr = rReply -> element[1] -> str;
      CoorCommand* coorCmd = new CoorCommand(reqStr);
      coorCmd->dump();
      int type = coorCmd->getType();
      cout << "type: " << type << endl;
      switch (type) {
        case 0: registerFile(coorCmd); break;
        default: break;
      }
      delete coorCmd;
    }
    // free reply object
    freeReplyObject(rReply);
  }
}

void Coordinator::registerFile(CoorCommand* coorCmd) {
  unsigned int clientIp = coorCmd->getClientip();
  string filename = coorCmd->getFilename();
  string ecid = coorCmd->getEcid();
  int mode = coorCmd->getMode();
  int filesizeMB = coorCmd->getFilesizeMB();

  registerOfflineEC(clientIp, filename, ecid, filesizeMB);
}

void Coordinator::registerOfflineEC(unsigned int clientIp, string filename, string ecpoolid, int filesizeMB) {
  cout << "Coordinator::registerOfflineEC" << endl;
  struct timeval time1, time2, time3, time4;
  // 0. make sure that there is no existing ssentry
  // assert (!_stripeStore->existEntry(filename));

  // 1. given ecpoolid, figure out whether there is offline pool created in stripe store
  assert(_conf->_offlineECMap.find(ecpoolid) != _conf->_offlineECMap.end());
  assert(_conf->_offlineECBase.find(ecpoolid) != _conf->_offlineECBase.end());
  string ecid = _conf->_offlineECMap[ecpoolid];
  int basesizeMB = _conf->_offlineECBase[ecpoolid];
  LOG_INFO("basesizeMB: %d", basesizeMB);
  // assert(_conf->_ecPolicyMap.find(ecid) != _conf->_ecPolicyMap.end());
  // ECPolicy* ecpolicy = _conf->_ecPolicyMap[ecid];
  // OfflineECPool* ecpool = _stripeStore->getECPool(ecpoolid, ecpolicy, basesizeMB);
  // ecpool->lock();

  // 2. get placement group 
  // ECBase* ec = ecpolicy->createECClass();
  // vector<vector<int>> group;
  // ec->Place(group);
  // unordered_map<int, vector<int>> idx2group;
  // for (auto item: group) {
  //   for (auto idx: item) {
  //     idx2group.insert(make_pair(idx, item));
  //   }
  // }

  // 3. check number of object that is going to be created for this file
  int objnum = filesizeMB/basesizeMB;

  // 4. for each object, add into a stripe an preassign location
  if (filesizeMB%basesizeMB) objnum += 1;

  // fileobjnames and fileobjlocs are used to create SSEntry for this file
  vector<string> fileobjnames;
  vector<unsigned int> fileobjlocs;

  // for (int objidx=0; objidx<objnum; objidx++) {
  //   string objname = filename+"_oecobj_"+to_string(objidx);
  //   fileobjnames.push_back(objname);

  //   // 4.1 get a stripe for obj 
  //   //   we get a stripename from ecpool, however ecpool does not add objname into it at this time
  //   string stripename = ecpool->getStripeForObj(objname); 
  //   //   given the stripe name, we get existing objlist for this stripename
  //   vector<string> stripeobjlist = ecpool->getStripeObjList(stripename);
  //   // stripeips records location indexed by erasure coding index for each split in the stripe
  //   vector<unsigned int> stripeips;
  //   // stripeplaced records the objnames that have been stored in this stripe
  //   vector<int> stripeplaced;
  //   // we check obj in stripeobjlist one by one to fill stripeips and stripeplaced
  //   for (int i=0; i<stripeobjlist.size(); i++) {
  //     string curobjname = stripeobjlist[i];
  //     // given curobjname, find ssentry of the original file for this curobjname
  //     // if this curobjname is from previous stored file, there must be an SSEntry in stripestore
  //     // else ssentry is NULL
  //     SSEntry* ssentry = _stripeStore->getEntryFromObj(curobjname);
  //     // given curobjname, find location recorded in ssentry
  //     unsigned int curip;
  //     bool find = false;
  //     if (ssentry != NULL) {
  //       curip = ssentry->getLocOfObj(curobjname);
  //       find = true;
  //     } else {
  //       // curobjname is in current file
  //       for (int j=0; j<fileobjnames.size(); j++) {
  //         if (curobjname == fileobjnames[j]) {
  //           curip = fileobjlocs[j];
  //           find = true;
  //           break;
  //         }
  //       }
  //     }
  //     assert (find);
  //     // add this ip to stripeips
  //     stripeips.push_back(curip);
  //     // add stripeidx to stripeplaced
  //     stripeplaced.push_back(i);
  //   }

    // 4.2 given stripeips and stripeplaced, also group information from erasure code, preassign location for $objname
    // int stripeidx = stripeplaced.size();
    // vector<int> colocWith;
    // if (idx2group.find(stripeidx) != idx2group.end()) colocWith = idx2group[stripeidx];
    // vector<unsigned int> candidates = getCandidates(stripeips, stripeplaced, colocWith);
    // unsigned int curIp; // choose from candidates
    // if (_conf->_avoid_local) {
    //   vector<unsigned int>::iterator position = find(candidates.begin(), candidates.end(), clientIp);
    //   if (position != candidates.end()) candidates.erase(position);
    //   curIp = chooseFromCandidates(candidates, _conf->_data_policy, "data");
    // } else {
    //   if (find(candidates.begin(), candidates.end(), clientIp) != candidates.end()) curIp = clientIp;
    //   else curIp = chooseFromCandidates(candidates, _conf->_data_policy, "data");
    // }

    // 1.3 now we have preassigned a location for this objname, add to fileobjlocs
    // fileobjlocs.push_back(curIp);

    // 1.4 add objname to ecpool
    // ecpool->addObj(objname, stripename);
  // }
  
  // 2. update ssentry
  // SSEntry* ssentry = new SSEntry(filename, 1, filesizeMB, ecpoolid, fileobjnames, fileobjlocs);
  // _stripeStore->insertEntry(ssentry);
  // ssentry->dump();

  // ecpool->unlock();

  // 3. send to agent instructions
  AGCommand* agCmd = new AGCommand();
  agCmd->buildType11(11, objnum, basesizeMB);
  agCmd->setRkey("registerFile:"+filename);
  agCmd->sendTo(clientIp);
  delete agCmd;

  // free
  // delete ec;
}
