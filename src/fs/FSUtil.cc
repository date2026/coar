#include "FSUtil.hh"

UnderFS* FSUtil::createFS(string type, vector<string> param, Config* conf) { 
  for (auto p : param) {
    cout << "param: " << p << endl;
  }
  UnderFS* toret;

  cout << "FSUtil::HDFS3" << endl;
  toret = new Hadoop3(param, conf);


  return toret;
}

void FSUtil::deleteFS(string type, UnderFS* fshandler) {
  cout << "FSUtil::HDFS3" << endl;
  delete (Hadoop3*)fshandler;
    
}
