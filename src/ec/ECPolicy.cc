#include "ECPolicy.hh"

//ECPolicy::ECPolicy(string id, string classname, int n, int k, int w, bool locality, int opt, vector<string> param) {
ECPolicy::ECPolicy(string id, string classname, int n, int k, int w, int opt, vector<string> param) {
  _id = id;
  _classname = classname;
  _n = n;
  _k = k;
  _w = w;
//  _locality = locality;
  _opt = opt;
  _param = param;
  _createdRPConvClass = 0;
  _nconvBindy = 0;
}
