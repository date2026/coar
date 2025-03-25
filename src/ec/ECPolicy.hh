#ifndef _ECPOLICY_HH_
#define _ECPOLICY_HH_

#include "../inc/include.hh"

using namespace std;

class ECPolicy {
  private:
    string _id;
    string _classname;
    int _n;
    int _k;
    int _w;
    bool _locality = false;
    int _opt;
    int _createdRPConvClass;
    int _nconvBindy;

    vector<string> _param;
    string _smlzSch = "single"; // multi
    bool _smlzConvEnhanced = false;
    bool _smlzRPEnhanced = false;
  public:
    ECPolicy(string id, string classname, int n, int k, int w, int opt, vector<string> param);
};

#endif
