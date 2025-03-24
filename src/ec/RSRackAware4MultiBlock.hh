#ifndef _RSRACKAWARE4MULTIBLOCK_HH_
#define _RSRACKAWARE4MULTIBLOCK_HH_

#include "../inc/include.hh"
#include "Computation.hh"

#include "ECBase.hh"
#include "../common/logger.hh"

#define RSSMARTLZ_N_MAX 128

#define RPNUM 1
#define CONVNUM 2

#define USE_MULTI_GLOBAL_COLLECTOR false
// #define USE_MULTI_GLOBAL_COLLECTOR true

#define USE_RACK_AWARE_CONV true
// #define USE_RACK_AWARE_CONV false

#define CHOOSE_LOCAL_COLLECTOR true
// #define CHOOSE_LOCAL_COLLECTOR false

#define CHOOSE_GLOBAL_COLLECTOR true
// #define CHOOSE_GLOBAL_COLLECTOR false
using namespace std;

class RSRackAware4MultiBlock : public ECBase {
private:
    int _m;
    int _encode_matrix[RSSMARTLZ_N_MAX * RSSMARTLZ_N_MAX];
    int _convbindY;
    std::unordered_map<int, int> _download_bandwidth_inrack;

    void generate_matrix(int* matrix, int rows, int cols, int w);
    bool UseRackAwareCr(std::vector<int> to) const;
    void RackAwareCr(ECDAG* ecdag, std::vector<int>& fromsids, std::vector<int>& tosids, std::vector<int>& data, std::vector<std::vector<int>>& coefs, int& tmpname);
    void ConvCr(ECDAG* ecdag, std::vector<int>& fromsids, std::vector<int>& tosids, std::vector<int>& data, std::vector<std::vector<int>>& coefs, int& tmpname);
    void ConvCrSingleGlobalCollector(ECDAG* ecdag, std::vector<int>& fromsids, std::vector<int>& tosids, std::vector<int>& data, std::vector<std::vector<int>>& coefs, int& tmpname);
    void ConvCrNultiGlobalCollector(ECDAG* ecdag, std::vector<int>& fromsids, std::vector<int>& tosids, std::vector<int>& data, std::vector<std::vector<int>>& coefs, int& tmpname);
    int SelectLocalCollector(std::vector<int> nodeids);
    int SelectGlobalCollector(std::vector<int> nodeids);
    void InitDownloadBandwidthInRack();
  public:
    RSRackAware4MultiBlock(int n, int k, int w, int opt, vector<string> param);
    ECDAG* Encode();
    ECDAG* Decode(vector<int> from, vector<int> to);
    void Place(vector<vector<int> >& group);
    void setConvBindY(int y);
};

#endif
