#include "ECPlan.hh"
#include "../util/jerasure.h"

class RSPlan : public ECPlan {
public:
    RSPlan(Config* conf, FileMeta* fileMeta, const std::string& ecdagPath, int k, int n, int w);
    RSPlan(Config* conf, FileMeta* fileMeta, const std::string& ecdagPath, int k, int n, int w, bool decode);
    ~RSPlan() = default;
    static void encode(std::vector<const char*> data, std::vector<char*> parity, 
                       const std::vector<std::vector<int>>& matrix, int w, int objSizeByte);
                       
    static void encode(std::vector<const char*> data, char* parity, 
                       const std::vector<int>& encodeMatrix, int w, int objSizeByte);
private:
    void setRSTasks();
    void setRSTask(const std::vector<std::string>& taskInfo, ECTask* task);
    void generateMatrix();
    void generateDecodeMatrix();


    int _k;
    int _n;
    int _w;
    std::vector<std::vector<int>> _encodeMatrix;


};