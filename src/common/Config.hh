#pragma once

#include "../inc/include.hh"



enum class ECType {
    RS, LRC, MSR
};
enum class ECPolicy {
    CONV, PPR, Pipe, PipeFG
};
struct RSParam {
    int n;
    int k;
    int w;
};
class Config {
public:
    Config(const std::string& file_path);
    ~Config();

    void DumpConfig() const;



    unsigned int _localIp;
    std::string _localIpStr;
    unsigned int _coorIp;
    int _agent_num;
    std::vector<unsigned int> _agent_ips;
    int _node_id;
    int _pktSize;           // Byte
    int _objSize;           // MB
    int _sliceSize;         // MB
    int _agWorkerThreadNum;
    int _coorThreadNum;
    int _distThreadNum;
    std::string _ioPolicy;

    std::vector<std::string> _fsParam;

    ECType _ecType;
    ECPolicy _ecPolicy;
    RSParam _rsParam;

    std::unordered_map<std::string, std::string> _offlineECMap = {{"rs_9_6_pool", "rs_9_6"}};
    std::unordered_map<std::string, int> _offlineECBase = {{"rs_9_6_pool", 1}};
    std::string _data_policy = "random";
    std::string _fsType = "HDFS3";
    std::unordered_map<std::string, std::vector<std::string>> _fsFactory = {{"HDFS3", {"192.168.0.219", "9000"}}};

    std::string _repair_scheduling = "delay";
    int _ec_concurrent = 64;
    bool _avoid_local = false;

};