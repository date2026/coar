#pragma once

#include "../inc/include.hh"
#include "../ec/ECPolicy.hh"

class Config {
public:
    Config(const std::string& file_path);
    ~Config();

    void DumpConfig() const;



    unsigned int _localIp;
    unsigned int _coorIp;
    int _agent_num;
    std::vector<unsigned int> _agent_ips;

    int _pktSize;
    int _objSize;
    int _agWorkerThreadNum;
    int _coorThreadNum;
    int _distThreadNum;

    std::unordered_map<std::string, ECPolicy*> _ecPolicyMap;    // ecid->ECPolicy

    std::vector<std::string> _fsParam;

    std::unordered_map<std::string, std::string> _offlineECMap = {{"rs_9_6_pool", "rs_9_6"}};
    std::unordered_map<std::string, int> _offlineECBase = {{"rs_9_6_pool", 1}};
    std::string _data_policy = "random";
    std::string _fsType = "HDFS3";
    std::unordered_map<std::string, std::vector<std::string>> _fsFactory = {{"HDFS3", {"192.168.220.160", "9000"}}};

    std::string _repair_scheduling = "delay";
    int _ec_concurrent = 64;
    bool _avoid_local = false;

};