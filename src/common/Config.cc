#include "Config.hh"
#include "../util/nlohmann/json.hpp"

using json = nlohmann::json;



Config::Config(const std::string& file_path) {
    std::ifstream ifile;
    ifile.open(file_path);

    assert(ifile.is_open() && "open confi file failed");


    json conf = json::parse(ifile);

    _localIp = inet_addr(conf.at("local_ip").get<std::string>().c_str());
    _coorIp = inet_addr(conf.at("coor_ip").get<std::string>().c_str());
    

    _agent_num = conf.at("agent_num").get<int>();
    std::vector<std::string> agentIps = conf.at("agent_ips").get<std::vector<std::string>>();
    for (const auto& agentIp : agentIps) {
        _agent_ips.push_back(inet_addr(agentIp.c_str()));
    }
    assert(_agent_num == _agent_ips.size() && "agent num not match agent ips");

    _fsParam = conf.at("fs_param").get<std::vector<std::string>>();
    _pktSize = conf.at("pkt_size").get<int>();
    _objSize = conf.at("obj_size").get<int>();
    _agWorkerThreadNum = conf.at("agent_worker_thread_num").get<int>();
    _coorThreadNum = conf.at("coordinator_thread_num").get<int>();
}


Config::~Config() {
    for (auto& item : _ecPolicyMap) {
        delete item.second;
    }
}


void Config::DumpConfig() const {
    std::cout << "local_ip: " << _localIp << std::endl;
    std::cout << "coor_ip: " << _coorIp << std::endl;
    std::cout << "agent_num: " << _agent_num << std::endl;
    std::cout << "agent_ips: ";
    for (auto& ip : _agent_ips) {
        std::cout << ipInt2String(ip) << " ";
    }
    std::cout << "pkt_size(Byte): " << _pktSize << std::endl;
    std::cout << "obj_size(MByte): " << _objSize << std::endl;
    std::cout << "agent_worker_thread_num: " << _agWorkerThreadNum << std::endl;
    std::cout << "coordination_thread_num: " << _coorThreadNum << std::endl;
    std::cout << std::endl;
}




