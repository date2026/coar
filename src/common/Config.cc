#include "Config.hh"
#include "../util/nlohmann/json.hpp"

using json = nlohmann::json;



Config::Config(const std::string& file_path) {
    std::ifstream ifile;
    ifile.open(file_path);

    assert(ifile.is_open() && "open confi file failed");


    json conf = json::parse(ifile);

    _localIp = inet_addr(conf.at("local_ip").get<std::string>().c_str());
    _localIpStr = conf.at("local_ip").get<std::string>();
    _coorIp = inet_addr(conf.at("coor_ip").get<std::string>().c_str());
    

    _agent_num = conf.at("agent_num").get<int>();
    std::vector<std::string> agentIps = conf.at("agent_ips").get<std::vector<std::string>>();
    if (_localIp != _coorIp) {
        _node_id = std::find(agentIps.begin(), agentIps.end(), _localIpStr) - agentIps.begin();
        assert(_node_id >= 0 && _node_id <= _agent_num - 1);
    }
    for (const auto& agentIp : agentIps) {
        _agent_ips.push_back(inet_addr(agentIp.c_str()));
    }
    assert(_agent_num == _agent_ips.size() && "agent num not match agent ips");

    _fsParam = conf.at("fs_param").get<std::vector<std::string>>();
    _pktSize = conf.at("pkt_size").get<int>();
    _objSize = conf.at("obj_size").get<int>();
    _sliceSize = conf.at("slice_size").get<int>();
    _agWorkerThreadNum = conf.at("agent_worker_thread_num").get<int>();
    _coorThreadNum = conf.at("coordinator_thread_num").get<int>();

    json ecParam = conf.at("ec_param");
    std::string ecType = ecParam.at("ec_type").get<std::string>();
    if (ecType == "RS") {
        _ecType = ECType::RS;
        _rsParam.n = ecParam.at("n").get<int>();
        _rsParam.k = ecParam.at("k").get<int>();
        _rsParam.w = ecParam.at("w").get<int>();
    } else if (ecType == "LRC") {
        _ecType = ECType::LRC;
        assert(false && "not implemented");
    } else if (ecType == "MSR") {
        _ecType = ECType::MSR;
        assert(false && "not implemented");
    } else {
        assert(false && "invalid ec type");
    }
    std::string ecPolicy = ecParam.at("ec_policy").get<std::string>();
    if (ecPolicy == "CONV") {
        _ecPolicy = ECPolicy::CONV;
    } else if (ecPolicy == "PPR") {
        _ecPolicy = ECPolicy::PPR;
    } else if (ecPolicy == "Pipe") {
        _ecPolicy = ECPolicy::Pipe;
    } else if (ecPolicy == "PipeFG") {
        _ecPolicy = ECPolicy::PipeFG;
    } else {
        assert(false && "invalid ec policy");
    }
    _ioPolicy = conf.at("io_policy").get<std::string>();

}


Config::~Config() {
    
}


void Config::DumpConfig() const {
    std::cout << "local_ip: " << ipInt2String(_localIp) << std::endl;
    std::cout << "coor_ip: " << ipInt2String(_coorIp) << std::endl;
    std::cout << "agent_num: " << _agent_num << std::endl;
    std::cout << "agent_ips: ";
    for (auto& ip : _agent_ips) {
        std::cout << ipInt2String(ip) << " ";
    }
    std::cout << "pkt_size(Byte): " << _pktSize << std::endl;
    std::cout << "obj_size(MByte): " << _objSize << std::endl;
    std::cout << "agent_worker_thread_num: " << _agWorkerThreadNum << std::endl;
    std::cout << "coordination_thread_num: " << _coorThreadNum << std::endl;
    std::cout << "k: " << _rsParam.k << ", n: " << _rsParam.n << ", w: " << _rsParam.w << std::endl;
    std::cout << std::endl;
}




