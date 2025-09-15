#ifndef _COMMON_HH_
#define _COMMON_HH_

#include <algorithm>
#include <chrono>
#include <deque>
#include <fstream>
#include <iostream>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <cassert>
#include <cstring>
#include <ctime>

#include <arpa/inet.h>
#include <hiredis/hiredis.h>

#include <stdio.h>
#include <fcntl.h>
#include "../common/logger.hh"

#define MAX_COMMAND_LEN 8192


static std::string ipInt2String(unsigned int ip) {
    struct in_addr addr;
    addr.s_addr = ip;
    return std::string(inet_ntoa(addr));
}


template<typename T>
static std::string vec2String(std::vector<T> v) {
    std::string ret;
    for (auto& e : v) {
        ret += std::to_string(e) + " ";
    }
    return ret;
}



#endif

