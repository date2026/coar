#pragma once

#include "../inc/include.hh"

using namespace std;

class ECDataPacket {
  private:
    int _dataLen;
    char* _raw;  // the first 4 bytes are _dataLen in network bytes order, follows the data content
                 // so the length of _raw is 4+_dataLen
    char* _data;

  public:
    ECDataPacket();
    ECDataPacket(char* raw);
    ECDataPacket(int len);
    ~ECDataPacket();
    void setRaw(char* raw);
    void setData(char* src);
    int getDatalen();
    char* getData();
    char* getRaw();
};
