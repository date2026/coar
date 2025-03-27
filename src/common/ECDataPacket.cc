#include "ECDataPacket.hh"

ECDataPacket::ECDataPacket() {
}

ECDataPacket::ECDataPacket(char* raw) {
  int tmplen;
  memcpy((char*)&tmplen, raw, 4);
  _dataLen = ntohl(tmplen);

  _raw = (char*)calloc(_dataLen+4, sizeof(char));
  memcpy(_raw, raw, _dataLen+4);

  _data = _raw+4;
}

ECDataPacket::ECDataPacket(int len) {
  _raw = (char*)calloc(len+4, sizeof(char));
  _data = _raw+4;
  _dataLen = len;

  int tmplen = htonl(len) ;
  memcpy(_raw, (char*)&tmplen, 4);
}

ECDataPacket::~ECDataPacket() {
  if (_raw) free(_raw);
}

void ECDataPacket::setRaw(char* raw) {
  int tmplen;
  memcpy((char*)&tmplen, raw, 4);
  _dataLen = ntohl(tmplen);

  _raw = raw;
  _data = _raw+4;
}

void ECDataPacket::setData(char* src) {
    memcpy(_data, src, _dataLen);
}

int ECDataPacket::getDatalen() {
  return _dataLen;
}

char* ECDataPacket::getData() {
  return _data;
}

char* ECDataPacket::getRaw() {
  return _raw;
}
