#include "StripeStore.hh"

using namespace std;
StripeStore::StripeStore(Config* conf) {
  _conf = conf;
}

bool StripeStore::existEntry(string filename) {
  unordered_map<string, SSEntry*>::iterator it = _ssEntryMap.find(filename);
  return it == _ssEntryMap.end() ? false:true;
}

void StripeStore::insertEntry(SSEntry* entry) {

}
