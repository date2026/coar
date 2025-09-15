#include "common/Config.hh"
#include "common/ECWorker.hh"

#include "inc/include.hh"

using namespace std;

int main(int argc, char** argv) {

	const string confPath = "/root/coar/conf/1.json";
	Config* conf = new Config(confPath);
	conf->DumpConfig();
	
	ECWorker** workers = (ECWorker**)calloc(conf -> _agWorkerThreadNum, sizeof(ECWorker*)); 

	std::thread thrds[conf -> _agWorkerThreadNum];
	for (int i = 0; i < conf -> _agWorkerThreadNum; i++) {
		workers[i] = new ECWorker(conf);
		thrds[i] = thread([=]{ workers[i]->doProcess(); });
	}
	
	
	// clean, shoule not reach here
	for (int i = 0; i < conf->_agWorkerThreadNum; i++) {
		thrds[i].join();
	}
	for (int i = 0; i < conf->_agWorkerThreadNum; i++) {
		delete workers[i];
	}
	delete [] workers;
	delete conf;

	return 0;
}