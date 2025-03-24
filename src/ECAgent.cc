#include "common/Config.hh"
#include "common/OECWorker.hh"

#include "inc/include.hh"

using namespace std;

int main(int argc, char** argv) {

	const string confPath = "/home/openec/lmq_openec/conf/1.json";
	Config* conf = new Config(confPath);
	conf->DumpConfig();
	
	OECWorker** workers = (OECWorker**)calloc(conf -> _agWorkerThreadNum, sizeof(OECWorker*)); 

	std::thread thrds[conf -> _agWorkerThreadNum];
	for (int i = 0; i < conf -> _agWorkerThreadNum; i++) {
		workers[i] = new OECWorker(conf);
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