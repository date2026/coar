#include "common/Config.hh"
#include "common/Coordinator.hh"
#include "common/StripeStore.hh"

#include "inc/include.hh"


using namespace std;

int main(int argc, char** argv) {
	const std::string confPath = "/root/coar/conf/1.json";
	Config* conf = new Config(confPath);
	StripeStore* ss = new StripeStore(conf); 


	Coordinator** coors = (Coordinator**)calloc(conf->_coorThreadNum, sizeof(Coordinator*));
	std::thread thrds[conf->_coorThreadNum];
	for (int i = 0; i < conf->_coorThreadNum; i++) {
		coors[i] = new Coordinator(conf, ss);
		thrds[i] = thread([=]{ coors[i]->doProcess(); });
	}


	// clean, shoule not reach here
	for (int i = 0; i < conf->_coorThreadNum; i++) {
		thrds[i].join();
	}
	for (int i = 0; i < conf->_coorThreadNum; i++) {
		delete coors[i];
	}
	delete [] coors;
	delete conf;
	delete ss;
	return 0;
}
