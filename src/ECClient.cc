#include "common/Config.hh"
#include "common/ECOutputStream.hh"
void usage() {
    std::cout << "usage: ./OECClient write filepath saveas ecid size(MB)" << std::endl;
    std::cout << "       ./OECClient read filename saveas" << std::endl;
    std::cout << "       ./OECClient encode" << std::endl;
    std::cout << "       ./OECClient repair" << std::endl;
}

const std::string confPath = "/home/openec/lmq_openec/conf/1.json";


void write(const std::string& file_path, const std::string& saveas, const std::string& ecidpool, int size);

int main(int argc, char** argv) {
    assert(argc >= 2);

    std::string req_type(argv[1]);
    if (req_type == "write") {
        assert(argc == 6);
        std::string file_path(argv[2]);
        std::string saveas(argv[3]);
        std::string ecidpool(argv[4]);
        int size = std::stoi(argv[5]);
        write(file_path, saveas, ecidpool, size);
    } else if (req_type == "read") {
        assert(argc == 4);
        std::string file_name(argv[2]);
        std::string saveas(argv[3]);
        LOG_INFO("read, file_name: %s, save_as: %s", file_name.c_str(), saveas.c_str());
    } else if (req_type == "encode") {
        assert(argc == 2);
    } else if (req_type == "repair") {
        assert(argc == 2);
    } else {
        assert("Invalid request type");
    }

    return 0;
}


void write(const std::string& filePath, const std::string& saveAs, const std::string& ecidpool, int sizeinMB) {
    LOG_INFO("write, filePath: %s, saveAs: %s, ecidpool: %s, sizeinMB: %d", filePath.c_str(), saveAs.c_str(), ecidpool.c_str(), sizeinMB);
    
    Config* conf = new Config(confPath);
    
    struct timeval writeStart, writeEnd;
    gettimeofday(&writeStart, NULL);


    FILE* inputfile = fopen(filePath.c_str(), "rb");
    assert(inputfile != NULL && "Failed to open file");

    ECOutputStream* outstream = new ECOutputStream(conf, saveAs, ecidpool, "offline", sizeinMB);


    int sizeinByte = sizeinMB * 1024 * 1024;
    assert(sizeinByte % conf->_pktSize == 0);
    int pktNum = sizeinByte / conf->_pktSize;

    char* buf = new char [conf->_pktSize + sizeof(int)];    // datalen | data
    for (int i = 0; i < pktNum; i++) {
        int tmplen = htonl(conf->_pktSize);
        memcpy(buf, (char*)&tmplen, sizeof(int));
        fread(buf + sizeof(int), conf->_pktSize, 1, inputfile);
        outstream->write(buf, conf->_pktSize + sizeof(int));
    }
    delete [] buf;

    outstream->close();
    delete outstream;
    fclose(inputfile);
    delete conf;


    gettimeofday(&writeEnd, NULL);
    LOG_INFO("write time: %f ms", RedisUtil::duration(writeStart, writeEnd));

}