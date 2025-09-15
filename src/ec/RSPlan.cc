#include "RSPlan.hh"


RSPlan::RSPlan(Config* conf, FileMeta* fileMeta, const std::string& ecdagPath, int k, int n, int w) 
               : ECPlan(conf, fileMeta, ecdagPath) {
    _k = k;
    _n = n;
    _w = w;
    generateMatrix();
    setRSTasks();
}


RSPlan::RSPlan(Config* conf, FileMeta* fileMeta, const std::string& ecdagPath, int k, int n, int w, 
               const std::vector<int>& survivedObjIds, int failedObjId) : ECPlan(conf, fileMeta, ecdagPath) {
    _k = k;
    _n = n;
    _w = w;
    // generateMatrix();
    // if (_conf->_ecPolicy == ECPolicy::CONV) {
    //     generateDecodeMatrix(survivedObjIds, failedObjId);        
    // }
    setRSTasks();
}

/**
 * set rs tasks info from ecdagPath
 */
void RSPlan::setRSTasks() {
    LOG_INFO("RSPlan::setRSTasks start, ecdagPath: %s", _ecdagPath.c_str());

    std::ifstream ifs(_ecdagPath);
    assert(ifs.is_open() && "Failed to open ecdag file");
    std::string line;
    while (std::getline(ifs, line)) {
        std::istringstream iss(line);
        std::vector<std::string> taskInfo; 
        std::string token, tmpType;
        assert(iss >> tmpType && "Failed to get token");
        ECTaskType taskType = str2ECTaskType(tmpType);
        while (iss >> token) {
            taskInfo.push_back(token);
        }
        ECTask* task = new ECTask();
        task->_type = taskType;
        if (_conf->_ecPolicy == ECPolicy::PipeFG) {
            setRSFGTask(taskInfo, task);
        } else {
            setRSTask(taskInfo, task); 
        }

        _tasks[task->_nodeId].push_back(task);
    }
    LOG_INFO("RSPlan::setRSTasks done, ecdagPath: %s", _ecdagPath.c_str());

    for (auto it : _tasks) {
        int nodeId = it.first;
        std::vector<ECTask*> tasks = it.second;
        printf("nodeId: %d, tasks size: %ld\n", nodeId, tasks.size());
        for (auto task : tasks) {
            printf("task type: %d, nodeId: %d, srcNodeId: %d, dstNodeId: %d, objId: %d, tmpObjId: %d, objIds: %s, encodePatternId: %d, coefs: %s\n", 
                    ECTaskType2int(task->_type), nodeId, task->_srcNodeId, task->_dstNodeId, task->_objId, task->_tmpObjId, 
                    vec2String(task->_objIds).c_str(), task->_encodePatternId, vec2String(task->_coefs).c_str());
        }
    }

    ifs.close();
}


/**
 * set task info from line from ecdagPath
 * SEND [nodeId/srcNodeId] [dstNodeId] [objId]
 * RECEIVE [nodeId/dstNodeId][srcNodeId] [objId] [tmpObjId]
 * ENCODE [nodeId] [objId...] [tmpObjId] [encodePatternId]
 * ENCODE_PARTIAL [nodeId] [objNum] [objIds...] [tmpObjId] [coefs...]
 * PERSIST [nodeId] [tmpObjId] [objId]
 */
void RSPlan::setRSTask(const std::vector<std::string>& taskInfo, ECTask* task) {
    int rowId;
    switch (task->_type) {
        case ECTaskType::SEND:
            assert(taskInfo.size() == 3 && "SEND task info size error");
            task->_srcNodeId = std::stoi(taskInfo[0]);
            task->_dstNodeId = std::stoi(taskInfo[1]);
            task->_objId = std::stoi(taskInfo[2]);
            task->_nodeId = task->_srcNodeId;
            break;
        case ECTaskType::RECEIVE:
            assert(taskInfo.size() == 4 && "RECEIVE task info size error");
            task->_dstNodeId = std::stoi(taskInfo[0]);
            task->_srcNodeId = std::stoi(taskInfo[1]);
            task->_objId = std::stoi(taskInfo[2]);
            task->_tmpObjId = std::stoi(taskInfo[3]);
            task->_nodeId = task->_dstNodeId;
            break;
        case ECTaskType::ENCODE:
            assert(taskInfo.size() == (3 + _k) && "PERSIST task info size error");
            task->_nodeId = std::stoi(taskInfo[0]);
            for (int i = 0; i < _k; i++) {
                task->_objIds.push_back(std::stoi(taskInfo[1 + i]));
            }
            task->_tmpObjId = std::stoi(taskInfo[1 + _k]);
            task->_encodePatternId = std::stoi(taskInfo[2 + _k]);
            if (task->_encodePatternId == -1) {             // decode
                task->_coefs = _encodeMatrix[_failedRowId];
                break;
            }
            assert(task->_encodePatternId >= 0 && task->_encodePatternId < _n - _k);
            task->_coefs = _encodeMatrix[_k + task->_encodePatternId];
            assert(task->_coefs.size() == _k && "encode pattern size error");
            break;
        case ECTaskType::ENCODE_PARTIAL:
            assert(taskInfo.size() >= 5 && "ENCODE_PARTIAL task info size error");
            task->_nodeId = std::stoi(taskInfo[0]);
            task->_objNum = std::stoi(taskInfo[1]);
            assert(taskInfo.size() == 3 + task->_objNum * 2 && "ENCODE_PARTIAL task info size error");
            for (int i = 0; i < task->_objNum; i++) {
                task->_objIds.push_back(std::stoi(taskInfo[2 + i]));
            }
            task->_tmpObjId = std::stoi(taskInfo[2 + task->_objNum]);
            for (int i = 0; i < task->_objNum; i++) {
                task->_coefs.push_back(std::stoi(taskInfo[3 + task->_objNum + i]));
            }
            break;
        case ECTaskType::PERSIST:
            assert(taskInfo.size() == 4 && "PERSIST task info size error");
            task->_nodeId = std::stoi(taskInfo[0]);
            task->_tmpObjId = std::stoi(taskInfo[1]);
            task->_objId = std::stoi(taskInfo[2]);
            rowId = std::stoi(taskInfo[3]);
            _fileMeta->setRowId(task->_objId, rowId);       // set rowId of this obj, used in decode to know which row this obj is
            break;
        case ECTaskType::FETCH:
            assert(taskInfo.size() == 3 && "FETCH task info size error");
            task->_nodeId = std::stoi(taskInfo[0]);
            task->_objId = std::stoi(taskInfo[1]);
            task->_tmpObjId = std::stoi(taskInfo[2]);
            break;
        default:
            assert(false && "undefined ECTaskType");
    }
}


/**
 * set task info from line from ecdagPath
 * SEND [nodeId/srcNodeId] [dstNodeId] [objId] [leftBound] [rightBound]
 * RECEIVE [nodeId/dstNodeId][srcNodeId] [objId] [tmpObjId] [leftBound] [rightBound]
 * ENCODE_PARTIAL [nodeId] [objNum] [objIds...] [tmpObjId] [coefs...] [leftBound] [rightBound]
 * PERSIST [nodeId] [tmpObjId] [objId] [leftBound] [rightBound]
 * FETCH [nodeId] [objId] [tmpObjId] [leftBound] [rightBound]
 */
void RSPlan::setRSFGTask(const std::vector<std::string>& taskInfo, ECTask* task) {
    int rowId;
    switch (task->_type) {
        case ECTaskType::SEND:
            assert(taskInfo.size() == 5 && "SEND task info size error");
            task->_srcNodeId = std::stoi(taskInfo[0]);
            task->_dstNodeId = std::stoi(taskInfo[1]);
            task->_objId = std::stoi(taskInfo[2]);
            task->_nodeId = task->_srcNodeId;
            task->_leftBound = std::stoi(taskInfo[3]);
            task->_rightBound = std::stoi(taskInfo[4]);
            break;
        case ECTaskType::RECEIVE:
            assert(taskInfo.size() == 6 && "RECEIVE task info size error");
            task->_dstNodeId = std::stoi(taskInfo[0]);
            task->_srcNodeId = std::stoi(taskInfo[1]);
            task->_objId = std::stoi(taskInfo[2]);
            task->_tmpObjId = std::stoi(taskInfo[3]);
            task->_nodeId = task->_dstNodeId;
            task->_leftBound = std::stoi(taskInfo[4]);
            task->_rightBound = std::stoi(taskInfo[5]);
            break;
        case ECTaskType::ENCODE:
            assert(false && "PipeFG should not have ENCODE command");
            assert(taskInfo.size() == (5 + _k) && "PERSIST task info size error");
            task->_nodeId = std::stoi(taskInfo[0]);
            for (int i = 0; i < _k; i++) {
                task->_objIds.push_back(std::stoi(taskInfo[1 + i]));
            }
            task->_tmpObjId = std::stoi(taskInfo[1 + _k]);
            task->_encodePatternId = std::stoi(taskInfo[2 + _k]);
            if (task->_encodePatternId == -1) {             // decode
                task->_coefs = _encodeMatrix[_failedRowId];
                break;
            }
            assert(task->_encodePatternId >= 0 && task->_encodePatternId < _n - _k);
            task->_coefs = _encodeMatrix[_k + task->_encodePatternId];
            assert(task->_coefs.size() == _k && "encode pattern size error");
            task->_leftBound = std::stoi(taskInfo[3 + _k]);
            task->_rightBound = std::stoi(taskInfo[4 + _k]);
            break;
        case ECTaskType::ENCODE_PARTIAL:
            assert(taskInfo.size() >= 7 && "ENCODE_PARTIAL task info size error");
            task->_nodeId = std::stoi(taskInfo[0]);
            task->_objNum = std::stoi(taskInfo[1]);
            assert(taskInfo.size() == 5 + task->_objNum * 2 && "ENCODE_PARTIAL task info size error");
            for (int i = 0; i < task->_objNum; i++) {
                task->_objIds.push_back(std::stoi(taskInfo[2 + i]));
            }
            task->_tmpObjId = std::stoi(taskInfo[2 + task->_objNum]);
            for (int i = 0; i < task->_objNum; i++) {
                task->_coefs.push_back(std::stoi(taskInfo[3 + task->_objNum + i]));
            }
            task->_leftBound = std::stoi(taskInfo[3 + task->_objNum * 2]);
            task->_rightBound = std::stoi(taskInfo[4 + task->_objNum * 2]);
            break;
        case ECTaskType::PERSIST:
            assert(taskInfo.size() == 6 && "PERSIST task info size error");
            task->_nodeId = std::stoi(taskInfo[0]);
            task->_tmpObjId = std::stoi(taskInfo[1]);
            task->_objId = std::stoi(taskInfo[2]);
            rowId = std::stoi(taskInfo[3]);
            _fileMeta->setRowId(task->_objId, rowId);       // set rowId of this obj, used in decode to know which row this obj is
            task->_leftBound = std::stoi(taskInfo[4]);
            task->_rightBound = std::stoi(taskInfo[5]);
            break;
        case ECTaskType::FETCH:
            assert(taskInfo.size() == 5 && "FETCH task info size error");
            task->_nodeId = std::stoi(taskInfo[0]);
            task->_objId = std::stoi(taskInfo[1]);
            task->_tmpObjId = std::stoi(taskInfo[2]);
            task->_leftBound = std::stoi(taskInfo[3]);
            task->_rightBound = std::stoi(taskInfo[4]);
            break;
        default:
            assert(false && "undefined ECTaskType");
    }
}


/**
 * generate encode matrix for rs
 */
void RSPlan::generateMatrix() {
    
    // n row, k col
    _encodeMatrix = std::vector<std::vector<int>>(_n, std::vector<int>(_k, 0));
    for (int i = 0; i < _k; i++) {
        _encodeMatrix[i][i] = 1;
    }

    int m = _n - _k;
    for (int i = 0; i < m; i++) {
        int tmp = 1;
        for (int j = 0; j < _k; j++) {
            _encodeMatrix[i + _k][j] = tmp;
            tmp = galois_single_multiply(tmp, i + 1, _w);
        }
    }
    LOG_INFO("encode matrix: ");
    for (int i = 0; i < _n; i++) {
        for (int j = 0; j < _k; j++) {
            printf("%d ", _encodeMatrix[i][j]);
        }
        printf("\n");
    }

    // dump ec info to file
    std::string ecInfoFile = "/root/coar/build/ec_info";
    std::ofstream ofs(ecInfoFile, std::ios::out | std::ios::trunc);
    assert(ofs.is_open() && "Failed to open ec info file");
    ofs << _n << " " << _k << std::endl;
    for (int i = 0; i < _n; i++) {
        for (int j = 0; j < _k; j++) {
            ofs << _encodeMatrix[i][j] << " ";
        }
        ofs << std::endl;
    }
    ofs.close();
}

void RSPlan::generateDecodeMatrix(const std::vector<int>& survivedObjIds, int failedObjId) {
    LOG_INFO("RSPlan::generateDecodeMatrix start, survivedObjIds: %s, failedObjId: %d", 
             vec2String(survivedObjIds).c_str(), failedObjId);
    int* selectMatrix = new int [_k * _k];
    memset(selectMatrix, 0, _k * _k * sizeof(int));
    std::vector<int> survivedRowIds;
    int failedRowId = _fileMeta->getRowId(failedObjId);
    _failedRowId = failedRowId;
    for (auto survivedObjId : survivedObjIds) {
        int rowId = _fileMeta->getRowId(survivedObjId);
        survivedRowIds.push_back(rowId);
    }
    LOG_INFO("survived obj ids: %s, failed obj id: %d", vec2String(survivedObjIds).c_str(), failedObjId);
    LOG_INFO("survived row ids: %s, failed row id: %d", vec2String(survivedRowIds).c_str(), failedRowId);

    // get select matrix
    for (int i = 0; i < _k; i++) {
        int survivedRowId = survivedRowIds[i];
        memcpy(selectMatrix + i * _k, _encodeMatrix[survivedRowId].data(), _k * sizeof(int));
    }

    LOG_INFO("select matrix: ");
    for (int i = 0; i < _k; i++) {
        for (int j = 0; j < _k; j++) {
            printf("%d ", selectMatrix[i * _k + j]);
        }
        printf("\n");
    }

    // get invert matrix
    int* invertMatrix = new int [_k * _k];
    jerasure_invert_matrix(selectMatrix, invertMatrix, _k, _w);
    int* selectVector = new int [_k];
    memcpy(selectVector, _encodeMatrix[failedRowId].data(), _k * sizeof(int));
    int* coefVector = jerasure_matrix_multiply(selectVector, invertMatrix, 1, _k, _k, _k, _w);
    LOG_INFO("coef vector: ");
    for (int i = 0; i < _k; i++) {
        printf("%d ", coefVector[i]);
    }
    printf("\n");
    memcpy(_encodeMatrix[failedRowId].data(), coefVector, _k * sizeof(int));
    
    delete [] coefVector;
    delete [] selectMatrix;
    delete [] invertMatrix;
    delete [] selectVector;
}

/**
 * data and parity have been allocated
 */
void RSPlan::encode(std::vector<const char*> data, std::vector<char*> parity, 
                    const std::vector<std::vector<int>>& encodeMatrix, int w, int objSizeByte) {
    assert(data.size() == encodeMatrix.size() && "data size not equal to matrix size");
    int k = data.size();
    int m = parity.size();
    assert(encodeMatrix.size() == m && encodeMatrix.front().size() == k && "matrix size error");
    char** data_ptrs = (char**)malloc(k * sizeof(char*));
    char** coding_ptrs = (char**)malloc(m * sizeof(char*));
    for (int i = 0; i < k; i++) {
        data_ptrs[i] = (char*)malloc(w * sizeof(char));
        memcpy(data_ptrs[i], data[i], w);
    }
    for (int i = 0; i < m; i++) {
        coding_ptrs[i] = parity[i];
    }
    int* matrix = (int*)malloc(k * m * sizeof(int));
    for (int i = 0; i < m; i++) {
        for (int j = 0; j < k; j++) {
            matrix[i * k + j] = encodeMatrix[i][j];
        }
    }
    jerasure_matrix_encode(k, m, w, matrix, data_ptrs, coding_ptrs, objSizeByte);


    // clear
    for (int i = 0; i < k; i++) {
        free(data_ptrs[i]);
    }
    free(data_ptrs);
    for (int i = 0; i < m; i++) {
        free(coding_ptrs[i]);
    }
    free(coding_ptrs);
    free(matrix);
}


void RSPlan::encode(std::vector<const char*> data, char* parity, 
                    const std::vector<int>& encodeMatrix, int w, int objSizeByte) {
    assert(data.size() == encodeMatrix.size() && "data size not equal to matrix size");
    int k = data.size();
    int m = 1;
    assert(encodeMatrix.size() == k && "matrix size error");
    char** data_ptrs = (char**)malloc(k * sizeof(char*));           // free after RSPlan::encode
    char** coding_ptrs = (char**)malloc(m * sizeof(char*));         // free after RSPlan::encode
    for (int i = 0; i < k; i++) {
        data_ptrs[i] = (char*)malloc(objSizeByte * sizeof(char));
        memcpy(data_ptrs[i], data[i], objSizeByte);
    }
    coding_ptrs[0] = (char*)malloc(objSizeByte * sizeof(char));
    int* matrix = (int*)malloc(k * m * sizeof(int));                // free after RSPlan::encode
    
    for (int j = 0; j < k; j++) {
        matrix[j] = encodeMatrix[j];
    }
    timeval start, end;
    gettimeofday(&start, NULL);
    jerasure_matrix_encode(k, m, w, matrix, data_ptrs, coding_ptrs, objSizeByte);
    gettimeofday(&end, NULL);
    memcpy(parity, coding_ptrs[0], objSizeByte);
    LOG_INFO("jerasure_matrix_encode time: %f ms", RedisUtil::duration(start, end));
    // clear
    for (int i = 0; i < k; i++) {
        free(data_ptrs[i]);
    }
    free(data_ptrs);
    for (int i = 0; i < m; i++) {
        free(coding_ptrs[i]);
    }
    free(coding_ptrs);
    free(matrix);
}