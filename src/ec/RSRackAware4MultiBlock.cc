#include "RSRackAware4MultiBlock.hh"

RSRackAware4MultiBlock::RSRackAware4MultiBlock(int n, int k, int w, int opt, vector<string> param) {
  _n = n;
  _k = k;
  _w = w;
  _opt = opt;
  _convbindY = 0;

  _m = _n - _k;
  memset(_encode_matrix, 0, (_k+_m) * _k * sizeof(int));
  InitDownloadBandwidthInRack();
}

ECDAG* RSRackAware4MultiBlock::Encode() {
    ECDAG* ecdag = new ECDAG();
    vector<int> data;
    vector<int> code;
    for (int i=0; i<_k; i++) data.push_back(i);
    for (int i=_k; i<_n; i++) code.push_back(i);
    // if (RSCONV_DEBUG_ENABLE) {
    //   cout << "RSCONV::Encode.data:";
    //   for (int i=0; i<data.size(); i++) cout << " " << data[i];
    //   cout << endl;
    //   cout << "RSCONV::Encode.code:";
    //   for (int i=0; i<code.size(); i++) cout << " " << code[i];
    //   cout << endl;
    // }
    
    generate_matrix(_encode_matrix, _n, _k, 8);
    for (int wi = 0; wi < _w; wi++)
    {
        for (int i = 0; i < _m; i++)
        {
            vector<int> wdata;
            vector<int> coef;
            for (int j = 0; j < _k; j++)
            {
                coef.push_back(_encode_matrix[(i + _k) * _k + j]);
            }
            for (int j = 0; j < _k; j++)
                wdata.push_back(data[j] * _w + wi);
            ecdag->Join(code[i] * _w + wi, wdata, coef);
        }
    }
    return ecdag;
}


/**
 * 
 */
ECDAG* RSRackAware4MultiBlock::Decode(vector<int> from, vector<int> to) {
	LOG_INFO("RSRackAware4MultiBlock::Decode start");
	assert(_w == RPNUM + CONVNUM);
	LOG_INFO("RSRackAware4MultiBlock::Decode, CONVNUM: %d, RPNUM: %d", CONVNUM, RPNUM);


	ECDAG *ecdag = new ECDAG();

	generate_matrix(_encode_matrix, _n, _k, 8);

	vector<int> fromsids;
	vector<int> tosids;

	for (int i = 0; i < from.size(); i += _w) {
		fromsids.push_back(from[i]/_w);
	}
	for (int i = 0; i < to.size(); i += _w) {
		tosids.push_back(to[i]/_w);
	}
	vector<int> data;

	assert(fromsids.size() >= _k);

	// set rack of fromsids
	int remain = _k;
	unordered_map<int, bool> selectedsid;
	vector<vector<int> > rack;
	int racknum = _n / _m + ((_n % _m) ? 1 : 0);
	for (int i = 0; i < racknum; i++) {
		rack.emplace_back(vector<int>{});
	}
	for (int i = 0; i < fromsids.size(); i++) {
		rack[fromsids[i]/_m].push_back(fromsids[i]);
	}
	for (int i = 0; i < racknum; i++) {
		if (rack[i].size() == _m && remain >= _m) {
			remain -= _m;
			for (int j = 0; j < rack[i].size(); j++) {
				data.push_back(rack[i][j]);
				selectedsid[rack[i][j]] = true;
			}
		}
		if (remain < _m) break;
	}
	if (remain) {
		for (int i = 0; i < fromsids.size(); i++) {
			if (selectedsid.find(fromsids[i]) == selectedsid.end()) {
				remain--;
				data.push_back(fromsids[i]);
			}
			if (!remain) break;
		}
	}
	assert(!remain);

	// set coef
	vector<vector<int>> coefs;
    int _select_matrix[_k * _k];
    for (int i = 0; i < _k; i++)
    {
        int sidx = data[i];
        memcpy(_select_matrix + i * _k,
               _encode_matrix + sidx * _k,
               sizeof(int) * _k);
    }
    int _invert_matrix[_k * _k];
    jerasure_invert_matrix(_select_matrix, _invert_matrix, _k, 8);
    for (int i = 0; i < tosids.size(); i++) {
        int ridx = tosids[i];
        int _select_vector[_k];
        memcpy(_select_vector,
               _encode_matrix + ridx * _k,
               _k * sizeof(int));
        int *_coef_vector = jerasure_matrix_multiply(
            _select_vector, _invert_matrix, 1, _k, _k, _k, 8);
        vector<int> coef;
        for (int i=0; i<_k; i++) coef.push_back(_coef_vector[i]);
        free(_coef_vector);
        coefs.emplace_back(std::move(coef));
    }

	// set ecdag
	int tmpname = BINDSTART + (_k + _m)*_w;
    int wi = 0;

	// RP ecdag
    for (int rpi = 0; rpi < RPNUM; rpi++) {
        wi = rpi;
        for (int i = 0; i < tosids.size(); i++) {
            int cidx = tosids[i]*_w + wi;
            deque<int> dataqueue;
            deque<int> coefqueue;
            for (int j = 0; j < _k; j++) {
                dataqueue.push_back(data[j]*_w + wi);
                coefqueue.push_back(coefs[i][j]);
            }
            // build repair pipelining.
            while(dataqueue.size() >= 2) {
                vector<int> datav;
                vector<int> coefv;
                for (int j = 0; j < 2; j++) {
                    int tmpd(dataqueue.front());
                    dataqueue.pop_front();
                    int tmpc(coefqueue.front());
                    coefqueue.pop_front();
                    datav.push_back(tmpd);
                    coefv.push_back(tmpc);
                }
                int toadd;
                if (dataqueue.size() == 0)
                {
                    toadd = cidx;
                }
                else
                {
                    toadd = tmpname++;
                }
                ecdag->Join(toadd, datav, coefv);
                ecdag->BindY(toadd, datav[1]);
                dataqueue.push_front(toadd);
                coefqueue.push_front(1);
            }
        }  
    }

	// CONV ecdag

	if (UseRackAwareCr(to)) {		// use rack_aware cr
		RackAwareCr(ecdag, fromsids, tosids, data, coefs, tmpname);
	} else {						// use conv cr
		ConvCr(ecdag, fromsids, tosids, data, coefs, tmpname);
	}
    return ecdag;





	int vracknum = _k / _m;
    // repair for word with offset [RPNUM + convi]
    for (int convi = 0; convi < CONVNUM; convi++) {
        wi = RPNUM + convi;
        vector<vector<int>> tmpridxs;
        vector<int> ridxs;
        for (int i = 0; i < tosids.size(); i++) {
            ridxs.push_back(tosids[i]*_w + wi);
            tmpridxs.emplace_back(vector<int>{});
        }
        
        // ineer-rack
        for (int ri = 0; ri < vracknum; ri++) {
            int startidx = ri * _m;
            // _m nodes in the same rack.
            vector<int> rdata;
            vector<vector<int>> rcoefs;
            vector<int> tobindx;
            // inner-rack ecdag data.
            for (int mi = 0; mi < _m; mi++) {
                rdata.push_back(data[startidx+mi]*_w + wi);
            }
            for (int i = 0; i < tosids.size(); i++) {
                rcoefs.emplace_back(vector<int>{});
                for (int j = 0; j < _m; j++) {
                    rcoefs[i].push_back(coefs[i][startidx+j]);
                }
                ecdag->Join(tmpname, rdata, rcoefs[i]); // repair for data[i], word[convi], in rack[ri]
                LOG_INFO("Join %s to %d", VectorToString(rdata).c_str(), tmpname);
                tobindx.push_back(tmpname);
                ecdag->BindY(tmpname, rdata[startidx]); // choose rdata[startidx] to be local collector
                LOG_INFO("BindY %d to %d", tmpname, rdata[startidx]);
                tmpridxs[i].push_back(tmpname++);
            }

            /**
             * ???
             */
            // int bindxret = ecdag->BindX(tobindx);
            // ecdag->BindY(bindxret, rdata[wi%_m]);
            // for (int i = 0; i < tosids.size(); i++) {
            //     ecdag->BindY(tobindx[i], bindxret);
            // }
        }

        // cross-rack conv
        for (int i = 0; i < tosids.size(); i++) {
            int cidx = tosids[i] * _w + wi;
            std::vector<int> data;
            std::vector<int> coef;
            for (int ri = 0; ri < vracknum; ri++) {
                coef.push_back(1);
                data.push_back(tmpridxs[i][ri]);
            }
            LOG_INFO("Join %s to %d", VectorToString(data).c_str(), cidx);
            ecdag->Join(cidx, data, coef);
        }




        // // cross-rack pipelining
        // for (int i = 0; i < tosids.size(); i++) {
        //     deque<int> dataqueue;
        //     deque<int> coefqueue;
        //     for (int j = 0; j < vracknum; j++) {
        //         dataqueue.push_back(tmpridxs[i][j]);
        //         coefqueue.push_back(1);
        //     }
        //     while (dataqueue.size() >= 2)
        //     {
        //         vector<int> datav;
        //         vector<int> coefv;

        //         for (int j = 0; j < 2; j++) {
        //             int tmpd(dataqueue.front());
        //             dataqueue.pop_front();
        //             int tmpc(coefqueue.front());
        //             coefqueue.pop_front();
        //             datav.push_back(tmpd);
        //             coefv.push_back(tmpc);
        //         }
        //         int toadd;
        //         toadd = tmpname++;
        //         ecdag->Join(toadd, datav, coefv);
        //         ecdag->BindY(toadd, datav[1]);  
        //         if (dataqueue.size() != 0) {
        //             dataqueue.push_front(toadd);
        //             coefqueue.push_front(1);
        //         }
        //         else {    
        //             ecdag->Join(tosids[i]*_w + wi, vector<int>{toadd}, vector<int>{1});
        //         }
        //     }

        // }

    }
    
    return ecdag;



}

void RSRackAware4MultiBlock::Place(vector<vector<int>>& group) {
}

void RSRackAware4MultiBlock::generate_matrix(int* matrix, int rows, int cols, int w) {
  int k = cols;
  int n = rows;
  int m = n - k;

  memset(matrix, 0, rows * cols * sizeof(int));
  for(int i=0; i<k; i++) {
    matrix[i*k+i] = 1;
  }

  for (int i=0; i<m; i++) {
    int tmp = 1;
    for (int j=0; j<k; j++) {
      matrix[(i+k)*cols+j] = tmp;
      tmp = Computation::singleMulti(tmp, i+1, w);
    }
  }
}


/**
 * if repair chunks if equal _m, dont use rack-aware-conv
 */
bool RSRackAware4MultiBlock::UseRackAwareCr(std::vector<int> to) const {
	// return to.size() < _m;
    return USE_RACK_AWARE_CONV;
}

/**
 * tosids: node ids of new node
 * data: node ids of worker
 * coefs: coefs of rs
 */
void RSRackAware4MultiBlock::RackAwareCr(ECDAG* ecdag, std::vector<int>& fromsids, std::vector<int>& tosids, 
                                        std::vector<int>& data, std::vector<std::vector<int>>& coefs, int& tmpname) {
	int vracknum = _k / _m;
    int wi = 0;
    // repair for word with offset [RPNUM + convi]
    for (int convi = 0; convi < CONVNUM; convi++) {
        wi = RPNUM + convi;
        vector<vector<int>> tmpridxs;
        vector<int> ridxs;
        for (int i = 0; i < tosids.size(); i++) {
            ridxs.push_back(tosids[i]*_w + wi);
            tmpridxs.emplace_back(vector<int>{});
        }
        
        // ineer-rack
        for (int ri = 0; ri < vracknum; ri++) {
            int startidx = ri * _m;
            // _m nodes in the same rack.
            vector<int> rdata;
            vector<vector<int>> rcoefs;
            vector<int> tobindx;
            // inner-rack ecdag data.
            for (int mi = 0; mi < _m; mi++) {
                rdata.push_back(data[startidx+mi]*_w + wi);
            }
            for (int i = 0; i < tosids.size(); i++) {
                rcoefs.emplace_back(vector<int>{});
                for (int j = 0; j < _m; j++) {
                    rcoefs[i].push_back(coefs[i][startidx+j]);
                }
                ecdag->Join(tmpname, rdata, rcoefs[i]); // repair for data[i], word[convi], in rack[ri]
                LOG_INFO("Join %s to %d", VectorToString(rdata).c_str(), tmpname);
                tobindx.push_back(tmpname);
                // ecdag->BindY(tmpname, rdata[startidx]); // choose rdata[startidx] to be local collector
                // LOG_INFO("BindY %d to %d", tmpname, rdata[startidx]);
                tmpridxs[i].push_back(tmpname++);
            }

            
            int local_collector_tmp_id = ecdag->BindX(tobindx);
            LOG_INFO("BindX %s to %d", VectorToString(tobindx).c_str(), local_collector_tmp_id);
            int local_collector_id;
            
            if (CHOOSE_LOCAL_COLLECTOR) {
                std::vector<int> rnode_ids;
                for (auto data_id: rdata) {
                    rnode_ids.push_back(data_id / _w);
                }
                local_collector_id = SelectLocalCollector(rnode_ids)*_w + wi;
            } else {
                local_collector_id = rdata[wi%_m];
            }
            ecdag->BindY(local_collector_tmp_id, local_collector_id);
            LOG_INFO("BindY %d to %d", local_collector_tmp_id, local_collector_id);
            // ???
            // for (int i = 0; i < tosids.size(); i++) {
            //     ecdag->BindY(tobindx[i], local_collector_tmp_id);
            // }
        }

        // cross-rack conv
        if (USE_MULTI_GLOBAL_COLLECTOR) {   // local collector send chunks to each node
            std::vector<int> tobindx;
            for (int i = 0; i < tosids.size(); i++) {
                int cidx = tosids[i] * _w + wi;
                std::vector<int> data;
                std::vector<int> coef;
                for (int ri = 0; ri < vracknum; ri++) {
                    coef.push_back(1);
                    data.push_back(tmpridxs[i][ri]);
                }
                LOG_INFO("Join %s to %d", VectorToString(data).c_str(), cidx);
                ecdag->Join(cidx, data, coef);
                tobindx.push_back(cidx);
            }
        } else {                            // local collector send chunks to global collector, then global collector send chunks to each node
            std::vector<int> tobindx;
            for (int i = 0; i < tosids.size(); i++) {   
                int cidx = tosids[i] * _w + wi;
                std::vector<int> data;
                std::vector<int> coef;
                for (int ri = 0; ri < vracknum; ri++) {
                    for (int j = 0; j < tosids.size(); j++) {
                        if (j == i) {
                            coef.push_back(1);
                        } else {
                            coef.push_back(0);
                        }
                        data.push_back(tmpridxs[j][ri]);    // tmpridsx[j][ri] is data in rack[ri], send to data[j]
                    }
                }

                LOG_INFO("Join %s to %d", VectorToString(data).c_str(), cidx);
                ecdag->Join(cidx, data, coef);
                tobindx.push_back(cidx);
            }

            int global_collector_tmp_id = ecdag->BindX(tobindx);
            LOG_INFO("BindX %s to %d", VectorToString(tobindx).c_str(), global_collector_tmp_id);
            int global_collector_id;   // single global collector
            if (CHOOSE_GLOBAL_COLLECTOR) {
                global_collector_id = SelectGlobalCollector(tosids)*_w + wi;
            } else {
                global_collector_id = tosids[_convbindY % tosids.size()]*_w + wi;
            }
            ecdag->BindY(global_collector_tmp_id, global_collector_id);
            LOG_INFO("BindY %d to %d", global_collector_tmp_id, global_collector_id);

        }



        // std::vector<int> tobindx;
        // for (int i = 0; i < tosids.size(); i++) {
        //     int cidx = tosids[i] * _w + wi;
        //     std::vector<int> data;
        //     std::vector<int> coef;
        //     for (int ri = 0; ri < vracknum; ri++) {
        //         coef.push_back(1);
        //         data.push_back(tmpridxs[i][ri]);
        //     }
        //     LOG_INFO("Join %s to %d", VectorToString(data).c_str(), cidx);
        //     ecdag->Join(cidx, data, coef);
        //     tobindx.push_back(cidx);
        // }

        // if (!USE_MULTI_GLOBAL_COLLECTOR) {
            
        //     int global_collector_tmp_id = ecdag->BindX(tobindx);
        //     LOG_INFO("BindX %s to %d", VectorToString(tobindx).c_str(), global_collector_tmp_id);
        //     int global_collector_id = tosids[_convbindY % tosids.size()]*_w + wi;   // single global collector
        //     ecdag->BindY(global_collector_tmp_id, global_collector_id);
        //     LOG_INFO("BindY %d to %d", global_collector_tmp_id, global_collector_id);
        // }

    }    
}


void RSRackAware4MultiBlock::ConvCr(ECDAG* ecdag, std::vector<int>& fromsids, std::vector<int>& tosids, 
                                    std::vector<int>& data, std::vector<std::vector<int>>& coefs, int& tmpname) {
    for (int convi = 0; convi < CONVNUM; convi++) {
        int wi = RPNUM + convi;
        vector<int> data;
        vector<int> tobindx;
        for (int ki = 0; ki < _k; ki++) {
            data.push_back(fromsids[ki]*_w + wi);
        }
        for (int i = 0; i < tosids.size(); i++) {
            int cidx = tosids[i]*_w + wi;
            ecdag->Join(cidx, data, coefs[i]);  // repair data[i], word[wi], send to data[i], word[wi]
            LOG_INFO("Join %s to %d", VectorToString(data).c_str(), cidx);
            tobindx.push_back(cidx);
        }

        if (!USE_MULTI_GLOBAL_COLLECTOR) {      // use single global collector
            int global_collector_tmp_id = ecdag->BindX(tobindx);
            LOG_INFO("BindX %s to %d", VectorToString(tobindx).c_str(), global_collector_tmp_id);
            int global_collector_id;
            if (CHOOSE_GLOBAL_COLLECTOR) {
                global_collector_id = SelectGlobalCollector(tosids)*_w + wi;
            } else {
                global_collector_id = tosids[_convbindY % tosids.size()]*_w + wi;
            }
            ecdag->BindY(global_collector_tmp_id, global_collector_id);
            LOG_INFO("BindY %d to %d", global_collector_tmp_id, global_collector_id);
        }
    }
}



void RSRackAware4MultiBlock::InitDownloadBandwidthInRack() {
    const std::string conf_path = "/home/openec/HMBR/conf/download_bws.conf";
    std::ifstream ifs(conf_path);
    assert(ifs.is_open() && "open conf file failed");

    int bw_temp;
    for (int i = 0; i < _n; i++) {
        if (ifs >> bw_temp) {
            _download_bandwidth_inrack[i] = bw_temp;
        } else {
            assert(false && "read conf file failed");
        }
    }
}

/**
 * choose one local collector from nodeids, according to download bandwidth
 */
int RSRackAware4MultiBlock::SelectLocalCollector(std::vector<int> nodeids) {
	int local_collector_id = nodeids[0];
    for (auto nodeid: nodeids) {
        assert(_download_bandwidth_inrack.find(nodeid) != _download_bandwidth_inrack.end());
        assert(_download_bandwidth_inrack.find(local_collector_id) != _download_bandwidth_inrack.end());
        if (_download_bandwidth_inrack[nodeid] > _download_bandwidth_inrack[local_collector_id]) {
            local_collector_id = nodeid;
        }
    }
    LOG_INFO("SelectLocalCollector, node id: %d", local_collector_id);
    return local_collector_id;
}

/**
 * nodeids is node id
 */
int RSRackAware4MultiBlock::SelectGlobalCollector(std::vector<int> nodeids) {
	int global_collector_id = nodeids[0];
    for (auto nodeid: nodeids) {
        assert(_download_bandwidth_inrack.find(nodeid) != _download_bandwidth_inrack.end());
        assert(_download_bandwidth_inrack.find(global_collector_id) != _download_bandwidth_inrack.end());
        if (_download_bandwidth_inrack[nodeid] > _download_bandwidth_inrack[global_collector_id]) {
            global_collector_id = nodeid;
        }
    }
    LOG_INFO("SelectGlobalCollector, node id: %d", global_collector_id);
    return global_collector_id;
}
