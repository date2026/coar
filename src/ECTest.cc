#include "util/jerasure.h"
#include "inc/include.hh"

#include <random>
#include <algorithm>
#include <emmintrin.h>


// 获取CPU最后一级缓存（LLC）大小
size_t get_llc_size() {
    FILE* fp = fopen("/sys/devices/system/cpu/cpu0/cache/index3/size", "r");
    if (!fp) return 40 * 1024 * 1024; // 阿里云ECS默认40MB LLC
    char buf[16];
    fread(buf, 1, sizeof(buf), fp);
    fclose(fp);
    size_t size = atoi(buf);
    if (strstr(buf, "K")) size *= 1024;
    if (strstr(buf, "M")) size *= 1024 * 1024;
    return size;
}

void flush_cpu_cache() {
    const size_t LLC_SIZE = get_llc_size();
    const size_t ELEMENTS = LLC_SIZE / sizeof(int) * 1.2; // 1.2倍LLC，确保溢出
    std::vector<int> dummy_data(ELEMENTS, 1);
    volatile int* p = dummy_data.data();
    const size_t len = dummy_data.size();

    // 生成随机访问索引（核心：随机访问触发缓存缺失）
    std::vector<size_t> indices(len);
    for (size_t i = 0; i < len; ++i) indices[i] = i;
    std::shuffle(indices.begin(), indices.end(), std::mt19937(42));

    // 随机访问+强制缓存失效
    for (size_t i = 0; i < len; ++i) {
        size_t idx = indices[i];
        p[idx] = p[idx] + 1;
        
        // 正确的类型转换：先移除volatile，再转const void*
        int* addr = const_cast<int*>(&p[idx]); // 移除volatile限定
        _mm_clflush(static_cast<const void*>(addr)); // 转const void*
    }

    // 内存屏障：确保缓存操作完成
    std::atomic_thread_fence(std::memory_order_seq_cst); // C++11+ 跨平台内存屏障
    _mm_sfence(); // 写屏障
    _mm_lfence(); // 读屏障
}

int main() {
    long long fileSizeByte = 640 * 1024 * 1024;
    const std::string filePath = "/root/lmq_openec/build/input_640MB_random";
    FILE* inputfile = fopen(filePath.c_str(), "rb");
    assert(inputfile != NULL && "Failed to open file");
    int k = 10;
    int m = 4;
    int n = 14;
    int w = 8;


    // long long fileSizeByte = 512 * 1024 * 1024;
    // const std::string filePath = "/root/lmq_openec/build/input_512MB_random";
    // FILE* inputfile = fopen(filePath.c_str(), "rb");
    // assert(inputfile != NULL && "Failed to open file");
    // int k = 8;
    // int m = 4;
    // int n = 12;
    // int w = 8;


    // long long fileSizeByte = 384 * 1024 * 1024;
    // const std::string filePath = "/root/lmq_openec/build/input_384MB_random";
    // FILE* inputfile = fopen(filePath.c_str(), "rb");
    // assert(inputfile != NULL && "Failed to open file");
    // int k = 6;
    // int m = 3;
    // int n = 9;
    // int w = 8;
    
    // long long fileSizeByte = 256 * 1024 * 1024;
    // const std::string filePath = "/root/lmq_openec/build/input_256MB_random";
    // FILE* inputfile = fopen(filePath.c_str(), "rb");
    // assert(inputfile != NULL && "Failed to open file");
    // int k = 4;
    // int m = 2;
    // int n = 6;
    // int w = 8;
    int objSizeByte = fileSizeByte / k;
    char** data_ptrs = new char* [k];
    for (int i = 0; i < k; i++) {
        data_ptrs[i] = new char [objSizeByte];
        fread(data_ptrs[i], objSizeByte, 1, inputfile);
    }
    fclose(inputfile);
    char** coding_ptrs = new char* [m];
    for (int i = 0; i < 1; i++) {
        coding_ptrs[i] = new char [objSizeByte];
    }

    // generate encode matrix
    int* matrix = new int [n * k];
    memset(matrix, 0, n * k * sizeof(int));
    for (int i = 0; i < k; i++) {
        matrix[i * k + i] = 1;
    }

    for (int i = 0; i < m; i++) {
        int tmp = 1;
        for (int j = 0; j < k; j++) {
            matrix[(i + k) * k + j] = tmp;
            tmp = galois_single_multiply(tmp, i + 1, w);
        }
    }
    // data_ptrs: k
    // coding_ptrs: m
    // matrix: m * k
               

    int* encodeMatrix;
    timeval start, end;
    double time = 0;

    // int* selectMatrix = new int [k * k];
    // memcpy(selectMatrix + 0 * k, matrix + 0 * k, k * sizeof(int));
    // memcpy(selectMatrix + 1 * k, matrix + 2 * k, k * sizeof(int));
    // memcpy(selectMatrix + 2 * k, matrix + 3 * k, k * sizeof(int));
    // memcpy(selectMatrix + 3 * k, matrix + 4 * k, k * sizeof(int));
    // memcpy(selectMatrix + 4 * k, matrix + 5 * k, k * sizeof(int));
    // memcpy(selectMatrix + 5 * k, matrix + 6 * k, k * sizeof(int));
    // memcpy(selectMatrix + 6 * k, matrix + 7 * k, k * sizeof(int));
    // memcpy(selectMatrix + 7 * k, matrix + 8 * k, k * sizeof(int));
    // memcpy(selectMatrix + 8 * k, matrix + 9 * k, k * sizeof(int));
    // memcpy(selectMatrix + 9 * k, matrix + 14 * k, k * sizeof(int));

    // int* invertMatrix = new int [k * k];
    // jerasure_invert_matrix(selectMatrix, invertMatrix, k, w);
    // int* selectVector = new int [k];
    // memcpy(selectVector, matrix + 1 * k, k * sizeof(int));

    // int* coefVector = jerasure_matrix_multiply(selectVector, invertMatrix, 1, k, k, k, w);

    // data needed to decode
    char* data_ptrs_4_decode[] = {data_ptrs[0], data_ptrs[2], data_ptrs[3], data_ptrs[4], data_ptrs[5], \
                                  data_ptrs[6], data_ptrs[7], data_ptrs[8], data_ptrs[9], coding_ptrs[0]};
    // char* data_ptrs_4_decode[] = {data_ptrs[0], data_ptrs[2], data_ptrs[3], data_ptrs[4], data_ptrs[5], \
    //                               data_ptrs[6], data_ptrs[7], coding_ptrs[0]};
    // char* data_ptrs_4_decode[] = {data_ptrs[0], data_ptrs[2], data_ptrs[3], data_ptrs[4], data_ptrs[5], coding_ptrs[0]};
    // char* data_ptrs_4_decode[] = {data_ptrs[0], data_ptrs[2], data_ptrs[3], coding_ptrs[0]};
    
    
    // decode result
    char** coding_ptrs_4_decode = new char* [1];
    coding_ptrs_4_decode[0] = new char [objSizeByte];

    // immediate result
    char* coding_ptrs_tmp_0 = new char [objSizeByte];
    char* coding_ptrs_tmp_1 = new char [objSizeByte];

    time = 0;
    int item_num = 10;
{
    char** data_ptrs = (char**)malloc(k * sizeof(char*));
    char** coding_ptrs = (char**)malloc(m * sizeof(char*));
    for (int i = 0; i < k; i++) {
        data_ptrs[i] = (char*)malloc(objSizeByte * sizeof(char));
        memcpy(data_ptrs[i], data_ptrs_4_decode[i], objSizeByte);
    }
    for (int i = 0; i < m; i++) {
        coding_ptrs[i] = (char*)malloc(objSizeByte * sizeof(char));
    }


    int matrix_tmp_5[] = {139, 22, 88, 125, 233, 131, 54, 216, 71, 139};

    // reuse
    for (int slice_id = 0; slice_id < 16; slice_id++) {
        for (int i = 0; i < k; i++) {
            memcpy(data_ptrs[i], data_ptrs_4_decode[i] + objSizeByte / 16 * slice_id, objSizeByte / 16);
        }

        for (int iter = 0; iter < item_num; iter++) {            
            // decode
            gettimeofday(&start, NULL);

            jerasure_matrix_encode(k, 1, w, matrix_tmp_5, data_ptrs, coding_ptrs, objSizeByte / 16);
            printf("done\n");

            gettimeofday(&end, NULL);
            time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
        }
    }
    
    // independently
    // for (int iter = 0; iter < item_num; iter++) {
    //     flush_cpu_cache();

    //     for (int slice_id = 0; slice_id < 16; slice_id++) {
    //         for (int i = 0; i < k; i++) {
    //             memcpy(data_ptrs[i], data_ptrs_4_decode[i] + objSizeByte / 16 * slice_id, objSizeByte / 16);
    //         }

    //         gettimeofday(&start, NULL);
            
    //         // decode
    //         jerasure_matrix_encode(k, 1, w, matrix_tmp_5, data_ptrs, coding_ptrs, objSizeByte / 16);

    //         gettimeofday(&end, NULL);
    //         time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
    //         printf("done\n");
    //     }
    // }

    // memcpy(coding_ptrs_4_decode[0], coding_ptrs[0], objSizeByte);        

    for (int i = 0; i < k; i++) {
        free(data_ptrs[i]);
    }
    free(data_ptrs);
    for (int i = 0; i < 1; i++) {
        free(coding_ptrs[i]);
    }
    free(coding_ptrs);        
}

    // printf("total time: %f ms\n", time);
    printf("Time: %f ms, Data: %d MB\n", time / item_num, objSizeByte * k / 1024 / 1024);
    return 0;
}
