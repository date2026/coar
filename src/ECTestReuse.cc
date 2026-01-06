#include "util/jerasure.h"
#include "inc/include.hh"

const size_t LLC_SIZE_THRESHOLD = 100 * 1024 * 1024; // 100 MB

void flush_cpu_cache() {

    std::vector<int> dummy_data(LLC_SIZE_THRESHOLD / sizeof(int), 1);

    
    volatile int* p = dummy_data.data();
    size_t len = dummy_data.size();

    for (size_t i = 0; i < len; ++i) {
        p[i] = p[i] + 1;
    }
    __sync_synchronize(); 
}

int main() {
    long long fileSizeByte = 512 * 1024 * 1024;
    const std::string filePath = "/root/lmq_openec/build/input_512MB_random";
    FILE* inputfile = fopen(filePath.c_str(), "rb");
    assert(inputfile != NULL && "Failed to open file");
    int k = 8;
    int m = 4;
    int n = 12;
    int w = 8;
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

    int* selectMatrix = new int [k * k];
    memcpy(selectMatrix + 0 * k, matrix + 0 * k, k * sizeof(int));
    memcpy(selectMatrix + 1 * k, matrix + 2 * k, k * sizeof(int));
    memcpy(selectMatrix + 2 * k, matrix + 3 * k, k * sizeof(int));
    memcpy(selectMatrix + 3 * k, matrix + 4 * k, k * sizeof(int));
    memcpy(selectMatrix + 4 * k, matrix + 5 * k, k * sizeof(int));
    memcpy(selectMatrix + 5 * k, matrix + 6 * k, k * sizeof(int));
    memcpy(selectMatrix + 6 * k, matrix + 7 * k, k * sizeof(int));
    memcpy(selectMatrix + 7 * k, matrix + 11 * k, k * sizeof(int));

    int* invertMatrix = new int [k * k];
    jerasure_invert_matrix(selectMatrix, invertMatrix, k, w);
    int* selectVector = new int [k];
    memcpy(selectVector, matrix + 1 * k, k * sizeof(int));


    int* coefVector = jerasure_matrix_multiply(selectVector, invertMatrix, 1, k, k, k, w);

    // data needed to decode
    char* data_ptrs_4_decode[] = {data_ptrs[0], data_ptrs[2], data_ptrs[3], data_ptrs[4], \
                                  data_ptrs[5], data_ptrs[6], data_ptrs[7], coding_ptrs[0]};
    // decode result
    char** coding_ptrs_4_decode = new char* [1];
    coding_ptrs_4_decode[0] = new char [objSizeByte];

    // immediate result
    char* coding_ptrs_tmp_0 = new char [objSizeByte];
    char* coding_ptrs_tmp_1 = new char [objSizeByte];



    int matrix_tmp_5[] = {239, 127, 73, 136, 36, 185, 121, 81};
    time = 0;
    int item_num = 10;

    int* bit_matrix = jerasure_matrix_to_bitmatrix(k, 1, w, matrix_tmp_5);
    int** schedule = jerasure_smart_bitmatrix_to_schedule(k, 1, w, bit_matrix);


    for (int iter = 0; iter < item_num; iter++) {
        char** data_ptrs = (char**)malloc(k * sizeof(char*));
        char** coding_ptrs = (char**)malloc(1 * sizeof(char*));
        for (int i = 0; i < k; i++) {
            data_ptrs[i] = (char*)malloc(objSizeByte * sizeof(char));
            memcpy(data_ptrs[i], data_ptrs_4_decode[i], objSizeByte);
        }
        coding_ptrs[0] = (char*)malloc(objSizeByte * sizeof(char));
        
        // clear cache
        // flush_cpu_cache();
        gettimeofday(&start, NULL);

        // generate auxiliary data
        // int* bit_matrix = jerasure_matrix_to_bitmatrix(k, 1, w, matrix_tmp_5);
        // int** schedule = jerasure_smart_bitmatrix_to_schedule(k, 1, w, bit_matrix);

        // decode
        jerasure_schedule_encode(k, 1, w, schedule, data_ptrs, coding_ptrs, objSizeByte, objSizeByte/w);

        // clear auxiliary data
        // free(bit_matrix);
        // jerasure_free_schedule(schedule);

        // jerasure_matrix_encode(k, 1, w, matrix_tmp_5, data_ptrs, coding_ptrs, objSizeByte);

        memcpy(coding_ptrs_4_decode[0], coding_ptrs[0], objSizeByte);        
        gettimeofday(&end, NULL);
        time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
    
        for (int i = 0; i < k; i++) {
            free(data_ptrs[i]);
        }
        free(data_ptrs);
        for (int i = 0; i < 1; i++) {
            free(coding_ptrs[i]);
        }
        free(coding_ptrs);        
    }
    printf("Time: %f s, Data: %d MB\n", time / item_num / 1000, objSizeByte * k / 1024 / 1024);
    return 0;
}
