#include "util/jerasure.h"
#include "inc/include.hh"

int main() {
    // long long fileSizeByte = 1536 * 1024 * 1024;
    // long long fileSizeByte = 3221225472;
    long long fileSizeByte = 6442450944;
    const std::string filePath = "/root/coar/build/input_6144MB_random";
    FILE* inputfile = fopen(filePath.c_str(), "rb");
    assert(inputfile != NULL && "Failed to open file");
    int k = 6;
    int m = 3;
    int n = 9;
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


    // encode 2
    int* encodeMatrix = matrix + (k + 2) * k;
    timeval start, end;
    double time = 0;

    // for (int i = 0; i < 10; i++) {
    //     gettimeofday(&start, NULL);
    //     jerasure_matrix_encode(2, 1, w, encodeMatrix, data_ptrs, coding_ptrs, objSizeByte);
    //     gettimeofday(&end, NULL);
    //     time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
    // }
    // printf("size: %d, multy: 2, time: %f ms\n", objSizeByte, time / 10.0);
// #define THDS_NUM 2

//     std::thread thds[THDS_NUM];

//     for (int thd_id = 0; thd_id < THDS_NUM; thd_id++) {
//         thds[thd_id] = std::thread([=](){
//             timeval start, end;
//             double time = 0.0;
//             for (int i = 0; i < 10; i++) {
//                 gettimeofday(&start, NULL);
//                 jerasure_matrix_encode(6, 1, w, encodeMatrix, data_ptrs, coding_ptrs, objSizeByte);
//                 gettimeofday(&end, NULL);
//                 time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
//             }
//             printf("size: %d, multy: 6, time: %f ms\n", objSizeByte, time / 10.0);
//         });
//     }
    
//     for (int thd_id = 0; thd_id < THDS_NUM; thd_id++) {
//         thds[thd_id].join();
//     }
    
//     for (int thd_id = 0; thd_id < THDS_NUM; thd_id++) {
//         thds[thd_id] = std::thread([=](){
//             timeval start, end;
//             double time = 0.0;
//             for (int i = 0; i < 10; i++) {
//                 gettimeofday(&start, NULL);
//                 jerasure_matrix_encode(5, 1, w, encodeMatrix, data_ptrs, coding_ptrs, objSizeByte);
//                 gettimeofday(&end, NULL);
//                 time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
//             }
//             printf("size: %d, multy: 5, time: %f ms\n", objSizeByte, time / 10.0);
//         });
//     }
    
//     for (int thd_id = 0; thd_id < THDS_NUM; thd_id++) {
//         thds[thd_id].join();
//     }


//     for (int thd_id = 0; thd_id < THDS_NUM; thd_id++) {
//         thds[thd_id] = std::thread([=](){
//             timeval start, end;
//             double time = 0.0;
//             for (int i = 0; i < 10; i++) {
//                 gettimeofday(&start, NULL);
//                 jerasure_matrix_encode(4, 1, w, encodeMatrix, data_ptrs, coding_ptrs, objSizeByte);
//                 gettimeofday(&end, NULL);
//                 time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
//             }
//             printf("size: %d, multy: 4, time: %f ms\n", objSizeByte, time / 10.0);
//         });
//     }
    
//     for (int thd_id = 0; thd_id < THDS_NUM; thd_id++) {
//         thds[thd_id].join();
//     }


//     for (int thd_id = 0; thd_id < THDS_NUM; thd_id++) {
//         thds[thd_id] = std::thread([=](){
//             timeval start, end;
//             double time = 0.0;
//             for (int i = 0; i < 10; i++) {
//                 gettimeofday(&start, NULL);
//                 jerasure_matrix_encode(3, 1, w, encodeMatrix, data_ptrs, coding_ptrs, objSizeByte);
//                 gettimeofday(&end, NULL);
//                 time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
//             }
//             printf("size: %d, multy: 3, time: %f ms\n", objSizeByte, time / 10.0);
//         });
//     }
    
//     for (int thd_id = 0; thd_id < THDS_NUM; thd_id++) {
//         thds[thd_id].join();
//     }



//     for (int thd_id = 0; thd_id < THDS_NUM; thd_id++) {
//         thds[thd_id] = std::thread([=](){
//             timeval start, end;
//             double time = 0.0;
//             for (int i = 0; i < 10; i++) {
//                 gettimeofday(&start, NULL);
//                 jerasure_matrix_encode(2, 1, w, encodeMatrix, data_ptrs, coding_ptrs, objSizeByte);
//                 gettimeofday(&end, NULL);
//                 time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
//             }
//             printf("size: %d, multy: 2, time: %f ms\n", objSizeByte, time / 10.0);
//         });
//     }
    
//     for (int thd_id = 0; thd_id < THDS_NUM; thd_id++) {
//         thds[thd_id].join();
//     }



/************************************************************************************************************ */

    encodeMatrix = matrix + (k + 2) * k;
    time = 0.0;
    for (int i = 0; i < 10; i++) {
        gettimeofday(&start, NULL);
        jerasure_matrix_encode(k, 1, w, encodeMatrix, data_ptrs, coding_ptrs, objSizeByte);
        gettimeofday(&end, NULL);
        time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
    }
    printf("size: %d, multy: 6, time: %f ms\n", objSizeByte, time / 10.0);


    // printf("print encode matrix\n");
    // for (int i = 0; i < k; i++) {
    //     printf("%d ", encodeMatrix[i]);
    // }
    // printf("\n");

    // ================================================================
    // decode for k - 1 erasure
    int* selectMatrix = new int [k * k];
    memcpy(selectMatrix + 0 * k, matrix + 0 * k, k * sizeof(int));
    memcpy(selectMatrix + 1 * k, matrix + 2 * k, k * sizeof(int));
    memcpy(selectMatrix + 2 * k, matrix + 3 * k, k * sizeof(int));
    memcpy(selectMatrix + 3 * k, matrix + 4 * k, k * sizeof(int));
    memcpy(selectMatrix + 4 * k, matrix + 5 * k, k * sizeof(int));
    memcpy(selectMatrix + 5 * k, matrix + 8 * k, k * sizeof(int));

    // printf("print select matrix\n");
    // for (int i = 0; i < k; i++) {
    //     for (int j = 0; j < k; j++) {
    //         printf("%d ", selectMatrix[i * k + j]);
    //     }
    //     printf("\n");
    // }
    int* invertMatrix = new int [k * k];
    jerasure_invert_matrix(selectMatrix, invertMatrix, k, w);
    // printf("print invert matrix\n");
    // for (int i = 0; i < k; i++) {
    //     for (int j = 0; j < k; j++) {
    //         printf("%d ", invertMatrix[i * k + j]);
    //     }
    //     printf("\n");
    // }
    // copy k - 1
    int* selectVector = new int [k];
    memcpy(selectVector, matrix + 5 * k, k * sizeof(int));

    // memcpy(selectVector, matrix + (k - 1) * k, k * sizeof(int));

    int* coefVector = jerasure_matrix_multiply(selectVector, invertMatrix, 1, k, k, k, w);
    // printf("print coefVector\n");
    // for (int i = 0; i < k; i++) {
    //     printf("%d ", coefVector[i]);
    // }
    // printf("\n");

    // data needed to decode
    char* data_ptrs_4_decode[] = {data_ptrs[0], data_ptrs[2], data_ptrs[3], data_ptrs[4], data_ptrs[5], coding_ptrs[0]};
    // decode result
    char** coding_ptrs_4_decode = new char* [1];
    coding_ptrs_4_decode[0] = new char [objSizeByte];

    // immediate result
    char* coding_ptrs_tmp_0 = new char [objSizeByte];
    char* coding_ptrs_tmp_1 = new char [objSizeByte];

    int matrix_tmp_0[] = {244, 3, 5};
    int matrix_tmp_1[] = {15, 17, 244};
    int matrix_tmp_2[] = {1, 1};

    // time = 0;
    // for (int i = 0; i < 10; i++) {
    //     gettimeofday(&start, NULL);
    //     jerasure_matrix_encode(2, 1, w, matrix_tmp_0, data_ptrs_4_decode, coding_ptrs_4_decode, objSizeByte);
    //     gettimeofday(&end, NULL);
    //     time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
    // }
    // printf("size: %d, multy: 2, time: %f ms\n", objSizeByte, time / 10.0);

    int matrix_tmp_5[] = {244, 3, 5, 15, 17, 244};
    time = 0;
    for (int i = 0; i < 10; i++) {
        gettimeofday(&start, NULL);
        jerasure_matrix_encode(6, 1, w, matrix_tmp_5, data_ptrs_4_decode, coding_ptrs_4_decode, objSizeByte);
        gettimeofday(&end, NULL);
        time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
    }
    printf("size: %d, multy: 6, time: %f ms\n", objSizeByte, time / 10.0);


    int matrix_tmp_4[] = {244, 3, 5, 15, 17};
    time = 0;
    for (int i = 0; i < 10; i++) {
        gettimeofday(&start, NULL);
        jerasure_matrix_encode(5, 1, w, matrix_tmp_4, data_ptrs_4_decode, coding_ptrs_4_decode, objSizeByte);
        gettimeofday(&end, NULL);
        time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
    }
    printf("size: %d, multy: 5, time: %f ms\n", objSizeByte, time / 10.0);
    
    
    int matrix_tmp_3[] = {244, 3, 5, 15};
    time = 0;
    for (int i = 0; i < 10; i++) {
        gettimeofday(&start, NULL);
        jerasure_matrix_encode(4, 1, w, matrix_tmp_3, data_ptrs_4_decode, coding_ptrs_4_decode, objSizeByte);
        gettimeofday(&end, NULL);
        time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
    }
    printf("size: %d, multy: 4, time: %f ms\n", objSizeByte, time / 10.0);


    time = 0;
    for (int i = 0; i < 10; i++) {
        gettimeofday(&start, NULL);
        jerasure_matrix_encode(3, 1, w, matrix_tmp_0, data_ptrs_4_decode, coding_ptrs_4_decode, objSizeByte);
        gettimeofday(&end, NULL);
        time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
    }
    printf("size: %d, multy: 3, time: %f ms\n", objSizeByte, time / 10.0);
    memcpy(coding_ptrs_tmp_0, coding_ptrs_4_decode[0], objSizeByte);

    time = 0;
    for (int i = 0; i < 10; i++) {
        gettimeofday(&start, NULL);
        jerasure_matrix_encode(3, 1, w, matrix_tmp_1, data_ptrs_4_decode + 3, coding_ptrs_4_decode, objSizeByte);
        gettimeofday(&end, NULL);
        time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
    }
    printf("size: %d, multy: 3, time: %f ms\n", objSizeByte, time / 10.0);
    memcpy(coding_ptrs_tmp_1, coding_ptrs_4_decode[0], objSizeByte);

    char* coding_ptrs_4_decode_tmp[] = {coding_ptrs_tmp_0, coding_ptrs_tmp_1};
    time = 0;
    for (int i = 0; i < 10; i++) {
        gettimeofday(&start, NULL);
        jerasure_matrix_encode(2, 1, w, matrix_tmp_2, coding_ptrs_4_decode_tmp, coding_ptrs_4_decode, objSizeByte);
        gettimeofday(&end, NULL);
        time += (end.tv_sec - start.tv_sec) * 1000.0 + (end.tv_usec - start.tv_usec) / 1000.0;
    }
    printf("size: %d, multy: 2, time: %f ms\n", objSizeByte, time / 10.0);

    // compare
    for (int i = 0; i < objSizeByte; i++) {
        if (coding_ptrs_4_decode[0][i] != data_ptrs[1][i]) {
            printf("conv decode error\n");
            break;
        }
    }
    return 0;
}
