#include "util/jerasure.h"
#include "inc/include.hh"

int main() {
    int fileSizeByte = 16 * 1024 * 1024;
    const std::string filePath = "/home/openec/lmq_openec/build/input_16MB_random";
    FILE* inputfile = fopen(filePath.c_str(), "rb");
    assert(inputfile != NULL && "Failed to open file");
    int k = 4;
    int m = 2;
    int n = 6;
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


    // encode 0
    int* encodeMatrix = matrix + k * k;
    // jerasure_matrix_encode(k, 1, w, encodeMatrix, data_ptrs, coding_ptrs, objSizeByte);
    // printf("print encode matrix\n");
    // for (int i = 0; i < k; i++) {
    //     printf("%d ", encodeMatrix[i]);
    // }
    // printf("\n");

    
    // encode 1
    encodeMatrix = matrix + (k + 1) * k;
    jerasure_matrix_encode(k, 1, w, encodeMatrix, data_ptrs, coding_ptrs, objSizeByte);
    printf("print encode matrix\n");
    for (int i = 0; i < k; i++) {
        printf("%d ", encodeMatrix[i]);
    }
    printf("\n");
    
    // ================================================================
    // decode for k - 1 erasure
    int* selectMatrix = new int [k * k];
    memcpy(selectMatrix + 0 * k, matrix + 0 * k, k * sizeof(int));
    memcpy(selectMatrix + 1 * k, matrix + 2 * k, k * sizeof(int));
    memcpy(selectMatrix + 2 * k, matrix + 3 * k, k * sizeof(int));
    memcpy(selectMatrix + 3 * k, matrix + 5 * k, k * sizeof(int));

    // copy 0 ... k - 2
    // for (int i = 0; i < k - 1; i++) {
    //     memcpy(selectMatrix + i * k, matrix + i * k, k * sizeof(int));
    // }
    // copy k
    // memcpy(selectMatrix + (k - 1) * k, matrix + k * k, k * sizeof(int));
    // memcpy(selectMatrix + (k - 1) * k, matrix + (k + 1) * k, k * sizeof(int));
    printf("print select matrix\n");
    for (int i = 0; i < k; i++) {
        for (int j = 0; j < k; j++) {
            printf("%d ", selectMatrix[i * k + j]);
        }
        printf("\n");
    }
    int* invertMatrix = new int [k * k];
    jerasure_invert_matrix(selectMatrix, invertMatrix, k, w);
    printf("print invert matrix\n");
    for (int i = 0; i < k; i++) {
        for (int j = 0; j < k; j++) {
            printf("%d ", invertMatrix[i * k + j]);
        }
        printf("\n");
    }
    // copy k - 1
    int* selectVector = new int [k];
    memcpy(selectVector, matrix + 1 * k, k * sizeof(int));

    // memcpy(selectVector, matrix + (k - 1) * k, k * sizeof(int));

    int* coefVector = jerasure_matrix_multiply(selectVector, invertMatrix, 1, k, k, k, w);
    printf("print coefVector\n");
    for (int i = 0; i < k; i++) {
        printf("%d ", coefVector[i]);
    }
    printf("\n");
   
    // data needed to decode
    char* data_ptrs_4_decode[] = {data_ptrs[0], data_ptrs[2], data_ptrs[3], coding_ptrs[0]};
    // decode result
    char** coding_ptrs_4_decode = new char* [1];
    coding_ptrs_4_decode[0] = new char [objSizeByte];

    // immediate result
    char* coding_ptrs_tmp_0 = new char [objSizeByte];
    char* coding_ptrs_tmp_1 = new char [objSizeByte];

    int matrix_tmp_0[] = {142, 2};
    int matrix_tmp_1[] = {4, 142};
    int matrix_tmp_2[] = {1, 1};

    jerasure_matrix_encode(2, 1, w, matrix_tmp_0, data_ptrs_4_decode, coding_ptrs_4_decode, objSizeByte);
    memcpy(coding_ptrs_tmp_0, coding_ptrs_4_decode[0], objSizeByte);
    std::cout << "decode 0" << std::endl;
    jerasure_matrix_encode(2, 1, w, matrix_tmp_1, data_ptrs_4_decode + 2, coding_ptrs_4_decode, objSizeByte);
    memcpy(coding_ptrs_tmp_1, coding_ptrs_4_decode[0], objSizeByte);
    std::cout << "decode 1" << std::endl;
    char* coding_ptrs_4_decode_tmp[] = {coding_ptrs_tmp_0, coding_ptrs_tmp_1};
    jerasure_matrix_encode(2, 1, w, matrix_tmp_2, coding_ptrs_4_decode_tmp, coding_ptrs_4_decode, objSizeByte);
    std::cout << "decode 2" << std::endl;
    // compare
    for (int i = 0; i < objSizeByte; i++) {
        if (coding_ptrs_4_decode[0][i] != data_ptrs[1][i]) {
            printf("conv decode error\n");
            break;
        }
    }
    return 0;
}