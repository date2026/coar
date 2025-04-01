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


    // write data file
    for (int i = 0; i < k; i++) {
        std::string dataFileName = filePath + "_data_" + std::to_string(i);
        FILE* dataFile = fopen(dataFileName.c_str(), "wb+");
        assert(dataFile != NULL && "Failed to open file");
        fwrite(data_ptrs[i], objSizeByte, 1, dataFile);
        fclose(dataFile);
    }

    // encode 0
    int* encodeMatrix = matrix + k * k;
    jerasure_matrix_encode(k, 1, w, encodeMatrix, data_ptrs, coding_ptrs, objSizeByte);
    printf("print encode matrix\n");
    for (int i = 0; i < k; i++) {
        printf("%d ", encodeMatrix[i]);
    }
    printf("\n");
    std::string codingFileName = filePath + "_encode_0";
    FILE* codingFile = fopen(codingFileName.c_str(), "wb+");
    assert(codingFile != NULL && "Failed to open file");
    fwrite(coding_ptrs[0], objSizeByte, 1, codingFile);
    fclose(codingFile);
    
    // encode 1
    // encodeMatrix = matrix + (k + 1) * k;
    // jerasure_matrix_encode(k, 1, w, encodeMatrix, data_ptrs, coding_ptrs, objSizeByte);
    // printf("print encode matrix\n");
    // for (int i = 0; i < k; i++) {
    //     printf("%d ", encodeMatrix[i]);
    // }
    // printf("\n");
    // codingFileName = filePath + "_encode_1";
    // codingFile = fopen(codingFileName.c_str(), "wb+");
    // assert(codingFile != NULL && "Failed to open file");
    // fwrite(coding_ptrs[0], objSizeByte, 1, codingFile);
    // fclose(codingFile);
    
    // ================================================================
    int* selectMatrix = new int [k * k];
    // copy 0 ... k - 2
    for (int i = 0; i < k - 1; i++) {
        memcpy(selectMatrix + i * k, matrix + i * k, k * sizeof(int));
    }
    // copy k
    memcpy(selectMatrix + (k - 1) * k, matrix + k * k, k * sizeof(int));
    printf("print select matrix\n");
    for (int i = 0; i < k; i++) {
        for (int j = 0; j < k; j++) {
            printf("%d ", selectMatrix[i * k + j]);
        }
        printf("\n");
    }

    int* invertMatrix = new int [k * k];
    jerasure_invert_matrix(selectMatrix, invertMatrix, k, w);
    // copy k - 1
    int* selectVector = new int [k];
    memcpy(selectVector, matrix + (k - 1) * k, k * sizeof(int));
    printf("print selectVector\n");
    for (int i = 0; i < k; i++) {
        printf("%d ", selectVector[i]);
    }
    printf("\n");
    int* coefVector = jerasure_matrix_multiply(selectVector, invertMatrix, 1, k, k, k, w);
    printf("print coefVector\n");
    for (int i = 0; i < k; i++) {
        printf("%d ", coefVector[i]);
    }
    printf("\n");
    char** data_ptrs_4_decode = new char* [k];
    // copy data
    for (int i = 0; i < k - 1; i++) {
        data_ptrs_4_decode[i] = new char [objSizeByte];
        memcpy(data_ptrs_4_decode[i], data_ptrs[i], objSizeByte);
    }
    data_ptrs_4_decode[k - 1] = new char [objSizeByte];
    memcpy(data_ptrs_4_decode[k - 1], coding_ptrs[0], objSizeByte);

    char** coding_ptrs_4_decode = new char* [1];
    coding_ptrs_4_decode[0] = new char [objSizeByte];

    // decode
    jerasure_matrix_encode(k, 1, w, coefVector, data_ptrs_4_decode, coding_ptrs_4_decode, objSizeByte);
    codingFileName = filePath + "_decode";
    codingFile = fopen(codingFileName.c_str(), "wb+");
    assert(codingFile != NULL && "Failed to open file");
    fwrite(coding_ptrs_4_decode[0], objSizeByte, 1, codingFile);
    fclose(codingFile);

 


    // clear
    for (int i = 0; i < k; i++) {
        delete [] data_ptrs[i];
    }
    delete [] data_ptrs;
    for (int i = 0; i < m; i++) {
        delete [] coding_ptrs[i];
    }
    delete [] coding_ptrs;
    delete [] matrix;



}