#include "util/jerasure.h"
#include "inc/include.hh"
#include <cassert>
#include <cstdlib>
#include <ctime>

int main() {
    // 纠删码参数（保持固定，仅改变数据块大小）
    int k = 6;          // 数据块数量
    int m = 3;          // 校验块数量
    int n = k + m;      // 总块数
    int w = 8;          // GF(2^w)域

    // 数据块大小范围：100MB到1000MB，步长100MB
    int startSizeMB = 700;
    int endSizeMB = 1000;
    int stepSizeMB = 100;

    // 初始化随机数种子
    srand(time(nullptr));

    // 遍历不同数据块大小
    for (int sizeMB = startSizeMB; sizeMB <= endSizeMB; sizeMB += stepSizeMB) {
        long long objSizeByte = sizeMB * 1024LL * 1024;  // 转换为字节数
        printf("\n===== 数据块大小: %d MB (%lld 字节) =====\n", sizeMB, objSizeByte);

        // 1. 分配并填充随机数据块
        char** data_ptrs = new char*[k];
        for (int i = 0; i < k; i++) {
            data_ptrs[i] = new char[objSizeByte];
            assert(data_ptrs[i] != nullptr && "数据块内存分配失败");
            // 填充随机字节
            for (long long j = 0; j < objSizeByte; j++) {
                data_ptrs[i][j] = static_cast<char>(rand() % 256);  // 0-255随机值
            }
        }

        // 2. 分配编码块内存
        char** coding_ptrs = new char*[m];
        for (int i = 0; i < m; i++) {
            coding_ptrs[i] = new char[objSizeByte];
            assert(coding_ptrs[i] != nullptr && "编码块内存分配失败");
        }

        // 3. 生成编码矩阵（与原程序保持一致）
        int* matrix = new int[n * k];
        memset(matrix, 0, n * k * sizeof(int));
        // 单位矩阵部分（数据块对应行）
        for (int i = 0; i < k; i++) {
            matrix[i * k + i] = 1;
        }
        // 校验块对应行（生成器矩阵）
        for (int i = 0; i < m; i++) {
            int tmp = 1;
            for (int j = 0; j < k; j++) {
                matrix[(i + k) * k + j] = tmp;
                tmp = galois_single_multiply(tmp, i + 1, w);
            }
        }

        // 4. 编码运算时间测量
        int* encodeMatrix = matrix + (k + 2) * k;  // 选择编码矩阵行
        timeval start, end;
        double avgTime;

        // 多次运行取平均值（减少误差）
        avgTime = 0.0;
        for (int i = 0; i < 10; i++) {
            gettimeofday(&start, nullptr);
            jerasure_matrix_encode(k, 1, w, encodeMatrix, data_ptrs, coding_ptrs, objSizeByte);
            gettimeofday(&end, nullptr);
            avgTime += (end.tv_sec - start.tv_sec) * 1000.0 + 
                      (end.tv_usec - start.tv_usec) / 1000.0;
        }
        avgTime /= 10.0;
        printf("编码运算 (k=%d) 平均时间: %.2f ms\n", k, avgTime);

        // 5. 解码运算时间测量（模拟单块丢失）
        // 5.1 构建选择矩阵和逆矩阵
        int* selectMatrix = new int[k * k];
        memcpy(selectMatrix + 0 * k, matrix + 0 * k, k * sizeof(int));
        memcpy(selectMatrix + 1 * k, matrix + 2 * k, k * sizeof(int));
        memcpy(selectMatrix + 2 * k, matrix + 3 * k, k * sizeof(int));
        memcpy(selectMatrix + 3 * k, matrix + 4 * k, k * sizeof(int));
        memcpy(selectMatrix + 4 * k, matrix + 5 * k, k * sizeof(int));
        memcpy(selectMatrix + 5 * k, matrix + 8 * k, k * sizeof(int));

        int* invertMatrix = new int[k * k];
        jerasure_invert_matrix(selectMatrix, invertMatrix, k, w);

        // 5.2 构建系数向量
        int* selectVector = new int[k];
        memcpy(selectVector, matrix + 5 * k, k * sizeof(int));
        int* coefVector = jerasure_matrix_multiply(selectVector, invertMatrix, 1, k, k, k, w);

        // 5.3 准备解码输入数据（模拟丢失data_ptrs[1]）
        char* data_ptrs_4_decode[] = {data_ptrs[0], data_ptrs[2], data_ptrs[3], 
                                     data_ptrs[4], data_ptrs[5], coding_ptrs[0]};
        char** coding_ptrs_4_decode = new char*[1];
        coding_ptrs_4_decode[0] = new char[objSizeByte];
        assert(coding_ptrs_4_decode[0] != nullptr && "解码输出块内存分配失败");

        // 临时缓冲区
        char* coding_ptrs_tmp_0 = new char[objSizeByte];
        char* coding_ptrs_tmp_1 = new char[objSizeByte];
        assert(coding_ptrs_tmp_0 != nullptr && coding_ptrs_tmp_1 != nullptr);

        // 5.4 不同乘法因子的解码时间测量
        int matrix_tmp_5[] = {244, 3, 5, 15, 17, 244};  // 6元素矩阵
        avgTime = 0.0;
        for (int i = 0; i < 10; i++) {
            gettimeofday(&start, nullptr);
            jerasure_matrix_encode(6, 1, w, matrix_tmp_5, data_ptrs_4_decode, coding_ptrs_4_decode, objSizeByte);
            gettimeofday(&end, nullptr);
            avgTime += (end.tv_sec - start.tv_sec) * 1000.0 + 
                      (end.tv_usec - start.tv_usec) / 1000.0;
        }
        avgTime /= 10.0;
        printf("解码运算 (multy=6) 平均时间: %.2f ms\n", avgTime);

        int matrix_tmp_4[] = {244, 3, 5, 15, 17};  // 5元素矩阵
        avgTime = 0.0;
        for (int i = 0; i < 10; i++) {
            gettimeofday(&start, nullptr);
            jerasure_matrix_encode(5, 1, w, matrix_tmp_4, data_ptrs_4_decode, coding_ptrs_4_decode, objSizeByte);
            gettimeofday(&end, nullptr);
            avgTime += (end.tv_sec - start.tv_sec) * 1000.0 + 
                      (end.tv_usec - start.tv_usec) / 1000.0;
        }
        avgTime /= 10.0;
        printf("解码运算 (multy=5) 平均时间: %.2f ms\n", avgTime);

        int matrix_tmp_3[] = {244, 3, 5, 15};  // 4元素矩阵
        avgTime = 0.0;
        for (int i = 0; i < 10; i++) {
            gettimeofday(&start, nullptr);
            jerasure_matrix_encode(4, 1, w, matrix_tmp_3, data_ptrs_4_decode, coding_ptrs_4_decode, objSizeByte);
            gettimeofday(&end, nullptr);
            avgTime += (end.tv_sec - start.tv_sec) * 1000.0 + 
                      (end.tv_usec - start.tv_usec) / 1000.0;
        }
        avgTime /= 10.0;
        printf("解码运算 (multy=4) 平均时间: %.2f ms\n", avgTime);

        int matrix_tmp_0[] = {244, 3, 5};  // 3元素矩阵
        avgTime = 0.0;
        for (int i = 0; i < 10; i++) {
            gettimeofday(&start, nullptr);
            jerasure_matrix_encode(3, 1, w, matrix_tmp_0, data_ptrs_4_decode, coding_ptrs_4_decode, objSizeByte);
            gettimeofday(&end, nullptr);
            avgTime += (end.tv_sec - start.tv_sec) * 1000.0 + 
                      (end.tv_usec - start.tv_usec) / 1000.0;
        }
        avgTime /= 10.0;
        printf("解码运算 (multy=3) 平均时间: %.2f ms\n", avgTime);
        memcpy(coding_ptrs_tmp_0, coding_ptrs_4_decode[0], objSizeByte);

        int matrix_tmp_1[] = {15, 17, 244};  // 3元素矩阵
        avgTime = 0.0;
        for (int i = 0; i < 10; i++) {
            gettimeofday(&start, nullptr);
            jerasure_matrix_encode(3, 1, w, matrix_tmp_1, data_ptrs_4_decode + 3, coding_ptrs_4_decode, objSizeByte);
            gettimeofday(&end, nullptr);
            avgTime += (end.tv_sec - start.tv_sec) * 1000.0 + 
                      (end.tv_usec - start.tv_usec) / 1000.0;
        }
        avgTime /= 10.0;
        printf("解码运算 (multy=3) 平均时间: %.2f ms\n", avgTime);
        memcpy(coding_ptrs_tmp_1, coding_ptrs_4_decode[0], objSizeByte);

        int matrix_tmp_2[] = {1, 1};  // 2元素矩阵
        char* coding_ptrs_4_decode_tmp[] = {coding_ptrs_tmp_0, coding_ptrs_tmp_1};
        avgTime = 0.0;
        for (int i = 0; i < 10; i++) {
            gettimeofday(&start, nullptr);
            jerasure_matrix_encode(2, 1, w, matrix_tmp_2, coding_ptrs_4_decode_tmp, coding_ptrs_4_decode, objSizeByte);
            gettimeofday(&end, nullptr);
            avgTime += (end.tv_sec - start.tv_sec) * 1000.0 + 
                      (end.tv_usec - start.tv_usec) / 1000.0;
        }
        avgTime /= 10.0;
        printf("解码运算 (multy=2) 平均时间: %.2f ms\n", avgTime);

        // 6. 验证解码正确性
        bool decodeSuccess = true;
        for (long long i = 0; i < objSizeByte; i++) {
            if (coding_ptrs_4_decode[0][i] != data_ptrs[1][i]) {
                decodeSuccess = false;
                break;
            }
        }
        printf("解码结果验证: %s\n", decodeSuccess ? "成功" : "失败");

        // 7. 释放当前数据块大小对应的内存
        for (int i = 0; i < k; i++) delete[] data_ptrs[i];
        delete[] data_ptrs;

        for (int i = 0; i < m; i++) delete[] coding_ptrs[i];
        delete[] coding_ptrs;

        delete[] matrix;
        delete[] selectMatrix;
        delete[] invertMatrix;
        delete[] selectVector;
        free(coefVector);  // jerasure内部用malloc分配，需用free释放

        delete[] coding_ptrs_4_decode[0];
        delete[] coding_ptrs_4_decode;
        delete[] coding_ptrs_tmp_0;
        delete[] coding_ptrs_tmp_1;
    }

    return 0;
}