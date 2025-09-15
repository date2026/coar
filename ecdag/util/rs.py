from pyfinite import ffield

class GFManager:
    def __init__(self, w):
        self.w = w
        self.gf = ffield.FField(w)  # 初始化伽罗华域
    
    def add(self, a, b):
        return a ^ b  # 加法即异或
    
    def multiply(self, a, b):
        return self.gf.Multiply(a, b)
    
    def inverse(self, a):
        if a == 0:
            raise ValueError("Zero has no inverse")
        return self.gf.Inverse(a)

def invert_matrix_gf(matrix, k, gf_manager):
    # 构造增广矩阵 [matrix | I]
    augmented = [row[:] + [1 if i == j else 0 for j in range(k)] for i, row in enumerate(matrix)]
    
    for col in range(k):
        # 寻找主元
        pivot = None
        for row in range(col, k):
            if augmented[row][col] != 0:
                pivot = row
                break
        if pivot is None:
            return [row[k:] for row in augmented]
            raise ValueError("Matrix is singular.")
        
        # 交换行
        augmented[col], augmented[pivot] = augmented[pivot], augmented[col]
        
        # 归一化主元行
        pivot_val = augmented[col][col]
        inv_pivot = gf_manager.inverse(pivot_val)
        for j in range(col, 2 * k):
            augmented[col][j] = gf_manager.multiply(augmented[col][j], inv_pivot)
        
        # 消元
        for row in range(k):
            if row != col and augmented[row][col] != 0:
                factor = augmented[row][col]
                for j in range(col, 2 * k):
                    augmented[row][j] = gf_manager.add(
                        augmented[row][j],
                        gf_manager.multiply(factor, augmented[col][j])
                    )
    
    # 提取逆矩阵部分
    return [row[k:] for row in augmented]

def vector_matrix_multiply_gf(vector, matrix, k, gf_manager):
    result = []
    for col in range(k):
        sum_val = 0
        for i in range(k):
            product = gf_manager.multiply(vector[i], matrix[i][col])
            sum_val = gf_manager.add(sum_val, product)
        result.append(sum_val)
    return result

def get_decode_vector(matrix, survivors, target_index, k, w):
    gf = GFManager(w)
    
    # 构造选择矩阵
    select_matrix = [matrix[i][:k] for i in survivors]
    
    # 矩阵求逆
    invert_matrix = invert_matrix_gf(select_matrix, k, gf)
    
    # 获取目标块的行向量
    target_row = matrix[target_index][:k]
    
    # 矩阵乘法获取解码系数
    decode_vector = vector_matrix_multiply_gf(target_row, invert_matrix, k, gf)
    
    return decode_vector


def GetCoefVector(matrix, all_node_ids, row_ids, select_node_ids, target_node_id, k, w):    
    assert len(matrix[0]) == k 
    survivor_index = []
    for node_id in select_node_ids:
        row_id = row_ids[all_node_ids.index(node_id)]
        survivor_index.append(row_id - 1)

    target_index = row_ids[all_node_ids.index(target_node_id)] - 1
    
    coefs = get_decode_vector(matrix, survivor_index, target_index, k, w)
    
    node_id_2_coef = {}
    for i in range(len(coefs)):
        node_id = survivor_index[i] + 1
        node_id_2_coef[node_id] = coefs[i]
    return node_id_2_coef

if __name__ == "__main__":

    # matrix = [
    #     [1, 0, 0, 0],
    #     [0, 1, 0, 0],
    #     [0, 0, 1, 0],
    #     [0, 0, 0, 1],
    #     [1, 1, 1, 1],
    #     [1, 2, 4, 8]
    # ]
    matrix = [
        [1,0,0,0,0,0],
        [0,1,0,0,0,0],
        [0,0,1,0,0,0], 
        [0,0,0,1,0,0],
        [0,0,0,0,1,0],
        [0,0,0,0,0,1],
        [1,1,1,1,1,1],
        [1,2,4,8,16,32], 
        [1,3,5,15,17,51]
    ]
    survivors = [0, 1, 2, 3, 4, 8]  # 使用数据块0和校验块2来恢复数据块1
    target_index = 5
    k = 6
    w = 8
    
    decode_vector = get_decode_vector(matrix, survivors, target_index, k, w)
    print("decode vector:", decode_vector)