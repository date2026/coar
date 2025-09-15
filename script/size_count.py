import subprocess
import re
import humanize

def get_hdfs_block_stats(hdfs_path):
    """执行hdfs fsck命令并返回输出结果（兼容Python 3.6）"""
    try:
        # 用universal_newlines=True替代text=True（适配Python 3.6）
        # 用stdout和stderr替代capture_output（适配Python 3.6）
        result = subprocess.run(
            ['hdfs', 'fsck', hdfs_path, '-files', '-blocks', '-locations'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,  # 替代text=True，将输出转为字符串
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"执行hdfs fsck命令失败: {e.stderr}")
        return None
    except FileNotFoundError:
        print("未找到hdfs命令，请确保Hadoop环境已正确配置")
        return None

def parse_block_info(fsck_output):
    """解析块信息，返回节点到字节数的映射"""
    node_bytes = {}
    
    # 正则表达式匹配块信息行 (包含长度和节点)
    block_pattern = re.compile(
        r'len=(\d+).*?\[DatanodeInfoWithStorage\[([\d.]+:\d+),',
        re.IGNORECASE
    )
    
    # 遍历输出的每一行查找块信息
    for line in fsck_output.split('\n'):
        match = block_pattern.search(line)
        if match:
            block_size = int(match.group(1))
            node = match.group(2)
            
            if node in node_bytes:
                node_bytes[node] += block_size
            else:
                node_bytes[node] = block_size
    
    return node_bytes

def calculate_percentages(node_bytes):
    """计算各节点存储占比"""
    total = sum(node_bytes.values())
    if total == 0:
        return {}, 0
    
    percentages = {}
    for node, bytes_count in node_bytes.items():
        percentages[node] = (bytes_count / total) * 100
    
    return percentages, total

def print_stats(node_bytes, percentages, total):
    """格式化输出统计结果"""
    print(f"文件路径: /tmp/kmeans-data.parquet")
    print(f"总数据量: {humanize.naturalsize(total)} ({total} bytes)")
    print("\n各节点存储统计:")
    print("-" * 80)
    print(f"{'节点IP:端口':<20} | {'字节数':<15} | {'人类可读大小':<15} | 占比")
    print("-" * 80)
    
    # 按存储字节数降序排列
    for node in sorted(node_bytes.keys(), key=lambda x: -node_bytes[x]):
        bytes_count = node_bytes[node]
        percent = percentages[node]
        print(
            f"{node:<20} | {bytes_count:<15} | {humanize.naturalsize(bytes_count):<15} | {percent:.2f}%"
        )
    print("-" * 80)

if __name__ == "__main__":
    hdfs_path = "/lr_data_repartitioned"
    
    fsck_output = get_hdfs_block_stats(hdfs_path)
    if not fsck_output:
        exit(1)
    
    node_bytes = parse_block_info(fsck_output)
    if not node_bytes:
        print("未找到任何块信息，请检查路径是否正确")
        exit(1)
    
    percentages, total = calculate_percentages(node_bytes)
    print_stats(node_bytes, percentages, total)
    