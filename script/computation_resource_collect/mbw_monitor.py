import time
import csv
import datetime
import subprocess
import re
from typing import Dict, List

def get_available_memory_mb() -> int:
    """获取系统可用物理内存（单位：MB），用于合理设置测试块大小"""
    try:
        result = subprocess.run(
            ["free", "-m"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8"
        )
        for line in result.stdout.splitlines():
            if "available" in line:
                return int(line.split()[2])
        return 0
    except Exception as e:
        print(f"获取可用内存失败：{e}，默认返回1024MB")
        return 1024

def validate_block_size(block_size_mb: int, available_mem_mb: int) -> int:
    """验证并调整测试块大小，确保兼容性"""
    # 1. 块大小不超过可用内存的1/4
    max_safe_block = int(available_mem_mb / 4)
    if block_size_mb > max_safe_block:
        print(f"警告：原始块大小{block_size_mb}MB超过可用内存1/4，调整为{max_safe_block}MB")
        block_size_mb = max_safe_block
    
    # 2. 调整为最接近的2的幂次（兼容mbw要求）
    power_of_2 = [16, 32, 64, 128, 256, 512]
    closest_block = min(power_of_2, key=lambda x: abs(x - block_size_mb))
    if block_size_mb != closest_block:
        print(f"警告：原始块大小{block_size_mb}MB非2的幂次，调整为兼容值{closest_block}MB")
        block_size_mb = closest_block
    
    # 3. 确保块大小不小于16MB，不大于512MB
    block_size_mb = max(16, min(block_size_mb, 512))
    return block_size_mb

def get_mbw_bandwidth_stats(
    mbw_path: str = "mbw",
    test_mode: int = 0,
    block_size_mb: int = 256,
    iterations: int = 5
) -> Dict[str, float]:
    """
    适配你的mbw输出格式，提取平均内存带宽
    """
    mbw_stats = {}
    # 保持与原脚本一致的时间戳格式
    mbw_stats["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    # 记录测试配置参数
    mbw_stats["test_mode"] = test_mode
    mbw_stats["block_size_mb"] = block_size_mb
    mbw_stats["iterations"] = iterations
    mbw_stats["avg_bandwidth_mb_s"] = 0.0
    mbw_stats["avg_bandwidth_gb_s"] = 0.0

    try:
        # 构造mbw命令（与你的执行格式一致）
        cmd = [
            mbw_path,
            "-t", str(test_mode),
            "-s", str(block_size_mb),
            "-n", str(iterations),
            "-t"
        ]

        # 执行mbw命令
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            timeout=30
        )

        # 检查块大小报错
        if "Error: array size wrong!" in result.stdout or "Error: array size wrong!" in result.stderr:
            print(f"警告：块大小{block_size_mb}MB不兼容，自动降级为128MB")
            cmd[5] = "128"  # 修改-s参数为128
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="utf-8",
                timeout=30
            )
            mbw_stats["block_size_mb"] = 128  # 更新块大小记录

        # 提取mbw输出（你的输出在stdout中）
        mbw_output = result.stdout
        
        # 关键优化：正则匹配你的输出格式，优先提取AVG行的平均带宽
        # 匹配AVG行格式："AVG     Method: MEMCPY  Elapsed: 0.03226        MiB: 256.00000  Copy: 7935.868 MiB/s"
        avg_bandwidth_pattern = re.compile(r"AVG\s+.*Copy:\s+(\d+\.?\d*)\s+MiB/s", re.IGNORECASE)
        # 备用：匹配单次行格式（防止AVG行匹配失败）
        single_bandwidth_pattern = re.compile(r"\d+\s+.*Copy:\s+(\d+\.?\d*)\s+MiB/s", re.IGNORECASE)

        # 优先匹配AVG行
        avg_match = avg_bandwidth_pattern.search(mbw_output)
        if avg_match:
            avg_bandwidth_mb_s = round(float(avg_match.group(1)), 2)
            mbw_stats["avg_bandwidth_mb_s"] = avg_bandwidth_mb_s
            mbw_stats["avg_bandwidth_gb_s"] = round(avg_bandwidth_mb_s / 1024, 4)
        else:
            # 备用：匹配最后一次单次测试的带宽
            single_matches = single_bandwidth_pattern.findall(mbw_output)
            if single_matches:
                # 取最后一次单次测试的数值作为备用
                last_bandwidth_mb_s = round(float(single_matches[-1]), 2)
                mbw_stats["avg_bandwidth_mb_s"] = last_bandwidth_mb_s
                mbw_stats["avg_bandwidth_gb_s"] = round(last_bandwidth_mb_s / 1024, 4)
                print(f"警告：未匹配到AVG行，使用最后一次单次测试带宽：{last_bandwidth_mb_s} MiB/s")
            else:
                print(f"警告：mbw输出解析失败，输出内容：\n{mbw_output}")

    except subprocess.TimeoutExpired:
        print("错误：mbw测试超时，本次统计填充0")
    except FileNotFoundError:
        print(f"错误：未找到mbw工具，请检查路径是否正确：{mbw_path}")
    except KeyError:
        print(f"错误：无效的测试模式{test_mode}，可选模式为0/1/2")
    except Exception as e:
        print(f"错误：获取内存带宽失败：{e}")

    return mbw_stats



# collect memory bandwidth usage of one node
def main():
    # 配置参数（与原脚本风格一致，可按需修改）
    interval = 1  # 统计间隔（秒）
    total_times = 256  # 总统计次数
    csv_filename = "linux_memory_bandwidth_mbw_stats.csv"  # 输出CSV文件名
    
    # mbw测试配置（与你的执行参数一致）
    mbw_absolute_path = "mbw"  # 若需绝对路径，替换为which mbw的输出
    mbw_test_mode = 0  # copy模式
    mbw_block_size = 256  # 你的测试块大小
    mbw_iterations = 5  # 你的迭代次数（与输出中的5 runs一致）

    # 获取系统可用内存并验证块大小
    available_mem_mb = get_available_memory_mb()
    safe_block_size_mb = validate_block_size(mbw_block_size, available_mem_mb)

    # 定义CSV表头（与原脚本格式一致）
    csv_headers = [
        "timestamp", "test_mode", "block_size_mb", "iterations",
        "avg_bandwidth_mb_s", "avg_bandwidth_gb_s"
    ]
    
    # 初始化CSV文件并写入表头
    with open(csv_filename, "w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_headers)
        writer.writeheader()
        
        print(f"开始通过mbw统计内存带宽信息，共{total_times}次，间隔{interval}秒...")
        print(f"mbw测试配置：模式={mbw_test_mode}，块大小={safe_block_size_mb}MB，迭代次数={mbw_iterations}")
        print(f"数据将保存至：{csv_filename}")
        
        # 循环统计
        for i in range(total_times):
            try:
                # 获取本次内存带宽数据
                stats = get_mbw_bandwidth_stats(
                    mbw_path=mbw_absolute_path,
                    test_mode=mbw_test_mode,
                    block_size_mb=safe_block_size_mb,
                    iterations=mbw_iterations
                )
                # 写入CSV行
                writer.writerow(stats)
                # 刷新缓冲区，确保数据实时写入
                csv_file.flush()
                
                # 打印进度信息（保持原脚本的输出风格）
                progress = (i + 1) / total_times * 100
                print(f"已完成 {i+1}/{total_times} 次统计（进度：{progress:.1f}%）| "
                      f"平均带宽: {stats['avg_bandwidth_mb_s']} MB/s ({stats['avg_bandwidth_gb_s']} GB/s) | "
                      f"使用块大小: {stats['block_size_mb']}MB | "
                      f"时间戳: {stats['timestamp']}")
                
                # 最后一次无需休眠
                if i < total_times - 1:
                    time.sleep(interval)
            
            except Exception as e:
                print(f"第{i+1}次统计出错：{e}，跳过本次统计")
                # 出错后仍休眠，保证间隔稳定
                if i < total_times - 1:
                    time.sleep(interval)
                continue
    
    print(f"\n统计完成！所有数据已保存至 {csv_filename}")

if __name__ == "__main__":
    
    main()