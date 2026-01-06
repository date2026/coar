import paramiko
import re
import os
USERNAME = "root"  

REMOTE_FILE_PATH = "/root/lmq_openec/build/repair.log"  

NODES = [f"node{i:02d}" for i in range(1, 10)]  


def get_last_line_data(hostname, username, remote_path, private_key_path=None, password=None):
    
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())


    try:
        client.connect(hostname, username="root")
        command = f"tail -n 1 {remote_path} 2>/dev/null"
        
        stdin, stdout, stderr = client.exec_command(command)
        last_line = stdout.read().decode().strip()

        if not last_line:
            print(f"⚠️ {hostname}: 文件不存在或为空: {remote_path}")
            return None

        match = re.match(r"([\d\.\-\+e]+)\s+([\d\.\-\+e]+)\s+([\d\.\-\+e]+)\s+([\d\.\-\+e]+)\s+([\d\.\-\+e]+)", last_line)
        
        if match:
            data = [float(match.group(i)) for i in range(1, 6)]
            return data
        else:
            print(f"⚠️ {hostname}: 最后一行格式不匹配: {last_line}")
            return None

    except paramiko.AuthenticationException:
        print(f"❌ {hostname}: 认证失败，请检查用户名/密码/私钥。")
    except paramiko.SSHException as e:
        print(f"❌ {hostname}: SSH 连接或执行命令失败: {e}")
    except Exception as e:
        print(f"❌ {hostname}: 发生其他错误: {e}")
    finally:
        client.close()
        
    return None



def extract_first_id_after_encode_partial(file_path):
    """
    读取文件，筛选以 'ENCODE_PARTIAL' 开头的行，并提取该行后面的第一个数字作为 ID。

    :param file_path: 输入文件的路径。
    :return: 一个包含所有提取出的第一个 ID 的列表。
    """
    # 存储提取出的所有第一个 ID
    first_ids = []
    
    # 定义要匹配的前缀
    prefix = "ENCODE_PARTIAL"
    
    try:
        # 检查文件是否存在
        if not os.path.exists(file_path):
            print(f"❌ 错误: 文件不存在于路径: {file_path}")
            return []

        # 打开并读取文件
        with open(file_path, 'r') as f:
            for line in f:
                # 移除行首尾的空白字符
                line = line.strip()
                
                # 检查行是否以 'ENCODE_PARTIAL' 开头
                if line.startswith(prefix):
                    # 1. 将行按空格分割成单词列表
                    parts = line.split()
                    
                    # 确保行中有足够的元素 (至少要包含 ENCODE_PARTIAL 和后面的一个 ID)
                    if len(parts) >= 2:
                        # 2. 尝试将第二个元素（第一个ID）转换为整数
                        first_id_str = parts[1]
                        try:
                            first_id = int(first_id_str)
                            first_ids.append(first_id)
                        except ValueError:
                            print(f"⚠️ 警告: 无法将 '{first_id_str}' 转换为数字，在行: {line}")
                            
        return first_ids

    except Exception as e:
        print(f"❌ 读取文件时发生错误: {e}")
        return []





def main():

    all_receive_times = []  
    all_compute_times = [] 


    
    FILE_PATH = "/root/lmq_openec/build/input_384MB_ecdag_temp"
    node_ids = extract_first_id_after_encode_partial(FILE_PATH)
    print(node_ids)
    for id in node_ids:
        node = NODES[id]
    # for node in NODES[]:
        data = get_last_line_data(node, USERNAME, REMOTE_FILE_PATH)
        
        if data:
            receive_time = data[2]
            compute_time = data[3]
            
            print(f"{node}: {data}")

            if receive_time > 0:
                all_receive_times.append(receive_time)

            if compute_time > 0:
                all_compute_times.append(compute_time)




    # 计算发送时间平均值
    if all_receive_times:
        print(f"receive time cnt: {len(all_receive_times)}")
        print(f"avg receive time: {sum(all_receive_times)/len(all_receive_times):.3f}")
        
    # 计算计算时间平均值
    print("-" * 50)
    if all_compute_times:
        print(f"compute time cnt: {len(all_compute_times)}")
        print(f"avg compute time: {sum(all_compute_times)/len(all_compute_times):.3f}")



if __name__ == "__main__":
    main()