import paramiko
import re
import os
USERNAME = "root"  

REMOTE_FILE_PATH = "[your path]/build/repair.log"  

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
            print(f"{hostname} not exist {remote_path}")
            return None

        match = re.match(r"([\d\.\-\+e]+)\s+([\d\.\-\+e]+)\s+([\d\.\-\+e]+)\s+([\d\.\-\+e]+)\s+([\d\.\-\+e]+)", last_line)
        
        if match:
            data = [float(match.group(i)) for i in range(1, 6)]
            return data
        else:
            print(f"{hostname} match fail {last_line}")
            return None

    except Exception as e:
        print(f"{hostname} error: {e}")
    finally:
        client.close()
        
    return None



def extract_first_id_after_encode_partial(file_path):
    
    first_ids = []
    
    prefix = "ENCODE_PARTIAL"
    
    try:
        if not os.path.exists(file_path):
            print(f"{file_path} not exist")
            return []

        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                
                if line.startswith(prefix):
                    parts = line.split()
                    
                    if len(parts) >= 2:
                        first_id_str = parts[1]
                        try:
                            first_id = int(first_id_str)
                            first_ids.append(first_id)
                        except ValueError:
                            print(f"fail to translate {first_id_str}' to value, line: {line}")
                            
        return first_ids

    except Exception as e:
        print(f"error: {e}")
        return []





def main():

    all_receive_times = []  
    all_compute_times = [] 


    
    FILE_PATH = "[your path]/build/input_384MB_ecdag_temp"
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




    if all_receive_times:
        print(f"receive time cnt: {len(all_receive_times)}")
        print(f"avg receive time: {sum(all_receive_times)/len(all_receive_times):.3f}")
        
    print("-" * 50)
    if all_compute_times:
        print(f"compute time cnt: {len(all_compute_times)}")
        print(f"avg compute time: {sum(all_compute_times)/len(all_compute_times):.3f}")



if __name__ == "__main__":
    main()