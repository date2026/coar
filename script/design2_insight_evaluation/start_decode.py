import subprocess
import time

def run_parallel_ssh():
    commands = [
        'ssh node01 "[your path]/build/ECClient decode /input_512MB [your path]/script/design2_insight_evaluation/ecdag_decode_512_cr_7 1 2 3 4 4095 5"',
        'ssh node01 "[your path]/build/ECClient decode /input_512MB_back [your path]/script/design2_insight_evaluation/ecdag_decode_512_cr_8 1 2 3 4 4095 5"',
        'ssh node01 "[your path]/build/ECClient decode /input_512MB_back_back [your path]/script/design2_insight_evaluation/ecdag_decode_512_cr_9 1 2 3 4 4095 5"',
        'ssh node01 "[your path]/build/ECClient decode /input_512MB_back_back_back [your path]/script/design2_insight_evaluation/ecdag_decode_512_cr_10 1 2 3 4 4095 5"',
    ]

    
    start_time = time.perf_counter()

    processes = []
    for cmd in commands:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        processes.append(p)

    for i, p in enumerate(processes):
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            print(f"{stdout}")
        else:
            print(f"error: {stderr.decode().strip()}")

    end_time = time.perf_counter()

    total_duration_ms = (end_time - start_time) * 1000

    print(f"overall repair time: {total_duration_ms:.3f} ms")

    log_file_path = "[your path]/build/design2.log" 
    with open(log_file_path, 'a', encoding='utf-8') as f:
        f.write(f"{total_duration_ms:.3f}\n")





if __name__ == "__main__":
    run_parallel_ssh()