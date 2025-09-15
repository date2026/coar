#encoding = utf8
import subprocess
import os
from pathlib import Path



dir="/root/coar/script/sysstat"


for sar_file in Path(dir).glob("*.sar"):
    base_name = sar_file.stem
            
    cpu_csv = Path(dir) / f"{base_name}-cpu.csv"
    mem_csv = Path(dir) / f"{base_name}-mem.csv"
    disk_csv = Path(dir) / f"{base_name}-disk.csv"
    net_csv = Path(dir) / f"{base_name}-net.csv"


    subprocess.run([
        "sadf", "-d", str(sar_file), "--", "-u"
    ], stdout=open(cpu_csv, "w"), check=True)
    
    subprocess.run([
        "sadf", "-d", str(sar_file), "--", "-r"
    ], stdout=open(mem_csv, "w"), check=True)
    
    subprocess.run([
        "sadf", "-d", str(sar_file), "--", "-d"
    ], stdout=open(disk_csv, "w"), check=True)

    subprocess.run([
        "sadf", "-d", str(sar_file), "--", "-n", "DEV"
    ], stdout=open(net_csv, "w"), check=True)



# for n in range(5, 81, 5):
#     subpath = "16-thread-100-lu"
#     sar_file = "./" + subpath + "/speed-" + str(n) + ".sar"


#     subprocess.call("sadf -d " + sar_file + " -- -u > ./" + subpath + "/speed-" + str(n) + "-cpu.csv", shell=True)
#     subprocess.call("sadf -d " + sar_file + " -- -r > ./" + subpath + "/speed-" + str(n) + "-mem.csv", shell=True)
#     subprocess.call("sadf -d " + sar_file + " -- -d > ./" + subpath + "/speed-" + str(n) + "-disk.csv", shell=True)