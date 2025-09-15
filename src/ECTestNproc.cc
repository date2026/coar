#include <iostream>
#include <unistd.h>
#include <sys/wait.h>
#include <vector>
#include <cstring>
#include <cstdlib>

int main(int argc, char* argv[]) {


    int num_processes = atoi(argv[1]);


    const char* program_path = "/root/coar/build/ECTest";
    
    char* args[] = {const_cast<char*>(program_path), nullptr};


    std::vector<pid_t> pids;

    for (int i = 0; i < num_processes; i++) {
        pid_t pid = fork();

        if (pid < 0) {
            for (pid_t p : pids) {
                kill(p, SIGTERM);
            }
            return 1;
        } 
        else if (pid == 0) {
            execvp(program_path, args); 
            exit(1);
        } 
        else {
            pids.push_back(pid);
        }
    }

    int completed = 0;
    int status;
    
    while (completed < num_processes) {
        pid_t finished_pid = wait(&status);
        
        if (finished_pid < 0) {
            break;
        }
        
        completed++;
        if (WIFEXITED(status)) {
            std::cout << "子进程 " << finished_pid << " 已完成，退出码: " << WEXITSTATUS(status) << " (" << completed << "/" << num_processes << ")" << std::endl;
        } else if (WIFSIGNALED(status)) {
            std::cout << "子进程 " << finished_pid << " 被信号 " << WTERMSIG(status) << " 终止 (" << completed << "/" << num_processes << ")" << std::endl;
        }
    }

    return 0;
}
