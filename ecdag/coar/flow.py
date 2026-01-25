from collections import deque
import time
# from black import format_str

class Edge:
    def __init__(self, to, rev, capacity):
        self.to = to                  
        self.rev = rev               
        self.capacity = capacity      
        self.original_capacity = capacity 

class Dinic:
    def __init__(self, n):
        self.size = n
        self.graph = [[] for _ in range(n)]  
    
    def add_edge(self, fr, to, capacity):
        forward = Edge(to, len(self.graph[to]), capacity)
        backward = Edge(fr, len(self.graph[fr]), 0)
        self.graph[fr].append(forward)
        self.graph[to].append(backward)
    
    def bfs_level(self, s, t, level):
        level[:] = [-1] * self.size
        q = deque()
        level[s] = 0
        q.append(s)
        
        while q:
            v = q.popleft()
            for edge in self.graph[v]:
                if edge.capacity > 0 and level[edge.to] < 0:
                    level[edge.to] = level[v] + 1
                    q.append(edge.to)
                    if edge.to == t:
                        return True
        return level[t] != -1
    
    def dfs_flow(self, v, t, upTo, iter_, level):
        if v == t:
            return upTo
        
        for i in range(iter_[v], len(self.graph[v])):
            edge = self.graph[v][i]
            if edge.capacity > 0 and level[v] < level[edge.to]:
                d = self.dfs_flow(edge.to, t, min(upTo, edge.capacity), iter_, level)
                if d > 0:
                    edge.capacity -= d
                    self.graph[edge.to][edge.rev].capacity += d
                    return d
            iter_[v] += 1
        return 0
    
    def max_flow(self, s, t):
        flow = 0
        level = [-1] * self.size
        
        while self.bfs_level(s, t, level):
            iter_ = [0] * self.size
            while True:
                f = self.dfs_flow(s, t, float('inf'), iter_, level)
                if f == 0:
                    break
                flow += f
        return flow
    
    def get_edge_flows(self):

        edge_flows = []
        for fr in range(self.size):
            for edge in self.graph[fr]:
                if edge.original_capacity > 0:
                    actual_flow = edge.original_capacity - edge.capacity
                    edge_flows.append((
                        fr,              
                        edge.to,           
                        edge.original_capacity,  
                        edge.capacity,     
                        actual_flow        
                    ))
        return edge_flows
    
    def print_edge_flows(self):

        edge_flows = self.get_edge_flows()
        print(f"{'start':<6}{'end':<6}{'origin capacity':<10}{'remain capacity':<10}{'actual capacity':<10}")
        print("-" * 50)


        for fr, to, orig_cap, remain_cap, act_flow in edge_flows:
            print(f"{fr:<6}{to:<6}{orig_cap:<10}{remain_cap:<10}{act_flow:<10}")
            # print(format_str.format(fr, to, orig_cap, remain_cap, act_flow))




# 10+4
# def generate_flow():
#     dinic = Dinic(28)
#     dinic.add_edge(0, 1, 0.1 * 9)
#     dinic.add_edge(0, 2, 0)
#     dinic.add_edge(0, 3, 0.1 * 9)
#     dinic.add_edge(0, 4, 0)
#     dinic.add_edge(0, 5, 0.1 * 9)
#     dinic.add_edge(0, 6, 0)
#     dinic.add_edge(0, 7, 0.1 * 9)
#     dinic.add_edge(0, 8, 0)
#     dinic.add_edge(0, 9, 0.1 * 9)
#     dinic.add_edge(0, 10, 0)
#     dinic.add_edge(0, 11, 0.1 * 9)
#     dinic.add_edge(0, 12, 0)
#     dinic.add_edge(0, 13, 0.4 * 9)  

#     dinic.add_edge(1,15,0.1);dinic.add_edge(1,16,0.1);dinic.add_edge(1,17,0.1);dinic.add_edge(1,18,0.1)
#     dinic.add_edge(1,19,0.1);dinic.add_edge(1,20,0.1);dinic.add_edge(1,21,0.1);dinic.add_edge(1,22,0.1)
#     dinic.add_edge(1,23,0.1);dinic.add_edge(1,24,0.1);dinic.add_edge(1,25,0.1);dinic.add_edge(1,26,0.1)
#     dinic.add_edge(3,14,0.1);dinic.add_edge(3,15,0.1);dinic.add_edge(3,17,0.1);dinic.add_edge(3,18,0.1)
#     dinic.add_edge(3,19,0.1);dinic.add_edge(3,20,0.1);dinic.add_edge(3,21,0.1);dinic.add_edge(3,22,0.1)
#     dinic.add_edge(3,23,0.1);dinic.add_edge(3,24,0.1);dinic.add_edge(3,25,0.1);dinic.add_edge(3,26,0.1)
#     dinic.add_edge(5,14,0.1);dinic.add_edge(5,15,0.1);dinic.add_edge(5,16,0.1);dinic.add_edge(5,17,0.1)
#     dinic.add_edge(5,19,0.1);dinic.add_edge(5,20,0.1);dinic.add_edge(5,21,0.1);dinic.add_edge(5,22,0.1)
#     dinic.add_edge(5,23,0.1);dinic.add_edge(5,24,0.1);dinic.add_edge(5,25,0.1);dinic.add_edge(5,26,0.1)
#     dinic.add_edge(7,14,0.1);dinic.add_edge(7,15,0.1);dinic.add_edge(7,16,0.1);dinic.add_edge(7,17,0.1)
#     dinic.add_edge(7,18,0.1);dinic.add_edge(7,19,0.1);dinic.add_edge(7,21,0.1);dinic.add_edge(7,22,0.1)
#     dinic.add_edge(7,23,0.1);dinic.add_edge(7,24,0.1);dinic.add_edge(7,25,0.1);dinic.add_edge(7,26,0.1)
#     dinic.add_edge(9,14,0.1);dinic.add_edge(9,15,0.1);dinic.add_edge(9,16,0.1);dinic.add_edge(9,17,0.1)
#     dinic.add_edge(9,18,0.1);dinic.add_edge(9,19,0.1);dinic.add_edge(9,20,0.1);dinic.add_edge(9,21,0.1)
#     dinic.add_edge(9,23,0.1);dinic.add_edge(9,24,0.1);dinic.add_edge(9,25,0.1);dinic.add_edge(9,26,0.1)
#     dinic.add_edge(11,14,0.1);dinic.add_edge(11,15,0.1);dinic.add_edge(11,16,0.1);dinic.add_edge(11,17,0.1)
#     dinic.add_edge(11,18,0.1);dinic.add_edge(11,19,0.1);dinic.add_edge(11,20,0.1);dinic.add_edge(11,21,0.1)
#     dinic.add_edge(11,22,0.1);dinic.add_edge(11,23,0.1);dinic.add_edge(11,25,0.1);dinic.add_edge(11,26,0.1)
#     dinic.add_edge(13,14,0.4);dinic.add_edge(13,15,0.4);dinic.add_edge(13,16,0.4);dinic.add_edge(13,17,0.4)
#     dinic.add_edge(13,18,0.4);dinic.add_edge(13,19,0.4);dinic.add_edge(13,20,0.4);dinic.add_edge(13,21,0.4)
#     dinic.add_edge(13,22,0.4);dinic.add_edge(13,23,0.4);dinic.add_edge(13,24,0.4);dinic.add_edge(13,25,0.4)

#     dinic.add_edge(14, 27, 0.8)
#     dinic.add_edge(15, 27, 1)
#     dinic.add_edge(16, 27, 0.4)
#     dinic.add_edge(17, 27, 1)
#     dinic.add_edge(18, 27, 0.6)
#     dinic.add_edge(19, 27, 0.8)
#     dinic.add_edge(20, 27, 0.6)
#     dinic.add_edge(21, 27, 0.8)
#     dinic.add_edge(22, 27, 0.6)
#     dinic.add_edge(23, 27, 0.8)
#     dinic.add_edge(24, 27, 0.4)
#     dinic.add_edge(25, 27, 0.8)
#     dinic.add_edge(26, 27, 0.4)

#     start = time.time()
#     max_flow_value = dinic.max_flow(0, 27)
#     end = time.time()
#     print(f"flow time: {(end - start) * 1000.0}")



# 8+4
def generate_flow():
    dinic = Dinic(24)
    dinic.add_edge(0, 1, 0.2 * 7)  
    dinic.add_edge(0, 2, 0)
    dinic.add_edge(0, 3, 0.1 * 7)       
    dinic.add_edge(0, 4, 0)
    dinic.add_edge(0, 5, 0.2 * 7)
    dinic.add_edge(0, 6, 0)
    dinic.add_edge(0, 7, 0.1 * 7)
    dinic.add_edge(0, 8, 0)
    dinic.add_edge(0, 9, 0.2 * 7)
    dinic.add_edge(0, 10, 0)
    dinic.add_edge(0, 11, 0.2 * 7)   

    dinic.add_edge(1,13,0.2);dinic.add_edge(1,14,0.2);dinic.add_edge(1,15,0.2);dinic.add_edge(1,16,0.2)
    dinic.add_edge(1,17,0.2);dinic.add_edge(1,18,0.2);dinic.add_edge(1,19,0.2);dinic.add_edge(1,20,0.2)
    dinic.add_edge(1,21,0.2);dinic.add_edge(1,22,0.2)
    dinic.add_edge(3,12,0.1);dinic.add_edge(3,13,0.1);dinic.add_edge(3,15,0.1);dinic.add_edge(3,16,0.1)
    dinic.add_edge(3,17,0.1);dinic.add_edge(3,18,0.1);dinic.add_edge(3,19,0.1);dinic.add_edge(3,20,0.1)
    dinic.add_edge(3,21,0.1);dinic.add_edge(3,22,0.1)
    dinic.add_edge(5,12,0.2);dinic.add_edge(5,13,0.2);dinic.add_edge(5,14,0.2);dinic.add_edge(5,15,0.2)
    dinic.add_edge(5,17,0.2);dinic.add_edge(5,18,0.2);dinic.add_edge(5,19,0.2);dinic.add_edge(5,20,0.2)
    dinic.add_edge(5,21,0.2);dinic.add_edge(5,22,0.2)
    dinic.add_edge(7,12,0.1);dinic.add_edge(7,13,0.1);dinic.add_edge(7,14,0.1);dinic.add_edge(7,15,0.1)
    dinic.add_edge(7,16,0.1);dinic.add_edge(7,17,0.1);dinic.add_edge(7,19,0.1);dinic.add_edge(7,20,0.1)
    dinic.add_edge(7,21,0.1);dinic.add_edge(7,22,0.1)
    dinic.add_edge(9,12,0.2);dinic.add_edge(9,13,0.2);dinic.add_edge(9,14,0.2);dinic.add_edge(9,15,0.2)
    dinic.add_edge(9,16,0.2);dinic.add_edge(9,17,0.2);dinic.add_edge(9,18,0.2);dinic.add_edge(9,19,0.2)
    dinic.add_edge(9,21,0.2);dinic.add_edge(9,22,0.2)
    dinic.add_edge(11,12,0.2);dinic.add_edge(11,13,0.2);dinic.add_edge(11,14,0.2);dinic.add_edge(11,15,0.2)
    dinic.add_edge(11,16,0.2);dinic.add_edge(11,17,0.2);dinic.add_edge(11,18,0.2);dinic.add_edge(11,19,0.2)
    dinic.add_edge(11,20,0.2);dinic.add_edge(11,21,0.2)

    dinic.add_edge(12, 23, 0.7)
    dinic.add_edge(13, 23, 0.9)
    dinic.add_edge(14, 23, 0.4)
    dinic.add_edge(15, 23, 0.9)
    dinic.add_edge(16, 23, 0.5)
    dinic.add_edge(17, 23, 0.7)
    dinic.add_edge(18, 23, 0.5)
    dinic.add_edge(19, 23, 0.7)
    dinic.add_edge(20, 23, 0.5)
    dinic.add_edge(21, 23, 0.7)
    dinic.add_edge(22, 23, 0.5) 

    start = time.time()
    max_flow_value = dinic.max_flow(0, 23)
    end = time.time()
    # print(f"max flow time: {(end - start) * 1000.0}")

    return dinic, max_flow_value


# 6+3
# def generate_flow():
#     total_nodes = 18
#     dinic = Dinic(total_nodes)
#     k = 6  # k-1=5

#     dinic.add_edge(0, 1, 0.1 * 5)  # 0.6
#     dinic.add_edge(0, 2, 0.1 * 5)  # 0.6
#     dinic.add_edge(0, 3, 0.1 * 5)  # 0.6
#     dinic.add_edge(0, 4, 0.1 * 5)  # 0.6
#     dinic.add_edge(0, 5, 0.1 * 5)  # 0.6
#     dinic.add_edge(0, 6, 0.1 * 5)  # 0.6
#     dinic.add_edge(0, 7, 0)        # 0
#     dinic.add_edge(0, 8, 0.4 * 5)  # 2.4

#     for to in range(10, 17):
#         dinic.add_edge(1, to, 0.1)
#     for to in [9] + list(range(11, 17)):
#         dinic.add_edge(2, to, 0.1)
#     for to in list(range(9, 11)) + list(range(12, 17)):
#         dinic.add_edge(3, to, 0.1)
#     for to in list(range(9, 12)) + list(range(13, 17)):
#         dinic.add_edge(4, to, 0.1)
#     for to in list(range(9, 13)) + list(range(14, 17)):
#         dinic.add_edge(5, to, 0.1)
#     for to in list(range(9, 14)) + list(range(15, 17)):
#         dinic.add_edge(6, to, 0.1)
#     for to in range(9, 16):
#         dinic.add_edge(8, to, 0.4)

#     second2sink = {
#         9:0.8, 10:0.8, 11:0.6, 12:0.8,
#         13:0.6, 14:0.8, 15:0.6, 16:0.6
#     }
#     for node, cap in second2sink.items():
#         dinic.add_edge(node, 17, cap)

#     start = time.time()
#     max_flow_val = dinic.max_flow(0, 17)
#     end = time.time()
#     print(f"max flow time: {(end - start) * 1000.0}")




# 4+2
# # k - 1 = 3
# # 1         2       3       4       5   
# # 0.4	    0	    0.4	    0	    0.2
# # 0.6	    0.8	    0.4	    0.8	    0.4

# 4+2
# def generate_flow():

#     dinic = Dinic(12)
#     dinic.add_edge(0, 1, 1.2)
#     dinic.add_edge(0, 2, 0)
#     dinic.add_edge(0, 3, 1.2)
#     dinic.add_edge(0, 4, 0)
#     dinic.add_edge(0, 5, 0.6)
    
#     dinic.add_edge(1, 7, 0.4)
#     dinic.add_edge(1, 8, 0.4)
#     dinic.add_edge(1, 9, 0.4)
#     dinic.add_edge(1, 10, 0.4)


#     dinic.add_edge(3, 6, 0.4)
#     dinic.add_edge(3, 7, 0.4)
#     dinic.add_edge(3, 9, 0.4)
#     dinic.add_edge(3, 10, 0.4)

#     dinic.add_edge(5, 6, 0.2)
#     dinic.add_edge(5, 7, 0.2)
#     dinic.add_edge(5, 8, 0.2)
#     dinic.add_edge(5, 9, 0.2)


#     dinic.add_edge(6, 11, 0.6)
#     dinic.add_edge(7, 11, 0.8)
#     dinic.add_edge(8, 11, 0.4)
#     dinic.add_edge(9, 11, 0.8)
#     dinic.add_edge(10, 11, 0.4)
    
#     start = time.time()
#     max_flow_value = dinic.max_flow(0, 11)
#     end = time.time()
#     print(f"max flow time: {(end - start) * 1000.0}")











if __name__ == "__main__":
    

    total_nodes = 18
    dinic = Dinic(total_nodes)
    k = 6  # k-1=5


    dinic.add_edge(0, 1, 0.1 * 5)  # 0.6
    dinic.add_edge(0, 2, 0.1 * 5)  # 0.6
    dinic.add_edge(0, 3, 0.1 * 5)  # 0.6
    dinic.add_edge(0, 4, 0.1 * 5)  # 0.6
    dinic.add_edge(0, 5, 0.1 * 5)  # 0.6
    dinic.add_edge(0, 6, 0.1 * 5)  # 0.6
    dinic.add_edge(0, 7, 0)        # 0
    dinic.add_edge(0, 8, 0.4 * 5)  # 2.4

    for to in range(10, 17):
        dinic.add_edge(1, to, 0.1)
    for to in [9] + list(range(11, 17)):
        dinic.add_edge(2, to, 0.1)
    for to in list(range(9, 11)) + list(range(12, 17)):
        dinic.add_edge(3, to, 0.1)
    for to in list(range(9, 12)) + list(range(13, 17)):
        dinic.add_edge(4, to, 0.1)
    for to in list(range(9, 13)) + list(range(14, 17)):
        dinic.add_edge(5, to, 0.1)
    for to in list(range(9, 14)) + list(range(15, 17)):
        dinic.add_edge(6, to, 0.1)
    for to in range(9, 16):
        dinic.add_edge(8, to, 0.4)


    second2sink = {
        9:0.8, 10:0.8, 11:0.6, 12:0.8,
        13:0.6, 14:0.8, 15:0.6, 16:0.6
    }
    for node, cap in second2sink.items():
        dinic.add_edge(node, 17, cap)

    max_flow_val = dinic.max_flow(0, 17)
