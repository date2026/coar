import docplex.mp.model as cpx
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s  - %(message)s'
)
SLICE_NUM = 4

def solve_ilp_problem(n, k, S, a, b, c):
    """
    n: src node num, only including surving nodes
    k
    S: chunk size
    a: download bandwidth, len is n + 1, a[n] is download bandwidth of nd
    b: upload bandwidth, len is n + 1, a[n] is upload bandwidth of nd
    c: gf bandwidth, len is n + 1, a[n] is gf bandwidth of nd
    x: download task
    y: upload task
    """

    logging.info(f"solve_ilp_problem, download bandwidths: {a}, upload_bandwidth: {b}, gf bandwidth: {c}")
    assert(len(a) == n + 1 and len(b) == n + 1 and len(c) == n + 1)

    model = cpx.Model(name="node_allocation")
    
    # download task num
    x = model.integer_var_list(n+1, lb=0, ub=k, name="x")
    model.add_constraint(x[n] >= 1, "xd_min_constraint")
    # upload task num, 0 or 1
    y = model.binary_var_list(n, name="y")
    
    # gf task num (chunk num)
    z = model.integer_var_list(n+1, lb=0, name="z")

    # 1. x1 + x2 + ... + xn + xd = k
    model.add_constraint(model.sum(x) == k, "sum_x_eq_k")
    
    # 2. y1 + y2 + ... + yn = k
    model.add_constraint(model.sum(y) == k, "sum_y_eq_k")

    for i in range(n):
        model.add_constraint(x[i] <= k * y[i], f"x_implies_y_{i}")


    for i in range(n):
        model.add_constraint(z[i] <= (k+1)*x[i], f"z_le_Mx_{i}")
        model.add_constraint(z[i] >= x[i] + y[i] - (k+1)*(1 - x[i]), f"z_ge_xy1_{i}")
        model.add_constraint(z[i] <= x[i] + y[i] + (k+1)*(1 - x[i]), f"z_le_xy1_{i}")

    d_idx = n
    model.add_constraint(z[d_idx] <= (k+1)*x[d_idx], f"z_le_Mx_d")
    model.add_constraint(z[d_idx] >= x[d_idx] - (k+1)*(1 - x[d_idx]), f"z_ge_x_d")
    model.add_constraint(z[d_idx] <= x[d_idx] + (k+1)*(1 - x[d_idx]), f"z_le_x_d")


    
    overhead = model.continuous_var_list(n+1, lb=0, name="overhead")
    max_overhead = model.continuous_var(lb=0, name="max_overhead")
    
    # [i] >= Ai, Bi, Ci
    for i in range(n):
        # Ai = xi/ai
        model.add_constraint(overhead[i] >= x[i] / a[i], f"overhead_ge_A_{i}")
        # Bi = yi/bi
        model.add_constraint(overhead[i] >= y[i] / b[i], f"overhead_ge_B_{i}")
        # Ci = zi/ci
        model.add_constraint(overhead[i] >= z[i] / c[i], f"overhead_ge_C_{i}")
    
    model.add_constraint(overhead[d_idx] >= x[d_idx] / a[d_idx], f"overhead_ge_A_d")
    model.add_constraint(overhead[d_idx] >= 0, f"overhead_ge_B_d")  # B_d = 0/bd = 0
    model.add_constraint(overhead[d_idx] >= z[d_idx] / c[d_idx], f"overhead_ge_C_d")
    
    # max_overhead >= all overhead[i]
    for i in range(n+1):
        model.add_constraint(max_overhead >= overhead[i], f"max_ge_overhead_{i}")
    
    model.minimize(max_overhead)
    
    solution = model.solve()
    
    if solution:
        download_tasks = [solution.get_value(xi) for xi in x]
        upload_tasks = [solution.get_value(yi) for yi in y] + [0]
        assert(len(download_tasks) == n + 1 and len(upload_tasks) == n + 1)
        return download_tasks, upload_tasks
    else:
        return None

