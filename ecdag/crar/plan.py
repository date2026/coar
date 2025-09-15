import docplex.mp.model as cpx
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s  - %(message)s'
)
SLICE_NUM = 4

def solve_lp_problem(n, k, S, a, b, c):
    """
    n: src node num, surving node can used to repair
    k: 
    S: chunk size
    a: download bandwidth
    b: gf bandwidth
    c: upload bandwidth
    x: repair ratio
    y: help ratio
    """

    logging.info(f"solve_lp_problem, download bandwidths: {a}, gf bandwidth: {b}, upload_bandwidth: {c}")
    assert(len(a) == n and len(b) == n and len(c) == n)

    model = cpx.Model(name="node_allocation")
    
    x_int = model.integer_var_list(n, lb=0, ub=SLICE_NUM, name="x_int")
    y_int = model.integer_var_list(n, lb=0, ub=SLICE_NUM, name="y_int")
    
    x = [xi / SLICE_NUM for xi in x_int]
    y = [yi / SLICE_NUM for yi in y_int]
    
    # one node could not contribute more than one total chunk to responsible
    for i in range(n):
        model.add_constraint(x_int[i] + y_int[i] <= SLICE_NUM, f"sum_xy_{i}_le_16")
    
    
    # repair ratio constraint: x1 + x2 + ... + xn = 1
    model.add_constraint(model.sum(x) == 1, "sum_x")

    # help ratio constraint: x1 + x2 + ... + xn = k - 1
    model.add_constraint(model.sum(y) == k - 1, "sum_y")
    
    # max(O1, O2, ..., On)
    t = model.continuous_var(lb=0, name="t")
    
    # t >= Oi
    for i in range(n):
        if(b[i] == 0):
            b[i] = a[i]
        o1 = (k-1) * x[i] * S / a[i]        # download overhead
        o2 = k * x[i] * S / b[i]            # gf overhead
        o3 = (x[i] + y[i]) * S / c[i]       # upload overhead
        
        model.add_constraint(t >= o1, f"t_ge_o1_{i}")
        model.add_constraint(t >= o2, f"t_ge_o2_{i}")
        model.add_constraint(t >= o3, f"t_ge_o3_{i}")
    
    # minimize t = max(O1, O2, ..., On)
    model.minimize(t)
    
    solution = model.solve()
    
    if solution:
        # print(f"min overhead: {solution.get_objective_value()}")
        for i in range(n):
            xi_val = solution.get_value(x[i])
            yi_val = solution.get_value(y[i])
            # print(f"node {i}: repair ratio: {xi_val*SLICE_NUM:.4f}/SLICE_NUM, help ratio: {yi_val*SLICE_NUM:.4f}/SLICE_NUM")
        
        for i in range(n):
            xi_val = solution.get_value(x[i])
            yi_val = solution.get_value(y[i])
            o1 = (k-1) * xi_val * S / a[i]
            o2 = k * xi_val * S / b[i]
            o3 = (xi_val + yi_val) * S / c[i]
            oi = max(o1, o2, o3)
            # print(f"node {i}: Overhead: {oi:.4f} (Overhead_down: {o1:.4f}, Overhead_gf: {o2:.4f}, Overhead_up: {o3:.4f})")
        
        x_values = [solution.get_value(xi) for xi in x]
        y_values = [solution.get_value(yi)  for yi in y]
        return x_values, y_values
    else:
        assert (false and "no solution")

