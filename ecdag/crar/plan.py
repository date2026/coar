import docplex.mp.model as cpx

SLICE_NUM = 16

def solve_lp_problem(n, k, S, a, b, c):
    """
    n: node num
    k: 
    S: chunk size
    a: download bandwidth
    b: gf bandwidth
    c: upload bandwidth
    x: repair ratio
    y: help ratio
    """

    """
    TODO: 
    modify download, gf, upload bandwidths to real
    is three bandwidth all MB/s?
    """
    assert(len(a) == n)


    model = cpx.Model(name="node_allocation")
    
    x_int = model.integer_var_list(n, lb=0, ub=SLICE_NUM, name="x_int")
    y_int = model.integer_var_list(n, lb=0, ub=SLICE_NUM, name="y_int")
    
    x = [xi / SLICE_NUM for xi in x_int]
    y = [yi / SLICE_NUM for yi in y_int]
    
    for i in range(n):
        model.add_constraint(x_int[i] + y_int[i] <= 16, f"sum_xy_{i}_le_16")
    
    
    # repair ratio constraint: x1 + x2 + ... + xn = 1
    model.add_constraint(model.sum(x) == 1, "sum_x")

    # help ratio constraint: x1 + x2 + ... + xn = k
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
        print(x_values, y_values)
        return x_values, y_values
    else:
        assert (false and "no solution")

