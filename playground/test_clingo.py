import clingo
import time

def solve_and_measure(input_file: str, output_file: str):
    ctl = clingo.Control()
    models: list[str] = []

    # 1. Measure Facts Loading
    t_start = time.perf_counter()
    ctl.load(input_file)
    t_load = time.perf_counter() - t_start

    # 2. Measure rules loading (if any)
    t_start = time.perf_counter()
    ctl.load('rules.lp')  # Assuming rules are in a separate file
    t_rules_load = time.perf_counter() - t_start

    # 3. Measure Grounding
    t_start = time.perf_counter()
    ctl.ground([('base', [])])
    t_ground = time.perf_counter() - t_start

    # 4. Measure Solving (with callback to collect models)
    t_start = time.perf_counter()
    ctl.solve(on_model=lambda m: models.append(str(m)))
    t_solve = time.perf_counter() - t_start

    # 5. Measure Writing to File
    t_start = time.perf_counter()
    with open(output_file, 'w', newline='') as f:
        for model in models:
            f.write(model + '\n')
    t_write = time.perf_counter() - t_start

    print(ctl.statistics )  
    # Reporting
    print(f'Load: {t_load:.4f}s | Rules: {t_rules_load:.4f}s | Ground: {t_ground:.4f}s | Solve: {t_solve:.4f}s | Write: {t_write:.4f}s')


if __name__ == '__main__':
    solve_and_measure('facts.lp', 'clingo.txt')
    # import json

    # ctl = clingo.Control(['--stats'])
    # ctl.add("base", [], "{a; b; c}.")
    # ctl.ground([("base", [])])
    # ctl.solve()

    # # Use json.dumps to see the full nested structure of available metrics
    # stats_json = json.dumps(ctl.statistics, indent=4)
    # print(stats_json)

