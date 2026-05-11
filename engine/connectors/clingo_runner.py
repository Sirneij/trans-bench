import os
import sys


def _estimate_os_times(
    t1: tuple[float, float, float, float, float], t2: tuple[float, float, float, float, float]
) -> tuple[float, float]:
    u1, s1, cu1, cs1, e1 = t1
    u2, s2, cu2, cs2, e2 = t2
    return e2 - e1, (u2 - u1) + (s2 - s1) + (cu2 - cu1) + (cs2 - cs1)


def main():
    if len(sys.argv) < 4:
        print("Usage: clingo_runner.py <rule_path> <input_path> <output_file>")
        sys.exit(1)

    rule_path = sys.argv[1]
    input_path = sys.argv[2]
    output_file = sys.argv[3]

    import clingo

    ctl = clingo.Control()
    models: list[str] = []

    t0 = os.times()
    ctl.load(rule_path)
    t1 = os.times()
    rule_real, rule_cpu = _estimate_os_times(t0, t1)

    t0 = os.times()
    ctl.load(input_path)
    t1 = os.times()
    fact_real, fact_cpu = _estimate_os_times(t0, t1)

    t0 = os.times()
    ctl.ground([('base', [])])
    t1 = os.times()
    ground_real, ground_cpu = _estimate_os_times(t0, t1)

    ctl.configuration.solve.models = '0'
    t0 = os.times()
    ctl.solve(on_model=lambda m: models.append(str(m)))
    t1 = os.times()
    solve_real, solve_cpu = _estimate_os_times(t0, t1)

    t0 = os.times()
    with open(output_file, 'w', newline='') as f:
        for model in models:
            f.write(model + '\n')
    t1 = os.times()
    write_real, write_cpu = _estimate_os_times(t0, t1)

    print(f"LoadRuleTime: {rule_real}")
    print(f"CPULoadRuleTime: {rule_cpu}")
    print(f"LoadFactsTime: {fact_real}")
    print(f"CPULoadFactsTime: {fact_cpu}")
    print(f"GroundTime: {ground_real}")
    print(f"CPUGroundTime: {ground_cpu}")
    print(f"QueryTime: {solve_real}")
    print(f"CPUQueryTime: {solve_cpu}")
    print(f"WriteTime: {write_real}")
    print(f"CPUWriteTime: {write_cpu}")


if __name__ == "__main__":
    main()
