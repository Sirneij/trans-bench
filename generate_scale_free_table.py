import pandas as pd
from pathlib import Path
import csv
import re

environments = ['cockroachdb', 'neo4j', 'mariadb', 'postgres', 'duckdb', 'xsb']
env_names = {
    'cockroachdb': 'CockroachDB',
    'neo4j': 'Neo4J',
    'mariadb': 'MariaDB',
    'postgres': 'PostgreSQL',
    'duckdb': 'DuckDB',
    'xsb': 'XSB'
}

def get_query_time(env, headers, values):
    try:
        if env == 'neo4j':
            idx = headers.index('WriteResultRealTime')
        else:
            if env == 'xsb':
                idx = headers.index('QueryRealTime')
            else:
                idx = headers.index('ExecuteQueryRealTime')
        return float(values[idx + 1])
    except ValueError:
        return None

def generate():
    data = {}
    pattern = re.compile(r'^timing_(.*?)_graph_(\d+)\.csv$')
    
    for file in Path('timing').rglob('*scale_free*/**/*.csv'):
        if file.name.startswith('timing_') and 'graph' in file.name:
            match = pattern.match(file.name)
            if not match: continue
            
            mode = match.group(1)
            size = int(match.group(2))
            env = file.parts[1]
            
            if env not in environments:
                continue
                
            with open(file) as f:
                reader = csv.reader(f)
                lines = list(reader)
                if len(lines) > 1 and lines[-1][0] == 'Average':
                    headers = lines[0]
                    avg_vals = lines[-1]
                    val = get_query_time(env, headers, avg_vals)
                    if val is not None:
                        if mode not in data:
                            data[mode] = {}
                        if size not in data[mode]:
                            data[mode][size] = {}
                        data[mode][size][env] = val

    latex_lines = [
        "\\documentclass[varwidth=6.5in]{standalone}",
        "\\usepackage{booktabs}",
        "\\usepackage{caption}",
        "\\usepackage{adjustbox}",
        "\\begin{document}",
        "",
        "\\begin{table}[htpb]",
        "\\centering",
        "\\caption*{Query execution times (in seconds) for Left and Right Recursion on scale-free graphs. Missing entries ($-$) indicate execution was manually aborted due to excessive runtime or Out-of-Memory.}",
        "\\label{tab:scale_free_side_by_side}",
        ""
    ]

    modes_to_print = ['left_recursion', 'right_recursion']
    
    for i, mode in enumerate(modes_to_print):
        if mode not in data:
            continue
        size_data = data[mode]
        sizes = sorted(list(size_data.keys()))
        
        latex_lines.extend([
            "\\begin{minipage}[t]{0.48\\textwidth}",
            "\\centering",
            f"\\textbf{{{mode.replace('_', ' ').title()}}}",
            "",
            "\\vspace{0.2cm}",
            "\\begin{adjustbox}{max width=\\linewidth}",
            "\\begin{tabular}{lrrrrrr}",
            "\\toprule",
            "\\textbf{Graph Size} & " + " & ".join(f"\\textbf{{{env_names[env]}}}" for env in environments) + " \\\\",
            "\\midrule"
        ])
        
        for size in sizes:
            row = [f"{size:,}"]
            for env in environments:
                val = size_data[size].get(env, None)
                if val is None:
                    row.append("-")  # denote missing/closed
                else:
                    if val > 100:
                        row.append(f"{val:.1f}")
                    elif val > 1:
                        row.append(f"{val:.2f}")
                    else:
                        row.append(f"{val:.4f}")
            latex_lines.append(" & ".join(row) + " \\\\")
            
        latex_lines.extend([
            "\\bottomrule",
            "\\end{tabular}",
            "\\end{adjustbox}",
            "\\end{minipage}" + ("\\hfill" if i == 0 else "")
        ])
        
    latex_lines.extend([
        "",
        "\\end{table}",
        "",
        "\\end{document}"
    ])
    
    output_file = Path('scale_free_tables_standalone.tex')
    output_file.write_text('\n'.join(latex_lines))
    print(f"Generated {output_file}")

if __name__ == '__main__':
    generate()
