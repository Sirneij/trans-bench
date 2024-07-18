### What happens?

When running recursive queries on a binary tree graph using DuckDB, results diverge between double recursion and left recursion for graphs with more than 15 nodes. This discrepancy also appears in reversed binary tree, cycle, cycles with extras, path, path disjoint, grid, and y graphs at different number of nodes but does not appear with complete, maximum acyclic, w, and x graphs, where both recursion variants yield the same results.

### To Reproduce

#### Reproduction Steps

1. **Generate a Binary Tree Graph**:

   ```python
   from typing import Generator
   import math

   def generate_binary_tree_graph(n: int) -> Generator[tuple[int, int], None, None]:
       h = math.floor(math.log2(n))
       parent_count = 2 ** (h - 1) - 1
       for i in range(1, parent_count + 1):
           yield (i, 2 * i)
           yield (i, 2 * i + 1)
   ```

   - Sample data created with 4 to 36 nodes (2 to 8 heights) and saved in tab-separated format `edge.facts`.

2. **Create SQL Script `test_duckdb_d.sql`**:

   ```sql
   CREATE TABLE edge (x INTEGER, y INTEGER);
   COPY edge FROM '{data_file}' (DELIMITER '\t');
   CREATE INDEX edge_yx ON edge (y, x);
   CREATE TABLE tc_result AS
   WITH RECURSIVE tc AS (
       SELECT x, y FROM edge
       UNION
       SELECT tc1.x, tc2.y FROM tc AS tc1, tc AS tc2 WHERE tc1.y = tc2.x
   )
   SELECT * FROM tc;
   COPY (SELECT * FROM tc_result) TO '{output_file}' WITH (HEADER, DELIMITER ',');
   ```

3. **Run the Test with Python Script**:

   ```python
   import duckdb
   import os
   import csv

   def measure_with_duckdb(sql_script_filename: str):
       prefix = sql_script_filename.split('.')[0]
       conn = duckdb.connect(database=f'{prefix}.db')

       with open(sql_script_filename, 'r') as f:
           sql_script = f.read()

       results_path = f'{prefix}.csv'
       sql_script = sql_script.replace('{data_file}', 'edge.facts')
       sql_script = sql_script.replace('{output_file}', results_path)

       commands = [f'{cmd.strip()};' for cmd in sql_script.split(';') if cmd.strip()]

       for cmd in commands:
           try:
               print(f"Executing command: {cmd}")
               conn.sql(cmd)
           except Exception as e:
               print(f'Error: {e}')
       conn.close()
       os.remove(f'{prefix}.db')

    def csv_files_are_equal(file1, file2):
        with open(file1, 'r') as f1, open(file2, 'r') as f2:
            reader1 = csv.reader(f1)
            reader2 = csv.reader(f2)

            for row1, row2 in zip(reader1, reader2):
                if row1 != row2:
                    return False

            # Check if any file still has data left
            try:
                if next(reader1) or next(reader2):
                    return False
            except StopIteration:
                pass

        return True
   if __name__ == '__main__':
       s_filenames = ['test_duckdb_d.sql', 'test_duckdb_l.sql']
       for s_filename in s_filenames:
           measure_with_duckdb(s_filename)

        # Check if the results are the same
        print(csv_files_are_equal('test_duckdb_d.csv', 'test_duckdb_l.csv'))
   ```

4. **Comparison of Results**:

   - For 2 to 15 nodes, results are consistent between double recursion and left recursion (in `test_duckdb_l.sql`):

     ```sql
     CREATE TABLE tc_result AS
     WITH RECURSIVE tc AS (
         SELECT x, y FROM edge
         UNION
         SELECT tc.x, edge.y FROM tc JOIN edge ON tc.y = edge.x
     )
     SELECT * FROM tc;
     ```

   - For 16 nodes, results diverge:
     | Left recursion | Double recursion |
     | -------------- | ---------------- |
     | x,y | x,y |
     | 1,2 | 1,2 |
     | 1,3 | 1,3 |
     | 2,4 | 2,4 |
     | 2,5 | 2,5 |
     | 3,6 | 3,6 |
     | 3,7 | 3,7 |
     | 4,8 | 4,8 |
     | 4,9 | 4,9 |
     | 5,10 | 5,10 |
     | 5,11 | 5,11 |
     | 6,12 | 6,12 |
     | 6,13 | 6,13 |
     | 7,14 | 7,14 |
     | 7,15 | 7,15 |
     | 1,4 | 1,4 |
     | 1,5 | 1,5 |
     | 1,6 | 1,6 |
     | 1,7 | 1,7 |
     | 2,8 | 2,8 |
     | 2,9 | 2,9 |
     | 2,10 | 2,10 |
     | 2,11 | 2,11 |
     | 3,12 | 3,12 |
     | 3,13 | 3,13 |
     | 3,14 | 3,14 |
     | 3,15 | 3,15 |
     | 1,8 | |
     | 1,9 | |
     | 1,10 | |
     | 1,11 | |
     | 1,12 | |
     | 1,13 | |
     | 1,14 | |
     | 1,15 | |

#### Request

Please investigate the cause of this discrepancy in results between the double recursion and left recursion CTEs for the graphs. The same test cases yield consistent results for complete, maximum acyclic, w, and x graphs.

For reference, the code for generating each of these graph types can be found in this [class](https://github.com/Sirneij/trans-bench/blob/c43733c9ccb27ce819017b6c6b56598caeb25153/generate_db.py#L15).

### OS:

MacOS Sonoma

### DuckDB Version:

1

### DuckDB Client:

Python

### Full Name:

John Owolabi Idogun

### Affiliation:

Stony Brook University

### What is the latest build you tested with? If possible, we recommend testing with the latest nightly build.

I have tested with a stable release

### Did you include all relevant data sets for reproducing the issue?

Yes

### Did you include all code required to reproduce the issue?

- [x] Yes, I have

### Did you include all relevant configuration (e.g., CPU architecture, Python version, Linux distribution) to reproduce the issue?

- [x] Yes, I have
