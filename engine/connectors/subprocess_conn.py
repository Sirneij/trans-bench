"""
Run trials on the logic systems, which run as separate processes.

The connectors are:
  - XSBConnector       (protocol: subprocess)
  - ClingoConnector    (protocol: clingo_python)
  - SouffleConnector   (protocol: souffle_subprocess)
  - AldaConnector      (protocol: alda_subprocess)
"""

from __future__ import annotations

import gc
import logging
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from engine.connectors.base import BaseConnector

if TYPE_CHECKING:
    from engine.loader import SystemDescriptor

log = logging.getLogger(__name__)


def _extract(pattern: str, text: str) -> float:
    """Return the number captured by `pattern` in `text`, or 0.0 if it does not occur."""
    m = re.search(pattern, text)
    return float(m.group(1)) if m else 0.0


def _field(key: str, text: str) -> float:
    """Return the number printed as `<key>: <number>` in a system's output, or 0.0."""
    return _extract(rf'{key}:\s+(-?\d+\.?\d*(?:e[+-]?\d+)?)', text or '')


# ─────────────────────────────────────────────────────────────────────────────
# XSB
# ─────────────────────────────────────────────────────────────────────────────


class XSBConnector(BaseConnector):
    """Runs transitive closure via XSB Prolog subprocesses."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        """Do nothing: every XSB run is a new process."""
        log.info('XSB connector ready (no persistent connection needed)')

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
        query_bindings: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        """Run XSB twice (query only, then query and write) and split the times into four phases."""
        xsb_export_path = rule_path.parent / 'xsb_export'
        cmd1, cmd2 = self._commands(
            rule_path,
            input_path,
            config.get('queries', '[[query1, path(X, Y)]]'),
            self.result_path(output_folder, descriptor, 'xsb_results.txt'),
        )

        # XSB evaluates the query twice: once without writing (query time) and once with writing
        # the result (write time = difference). Timings are printed by xsb_export/extfilequery.P.
        samples: list[tuple[float, float]] = []
        real1, _, mem1, out1 = self.timed_subprocess(cmd1, samples=samples)
        _, _, mem2, out2 = self.timed_subprocess(cmd2)
        self._check_output('query only', out1, 'QueryOnlyTime')
        self._check_output('query and write', out2, 'QueryAndWriteTime')

        qonly_real = _field('QueryOnlyTime', out1.stdout)
        qonly_cpu = _field('CPUQueryOnlyTime', out1.stdout)
        # write time = (query and write) - (query only)
        measurements = [
            (_field('LoadRuleTime', out1.stdout), _field('CPULoadRuleTime', out1.stdout)),
            (_field('LoadFactsTime', out1.stdout), _field('CPULoadFactsTime', out1.stdout)),
            (qonly_real, qonly_cpu),
            (
                _field('QueryAndWriteTime', out2.stdout) - qonly_real,
                _field('CPUTimeQueryAndWriteTime', out2.stdout) - qonly_cpu,
            ),
        ]

        self.memory = self._query_memory(samples, real1, qonly_real)

        # Cleanup compiled .xwam files
        for f in rule_path.parent.glob('*.xwam'):
            f.unlink(missing_ok=True)
        for f in xsb_export_path.glob('*.xwam'):
            f.unlink(missing_ok=True)

        gc.collect()
        n = len(descriptor.timing_phases)
        memory = [0.0, 0.0, mem1, max(0.0, mem2 - mem1)]
        return self.build_timing_row(descriptor.timing_phases, measurements[:n], memory=memory[:n])

    def _check_output(self, label: str, out, key: str) -> None:
        """Record an error if an XSB run failed or did not print its `key` timing."""
        if out.returncode != 0 or f'{key}:' not in (out.stdout or ''):
            tail = ' '.join(((out.stderr or '') + (out.stdout or '')).split())[-500:]
            self._record_error(f'XSB {label} run failed (exit code {out.returncode}): {tail}')

    @staticmethod
    def _commands(rule_path: Path, input_path: Path, queries: str, results_path: Path) -> tuple[list, list]:
        """Return the XSB command lines of the query-only run and of the query-and-write run."""
        base_args = [
            'xsb',
            '--nobanner',
            '--quietload',
            '--noprompt',
            '-e',
            f"add_lib_dir('{rule_path.parent / 'xsb_export'}').",
        ]
        args = f"('{rule_path}','{input_path}',{queries},'{results_path}')."
        return (
            base_args + ['-e', f'extfilequery:external_file_query_only{args}'],
            base_args + ['-e', f'extfilequery:external_file_query{args}'],
        )

    @staticmethod
    def _query_memory(samples: list, run_s: float, query_s: float) -> dict:
        """
        Compute the memory of the query from the RSS samples (every 10 ms) of the query-only process.

        The value is the peak RSS minus the RSS just before the query started. The query ends right
        before XSB prints its timings and halts, so it started about run_s - query_s seconds after
        the process.
        """
        if not samples:
            return {'probe': 'xsb process RSS (query-only run)', 'error': 'no samples'}
        t_query = max(0.0, run_s - query_s)
        before = max((m for t, m in samples if t <= t_query), default=samples[0][1])
        peak = max(m for _, m in samples)
        mb = 1024 * 1024
        return {
            'probe': 'xsb process RSS (query-only run)',
            'before_mb': round(before / mb, 3),
            'peak_mb': round(peak / mb, 3),
            'used_mb': round((peak - before) / mb, 3),
            'samples': len(samples),
            'interval_s': 0.01,
        }

    def close(self) -> None:
        """Do nothing: there is no connection to close."""


# ─────────────────────────────────────────────────────────────────────────────
# Clingo
# ─────────────────────────────────────────────────────────────────────────────


class ClingoConnector(BaseConnector):
    """Runs transitive closure via the clingo Python library."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        """Do nothing: every Clingo run is a new process (clingo_runner.py)."""
        log.info('Clingo connector ready')

    def _patch_rule_file(self, rule_path: Path, queries: str) -> Optional[str]:
        """Return a patched temp file path if query substitution is needed, else None."""
        queries_pattern = re.compile(r'path\(\s*\w+\s*,\s*\w+\s*\)')
        match = queries_pattern.search(queries)
        if not match:
            return None
        query = match.group()
        if query.lower() in ('path(x, y)', 'path(x,y)'):
            return None

        content = rule_path.read_text(encoding='utf-8')
        content = re.sub(
            r'#show path/\d+\.',
            f'ppath(X) :- {query}.\n\n#show ppath/1.',
            content,
        )
        with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.lp', encoding='utf-8') as f:
            f.write(content)
            return f.name

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
        query_bindings: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        """Run clingo_runner.py in its own process and read the five phase times it prints."""
        queries = config.get('queries', '[[query1, path(X, Y)]]')
        temp_path = self._patch_rule_file(rule_path, queries)
        effective_rule = temp_path if temp_path else str(rule_path)

        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)
        memory: list[float] = [0.0] * len(phases)

        try:
            output_file = output_folder / 'clingo_results.txt'
            runner_script = Path(__file__).parent / 'clingo_runner.py'

            cmd = [sys.executable, str(runner_script), effective_rule, str(input_path), str(output_file)]
            _, _, mem, result = self.timed_subprocess(cmd)
            for i, (real_key, cpu_key) in enumerate(
                (
                    ('LoadRuleTime', 'CPULoadRuleTime'),
                    ('LoadFactsTime', 'CPULoadFactsTime'),
                    ('GroundTime', 'CPUGroundTime'),
                    ('QueryTime', 'CPUQueryTime'),
                    ('WriteTime', 'CPUWriteTime'),
                )
            ):
                measurements[i] = (_field(real_key, result.stdout), _field(cpu_key, result.stdout))

            # Attribute peak memory to the Solve (Query) phase
            memory[3] = mem

        except Exception as e:
            log.error(f'Clingo experiment error: {e}')
        finally:
            if temp_path:
                os.remove(temp_path)
            gc.collect()

        return self.build_timing_row(phases, measurements[: len(phases)], memory=memory[: len(phases)])

    def close(self) -> None:
        """Do nothing: there is no connection to close."""


# ─────────────────────────────────────────────────────────────────────────────
# Soufflé
# ─────────────────────────────────────────────────────────────────────────────


class SouffleConnector(BaseConnector):
    """Compiles and runs Soufflé Datalog programs."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        """Do nothing: every Souffle step is a new process."""
        log.info('Soufflé connector ready')

    def _patch_rule_file(self, rule_path: Path, queries: str) -> Optional[str]:
        """Return a patched temp file path if query substitution is needed, else None."""
        queries_pattern = re.compile(r'path\(\s*\w+\s*,\s*\w+\s*\)')
        match = queries_pattern.search(queries)
        if not match:
            return None
        query = match.group().lower()
        if query in ('path(x, y)', 'path(x,y)'):
            return None

        content = rule_path.read_text(encoding='utf-8')
        content = re.sub(
            r'\.output path',
            f'.decl ppath(x:number)\nppath(x) :- {query}.\n\n.output ppath',
            content,
        )
        with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.dl', encoding='utf-8') as f:
            f.write(content)
            return f.name

    def _run_cmd(self, cmd: list[Any]) -> tuple[tuple[float, float], dict, float]:
        """Run one step; return ((real, cpu), the times the step printed, max_rss_mb), zeros on failure."""
        try:
            real, cpu, max_rss, result = self.timed_subprocess([str(c) for c in cmd])
            if result.returncode != 0:
                log.error(f'Soufflé cmd failed: {result.stderr}')
                return (0.0, 0.0), {}, 0.0
            timing = {k: float(v) for k, v in re.findall(r'(\w+ time): (\d+\.\d+) seconds', result.stdout)}
            return (real, cpu), timing, max_rss
        except Exception as e:
            log.error(f'Soufflé cmd failed: {e}')
            return (0.0, 0.0), {}, 0.0

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
        query_bindings: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        """Translate the program to C++, compile it, run the binary, and read the times it prints."""
        queries = config.get('queries', '[[query1, path(X, Y)]]')
        include_dir = config.get('souffle_include_dir', '/opt/homebrew/Cellar/souffle/HEAD-8abf896/include')
        export_path = rule_path.parent / 'souffle_export'
        export_file = export_path / 'main'
        generated_cpp = export_path / 'souffle_generated.cpp'

        temp_path = self._patch_rule_file(rule_path, queries)
        effective_rule = temp_path if temp_path else str(rule_path)

        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)
        memory: list[float] = [0.0] * len(phases)

        try:
            # phase 0: Datalog to C++
            measurements[0], _, memory[0] = self._run_cmd(
                ['souffle', effective_rule, '-F', input_path, '-w', '-g', generated_cpp, '-D', output_folder]
            )
            # phase 1: compile the C++ program
            measurements[1], _, memory[1] = self._run_cmd(
                [
                    'g++',
                    f'{export_file}.cpp',
                    generated_cpp,
                    '-std=c++17',
                    '-I',
                    include_dir,
                    '-o',
                    export_file,
                    '-D__EMBEDDED_SOUFFLE__',
                ]
            )
            # phases 2 to 5: run the binary, which prints the time of each of them
            _, run_timing, run_mem = self._run_cmd([export_file, input_path])
            measurements[2] = (run_timing.get('Instance time', 0.0), run_timing.get('InstanceCPU time', 0.0))
            measurements[3] = (run_timing.get('LoadingFacts time', 0.0), run_timing.get('LoadingFactsCPU time', 0.0))
            measurements[4] = (run_timing.get('Query time', 0.0), run_timing.get('QueryCPU time', 0.0))
            measurements[5] = (run_timing.get('Writing time', 0.0), run_timing.get('WritingCPU time', 0.0))
            memory[4] = run_mem  # Assign peak execution memory to the Query phase
        except Exception as e:
            log.error(f'Soufflé experiment error: {e}')
        finally:
            export_file.unlink(missing_ok=True)
            generated_cpp.unlink(missing_ok=True)
            if temp_path:
                os.remove(temp_path)
            gc.collect()

        return self.build_timing_row(phases, measurements[: len(phases)], memory=memory[: len(phases)])

    def close(self) -> None:
        """Do nothing: there is no connection to close."""


# ─────────────────────────────────────────────────────────────────────────────
# Alda (DistAlgo)
# ─────────────────────────────────────────────────────────────────────────────


class AldaConnector(BaseConnector):
    """Runs transitive closure via Alda (DistAlgo) subprocesses."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        """Do nothing: every ALDA run is a new process."""
        log.info('Alda connector ready')

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
        query_bindings: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        """Run the ALDA program once and split its time into the four phases."""
        # Parse size from input path for buffer sizing
        try:
            size = int(input_path.stem.split('_')[-1])
        except (ValueError, IndexError):
            size = 1000
        step = config.get('defaults', {}).get('step', 10)

        # Extract mode and graph_type from the rule_path stem
        parts = rule_path.stem.split('_', 1)
        mode = parts[1] if len(parts) > 1 else 'right_recursion'
        graph_type = input_path.parent.name if input_path.parent.name != 'alda' else 'cycle'

        cmd = [
            sys.executable,
            '-m',
            'da',
            '-r',
            f'--message-buffer-size={max(size // max(step, 1), 1)}409600',
            '--rules',
            str(rule_path),
            '--size',
            str(size),
            '--mode',
            mode,
            '--graph-type',
            graph_type,
        ]

        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)
        memory: list[float] = [0.0] * len(phases)

        real, cpu, mem, result = self.timed_subprocess(cmd)

        # ALDA prints its phase times when it can. When it does not, the whole run's time is split
        # 10/20/60/10 over the phases: an estimate, not a measurement. The CPU time is always split.
        load_rules_real = _field('LoadRuleTime', result.stdout) or (real * 0.1)
        load_facts_real = _field('LoadFactsTime', result.stdout) or (real * 0.2)
        query_real = _field('QueryOnlyTime', result.stdout) or (real * 0.6)
        write_real = _field('WriteTime', result.stdout) or (real * 0.1)

        measurements = [
            (load_rules_real, cpu * 0.1),
            (load_facts_real, cpu * 0.2),
            (query_real, cpu * 0.6),
            (write_real, cpu * 0.1),
        ]

        # Attribute all memory to the query phase since it's a single run
        memory[2] = mem

        return self.build_timing_row(phases, measurements[: len(phases)], memory=memory[: len(phases)])

    def close(self) -> None:
        """Do nothing: there is no connection to close."""
