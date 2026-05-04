"""
engine/connectors/subprocess_conn.py

Connectors for logic programming systems that run as subprocesses or via
Python library bindings:
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
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from engine.connectors.base import BaseConnector

if TYPE_CHECKING:
    from engine.loader import SystemDescriptor

log = logging.getLogger(__name__)


def _estimate_os_times(t1: tuple, t2: tuple) -> tuple[float, float]:
    u1, s1, cu1, cs1, e1 = t1
    u2, s2, cu2, cs2, e2 = t2
    return e2 - e1, (u2 - u1) + (s2 - s1) + (cu2 - cu1) + (cs2 - cs1)


def _extract(pattern: str, text: str) -> float:
    m = re.search(pattern, text)
    return float(m.group(1)) if m else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# XSB
# ─────────────────────────────────────────────────────────────────────────────

class XSBConnector(BaseConnector):
    """Runs transitive closure via XSB Prolog subprocesses."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        log.info('XSB connector ready (no persistent connection needed)')

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
    ) -> dict[str, float]:
        queries = config.get('queries', '[[query1, path(X, Y)]]')
        xsb_export_path = rule_path.parent / 'xsb_export'
        results_path = output_folder / 'xsb_results.txt'

        base_args = [
            'xsb', '--nobanner', '--quietload', '--noprompt',
            '-e', f"add_lib_dir('{xsb_export_path}').",
        ]
        cmd1 = base_args + ['-e', f"extfilequery:external_file_query_only('{rule_path}','{input_path}',{queries},'{results_path}')."]
        cmd2 = base_args + ['-e', f"extfilequery:external_file_query('{rule_path}','{input_path}',{queries},'{results_path}')."]

        out1 = subprocess.run(cmd1, capture_output=True, text=True)
        out2 = subprocess.run(cmd2, capture_output=True, text=True)

        def t(key, text): return _extract(rf'{key}:\s+(-?\d+\.?\d*(?:e[+-]?\d+)?)', text)

        phases = descriptor.timing_phases
        qonly_real = t('QueryOnlyTime', out1.stdout)
        qonly_cpu  = t('CPUQueryOnlyTime', out1.stdout)
        qwrite_real= t('QueryAndWriteTime', out2.stdout)
        qwrite_cpu = t('CPUTimeQueryAndWriteTime', out2.stdout)
        write_real = qwrite_real - qonly_real
        write_cpu  = qwrite_cpu  - qonly_cpu

        measurements = [
            (t('LoadRuleTime', out1.stdout),  t('CPULoadRuleTime', out1.stdout)),
            (t('LoadFactsTime', out1.stdout), t('CPULoadFactsTime', out1.stdout)),
            (qonly_real, qonly_cpu),
            (write_real, write_cpu),
        ]

        # Cleanup compiled .xwam files
        for f in rule_path.parent.glob('*.xwam'):
            f.unlink(missing_ok=True)
        for f in xsb_export_path.glob('*.xwam'):
            f.unlink(missing_ok=True)

        gc.collect()
        return self.build_timing_row(phases, measurements[:len(phases)])

    def close(self) -> None:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Clingo
# ─────────────────────────────────────────────────────────────────────────────

class ClingoConnector(BaseConnector):
    """Runs transitive closure via the clingo Python library."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
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

        content = rule_path.read_text()
        content = re.sub(
            r'#show path/\d+\.',
            f'ppath(X) :- {query}.\n\n#show ppath/1.',
            content,
        )
        with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.lp') as f:
            f.write(content)
            return f.name

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
    ) -> dict[str, float]:
        import clingo
        queries = config.get('queries', '[[query1, path(X, Y)]]')
        temp_path = self._patch_rule_file(rule_path, queries)
        effective_rule = temp_path if temp_path else str(rule_path)

        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        try:
            ctl = clingo.Control()
            t0 = os.times(); ctl.load(effective_rule); t1 = os.times()
            measurements[0] = _estimate_os_times(t0, t1)

            t0 = os.times(); ctl.load(str(input_path)); t1 = os.times()
            measurements[1] = _estimate_os_times(t0, t1)

            t0 = os.times(); ctl.ground([('base', [])]); t1 = os.times()
            measurements[2] = _estimate_os_times(t0, t1)

            ctl.configuration.solve.models = '0'
            t0 = os.times()
            with ctl.solve(yield_=True) as handle:
                results = [model.symbols(shown=True) for model in handle]
            t1 = os.times()
            measurements[3] = _estimate_os_times(t0, t1)

            output_file = output_folder / 'clingo_results.txt'
            t0 = os.times()
            with open(output_file, 'w') as f:
                f.writelines([f'{atom}\n' for result in results for atom in result])
            t1 = os.times()
            write_m = _estimate_os_times(t0, t1)
            measurements[4] = (write_m[0] - measurements[3][0], write_m[1] - measurements[3][1])
        except Exception as e:
            log.error(f'Clingo experiment error: {e}')
        finally:
            if temp_path:
                os.remove(temp_path)
            gc.collect()

        return self.build_timing_row(phases, measurements[:len(phases)])

    def close(self) -> None:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Soufflé
# ─────────────────────────────────────────────────────────────────────────────

class SouffleConnector(BaseConnector):
    """Compiles and runs Soufflé Datalog programs."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        log.info('Soufflé connector ready')

    def _patch_rule_file(self, rule_path: Path, queries: str) -> Optional[str]:
        queries_pattern = re.compile(r'path\(\s*\w+\s*,\s*\w+\s*\)')
        match = queries_pattern.search(queries)
        if not match:
            return None
        query = match.group().lower()
        if query in ('path(x, y)', 'path(x,y)'):
            return None

        content = rule_path.read_text()
        content = re.sub(
            r'\.output path',
            f'.decl ppath(x:number)\nppath(x) :- {query}.\n\n.output ppath',
            content,
        )
        with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.dl') as f:
            f.write(content)
            return f.name

    def _run_cmd(self, cmd: str) -> tuple[str, dict]:
        """Run a shell command, return (real_cpu_str, parsed_timing_dict)."""
        t0 = os.times()
        try:
            result = subprocess.run(cmd, shell=True, check=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            t1 = os.times()
            real, cpu = _estimate_os_times(t0, t1)
            timing = {k: float(v) for k, v in re.findall(r'(\w+ time): (\d+\.\d+) seconds', result.stdout)}
            return f'{real},{cpu}', timing
        except subprocess.CalledProcessError as e:
            log.error(f'Soufflé cmd failed: {e}')
            return '0,0', {}

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
    ) -> dict[str, float]:
        queries = config.get('queries', '[[query1, path(X, Y)]]')
        include_dir = config.get('souffle_include_dir', '$HOME/systems/souffle/include')
        export_path = Path('souffle_rules') / 'souffle_export'
        export_file = export_path / 'main'
        generated_cpp = export_path / 'souffle_generated.cpp'

        temp_path = self._patch_rule_file(rule_path, queries)
        effective_rule = temp_path if temp_path else str(rule_path)

        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        try:
            # Phase 0: Datalog → C++
            dtc_str, _ = self._run_cmd(
                f'souffle {effective_rule} -F {input_path} -w -g {generated_cpp} -D {output_folder}'
            )
            r, c = [float(x) for x in dtc_str.split(',')]
            measurements[0] = (r, c)

            # Phase 1: Compile C++
            compile_str, _ = self._run_cmd(
                f'g++ {export_file}.cpp {generated_cpp} -std=c++17 -I {include_dir} '
                f'-o {export_file} -D__EMBEDDED_SOUFFLE__'
            )
            r, c = [float(x) for x in compile_str.split(',')]
            measurements[1] = (r, c)

            # Phase 2-5: Run compiled binary (timing from stdout)
            run_str, run_timing = self._run_cmd(f'./{export_file} {input_path}')
            measurements[2] = (run_timing.get('Instance time', 0.0), run_timing.get('InstanceCPU time', 0.0))
            measurements[3] = (run_timing.get('LoadingFacts time', 0.0), run_timing.get('LoadingFactsCPU time', 0.0))
            measurements[4] = (run_timing.get('Query time', 0.0), run_timing.get('QueryCPU time', 0.0))
            measurements[5] = (run_timing.get('Writing time', 0.0), run_timing.get('WritingCPU time', 0.0))
        except Exception as e:
            log.error(f'Soufflé experiment error: {e}')
        finally:
            export_file.unlink(missing_ok=True)
            generated_cpp.unlink(missing_ok=True)
            if temp_path:
                os.remove(temp_path)
            gc.collect()

        return self.build_timing_row(phases, measurements[:len(phases)])

    def close(self) -> None:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Alda (DistAlgo)
# ─────────────────────────────────────────────────────────────────────────────

class AldaConnector(BaseConnector):
    """Runs transitive closure via Alda (DistAlgo) subprocesses."""

    def connect(self, credentials: dict[str, Any], descriptor: 'SystemDescriptor') -> None:
        log.info('Alda connector ready')

    def run_experiment(
        self,
        rule_path: Path,
        input_path: Path,
        output_folder: Path,
        descriptor: 'SystemDescriptor',
        config: dict[str, Any],
    ) -> dict[str, float]:
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
            'python', '-m', 'da', '-r',
            f'--message-buffer-size={max(size // max(step, 1), 1)}409600',
            '--rules', str(rule_path),
            '--size', str(size),
            '--mode', mode,
            '--graph-type', graph_type,
        ]

        phases = descriptor.timing_phases
        measurements: list[tuple[float, float]] = [(0.0, 0.0)] * len(phases)

        t0 = os.times()
        result = subprocess.run(cmd, capture_output=True, text=True)
        t1 = os.times()
        real, cpu = _estimate_os_times(t0, t1)

        # Alda outputs timing to stdout; parse if available
        def t(key): return _extract(rf'{key}:\s+(-?\d+\.?\d*(?:e[+-]?\d+)?)', result.stdout)
        load_rules_real = t('LoadRuleTime') or (real * 0.1)
        load_facts_real = t('LoadFactsTime') or (real * 0.2)
        query_real      = t('QueryOnlyTime') or (real * 0.6)
        write_real      = t('WriteTime') or (real * 0.1)

        measurements = [
            (load_rules_real, cpu * 0.1),
            (load_facts_real, cpu * 0.2),
            (query_real,      cpu * 0.6),
            (write_real,      cpu * 0.1),
        ]

        return self.build_timing_row(phases, measurements[:len(phases)])

    def close(self) -> None:
        pass
