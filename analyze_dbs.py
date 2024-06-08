import argparse
from pathlib import Path

from .common import get_files


class AnalyzeDBs:
    def __init__(
        self,
        environment: str,
        rule_path: Path,
        input_path: Path,
        timing_path: Path,
        souffle_include_dir: str,
    ):
        self.environment = environment
        self.rule_path = rule_path
        self.input_path = input_path
        self.timing_path = timing_path
        self.souffle_include_dir = souffle_include_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--size', type=int, default=100, help='Size of the input graph. Default is 100.'
    )
    parser.add_argument(
        '--mode',
        type=str,
        default='right_recursion',
        help='Mode of the rule file to use. Default is right_recursion.',
    )
    parser.add_argument(
        '--graph-type', type=str, required=True, help='Type of graph to analyze'
    )
    parser.add_argument(
        '--environment',
        choices=['clingo', 'xsb', 'souffle', 'postgres'],
        required=True,
        help='Logic programming environment to use. Choose from clingo, xsb, postgres, or souffle.',
    )
    args = parser.parse_args()

    rule_path, input_path, timing_path = get_files(
        args.environment, args.mode, args.graph_type, args.size
    )
