"""
engine/parameter_sweep.py

Framework for systematic parameter sweeping in experiments.

Enables users to define parameter variations (e.g., query parameters, modes, sizes)
and automatically expand them into multiple experiment runs.

Example:
    sweep = ParameterSweep(
        query_bindings={'limit': [10, 20, 50, 100]},
    )
    for params in sweep.expand():
        # params = {'limit': '10'}, {'limit': '20'}, ...
        runner.run_experiment(..., query_bindings=params)
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any, Generator


@dataclass
class ParameterSweep:
    """
    Defines a grid of parameter values to sweep over.

    Attributes
    ----------
    query_bindings : dict[str, list]
        Query parameter sweeps. E.g., {'limit': [10, 20, 50]}
    modes : list[str], optional
        If provided, sweep over recursion modes.
    graph_sizes : list[int], optional
        If provided, sweep over graph sizes.
    systems : list[str], optional
        If provided, sweep over systems.
    custom_params : dict[str, list], optional
        Any custom parameters to sweep.
    """

    query_bindings: dict[str, list[Any]] = field(default_factory=dict)
    modes: list[str] | None = None
    graph_sizes: list[int] | None = None
    systems: list[str] | None = None
    custom_params: dict[str, list[Any]] = field(default_factory=dict)

    def expand(self) -> Generator[dict[str, Any], None, None]:
        """
        Expand all parameter combinations into individual parameter sets.

        Yields
        ------
        dict[str, Any]
            A single parameter combination.
        """
        # Prepare parameter lists
        param_sets: dict[str, list] = {}

        # Query bindings sweep
        if self.query_bindings:
            for param_name, values in self.query_bindings.items():
                if not isinstance(values, list):
                    values = [values]
                param_sets[f'query_binding_{param_name}'] = values

        # Modes sweep
        if self.modes:
            param_sets['mode'] = self.modes

        # Graph sizes sweep
        if self.graph_sizes:
            param_sets['size'] = self.graph_sizes

        # Systems sweep
        if self.systems:
            param_sets['system'] = self.systems

        # Custom parameters
        if self.custom_params:
            for param_name, values in self.custom_params.items():
                if not isinstance(values, list):
                    values = [values]
                param_sets[f'custom_{param_name}'] = values

        # Generate all combinations
        if not param_sets:
            yield {}
            return

        keys = list(param_sets.keys())
        values = [param_sets[k] for k in keys]

        for combo in itertools.product(*values):
            result = dict(zip(keys, combo))

            # Convert back to query_bindings format
            query_bindings = {}
            modes = None
            sizes = None
            systems = None
            custom = {}

            for key, val in result.items():
                if key.startswith('query_binding_'):
                    param_name = key.replace('query_binding_', '')
                    query_bindings[param_name] = str(val)
                elif key == 'mode':
                    modes = val
                elif key == 'size':
                    sizes = val
                elif key == 'system':
                    systems = val
                elif key.startswith('custom_'):
                    param_name = key.replace('custom_', '')
                    custom[param_name] = val

            output = {}
            if query_bindings:
                output['query_bindings'] = query_bindings
            if modes:
                output['mode'] = modes
            if sizes is not None:
                output['size'] = sizes
            if systems:
                output['system'] = systems
            if custom:
                output['custom_params'] = custom

            yield output

    def count(self) -> int:
        """Return the total number of combinations this sweep will generate."""
        total = 1

        if self.query_bindings:
            for values in self.query_bindings.values():
                if isinstance(values, list):
                    total *= len(values)

        if self.modes:
            total *= len(self.modes)

        if self.graph_sizes:
            total *= len(self.graph_sizes)

        if self.systems:
            total *= len(self.systems)

        if self.custom_params:
            for values in self.custom_params.values():
                if isinstance(values, list):
                    total *= len(values)

        return max(1, total)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> ParameterSweep:
        """
        Construct a ParameterSweep from a dictionary (e.g., from JSON config).

        Example:
            config = {
                'query_bindings': {'limit': [10, 20, 50]},
                'modes': ['right_recursion'],
            }
            sweep = ParameterSweep.from_dict(config)
        """
        return ParameterSweep(
            query_bindings=data.get('query_bindings', {}),
            modes=data.get('modes'),
            graph_sizes=data.get('graph_sizes'),
            systems=data.get('systems'),
            custom_params=data.get('custom_params', {}),
        )
